#include "rocket.h"

#include <cassert>
#include <cstring>

#include "rdma_cm.h"
#include "rrpc/ring_buf.h"
#include "rrpc/rrpc_config.h"
#include "util/common.h"
#include "util/logging.h"
#include "util/mem_pool.h"

Rocket::Rocket(const Options& opt)
    : opt_(opt),
      send_ring_(opt.ring_opt_, opt.pref_dev_id),
      recv_ring_(opt.ring_opt_, opt.pref_dev_id),
      max_recv_ack_id_(0) {
  assert(opt.mx_doorbell_batch_sz_ <= opt.ring_opt_.bucket_sz_);
  assert(opt.mx_doorbell_batch_sz_ > 0 && opt.mx_doorbell_batch_sz_ <= MAX_DOORBELL_BATCH_SIZE);
  assert(opt.mx_sges_sz_ > 0 && opt.mx_sges_sz_ <= MAX_SGES_SIZE);

  current_idx_ = 0;
  srs_ = (ibv_send_wr*)malloc(sizeof(ibv_send_wr) * opt.mx_doorbell_batch_sz_);
  memset(srs_, 0, sizeof(ibv_send_wr) * opt.mx_doorbell_batch_sz_);
  one_side_current_idx_ = 0;
  one_side_srs_ = (ibv_send_wr*)malloc(sizeof(ibv_send_wr) * MAX_ONE_SIDE_DOORBELL_BATCH_SIZE);
  memset(one_side_srs_, 0, sizeof(ibv_send_wr) * MAX_ONE_SIDE_DOORBELL_BATCH_SIZE);

  sgs_ = (ibv_sge**)malloc(sizeof(ibv_sge*) * opt.mx_doorbell_batch_sz_);
  one_side_sgs_ = (ibv_sge**)malloc(sizeof(ibv_sge*) * MAX_ONE_SIDE_DOORBELL_BATCH_SIZE);
  for (int i = 0; i < opt.mx_doorbell_batch_sz_; ++i) {
    sgs_[i] = (ibv_sge*)malloc(sizeof(ibv_sge) * opt.mx_sges_sz_);
    memset(sgs_[i], 0, sizeof(ibv_sge) * opt.mx_sges_sz_);
  }
  for (int i = 0; i < MAX_ONE_SIDE_DOORBELL_BATCH_SIZE; ++i) {
    one_side_sgs_[i] = (ibv_sge*)malloc(sizeof(ibv_sge) * opt.mx_sges_sz_);
    memset(one_side_sgs_[i], 0, sizeof(ibv_sge) * opt.mx_sges_sz_);
  }

  for (int i = 0; i < opt.mx_doorbell_batch_sz_; ++i) {
    srs_[i].sg_list = &(sgs_[i][0]);
    srs_[i].next = (i == opt.mx_doorbell_batch_sz_ - 1) ? &(srs_[0]) : &(srs_[i + 1]);
  }
  for (int i = 0; i < MAX_ONE_SIDE_DOORBELL_BATCH_SIZE; ++i) {
    one_side_srs_[i].sg_list = &(one_side_sgs_[i][0]);
    one_side_srs_[i].next = (i == MAX_ONE_SIDE_DOORBELL_BATCH_SIZE - 1) ? &(one_side_srs_[0]) : &(one_side_srs_[i + 1]);
  }

  all_bags = new rdma_bag[MAX_ONE_SIDE_BAG_NUM];
  for (int i = 0; i < MAX_ONE_SIDE_BAG_NUM; ++i) {
    available_.push_back(&all_bags[i]);
  }
  global_cm->get_mr_rkey_by_dev_id(opt_.pref_dev_id, &lkey_);
}

Rocket::~Rocket() {
  for (int i = 0; i < opt_.mx_doorbell_batch_sz_; ++i) {
    free(sgs_[i]);
  }
  for (int i = 0; i < MAX_ONE_SIDE_DOORBELL_BATCH_SIZE; ++i) {
    free(one_side_sgs_[i]);
  }
  free(sgs_);
  free(one_side_sgs_);
  free(srs_);
  free(one_side_srs_);
  delete[] all_bags;
}

RDMA_CM_ERROR_CODE Rocket::connect(std::string ip, int port, const ConnectOptions& opts) {
  connector_ = global_cm->get_rdma_straws(ip, port);
  auto rc =
      connector_->init_connection(opts.pref_rdma_dev_id, opts.pref_rdma_port_idx, opts.pref_remote_dev_id,
                                  opts.pref_remote_port_idx, opts.create_new_qp, opts.qp_num, opts.one_side_qp_num);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) return rc;

  send_ring_.init();
  recv_ring_.init();

  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REQ);
  RocketConnectReq req;
  memset(&req, 0, sizeof(RocketConnectReq));
  req.send_ring_addr = send_ring_.get_local_addr();
  req.send_ring_key = send_ring_.get_local_mr_rkey();
  req.recv_ring_addr = recv_ring_.get_local_addr();
  req.recv_ring_key = recv_ring_.get_local_mr_rkey();
  req.port = global_cm->port();
  int res = global_cm->get_local_ip(req.ip);
  assert(!res);

  if (TcpAdaptor::send_to(&socket, ip.c_str(), port, (void*)(&req), sizeof(RocketConnectReq),
                          TCP_CALLBACK::ROCKET_CONNECT)) {
    zmq::message_t reply;
    socket.recv(reply);

    RocketConnectReply* rocket_connect_reply = (RocketConnectReply*)(reply.data());
    if (rocket_connect_reply->reply_code == 0) {
      // printf("[RRPC][Rocket] get rocket connet reply: send_ring_addr:%lu send_ring_key:%u "
      //     "recv_ring_addr:%lu recv_ring_key:%u\n",
      //     rocket_connect_reply->send_ring_addr, rocket_connect_reply->send_ring_key,
      //     rocket_connect_reply->recv_ring_addr, rocket_connect_reply->recv_ring_key);
      // send->recv recv->send
      send_ring_.set_remote_addr(rocket_connect_reply->recv_ring_addr);
      send_ring_.set_remote_key(rocket_connect_reply->recv_ring_key);
      recv_ring_.set_remote_addr(rocket_connect_reply->send_ring_addr);
      recv_ring_.set_remote_key(rocket_connect_reply->send_ring_key);
    } else {
      printf("[RRPC][Rocket] get rocket connect tcp error code: %u\n", rocket_connect_reply->reply_code);
      socket.close();
      context.close();
      return (RDMA_CM_ERROR_CODE)(rocket_connect_reply->reply_code);
    }
  } else {
    socket.close();
    context.close();
    return RDMA_CM_ERROR_CODE::CM_TCP_CONNECTION_FAILED;
  }

  socket.close();
  context.close();
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE Rocket::connect(uint16_t node_id, const ConnectOptions& opts) {
  RdmaCM::Iport iport;
  RDMA_CM_ERROR_CODE rc = global_cm->get_iport(node_id, &iport);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) return rc;

  return connect(iport.ip, iport.port, opts);
}

void Rocket::init_ring(uint64_t sr_addr, uint32_t sr_key, uint64_t rr_addr, uint32_t rr_key) {
  send_ring_.init();
  recv_ring_.init();
  // send->recv recv->send
  send_ring_.set_remote_addr(rr_addr);
  send_ring_.set_remote_key(rr_key);
  recv_ring_.set_remote_addr(sr_addr);
  recv_ring_.set_remote_key(sr_key);
}

RDMA_CM_ERROR_CODE Rocket::send(bool enable_doorbell) {
  if (current_idx_ == 0) {
    return RDMA_CM_ERROR_CODE::CM_SUCCESS;
  }
  inflight_ += queued_;
  queued_ = 0;
  queued_msg_size_ = 0;
  srs_[current_idx_ - 1].next = nullptr;
  // srs_[0].send_flags = (srs_[0].sg_list->length <= 64) ? IBV_SEND_INLINE : 0;
  send_ring_.set_bucket_footer();

  assert(connector_);
  RDMA_CM_ERROR_CODE rc = connector_->post_send(srs_, &bad_send_wr_, current_idx_, enable_doorbell);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
    // DoorBell batch isn't reset for retry
    return rc;
  }
  srs_[current_idx_ - 1].next = (current_idx_ >= opt_.mx_doorbell_batch_sz_) ? &(srs_[0]) : &(srs_[current_idx_]);
  current_idx_ = 0;
  return rc;
}

RDMA_CM_ERROR_CODE Rocket::post_one_side_req(bool sync) {
  assert(connector_);
  if (sync || one_side_current_idx_ >=MAX_ONE_SIDE_DOORBELL_BATCH_SIZE) {
    return rdma_burst(true);
  }
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE Rocket::rdma_burst(bool enable_doorbell) {
  if (one_side_current_idx_ == 0) {
    return RDMA_CM_ERROR_CODE::CM_SUCCESS;
  }
  one_side_srs_[one_side_current_idx_ - 1].next = nullptr;

  RDMA_CM_ERROR_CODE rc =
      connector_->post_one_side(one_side_srs_, &bad_send_wr_, one_side_current_idx_, enable_doorbell);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
    return rc;
  }
  one_side_srs_[one_side_current_idx_ - 1].next = (one_side_current_idx_ >= MAX_ONE_SIDE_DOORBELL_BATCH_SIZE)
                                                      ? &(one_side_srs_[0])
                                                      : &(one_side_srs_[one_side_current_idx_]);
  one_side_current_idx_ = 0;
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

void Rocket::poll_reply_msg() {
  BatchIter iter;
  reply_callback cb;
  while (inflight_ != 0) {
    bool has_next_bucket;
    do {
      iter.Reset(recv_ring_.wait_msg_arrive(has_next_bucket));
      do {
        cb = (reply_callback)(uintptr_t)(iter.get_callback_func_addr());
        if (likely(cb)) {
          cb(iter.get_msg_data(), (void*)(uintptr_t)(iter.get_cb_func_args_addr()));
        }
        // printf("MSG: tag:%u total_bucket_num:%u doorbell_batch_idx:%u rpc_id:%u "
        //         "ack_id:%u msg_sz:%u\n",
        //         iter.get_tag(), iter.get_total_bucket_num(),
        //         iter.get_doorbell_batch_idx(), iter.get_rpc_id(), iter.get_ack_id(),
        //         iter.get_msg_sz());
        inflight_--;
      } while (iter.Next());
      recv_ring_.advance_tail(1);
    } while (has_next_bucket);
    // 因为服务端一次只处理一个bucket，所以每收到一次回复，一定对应服务端处理了一个bucket，所以只需要将tail推进1格
    send_ring_.advance_tail(1);
  }
}

void Rocket::try_poll_reply_msg() {
  BatchIter iter;
  reply_callback cb;
  if (inflight_ != 0) {
    bool has_next_bucket;
    bool arrival = try_poll_msg(&iter, &has_next_bucket);
    if (arrival) {
      do {
        cb = (reply_callback)(uintptr_t)(iter.get_callback_func_addr());
        if (likely(cb)) {
          cb(iter.get_msg_data(), (void*)(uintptr_t)(iter.get_cb_func_args_addr()));
        }
        // printf("MSG: tag:%u total_bucket_num:%u doorbell_batch_idx:%u rpc_id:%u "
        //         "ack_id:%u msg_sz:%u\n",
        //         iter.get_tag(), iter.get_total_bucket_num(),
        //         iter.get_doorbell_batch_idx(), iter.get_rpc_id(), iter.get_ack_id(),
        //         iter.get_msg_sz());
        inflight_--;
      } while (iter.Next());
      recv_ring_.advance_tail(1);
      if (!has_next_bucket) {
        send_ring_.advance_tail(1);
      }
    }
  }
}

RDMA_CM_ERROR_CODE Rocket::poll_one_side(int cnt) {
  RDMA_CM_ERROR_CODE rc = RDMA_CM_ERROR_CODE::CM_SUCCESS;
  do {
    rc = connector_->try_poll_one_side();
    if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) return rc;
    cnt -= poll_rdma_bag();
  } while (cnt);
  return rc;
}

RDMA_CM_ERROR_CODE Rocket::try_poll_one_side() {
  auto rc = connector_->try_poll_one_side();
  poll_rdma_bag();
  return rc;
}

bool Rocket::try_poll_msg(BatchIter* iter, bool* has_next_bucket) {
  uint64_t bucket_addr;
  if (recv_ring_.try_poll_msg_arrive(&bucket_addr, has_next_bucket)) {
    // recv_ring_.wait_msg_arrive();
    iter->Reset(bucket_addr);
    return true;
  }
  return false;
}

RDMA_CM_ERROR_CODE Rocket::get_msg_buf(void** msg_buf, uint64_t size, uint32_t rpc_id, reply_callback callback,
                                       void* cb_args, bool force_new_bucket) {
  RingBuf* buf = &send_ring_;
  bool is_bucket_hd = false;
  uint32_t msg_off;
  uint32_t prev_alloc_sz;
  if (force_new_bucket) {
    buf->set_bucket_footer();
  }

  if (!buf->alloc_msg_buf_offset(&msg_off, &prev_alloc_sz, is_bucket_hd, size + sizeof(MsgHead))) {
    // TODO: poll_reply
    assert(false);
  }

  void* buf_addr = (void*)(uintptr_t)(buf->get_local_addr() + msg_off);
  MsgHead* msg_hd = (MsgHead*)buf_addr;
  msg_hd->tag = 0;
  msg_hd->rpc_id = rpc_id;
  msg_hd->recv_buf_ack_id = recv_ring_.get_tail();
  msg_hd->msg_sz = size;
  msg_hd->callback_func_addr = (uint64_t)(uintptr_t)(callback);
  msg_hd->cb_func_args_addr = (uint64_t)(uintptr_t)(cb_args);
  if (prev_alloc_sz) {
    MsgHead* prev_buf_hd = (MsgHead*)(uintptr_t)(buf->get_local_addr() + msg_off - prev_alloc_sz);
    prev_buf_hd->msg_sz |= RingBuf::CONTINUATION;
  }

  ibv_send_wr* swr = nullptr;
  ibv_sge* sge = nullptr;
  if (is_bucket_hd) {
    int swr_idx = alloc_send_wr(&swr, &sge);
    if (swr_idx >= 1) {
      uint32_t* prev_bucket_tag = (uint32_t*)get_prev_wr_laddr_ptr();
      assert(((uint64_t)(uintptr_t)(prev_bucket_tag)) ==
             (buf->get_local_addr() + buf->get_prev_bucket_idx() * buf->get_bucket_size()));
      *prev_bucket_tag |= RingBuf::CONTINUATION;
    }
    sge->addr = (uint64_t)(uintptr_t)(buf_addr);
    sge->length = size + sizeof(MsgHead) + sizeof(uint32_t);  // footer
    msg_hd->tag += (size + sizeof(MsgHead));
    // printf("[get_msg_buf] tag:%u size:%lu sizeof(MsgHead):%lu\n", msg_hd->tag, size,
    // sizeof(MsgHead));
    sge->lkey = buf->get_local_mr_lkey();

    swr->wr_id = 0;
    swr->sg_list = sge;
    swr->num_sge = 1;
    swr->opcode = IBV_WR_RDMA_WRITE;
    swr->send_flags = 0;
    swr->imm_data = 0;
    swr->wr.rdma.remote_addr = (uint64_t)(uintptr_t)(buf->get_remote_addr() + msg_off);
    swr->wr.rdma.rkey = buf->get_remote_mr_key();
  } else {
    MsgHead* bucket_hd =
        (MsgHead*)(uintptr_t)(buf->get_local_addr() + buf->get_current_bucket_idx() * buf->get_bucket_size());
    get_current_wr(&swr, &sge);
    sge->length += (size + sizeof(MsgHead));
    bucket_hd->tag += (size + sizeof(MsgHead));
  }
  *msg_buf = (void*)(((char*)buf_addr) + sizeof(MsgHead));
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE Rocket::prepare_one_side_read(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr,
                                                 uint32_t rkey, rdma_bag* bag) {
  ibv_send_wr* swr = nullptr;
  ibv_sge* sge = nullptr;
  alloc_one_side_wr(&swr, &sge);
  sge->addr = laddr;
  sge->length = len;
  sge->lkey = lkey;

  swr->wr_id = (uint64_t)bag;
  swr->sg_list = sge;
  swr->num_sge = 1;
  swr->opcode = IBV_WR_RDMA_READ;
  bag->op = swr->opcode;
  swr->send_flags = IBV_SEND_SIGNALED;
  swr->imm_data = 0;
  swr->wr.rdma.remote_addr = raddr;
  swr->wr.rdma.rkey = rkey;
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE Rocket::prepare_one_side_write(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr,
                                                  uint32_t rkey, rdma_bag* bag) {
  ibv_send_wr* swr = nullptr;
  ibv_sge* sge = nullptr;
  alloc_one_side_wr(&swr, &sge);
  sge->addr = laddr;
  sge->length = len;
  sge->lkey = lkey;

  swr->wr_id = (uint64_t)bag;
  swr->sg_list = sge;
  swr->num_sge = 1;
  swr->opcode = IBV_WR_RDMA_WRITE;
  bag->op = swr->opcode;
  swr->send_flags = IBV_SEND_SIGNALED;
  swr->imm_data = 0;
  swr->wr.rdma.remote_addr = raddr;
  swr->wr.rdma.rkey = rkey;
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE Rocket::prepare_one_side_faa(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr,
                                                uint32_t rkey, uint64_t addv, rdma_bag* bag) {
  ibv_send_wr* swr = nullptr;
  ibv_sge* sge = nullptr;
  alloc_one_side_wr(&swr, &sge);
  sge->addr = laddr;
  sge->length = len;
  sge->lkey = lkey;

  swr->wr_id = (uint64_t)bag;
  swr->sg_list = sge;
  swr->num_sge = 1;
  swr->opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  bag->op = swr->opcode;
  swr->send_flags = IBV_SEND_SIGNALED;
  swr->imm_data = 0;
  swr->wr.atomic.remote_addr = raddr;
  swr->wr.atomic.rkey = rkey;
  swr->wr.atomic.compare_add = addv;
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE Rocket::prepare_one_side_cas(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr,
                                                uint32_t rkey, uint64_t cmpv, uint64_t setv, rdma_bag* bag) {
  ibv_send_wr* swr = nullptr;
  ibv_sge* sge = nullptr;
  alloc_one_side_wr(&swr, &sge);
  sge->addr = laddr;
  sge->length = len;
  sge->lkey = lkey;
  bag->op = swr->opcode;

  swr->wr_id = (uint64_t)bag;
  swr->sg_list = sge;
  swr->num_sge = 1;
  swr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  swr->send_flags = IBV_SEND_SIGNALED;
  swr->imm_data = 0;
  swr->wr.atomic.remote_addr = raddr;
  swr->wr.atomic.rkey = rkey;
  swr->wr.atomic.compare_add = cmpv;
  swr->wr.atomic.swap = setv;
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE Rocket::remote_read(void* user_buf, uint32_t len, uint64_t raddr, uint32_t rkey, one_side_cb cb,
                                       void* args) {
  void* inner_buf = BuddyThreadHeap::get_instance()->alloc(len);
  ENSURE(inner_buf != nullptr, "");
  auto* bag = available_.front();
  ENSURE(bag != nullptr, "insufficient RDMA bag");
  *bag = {
      .cb = cb,
      .args = args,
      .r_ptr = inner_buf,
      .user_buf = user_buf,
      .len = len,
  };
  available_.pop_front();
  rdma_inflight_.push_back(bag);

  prepare_one_side_read((uint64_t)inner_buf, len, lkey_, raddr, rkey, bag);
  return post_one_side_req(false);
}

RDMA_CM_ERROR_CODE Rocket::remote_write(void* user_buf, uint32_t len, uint64_t raddr, uint32_t rkey, one_side_cb cb,
                                        void* args) {
  void* inner_buf = BuddyThreadHeap::get_instance()->alloc(len);
  ENSURE(inner_buf != nullptr, "");
  memcpy(inner_buf, user_buf, len);
  auto* bag = available_.front();
  ENSURE(bag != nullptr, "insufficient RDMA bag");
  *bag = {
      .cb = cb,
      .args = args,
      .r_ptr = inner_buf,
  };
  available_.pop_front();
  rdma_inflight_.push_back(bag);
  prepare_one_side_write((uint64_t)inner_buf, len, lkey_, raddr, rkey, bag);
  return post_one_side_req(false);
}

RDMA_CM_ERROR_CODE Rocket::remote_read_zero_cp(void* user_buf, uint32_t len, uint32_t lkey, uint64_t raddr,
                                               uint32_t rkey, one_side_cb cb, void* args) {
  auto* bag = available_.front();
  ENSURE(bag != nullptr, "insufficient RDMA bag");
  *bag = {
      .cb = cb,
      .args = args,
  };
  available_.pop_front();
  rdma_inflight_.push_back(bag);
  prepare_one_side_read((uint64_t)user_buf, len, lkey, raddr, rkey, bag);
  return post_one_side_req(false);
}

RDMA_CM_ERROR_CODE Rocket::remote_write_zero_cp(void* user_buf, uint32_t len, uint32_t lkey, uint64_t raddr,
                                                uint32_t rkey, one_side_cb cb, void* args) {
  auto* bag = available_.front();
  ENSURE(bag != nullptr, "insufficient RDMA bag");
  *bag = {
      .cb = cb,
      .args = args,
  };
  available_.pop_front();
  rdma_inflight_.push_back(bag);
  prepare_one_side_write((uint64_t)user_buf, len, lkey, raddr, rkey, bag);
  return post_one_side_req(false);
};

RDMA_CM_ERROR_CODE Rocket::remote_fetch_add(void* user_buf, uint32_t len, uint64_t raddr, uint32_t rkey, uint64_t addv,
                                            one_side_cb cb, void* args) {
  void* inner_buf = BuddyThreadHeap::get_instance()->alloc(len);
  ENSURE(inner_buf != nullptr, "");
  auto* bag = available_.front();
  ENSURE(bag != nullptr, "insufficient RDMA bag");
  *bag = {
      .cb = cb,
      .args = args,
      .r_ptr = inner_buf,
      .user_buf = user_buf,
      .len = len,
  };
  available_.pop_front();
  rdma_inflight_.push_back(bag);
  prepare_one_side_faa((uint64_t)inner_buf, len, lkey_, raddr, rkey, addv, bag);
  return post_one_side_req(false);
};

RDMA_CM_ERROR_CODE Rocket::batching() {
  if (queued_ >= opt_.kMaxBatchNum || queued_msg_size_ >= RingBuf::DEFAULT_BUCKET_SIZE) {
    return send();
  }
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
};

int Rocket::poll_rdma_bag() {
  int cnt = 0;
  auto iter = rdma_inflight_.begin();
  while (iter != rdma_inflight_.end()) {
    auto* bag = *iter;
    if (bag->ready) {
      // copy data from reserved RDMA buf to user buf
      if (bag->r_ptr) {
        switch (bag->op) {
          case IBV_WR_RDMA_READ:
          case IBV_WR_ATOMIC_FETCH_AND_ADD:
          case IBV_WR_ATOMIC_CMP_AND_SWP:
            if (likely(bag->user_buf)) {
              memcpy(bag->user_buf, bag->r_ptr, bag->len);
            }
          default:
            break;
        }
        BuddyThreadHeap::get_instance()->free_local(bag->r_ptr);
      }
      // do the continuation work
      if (likely(bag->cb)) {
        bag->cb(bag->args);
      }
      // revert RDMA bag
      bag->reset();
      available_.push_back(bag);
      rdma_inflight_.erase(iter++);
      cnt++;
    } else {
      iter++;
    }
  }
  return cnt;
};