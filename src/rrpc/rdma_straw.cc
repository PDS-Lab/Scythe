#include <infiniband/verbs.h>

#include <cassert>
#include <cstring>
#include <thread>
#include <zmq.hpp>

#include "coroutine_pool/scheduler.h"
#include "rdma_cm.h"
#include "rrpc/rocket.h"
#include "util/common.h"
#include "util/logging.h"
#include "util/mem_pool.h"

RdmaQp::~RdmaQp() {
  // if (qp_ || send_cq_ || recv_cq_) printf("[RdmaQp] ~RdmaQp called\n");
  if (qp_) {
    // printf("[RdmaQp] destory qp\n");
    ibv_destroy_qp(qp_);
  }
  if (send_cq_) {
    // printf("[RdmaQp] destory send cq\n");
    ibv_destroy_cq(send_cq_);
  }
  if (recv_cq_) {
    // printf("[RdmaQp] destory recv cq\n");
    ibv_destroy_cq(recv_cq_);
  }
  if (wcs_) {
    free(wcs_);
  }
}

void RdmaQp::init_rc(RdmaDev* dev, int port_idx, RDMA_CM_ERROR_CODE* rc) {
  assert(dev && dev->ctx && dev->pd);
  dev_ = dev;
  port_idx_ = port_idx;
  send_cq_ = ibv_create_cq(dev_->ctx, mx_send_cq_size_, NULL, NULL, 0);
  if (!send_cq_) {
    *rc = RDMA_CM_ERROR_CODE::CM_CREATE_CQ_FAILED;
    return;
  }

  static __thread struct ibv_cq* r_cq = nullptr;
  if (!r_cq) {
    r_cq = ibv_create_cq(dev_->ctx, mx_shared_rcq_size_, NULL, NULL, 0);
    if (!r_cq) {
      printf("[RdmaQp][init_rc] destory send cq\n");
      ibv_destroy_cq(send_cq_);
      send_cq_ = nullptr;
      *rc = RDMA_CM_ERROR_CODE::CM_CREATE_CQ_FAILED;
      return;
    }
  }
  recv_cq_ = r_cq;

  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));
  qp_init_attr.send_cq = send_cq_;
  qp_init_attr.recv_cq = recv_cq_;
  qp_init_attr.qp_type = IBV_QPT_RC;

  qp_init_attr.cap.max_send_wr = mx_sq_size_;
  qp_init_attr.cap.max_recv_wr = mx_rq_size_;
  qp_init_attr.cap.max_send_sge = mx_send_sge_;
  qp_init_attr.cap.max_recv_sge = mx_recv_sge_;
  qp_init_attr.cap.max_inline_data = mx_inline_size_;

  qp_ = ibv_create_qp(dev_->pd, &qp_init_attr);
  if (!qp_) {
    printf("[RdmaQp][init_rc] destory send cq 2\n");
    ibv_destroy_cq(send_cq_);
    send_cq_ = nullptr;
    *rc = RDMA_CM_ERROR_CODE::CM_CREATE_QP_FAILED;
    return;
  }
  rc_ready2init(rc);
  if (*rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
    printf("[RdmaQp][init_rc] destory qp\n");
    ibv_destroy_qp(qp_);
    qp_ = nullptr;
    printf("[RdmaQp][init_rc] destory send cq 3\n");
    ibv_destroy_cq(send_cq_);
    send_cq_ = nullptr;
    *rc = RDMA_CM_ERROR_CODE::CM_QP_READY_TO_INIT_FAILED;
    return;
  }
  // printf("[RRPC][RdmaQp] qp(%u) init\n", qp_->qp_num);
  *rc = RDMA_CM_ERROR_CODE::CM_SUCCESS;
  return;
}

void RdmaQp::rc_ready2init(RDMA_CM_ERROR_CODE* rc) {
  int res, flags;
  struct ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
  qp_attr.qp_state = IBV_QPS_INIT;
  qp_attr.pkey_index = 0;
  qp_attr.port_num = port_idx_;
  // printf("[RdmaQp] [rc_ready2init] port_idx_:%d\n", port_idx_);
  qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;

  flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  res = ibv_modify_qp(qp_, &qp_attr, flags);
  if (res) {
    *rc = RDMA_CM_ERROR_CODE::CM_QP_READY_TO_INIT_FAILED;
    return;
  }
  *rc = RDMA_CM_ERROR_CODE::CM_SUCCESS;
  // printf("[RdmaQP] ready2init success\n");
  return;
}

void RdmaQp::rc_init2rtr(int qpn, int dlid, uint64_t subnet_prefix, uint64_t interface_id, int local_id,
                         RDMA_CM_ERROR_CODE* rc) {
  int res, flags;
  struct ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
  qp_attr.qp_state = IBV_QPS_RTR;
  qp_attr.path_mtu = IBV_MTU_1024;
  qp_attr.dest_qp_num = qpn;
  qp_attr.rq_psn = DEFAULT_PSN;
  qp_attr.max_dest_rd_atomic = 16;
  qp_attr.min_rnr_timer = 12;

  qp_attr.ah_attr.dlid = dlid;
  qp_attr.ah_attr.sl = 0;
  qp_attr.ah_attr.src_path_bits = 0;
  qp_attr.ah_attr.port_num = port_idx_;

  if (interface_id) {
    qp_attr.ah_attr.is_global = 1;
    qp_attr.ah_attr.grh.hop_limit = 255;
    qp_attr.ah_attr.grh.dgid.global.subnet_prefix = subnet_prefix;
    qp_attr.ah_attr.grh.dgid.global.interface_id = interface_id;
    // printf("[RDMAQP][rc_init2rtr] grh sgid_index:%d\n", local_id);
    qp_attr.ah_attr.grh.sgid_index = local_id;
    qp_attr.ah_attr.grh.flow_label = 0;
  }

  flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
          IBV_QP_MIN_RNR_TIMER;
  res = ibv_modify_qp(qp_, &qp_attr, flags);
  if (res) {
    printf("[RdmaQp] rc_init2rtr failed!!!!:%d remote qpn:%d port_idx_:%d dlid:%d\n", res, qpn, port_idx_, dlid);
    *rc = RDMA_CM_ERROR_CODE::CM_QP_INIT_TO_RTR_FAILED;
    return;
  }
  *rc = RDMA_CM_ERROR_CODE::CM_SUCCESS;
  // printf("[RdmaQP] rc_init2rtr success\n");
  return;
}

void RdmaQp::rc_rtr2rts(RDMA_CM_ERROR_CODE* rc) {
  int res, flags;
  struct ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));
  qp_attr.qp_state = IBV_QPS_RTS;
  qp_attr.sq_psn = DEFAULT_PSN;
  qp_attr.timeout = 15;
  qp_attr.retry_cnt = 7;
  qp_attr.rnr_retry = 7;
  qp_attr.max_rd_atomic = 16;
  qp_attr.max_dest_rd_atomic = 16;

  flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
  res = ibv_modify_qp(qp_, &qp_attr, flags);
  if (res) {
    *rc = RDMA_CM_ERROR_CODE::CM_QP_RTR_TO_RTS_FAILED;
    return;
  }
  *rc = RDMA_CM_ERROR_CODE::CM_SUCCESS;
  // printf("[RdmaQP] rc_rtr2rts success\n");
  return;
}

RDMA_CM_ERROR_CODE RdmaQp::post_send(ibv_send_wr* srs, ibv_send_wr** bad_wrs, size_t wr_num, bool enable_doorbell) {
  RDMA_CM_ERROR_CODE rc;
  lock_.Lock();
  if (wrs_in_sq_ + wr_num >= poll_cq_watermark_) {
    rc = poll_comp();
    if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
      lock_.Unlock();
      return rc;
    }
  }
  wrs_in_sq_ += wr_num;
  if (wrs_posted_from_last_signal_ + wr_num >= cq_signal_watermark_) {
    srs[0].wr_id = (wr_num + wrs_posted_from_last_signal_);
    srs[0].send_flags |= IBV_SEND_SIGNALED;
    wrs_posted_from_last_signal_ = 0;
  } else {
    wrs_posted_from_last_signal_ += wr_num;
  }
  if (enable_doorbell) {
    int res = ibv_post_send(qp_, srs, bad_wrs);
    lock_.Unlock();
    switch (res) {
      case 0:
        return RDMA_CM_ERROR_CODE::CM_SUCCESS;
      case EINVAL: {
        printf("[RdmaStraws] post invalid wr\n");
        return RDMA_CM_ERROR_CODE::CM_POST_INVALID_WR;
      }
      case ENOMEM: {
        printf("[RdmaStraws] post no memory\n");
        return RDMA_CM_ERROR_CODE::CM_POST_NO_MEM;
      }
      case EFAULT: {
        printf("[RdmaStraws] post while QP broken\n");
        return RDMA_CM_ERROR_CODE::CM_POST_INVALID_QP;
      }
      default: {
        printf("[RdmaStraws] first bad wr idx: %d\n", res);
        return RDMA_CM_ERROR_CODE::CM_POST_BAD_WR;
      }
    };
  } else {
    ibv_send_wr* tmp;
    int res;
    for (int i = 0; i < wr_num; ++i) {
      tmp = srs[i].next;
      srs[i].next = nullptr;
      res = ibv_post_send(qp_, &(srs[i]), bad_wrs);
      srs[i].next = tmp;
      switch (res) {
        case 0:
          continue;
        case EINVAL: {
          lock_.Unlock();
          printf("[RdmaStraws] post invalid wr\n");
          return RDMA_CM_ERROR_CODE::CM_POST_INVALID_WR;
        }
        case ENOMEM: {
          lock_.Unlock();
          printf("[RdmaStraws] post no memory\n");
          return RDMA_CM_ERROR_CODE::CM_POST_NO_MEM;
        }
        case EFAULT: {
          lock_.Unlock();
          printf("[RdmaStraws] post while QP broken\n");
          return RDMA_CM_ERROR_CODE::CM_POST_INVALID_QP;
        }
        default: {
          lock_.Unlock();
          printf("[RdmaStraws] first bad wr idx: %d\n", res);
          return RDMA_CM_ERROR_CODE::CM_POST_BAD_WR;
        }
      };
    }
    lock_.Unlock();
    return RDMA_CM_ERROR_CODE::CM_SUCCESS;
  }
}

RDMA_CM_ERROR_CODE RdmaQp::post_one_side(ibv_send_wr* srs, ibv_send_wr** bad_wrs, size_t wr_num, bool enable_doorbell) {
  RDMA_CM_ERROR_CODE rc;
  while (unlikely(wrs_in_sq_ + wr_num >= poll_cq_watermark_)) {
    rc = poll_comp(true, false);
    if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
      return rc;
    }
  }
  wrs_in_sq_ += wr_num;
  // printf("[RdmaQp][post_one_side][%lu] wrs_in_sq_:%lu wr_num:%lu\n", pthread_self(),
  // wrs_in_sq_, wr_num);

  if (enable_doorbell) {
    int res = ibv_post_send(qp_, srs, bad_wrs);
    switch (res) {
      case 0:
        return RDMA_CM_ERROR_CODE::CM_SUCCESS;
      case EINVAL: {
        printf("[RdmaStraws] post invalid wr\n");
        return RDMA_CM_ERROR_CODE::CM_POST_INVALID_WR;
      }
      case ENOMEM: {
        printf("[RdmaStraws] post no memory\n");
        return RDMA_CM_ERROR_CODE::CM_POST_NO_MEM;
      }
      case EFAULT: {
        printf("[RdmaStraws] post while QP broken\n");
        return RDMA_CM_ERROR_CODE::CM_POST_INVALID_QP;
      }
      default: {
        printf("[RdmaStraws] first bad wr idx: %d\n", res);
        return RDMA_CM_ERROR_CODE::CM_POST_BAD_WR;
      }
    };
  } else {
    ibv_send_wr* tmp;
    int res;
    for (int i = 0; i < wr_num; ++i) {
      tmp = srs[i].next;
      srs[i].next = nullptr;
      res = ibv_post_send(qp_, &(srs[i]), bad_wrs);
      srs[i].next = tmp;
      switch (res) {
        case 0:
          continue;
        case EINVAL: {
          printf("[RdmaStraws] post invalid wr\n");
          return RDMA_CM_ERROR_CODE::CM_POST_INVALID_WR;
        }
        case ENOMEM: {
          printf("[RdmaStraws] post no memory\n");
          return RDMA_CM_ERROR_CODE::CM_POST_NO_MEM;
        }
        case EFAULT: {
          printf("[RdmaStraws] post while QP broken\n");
          return RDMA_CM_ERROR_CODE::CM_POST_INVALID_QP;
        }
        default: {
          printf("[RdmaStraws] first bad wr idx: %d\n", res);
          return RDMA_CM_ERROR_CODE::CM_POST_BAD_WR;
        }
      };
    }
  }
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

// requirement: lock_ is hold.
RDMA_CM_ERROR_CODE RdmaQp::poll_comp(bool one_side, bool noblock, bool rocket_direct) {
  int wcn = 0;
  if (rocket_direct) lock_.Lock();
  if (wrs_in_sq_ == 0) {
    if (rocket_direct) lock_.Unlock();
    return RDMA_CM_ERROR_CODE::CM_SUCCESS;
  }
  ibv_wc wc_tmp[max_wc_num_];
  if (noblock) {
    wcn = ibv_poll_cq(send_cq_, max_wc_num_, wc_tmp);
  } else {
    do {
      wcn = ibv_poll_cq(send_cq_, max_wc_num_, wc_tmp);
    } while (wcn == 0);
  }

  if (wcn < 0) {
    if (rocket_direct) lock_.Unlock();
    return RDMA_CM_ERROR_CODE::CM_POLL_CQ_FAILED;
  }
  uint64_t compn = 0;
  for (int i = 0; i < wcn; ++i) {
    if (wc_tmp[i].status != IBV_WC_SUCCESS) {
      LOG_ERROR("[RDMAQP][poll comp] wcs fail status: %u", (uint32_t)wc_tmp[i].status);
      LOG_ERROR("oneside %d", one_side);
      LOG_ERROR("op code %d", wc_tmp[i].opcode);

      if (rocket_direct) lock_.Unlock();
      return RDMA_CM_ERROR_CODE::CM_WC_STATUS_FAILED;
    }
    if (!one_side) {
      compn += wc_tmp[i].wr_id;
    } else {
      compn += 1;
      Rocket::rdma_bag* bag =
          reinterpret_cast<Rocket::rdma_bag*>(wc_tmp[i].wr_id & (~((uint64_t)(sizeof(uint64_t)) - 1ULL)));
      bag->ready = true;
    }
  }
  wrs_in_sq_ -= compn;
  // printf("[RdmaQp][poll_comp][%lu] wrs_in_sq_:%lu compn:%lu\n", pthread_self(), wrs_in_sq_,
  // compn);
  if (rocket_direct) lock_.Unlock();
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE RdmaStraws::post_send(ibv_send_wr* wr, ibv_send_wr** bad_wr, size_t wr_num, bool enable_doorbell) {
  // lock_.Lock();
  // if (!valid_) {
  //     lock_.Unlock();
  //     return RDMA_CM_ERROR_CODE::CM_STALE_RDMA_STRAWS;
  // }
  // lock_.Unlock();
  // round-robin
  RdmaQp& rdma_qp = qps_[self_thread_id() % qps_.size()];
  // printf("[RRPC][RdmaStraws] thread-%lu post send to qp<%d>\n",
  //         (uint64_t)(pthread_self()), qp_id_);
  return rdma_qp.post_send(wr, bad_wr, wr_num, enable_doorbell);
}

RDMA_CM_ERROR_CODE RdmaStraws::post_one_side(ibv_send_wr* wr, ibv_send_wr** bad_wr, size_t wr_num, bool enable_doorbell,
                                             bool server_mode) {
  // lock_.Lock();
  // if (!valid_) {
  //     lock_.Unlock();
  //     return RDMA_CM_ERROR_CODE::CM_STALE_RDMA_STRAWS;
  // }
  // lock_.Unlock();
  if (!server_mode)
    return one_side_qps_[self_thread_id() % one_side_qps_.size()].post_one_side(wr, bad_wr, wr_num, enable_doorbell);
  return one_side_qps_[self_thread_id() % one_side_qps_.size()].post_send(wr, bad_wr, wr_num, enable_doorbell);
}

RDMA_CM_ERROR_CODE RdmaStraws::try_poll_one_side() {
  return one_side_qps_[self_thread_id() % one_side_qps_.size()].poll_comp(true, true);
}

RDMA_CM_ERROR_CODE RdmaStraws::init_connection(int pref_rdma_dev_id, int pref_rdma_port_idx, int pref_remote_dev_id,
                                               int pref_remote_port_idx, bool create_new_qp, int qp_num,
                                               int one_side_qp_num, RdmaQp::Options opt) {
  RDMA_CM_ERROR_CODE rc;
  bool qps_empty = false;
  lock_.Lock();
  if (qps_.empty()) {
    qps_empty = true;
    create_new_qp = true;
  } else {
    lock_.Unlock();
  }

  if (create_new_qp) {
    RdmaDev* dev = nullptr;
    rc = cm_->open_device(pref_rdma_dev_id, &dev);
    if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS && rc != RDMA_CM_ERROR_CODE::CM_OBJ_INITIALIZED) {
      return rc;
    }
    int valid_port_cnt = dev->valid_port_ids.size();
    bool active_port = false;
    for (int i = 0; i < valid_port_cnt; ++i) {
      if (pref_rdma_port_idx == dev->valid_port_ids[i]) {
        active_port = true;
        break;
      }
    }
    if (!active_port) return RDMA_CM_ERROR_CODE::CM_INACTIVE_RDMA_PORT_IDX;

    int total_qp_num = qp_num + one_side_qp_num;
    while (total_qp_num--) {
      RdmaQp rdma_qp(opt);
      rdma_qp.init_rc(dev, pref_rdma_port_idx, &rc);
      if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) return rc;

      zmq::context_t context(1);
      zmq::socket_t socket(context, ZMQ_REQ);
      QPConnectReq req;
      memset(&req, 0, sizeof(QPConnectReq));
      req.for_one_side = (total_qp_num < one_side_qp_num);
      req.remote_dev_id = pref_remote_dev_id;
      req.remote_port_idx = pref_remote_port_idx;
      struct ibv_port_attr port_attr;
      int res = ibv_query_port(dev->ctx, pref_rdma_port_idx, &port_attr);
      assert(!res);
      req.qp_attr.lid = port_attr.lid;
      req.qp_attr.qpn = rdma_qp.get_qp_num();
      ibv_gid gid;
      res = ibv_query_gid(dev->ctx, pref_rdma_port_idx, 1, &gid);
      assert(!res);
      req.qp_attr.subnet_prefix = gid.global.subnet_prefix;
      req.qp_attr.interface_id = gid.global.interface_id;
      req.qp_attr.local_id = 1;
      req.port = global_cm->port();  // local port
      res = global_cm->get_local_ip(req.ip);
      assert(!res);

      if (TcpAdaptor::send_to(&socket, dest_ip_.c_str(), port_, (void*)(&req), sizeof(QPConnectReq),
                              TCP_CALLBACK::QP_CONNECT)) {
        zmq::message_t reply;
        socket.recv(reply, zmq::recv_flags::none);

        QPConnectReply* qp_conn_reply = (QPConnectReply*)(reply.data());
        if (qp_conn_reply->reply_code == 0) {
          // printf("[RRPC][RdmaStraws] get qp connect reply: lid:%u qpn:%lu "
          //         "subnet_prefix:%lu interface_id:%lu local_id:%lu\n",
          //         qp_conn_reply->qp_attr.lid, qp_conn_reply->qp_attr.qpn,
          //         qp_conn_reply->qp_attr.subnet_prefix,
          //         qp_conn_reply->qp_attr.interface_id,
          //         qp_conn_reply->qp_attr.local_id);
          rdma_qp.rc_init2rtr(qp_conn_reply->qp_attr.qpn, qp_conn_reply->qp_attr.lid,
                              qp_conn_reply->qp_attr.subnet_prefix, qp_conn_reply->qp_attr.interface_id,
                              qp_conn_reply->qp_attr.local_id, &rc);
          if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
            printf("[RRPC][RdmaStraws] qp change status to RTR failed\n");
            return rc;
          }
          rdma_qp.rc_rtr2rts(&rc);
          if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
            printf("[RRPC][RdmaStraws] qp change status to RTS failed\n");
            return rc;
          }
        } else {
          printf("[RRPC][RdmaStraws] init connection get remote tcp error code: %u\n", qp_conn_reply->reply_code);
          socket.close();
          context.close();
          return (RDMA_CM_ERROR_CODE)(qp_conn_reply->reply_code);
        }
      } else {
        socket.close();
        context.close();
        return RDMA_CM_ERROR_CODE::CM_TCP_CONNECTION_FAILED;
      }

      socket.close();
      context.close();
      if (!qps_empty) lock_.Lock();
      if (total_qp_num >= one_side_qp_num) {
        assert(qps_.size() < MAX_QP_PER_STRAWS);
        qps_.push_back(std::move(rdma_qp));
      } else {
        assert(one_side_qps_.size() < MAX_QP_PER_STRAWS);
        one_side_qps_.push_back(std::move(rdma_qp));
      }
      if (!qps_empty) lock_.Unlock();
    }
    if (qps_empty) {
      lock_.Unlock();
    }
  }

  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

thread_local uint32_t self_thread = -1;
std::atomic_uint32_t _global_thread_id{0};

uint32_t self_thread_id() {
  if (unlikely(self_thread == -1)) {
    self_thread = _global_thread_id.fetch_add(1, std::memory_order_relaxed);
  }
  return self_thread;
};