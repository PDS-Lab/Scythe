#pragma once

#include <infiniband/verbs.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>

#include "concurrent_queue.h"
#include "ring_buf.h"
#include "rrpc/rrpc_config.h"
#include "util/lock.h"
#include "util/mem_pool.h"

/*
 * Rocket is a class similar to socket, it is responsible for ring buffer management.
 * RDMA doorbell batch is also maintained by Rocket.
 */
// Rocket 中使用了thread local的资源，禁止在线程间共享Rocket
class Rocket {
 public:
  static const uint16_t MAX_DOORBELL_BATCH_SIZE = 32;
  static const uint16_t MAX_ONE_SIDE_DOORBELL_BATCH_SIZE = sizeof(uint64_t);
  static const uint16_t MAX_SGES_SIZE = 4;
  static const int DEFAULT_DEV_ID = 0;
  static const int DEFAULT_PORT_IDX = 1;

  struct Options {
    uint16_t mx_doorbell_batch_sz_ = MAX_DOORBELL_BATCH_SIZE;
    uint16_t mx_sges_sz_ = MAX_SGES_SIZE;
    int pref_dev_id = DEFAULT_DEV_ID;
    int pref_port_idx = DEFAULT_PORT_IDX;
    int kMaxBatchNum = 10;
    RingBuf::Options ring_opt_;
  };

  struct ConnectOptions {
    int pref_rdma_dev_id = 0;
    int pref_rdma_port_idx = 1;
    int pref_remote_dev_id = 0;
    int pref_remote_port_idx = 1;
    bool create_new_qp = false;
    int qp_num = 8;
    int one_side_qp_num = 16;
  };

  typedef void (*reply_callback)(void*, void*);
  struct MsgHead {
    volatile uint32_t tag;  // should be first
    // volatile
    // valid in first doorbell batch bucket
    // Only set by server
    volatile uint32_t rpc_id;
    volatile uint32_t recv_buf_ack_id;
    volatile uint32_t msg_sz;  // ((0|1) << 31) | msg size
    volatile uint64_t callback_func_addr;
    volatile uint64_t cb_func_args_addr;
  };
  class BatchIter {
    friend class Rocket;
    friend class PollWorker;
    friend class RdmaCM;

   public:
    template <typename T>
    T* get_request() {
      return reinterpret_cast<T*>((char*)(msg_addr) + sizeof(MsgHead));
    };

   private:
    void Reset(uint64_t addr) { msg_addr = (void*)((uintptr_t)addr); }
    bool Next() {
      volatile MsgHead* hd = (volatile MsgHead*)(uintptr_t)(msg_addr);
      if (hd->msg_sz & RingBuf::CONTINUATION) {
        msg_addr = (void*)((char*)(msg_addr) + sizeof(MsgHead) + (hd->msg_sz & RingBuf::LENGTH_MASK));
        return true;
      }
      return false;
    }
    uint32_t get_tag() { return ((volatile MsgHead*)(msg_addr))->tag; }
    uint32_t get_rpc_id() { return ((volatile MsgHead*)(msg_addr))->rpc_id; }
    uint32_t get_ack_id() { return ((volatile MsgHead*)(msg_addr))->recv_buf_ack_id; }
    uint32_t get_msg_sz() {
      uint64_t tmp = ((volatile MsgHead*)(msg_addr))->msg_sz;
      return tmp & RingBuf::LENGTH_MASK;
    }
    uint64_t get_callback_func_addr() { return ((volatile MsgHead*)(msg_addr))->callback_func_addr; }
    uint64_t get_cb_func_args_addr() { return ((volatile MsgHead*)(msg_addr))->cb_func_args_addr; }
    void* get_msg_data() { return (void*)((char*)(msg_addr) + sizeof(MsgHead)); }
    void* get_addr() { return msg_addr; }
    void* msg_addr;
  };

  Rocket(const Options& opt);

  Rocket() = delete;
  Rocket(const Rocket&) = delete;
  Rocket& operator=(const Rocket&) = delete;

  ~Rocket();

  void init_ring(uint64_t sr_addr, uint32_t sr_key, uint64_t rr_addr, uint32_t rr_key);
  RDMA_CM_ERROR_CODE connect(uint16_t node_id, const ConnectOptions& opts);
  RDMA_CM_ERROR_CODE connect(std::string ip, int port, const ConnectOptions& opts);

  template <typename T>
  T* gen_request(uint64_t size, uint32_t rpc_id, reply_callback callback = nullptr, void* cb_args = nullptr,
                 bool force_new_bucket = false) {
    void* msg_buf;
    queued_++;
    queued_msg_size_ += size;
    auto rc = get_msg_buf(&msg_buf, size, rpc_id, callback, cb_args, force_new_bucket);
    assert(rc == RDMA_CM_ERROR_CODE::CM_SUCCESS);
    return reinterpret_cast<T*>(msg_buf);
  }

  template <typename T>
  T* gen_reply(uint64_t size, BatchIter* iter, bool force_new_bucket = false) {
    void* msg_buf;
    auto rc = get_msg_buf(&msg_buf, size, 0, (reply_callback)iter->get_callback_func_addr(),
                          (void*)iter->get_cb_func_args_addr(), force_new_bucket);

    assert(rc == RDMA_CM_ERROR_CODE::CM_SUCCESS);
    return reinterpret_cast<T*>(msg_buf);
  }

  RDMA_CM_ERROR_CODE batching();

  RDMA_CM_ERROR_CODE get_msg_buf(void** msg_buf, uint64_t size, uint32_t rpc_id = 0, reply_callback callback = nullptr,
                                 void* cb_args = nullptr, bool force_new_bucket = false);
  // RDMA one side operation
  using one_side_cb = void (*)(void* args);
  struct alignas(8) rdma_bag {
    volatile bool ready = false;
    one_side_cb cb = nullptr;
    void* args = nullptr;

    void* r_ptr = nullptr;  // temporary one side memroy ptr
    void* user_buf = nullptr;
    uint32_t len = 0;

    enum ibv_wr_opcode op;
    void reset() { memset(this, 0, sizeof(rdma_bag)); }
    rdma_bag() = default;
  };

  // ---------RDMA one side helper function ---------
  // There is no need for user to concern local RDMA region regist. It will use `Rmalloc` to
  // get registered memory for RDMA one side operation
  RDMA_CM_ERROR_CODE remote_read(void* user_buf, uint32_t len, uint64_t raddr, uint32_t rkey, one_side_cb cb,
                                 void* args);
  RDMA_CM_ERROR_CODE remote_read_zero_cp(void* user_buf, uint32_t len, uint32_t lkey, uint64_t raddr, uint32_t rkey,
                                         one_side_cb cb, void* args);

  RDMA_CM_ERROR_CODE remote_write(void* user_buf, uint32_t len, uint64_t raddr, uint32_t rkey, one_side_cb cb,
                                  void* args);
  RDMA_CM_ERROR_CODE remote_write_zero_cp(void* user_buf, uint32_t len, uint32_t lkey, uint64_t raddr, uint32_t rkey,
                                          one_side_cb cb, void* args);
  RDMA_CM_ERROR_CODE remote_fetch_add(void* user_buf, uint32_t len, uint64_t raddr, uint32_t rkey, uint64_t addv, one_side_cb cb,
                                      void* args);
  //------------------------------------------------

  RDMA_CM_ERROR_CODE prepare_one_side_read(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr, uint32_t rkey,
                                           rdma_bag* bag);
  RDMA_CM_ERROR_CODE prepare_one_side_write(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr, uint32_t rkey,
                                            rdma_bag* bag);
  RDMA_CM_ERROR_CODE prepare_one_side_faa(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr, uint32_t rkey,
                                          uint64_t addv, rdma_bag* bag);
  RDMA_CM_ERROR_CODE prepare_one_side_cas(uint64_t laddr, uint32_t len, uint32_t lkey, uint64_t raddr, uint32_t rkey,
                                          uint64_t cmpv, uint64_t setv, rdma_bag* bag);

  RDMA_CM_ERROR_CODE rdma_burst(bool enable_doorbell = true);
  
  //
  // 1. set last Msg::wr send_flags
  // 2. set last Msg::wr next = nullptr
  // 3. ibv post send
  // 4. reset next
  // 5. reset current_idx_
  RDMA_CM_ERROR_CODE send(bool enable_doorbell = true);
  // for callback mode
  RDMA_CM_ERROR_CODE post_one_side_req(bool sync);

  // for client
  void try_poll_reply_msg();
  void poll_reply_msg();
  RDMA_CM_ERROR_CODE try_poll_one_side();     // no blocking
  RDMA_CM_ERROR_CODE poll_one_side(int cnt);  // blocking

  // for server
  bool try_poll_msg(BatchIter* iter, bool* has_next_bucket = nullptr);

  bool is_recv_ring_empty() { return recv_ring_.is_empty(); }
  void set_send_ring_tail(uint32_t tail) { send_ring_.set_tail(tail); }
  void set_recv_ring_tail(uint32_t tail) { recv_ring_.set_tail(tail); }
  void advance_recv_ring_tail() { recv_ring_.advance_tail(1); }
  void try_update_max_recv_ack_id(uint32_t cur) {
    if (cur > max_recv_ack_id_) {
      max_recv_ack_id_ = cur;
    }
  }
  uint32_t get_max_recv_ack_id() { return max_recv_ack_id_; }
  /*
   *   raw rdma ops without allocating send buffer from ring buffer
   *  if need_notify is set, RingBuffer will alloc an empty buffer bucket
   *  in order to flow control
   *
   * 1. ibv_sge is available
   */
  // RRPC_ERROR_CODE raw_send_pending(bool need_notify, std::pair<const void*, size_t> *msgs,
  //                                  size_t msgs_num, uint32_t lkey, ibv_wr_opcode opcode,
  //                                  uint64_t wr_id, uint64_t raddr, uint32_t rkey);
  /*
   *   rdma ops with send buffer from ring buffer for RPC
   *
   * 1. num_sge should be 1
   */
  // RRPC_ERROR_CODE rpc_send_pending(ibv_wr_opcode opcode, uint64_t laddr, uint64_t msg_sz);

  uint64_t get_send_ring_addr() { return send_ring_.get_local_addr(); }
  uint64_t get_send_ring_key() { return send_ring_.get_local_mr_rkey(); }
  uint64_t get_recv_ring_addr() { return recv_ring_.get_local_addr(); }
  uint64_t get_recv_ring_key() { return recv_ring_.get_local_mr_rkey(); }
  void set_connector(std::shared_ptr<RdmaStraws> straws) { connector_ = straws; }

  int get_queued() const { return queued_; }

  int get_inflight() const { return inflight_; }

  uint32_t get_lkey() const { return lkey_; }

 private:
  // 在Rocket所在线程执行RDMA单边回调，返回轮询到的RDMA单边ACK的个数
  int poll_rdma_bag();

  void* get_prev_wr_laddr_ptr() {
    assert(current_idx_ >= 2);
    return (void*)(uintptr_t)(srs_[current_idx_ - 2].sg_list[0].addr);
  }

  int alloc_send_wr(ibv_send_wr** wr, ibv_sge** sges) {
    assert(current_idx_ < opt_.mx_doorbell_batch_sz_);
    *wr = &(srs_[current_idx_]);
    *sges = sgs_[current_idx_];
    return current_idx_++;
  }

  int alloc_one_side_wr(ibv_send_wr** wr, ibv_sge** sges) {
    assert(one_side_current_idx_ < MAX_ONE_SIDE_DOORBELL_BATCH_SIZE);
    *wr = &(one_side_srs_[one_side_current_idx_]);
    *sges = one_side_sgs_[one_side_current_idx_];
    one_side_current_idx_++;
    return one_side_current_idx_;
  }

  int get_current_wr(ibv_send_wr** wr, ibv_sge** sges) {
    assert(current_idx_ >= 1 && current_idx_ <= opt_.mx_doorbell_batch_sz_);
    *wr = &(srs_[current_idx_ - 1]);
    *sges = sgs_[current_idx_ - 1];
    return current_idx_;
  }

  Options opt_;

  RingBuf send_ring_;
  RingBuf recv_ring_;
  uint32_t max_recv_ack_id_;

  int current_idx_;
  ibv_send_wr* srs_;  // size: Options::mx_doorbell_batch_sz_
  ibv_sge** sgs_;     // size: Options::mx_doorbell_batch_sz_ * Options::mx_sges_sz_
  int one_side_current_idx_;
  ibv_send_wr* one_side_srs_;
  ibv_sge** one_side_sgs_;
  ibv_send_wr* bad_send_wr_ = nullptr;

  uint32_t lkey_;
  rdma_bag* all_bags;
  std::list<rdma_bag*> rdma_inflight_;
  std::list<rdma_bag*> available_;

  std::shared_ptr<RdmaStraws> connector_;

  int queued_{0};           // 待发送的rpc请求数量
  int queued_msg_size_{0};  // 待发送的rpc消息大小
  int inflight_{0};         // 已经发送的请求数量
};