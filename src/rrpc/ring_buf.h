#pragma once

#include <stdint.h>

#include <cstring>

#include "common.h"
#include "rdma_straw.h"

// BUG(512B msg_size)
class RingBuf {
 public:
  static const uint64_t DEFAULT_BUCKET_SIZE = KB(16);
  static const uint64_t DEFAULT_BUCKET_NUM = 128;
  static const uint32_t LENGTH_MASK = (1ULL << 31) - 1;
  static const uint32_t CONTINUATION = (1ULL << 31);
  struct Options {
    uint64_t bucket_sz_ = DEFAULT_BUCKET_SIZE;
    uint64_t bucket_num_ = DEFAULT_BUCKET_NUM;
  };

  RingBuf(const Options& opt, int dev_id);

  RDMA_CM_ERROR_CODE init();
  bool is_empty() { return head_ == tail_; }
  uint32_t get_head() const { return head_; }
  uint32_t get_tail() const { return tail_; }
  uint32_t get_current_bucket_idx() const { return cur_bucket_idx_; }
  uint32_t get_prev_bucket_idx() { return (cur_bucket_idx_ + bucket_num_ - 1) % bucket_num_; }
  uint64_t get_local_addr() const { return laddr_; }
  uint32_t get_local_mr_rkey() const { return lmr_rkey_; }
  uint32_t get_local_mr_lkey() const { return lmr_lkey_; }
  uint64_t get_remote_addr() const { return raddr_; }
  uint32_t get_remote_mr_key() const { return rkey_; }
  void set_remote_addr(uint64_t addr) { raddr_ = addr; }
  void set_remote_key(uint32_t key) { rkey_ = key; }
  uint64_t get_bucket_size() { return bucket_sz_; }
  void advance_tail(uint32_t step) { tail_ += step; }
  void set_tail(uint32_t tail) {
    assert(tail <= head_ && tail >= tail_);
    tail_ = tail;
  }
  void set_bucket_footer();

  bool alloc_msg_buf_offset(uint32_t* off, uint32_t* prev_alloc_sz, bool& is_bucket_hd, uint32_t sz);

 public:
  // for recv ring
  uint64_t wait_msg_arrive(bool& has_next_bucket);

  bool try_poll_msg_arrive(uint64_t* msg_addr, bool* has_next_bucket);

 private:
  bool alloc_bucket();

  uint64_t bucket_sz_;
  uint64_t bucket_num_;
  int dev_id_;

  uint64_t laddr_;
  uint32_t lmr_lkey_;
  uint32_t lmr_rkey_;
  uint64_t raddr_;
  uint32_t rkey_;

  volatile uint32_t head_;
  volatile uint32_t tail_;
  uint32_t cur_bucket_idx_;
  uint32_t cur_offset_in_bucket_;
  uint32_t prev_alloc_sz_;  // 用来找到桶中上一条消息的头部位置
};