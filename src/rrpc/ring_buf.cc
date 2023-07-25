#include "ring_buf.h"

#include "rdma_cm.h"
#include "util/common.h"
#include "util/mem_pool.h"

RDMA_CM_ERROR_CODE RingBuf::init() {
  laddr_ = (uint64_t)(uintptr_t)(global_mem_pool->alloc_chunk());
  if (!laddr_) {
    return RDMA_CM_ERROR_CODE::CM_ALLOC_RING_BUF_FAILED;
  }
  memset((void*)(uintptr_t)laddr_, 0, bucket_sz_ * bucket_num_);
  RdmaDev* dev = nullptr;
  RDMA_CM_ERROR_CODE rc = global_cm->open_device(dev_id_, &dev);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS && rc != RDMA_CM_ERROR_CODE::CM_OBJ_INITIALIZED) {
    return rc;
  }
  lmr_lkey_ = dev->mr->lkey;
  lmr_rkey_ = dev->mr->rkey;
  // printf("[RingBuf][init] addr:%lu lkey:%u rkey:%u\n", laddr_, lmr_lkey_, lmr_rkey_);
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RingBuf::RingBuf(const Options& opt, int dev_id)
    : bucket_sz_(opt.bucket_sz_), bucket_num_(opt.bucket_num_), dev_id_(dev_id) {
  assert((bucket_num_ & (-bucket_num_)) == bucket_num_);
  laddr_ = 0;
  lmr_lkey_ = 0;
  lmr_rkey_ = 0;
  raddr_ = 0;
  rkey_ = 0;
  head_ = 0;
  tail_ = 0;
  cur_bucket_idx_ = 0;
  cur_offset_in_bucket_ = bucket_sz_;
  prev_alloc_sz_ = 0;
}

void RingBuf::set_bucket_footer() {
  if (likely(cur_offset_in_bucket_ != bucket_sz_)) {
    uint32_t* header = (uint32_t*)(uintptr_t)(laddr_ + cur_bucket_idx_ * bucket_sz_);
    assert(*header == cur_offset_in_bucket_);
    uint32_t* footer = (uint32_t*)(uintptr_t)((uint64_t)(uintptr_t)(header) + cur_offset_in_bucket_);
    *footer = *header;
    cur_offset_in_bucket_ = bucket_sz_;
  }
}

bool RingBuf::alloc_msg_buf_offset(uint32_t* off, uint32_t* prev_alloc_sz, bool& is_bucket_hd, uint32_t sz) {
  assert(sz < bucket_sz_);
  is_bucket_hd = false;
  if (cur_offset_in_bucket_ + sz + sizeof(uint32_t) > bucket_sz_) {
    set_bucket_footer();
    if (!alloc_bucket()) return false;
    cur_offset_in_bucket_ = 0;
    is_bucket_hd = true;
    prev_alloc_sz_ = 0;
  }
  *prev_alloc_sz = prev_alloc_sz_;
  *off = bucket_sz_ * cur_bucket_idx_ + cur_offset_in_bucket_;
  cur_offset_in_bucket_ += sz;
  prev_alloc_sz_ = sz;
  return true;
}

uint64_t RingBuf::wait_msg_arrive(bool& has_next_bucket) {
  uint64_t tag_addr = laddr_ + bucket_sz_ * (tail_ % bucket_num_);
  volatile uint32_t* tag = (volatile uint32_t*)tag_addr;
  while (*tag == 0)
    ;

  volatile uint32_t* footer = (volatile uint32_t*)(tag_addr + ((*tag) & LENGTH_MASK));
  while (((*tag) & LENGTH_MASK) != (*footer))
    ;

  has_next_bucket = (*tag) & CONTINUATION;
  *tag = 0;
  *footer = 0;
  return tag_addr;
}

bool RingBuf::try_poll_msg_arrive(uint64_t* msg_addr, bool* has_next_bucket) {
  uint64_t tag_addr = laddr_ + bucket_sz_ * (tail_ % bucket_num_);
  uint32_t tag = *(volatile uint32_t*)tag_addr;
  if (tag) {
    volatile uint32_t* footer = (volatile uint32_t*)(tag_addr + (tag & LENGTH_MASK));
    if ((tag & LENGTH_MASK) == *footer) {
      if (has_next_bucket) *has_next_bucket = tag & CONTINUATION;
      *msg_addr = tag_addr;
      *(volatile uint32_t*)tag_addr = 0;
      *footer = 0;
      return true;
    }
  }
  return false;
}

bool RingBuf::alloc_bucket() {
  uint32_t free_entries_num;
  free_entries_num = (bucket_num_ - 1) + tail_ - head_;
  if (free_entries_num == 0) return false;
  cur_bucket_idx_ = head_ % bucket_num_;
  head_++;
  return true;
}
