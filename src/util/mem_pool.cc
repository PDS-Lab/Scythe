#include "mem_pool.h"

#include <mutex>
#include <stdexcept>
MemPool *global_mem_pool = nullptr;

volatile bool inited = false;

void InitMemPool(void *pool_addr, size_t pool_size,
                 size_t chunk_size) {
  if (inited) {
    throw std::runtime_error("repeated initialization");
  }
  inited = true;
  global_mem_pool = new MemPool(chunk_size, pool_addr, pool_size);
}

void DestroyMemPool() {
  delete global_mem_pool;
  global_mem_pool = nullptr;
  inited = false;
}

MemPool::MemPool(uint64_t chunk_size, void *pool_addr, uint64_t pool_size)
    : chunk_size_(chunk_size), pool_size_(pool_size), pool_addr_(pool_addr) {
  assert(pool_size_ % chunk_size_ == 0 && chunk_size_ % GLOBAL_MEM_POOL_BUDDY_UNIT == 0);
  chunk_num_ = pool_size_ / chunk_size_;
  assert(chunk_num_ <= MEM_POOL_MAX_CHUNK_NUM && ((chunk_num_ & (chunk_num_ - 1)) == 0));
  first_free_id_ = 1;
  for (int i = 1; i <= chunk_num_; ++i) {
    if (i == chunk_num_)
      next_chunk_id[i] = 0;
    else
      next_chunk_id[i] = i + 1;
  }
}

void *MemPool::alloc_chunk() {
  size_t free_id = 0;
  std::lock_guard<std::mutex> lg(mutex_);
  if (first_free_id_) {
    free_id = first_free_id_;
    first_free_id_ = next_chunk_id[first_free_id_];
  }

  assert(free_id);
  return (char *)(pool_addr_) + (free_id - 1) * chunk_size_;
}

void MemPool::free_chunk(void *addr) {
  size_t chunk_id = (((uint64_t)(addr) - (uint64_t)(pool_addr_)) / chunk_size_) + 1;
  assert(chunk_id >= 1 && chunk_id <= chunk_num_);
  std::lock_guard<std::mutex> lg(mutex_);
  next_chunk_id[chunk_id] = first_free_id_;
  first_free_id_ = chunk_id;
}

BuddySystem::BuddySystem(void *chunk_addr) : chunk_addr_(chunk_addr) {
  size_ = global_mem_pool->chunk_size_ / GLOBAL_MEM_POOL_BUDDY_UNIT;
  assert(size_ > 1 && IS_POW_OF_2(size_) && size_ * 2 <= BUDDY_SYSTEM_MAX_NODE_NUM);

  size_t node_size = size_ * 2;
  for (int i = 0; i < size_ * 2 - 1; ++i) {
    if (IS_POW_OF_2(i + 1)) node_size >>= 1;
    longest_[i] = node_size;
  }
}

void *BuddySystem::alloc(size_t sz) {
  size_t unit_cnt;
  if (((sz >> LOG_GLOBAL_MEM_POOL_BUDDY_UNIT) << LOG_GLOBAL_MEM_POOL_BUDDY_UNIT) == sz)
    unit_cnt = (sz >> LOG_GLOBAL_MEM_POOL_BUDDY_UNIT);
  else
    unit_cnt = (sz >> LOG_GLOBAL_MEM_POOL_BUDDY_UNIT) + 1;

  size_t index = 0;
  size_t node_size;
  size_t unit_offset = 0;

  if (unit_cnt > longest_[index]) return nullptr;
  // 向上取到最近的2的幂
  if (!(IS_POW_OF_2(unit_cnt))) {
    unit_cnt |= (unit_cnt >> 1);
    unit_cnt |= (unit_cnt >> 2);
    unit_cnt |= (unit_cnt >> 4);
    unit_cnt |= (unit_cnt >> 8);
    unit_cnt |= (unit_cnt >> 16);
    unit_cnt++;
  }

  for (node_size = size_; node_size != unit_cnt; node_size >>= 1) {
    if (longest_[LEFT_LEAF(index)] >= unit_cnt)
      index = LEFT_LEAF(index);
    else
      index = RIGHT_LEAF(index);
  }

  longest_[index] = 0;
  // unit_offset = (index - (size_ / node_size - 1)) * node_size
  unit_offset = (index + 1) * node_size - size_;

  while (index) {
    index = PARENT(index);
    if (longest_[LEFT_LEAF(index)] > longest_[RIGHT_LEAF(index)])
      longest_[index] = longest_[LEFT_LEAF(index)];
    else
      longest_[index] = longest_[RIGHT_LEAF(index)];
  }
  cnt_++;
  return (char *)(chunk_addr_) + (unit_offset << LOG_GLOBAL_MEM_POOL_BUDDY_UNIT);
}

void BuddySystem::free(void *addr) {
  cnt_--;
  size_t offset = ((char *)addr) - ((char *)chunk_addr_);
  assert(((offset >> LOG_GLOBAL_MEM_POOL_BUDDY_UNIT) << LOG_GLOBAL_MEM_POOL_BUDDY_UNIT) == offset);
  size_t unit_offset = offset >> LOG_GLOBAL_MEM_POOL_BUDDY_UNIT;
  size_t node_size, index = 0;
  size_t left_longest, right_longest;

  assert(unit_offset >= 0 && unit_offset < size_);

  node_size = 1;
  index = unit_offset + size_ - 1;

  for (; longest_[index]; index = PARENT(index)) {
    node_size <<= 1;
    if (index == 0) return;
  }

  longest_[index] = node_size;

  while (index) {
    index = PARENT(index);
    node_size <<= 1;

    left_longest = longest_[LEFT_LEAF(index)];
    right_longest = longest_[RIGHT_LEAF(index)];

    if (left_longest + right_longest == node_size)
      longest_[index] = node_size;
    else {
      longest_[index] = ((left_longest > right_longest) ? left_longest : right_longest);
    }
  }
}

void *BuddyThreadHeap::alloc(size_t sz) {
  // free
  if (need_free_cnt_ == 0) {
    need_free_cnt_ = free_list_.try_dequeue_bulk(free_buf_, FREE_BUF_SIZE);
  }
  for (int i = 0; i < FREE_BATCH && need_free_cnt_ > 0; i++, need_free_cnt_--) {
    free_impl(free_buf_[need_free_cnt_ - 1]);
  }
  // and then alloc
  assert(sz < global_mem_pool->chunk_size_);
  void *addr = nullptr;
BUDDY_RETRY:
  if (active_buddy2_) {
    if ((addr = active_buddy2_->alloc(sz))) return addr;
  }
  if ((addr = active_buddy1_->alloc(sz))) return addr;
  BuddySystem *new_buddy = new BuddySystem(global_mem_pool->alloc_chunk());
  if (active_buddy2_ != nullptr) {
    freeze_buddies_.push_back(active_buddy2_);
  }
  active_buddy2_ = active_buddy1_;
  active_buddy1_ = new_buddy;
  goto BUDDY_RETRY;
}

void BuddyThreadHeap::free_impl(void *addr) {
  if (active_buddy2_ && in_chunk(active_buddy2_, addr)) {
    active_buddy2_->free(addr);
    return;
  }
  if (in_chunk(active_buddy1_, addr)) {
    active_buddy1_->free(addr);
    return;
  }

  for (auto iter = freeze_buddies_.begin(); iter != freeze_buddies_.end(); ++iter) {
    auto chunk = *iter;
    if (in_chunk(chunk, addr)) {
      chunk->free(addr);
      if (chunk->empty()) {
        chunk->release();
        delete chunk;
        freeze_buddies_.erase(iter);
      }
      return;
    }
  }
}

void BuddyThreadHeap::free(void *addr, BuddyThreadHeap *heap) { heap->free_list_.enqueue(addr); }

BuddyThreadHeap *BuddyThreadHeap::get_instance() {
  thread_local BuddyThreadHeap buddy_thread_lheap;
  return &buddy_thread_lheap;
}
