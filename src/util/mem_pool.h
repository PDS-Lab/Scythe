#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "concurrent_queue.h"
#include "rrpc/ring_buf.h"

// #define GLOBAL_MEM_POOL_DEBUG
constexpr size_t LOG_GLOBAL_MEM_POOL_BUDDY_UNIT = 6;
constexpr size_t GLOBAL_MEM_POOL_BUDDY_UNIT = (1 << (LOG_GLOBAL_MEM_POOL_BUDDY_UNIT));
class MemPool {
 public:
  static const size_t MEM_POOL_MAX_CHUNK_NUM = 65536;

  MemPool(uint64_t chunk_size, void *pool_addr, uint64_t pool_size);

  void *alloc_chunk();

  void free_chunk(void *addr);

  uint64_t chunk_size_;
  uint64_t pool_size_;
  void *pool_addr_;
  size_t chunk_num_;
  size_t first_free_id_;
  size_t next_chunk_id[MEM_POOL_MAX_CHUNK_NUM + 1];
  std::mutex mutex_;
};

extern MemPool *global_mem_pool;

class BuddySystem {
 public:
  static const size_t BUDDY_SYSTEM_MAX_NODE_NUM = 8192 * 16;
#define IS_POW_OF_2(x) (((x) & ((x)-1)) == 0)
#define LEFT_LEAF(x) (((x) << 1) + 1)
#define RIGHT_LEAF(x) (((x) << 1) + 2)
#define PARENT(x) ((x - 1) >> 1)
  BuddySystem(void *chunk_addr);
  ~BuddySystem() {}

  void *alloc(size_t sz);

  void free(void *addr);

  bool empty() const {
    assert(cnt_ >= 0);
    return cnt_ == 0;
  }

  void release() {
    if (chunk_addr_ && global_mem_pool) {
      global_mem_pool->free_chunk(chunk_addr_);
      chunk_addr_ = nullptr;
    }
  }

  void *chunk_addr_;
  size_t size_;
  uint16_t longest_[BUDDY_SYSTEM_MAX_NODE_NUM];
  int cnt_;
};

class BuddyThreadHeap {
  static constexpr int FREE_BUF_SIZE = 128;
  static constexpr int FREE_BATCH = 4;

 public:
  BuddyThreadHeap() {
    assert(global_mem_pool);
    active_buddy1_ = new BuddySystem(global_mem_pool->alloc_chunk());
    active_buddy2_ = nullptr;
  }

  ~BuddyThreadHeap() {
    delete active_buddy1_;
    delete active_buddy2_;
    for (auto &buddy : freeze_buddies_) {
      delete buddy;
    }
  }

  void *alloc(size_t sz);

  // 用来跨线程free, 需要保证该heap没有被析构
  void free(void *addr, BuddyThreadHeap *heap);

  // 确定与alloc为相同线程的free
  void free_local(void *addr) { free_impl(addr); }

  bool in_chunk(BuddySystem *buddy, void *addr) {
    uint64_t addr_u64 = (uint64_t)(uintptr_t)(addr);
    uint64_t buddy_begin = (uint64_t)(uintptr_t)(buddy->chunk_addr_);
    return (addr_u64 >= buddy_begin) && (addr_u64 < (buddy_begin + global_mem_pool->chunk_size_));
  }

  static BuddyThreadHeap *get_instance();

 private:
  void free_impl(void *addr);
  moodycamel::ConcurrentQueue<void *> free_list_;
  size_t need_free_cnt_{0};
  void *free_buf_[FREE_BUF_SIZE];

  std::list<BuddySystem *> freeze_buddies_;
  BuddySystem *active_buddy2_;
  BuddySystem *active_buddy1_;
};

void InitMemPool(void *pool_addr, size_t pool_size,
                 size_t chunk_size  = RingBuf::DEFAULT_BUCKET_SIZE * RingBuf::DEFAULT_BUCKET_NUM);

void DestroyMemPool();