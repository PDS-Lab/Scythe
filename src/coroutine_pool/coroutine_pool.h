#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "concurrent_queue.h"
#include "coroutine_pool/scheduler.h"

class CoroutinePool {
 public:
  CoroutinePool(int thread_num, int coroutine_per_thread);

  void start();

  ~CoroutinePool();

  void enqueue(CoroutineTask &&task) {
    uint32_t cur = idx.fetch_add(1, std::memory_order_relaxed);
    schedulers_[cur % worker_num_]->addTask(std::move(task));
  }

  void enqueue(CoroutineTask &&task, int tid) { schedulers_[tid]->addTask(std::move(task)); }

 private:
  int worker_num_;
  std::vector<std::thread> workers_;
  std::vector<std::shared_ptr<Scheduler>> schedulers_;
  std::atomic_uint32_t idx;
};