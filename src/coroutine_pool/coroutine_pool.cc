#include "coroutine_pool.h"

#include "coroutine.h"
#include "coroutine_pool/scheduler.h"

thread_local coro_yield_t yield{};
CoroutinePool::CoroutinePool(int thread_num, int coroutine_per_thread) : worker_num_(thread_num) {
  schedulers_.reserve(thread_num);
  for (int i = 0; i < thread_num; i++) {
    schedulers_.emplace_back(new Scheduler(coroutine_per_thread));
  }
};

CoroutinePool::~CoroutinePool() {
  for (auto &sched : schedulers_) {
    sched->exit();
  }
  for (auto &thr : workers_) {
    thr.join();
  }
}

// do task in background
void CoroutinePool::start() {
  for (auto &sched : schedulers_) {
    workers_.emplace_back(&Scheduler::scheduling, sched.get());
  }
};