#pragma once
#include <atomic>
#include <boost/coroutine/coroutine.hpp>
#include <list>

#include "concurrent_queue.h"

using coro_call_t = boost::coroutines::symmetric_coroutine<void>::call_type;
using coro_yield_t = boost::coroutines::symmetric_coroutine<void>::yield_type;

using coro_id_t = int;
using CoroutineTask = std::function<void()>;

// idle -> runnable -> waiting -> runnable -> idle
enum class CoroutineState {
  IDLE,
  RUNNABLE,
  WAITING,
};

class Scheduler;
class Coroutine {
 public:
  friend class Scheduler;
  Coroutine(coro_id_t id, Scheduler *sched)
      : coro_id_(id), sched_(sched), func_(std::bind(&Coroutine::routine, this, std::placeholders::_1)) {}
  void yield();
  void co_wait(int events = 1);
  void wakeup_once();
  coro_id_t id() const { return coro_id_; }

 private:
  void routine(coro_yield_t &main_context);

  coro_id_t coro_id_;
  Scheduler *sched_;

  coro_call_t func_;
  coro_yield_t *yield_;
  CoroutineTask task_;
  std::atomic_int waiting_events_{};
  std::list<Coroutine *>::iterator iter{};
  CoroutineState state_{CoroutineState::IDLE};
};