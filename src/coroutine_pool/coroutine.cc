#include "coroutine.h"

#include "coroutine_pool.h"
#include "coroutine_pool/scheduler.h"
#include "util/common.h"
#include "util/logging.h"

void Coroutine::routine(coro_yield_t &main_context) {
  yield_ = &main_context;
  while (true) {
    state_ = CoroutineState::RUNNABLE;
    // do some work
    task_();
    // exit
    state_ = CoroutineState::IDLE;
    (*yield_)();
  }
}

void Coroutine::yield() { (*yield_)(); }

void Coroutine::co_wait(int events) {
  if (events == 0) return;
  if (waiting_events_.fetch_add(events) + events > 0) {
    state_ = CoroutineState::WAITING;
    yield();
  }
}

extern thread_local Scheduler *scheduler;

void Coroutine::wakeup_once() {
  if ((--waiting_events_) == 0) {
    LOG_ASSERT(scheduler != nullptr, "invalid scheduler");
    sched_->addWakupCoroutine(this);
  }
}
