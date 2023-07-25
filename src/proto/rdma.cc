#include "rdma.h"

#include "coroutine_pool/scheduler.h"
#include "dtxn/dtxn.h"
#include "util/logging.h"
#include "util/timer.h"
void validate_cb(void* _ctx) {
  auto ctx = reinterpret_cast<ValidateCtx*>(_ctx);
  ctx->self->wakeup_once();
};

void unlock_cb(void* _ctx) {
  auto ctx = reinterpret_cast<UnlockCtx*>(_ctx);
  if (ctx->self) {
    ctx->self->wakeup_once();
  }
  delete ctx;
};

void poll_lock_cb(void* _ctx) {
  auto ctx = reinterpret_cast<PollLockCtx*>(_ctx);
  auto& tlp = ctx->tlp;
  if (tlp->tl.ready()) {
    if (tlp->txn_coro != nullptr) {
      tlp->obj->hold_lock = true;
      tlp->txn_coro->wakeup_once();
    } else {
      tlp->unlock();
    }
  } else {
    tlp->us_since_poll = RdtscTimer::instance().us();
    tlp->hangout = tlp->tl.queued_num() * RTT;
    this_coroutine::scheduler_delegate(tlp);
  }
  delete ctx;
};