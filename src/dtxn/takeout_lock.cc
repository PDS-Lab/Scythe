#include "takeout_lock.h"

#include <algorithm>

#include "common.h"
#include "coroutine_pool/scheduler.h"
#include "proto/rdma.h"
#include "rrpc/rrpc.h"
#include "util/logging.h"

LockReply TakeoutLock::Lock(timestamp_t ts, Mode mode) {
  timestamp_t old_queued_ts;
  TakeoutLock tmp;
  do {
    auto CurrentQueuedTxnNum = queued_num();
    // try to take a ticket
    old_queued_ts = queued_ts;
    if ((ts < old_queued_ts) || (mode == Mode::COLD && CurrentQueuedTxnNum > kColdWatermark) ||
        (CurrentQueuedTxnNum > kHotWatermark)) {
      return {false, *this, reinterpret_cast<uint64_t>(this)};
    }
    tmp = *this;
  } while (!__atomic_compare_exchange_n(&queued_ts, &old_queued_ts, ts, true, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE));
  // update lock state
  __atomic_fetch_add(&lower, 1, __ATOMIC_RELAXED);
  return {true, tmp, reinterpret_cast<uint64_t>(this)};
}

void TakeoutLockProxy::poll_lock() {
  auto rkt = GetRocket(0);
  auto *ctx = new PollLockCtx{getShared()};
  auto rc =
      rkt->remote_read(&tl.upper, sizeof(uint64_t), lock_addr + OFFSET(TakeoutLock, upper), rkey, poll_lock_cb, ctx);
  ENSURE(rc == RDMA_CM_ERROR_CODE::CM_SUCCESS, "RDMA read failed, %d", (int)rc);
};

void TakeoutLockProxy::unlock() {
  auto rkt = GetRocket(0);
  // RDMA Fatch And Add
  auto *ctx = new UnlockCtx{this_coroutine::current()};
  auto rc =
      rkt->remote_fetch_add(nullptr, sizeof(uint64_t), lock_addr + OFFSET(TakeoutLock, upper), rkey, 1, unlock_cb, ctx);
  ENSURE(rc == RDMA_CM_ERROR_CODE::CM_SUCCESS, "RDMA fetch add failed, %d", (int)rc);
};