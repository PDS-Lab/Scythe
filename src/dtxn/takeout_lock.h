#pragma once

#include <cstdint>

#include "common.h"
#include "coroutine_pool/coroutine.h"

struct LockReply;

struct TakeoutLock {
  uint64_t lower{0};
  uint64_t upper{0};
  timestamp_t queued_ts{0};

  LockReply Lock(timestamp_t ts, Mode mode);

  uint64_t queued_num() const { return lower - upper; }

  bool ready() const { return lower == upper; }
};

struct LockReply {
  bool is_queued{false};
  TakeoutLock lock{};
  uint64_t lock_addr{0};
};

class TxnObj;
struct TakeoutLockProxy : public std::enable_shared_from_this<TakeoutLockProxy> {
  struct Compare {
   public:
    bool operator()(const std::shared_ptr<TakeoutLockProxy> &a, const std::shared_ptr<TakeoutLockProxy> &b) {
      return a->us_since_poll + a->hangout > b->us_since_poll + b->hangout;
    }
  };
  void poll_lock();
  void unlock();
  uint64_t next_poll_time() const { return us_since_poll + hangout; }
  std::shared_ptr<TakeoutLockProxy> getShared() { return shared_from_this(); }

  TakeoutLock tl;
  uint64_t us_since_poll;  // 上一次轮询时的时间
  uint64_t hangout;
  uint64_t lock_addr;
  uint32_t rkey;
  
  TxnObj *obj;
  Coroutine *txn_coro;
  TakeoutLockProxy(Coroutine *coro, TxnObj *obj) : txn_coro(coro), obj(obj) {}
};

using TLP = std::shared_ptr<TakeoutLockProxy>;
