#pragma once

#include <sys/types.h>

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common.h"
#include "object.h"
#include "dtxn/takeout_lock.h"
#include "util/lock.h"

// simple KV engine used for disaggregated memroy transaction
class KVEngine {
  static constexpr size_t kReserveSlot = KB(32);
  static constexpr size_t kSharding = 1 << 8;

 public:
  KVEngine() {
    for (size_t i = 0; i < kSharding; i++) {
      shardings_[i].reserve(kReserveSlot);
    }
  }

  KVEngine(const KVEngine &) = delete;
  KVEngine(KVEngine &&) = delete;
  KVEngine &operator=(const KVEngine &) = delete;
  KVEngine &operator=(KVEngine &&) = delete;

  DbStatus get(uint64_t key, ReadResult &res, timestamp_t ts, bool check_lock);

  DbStatus put(uint64_t key, void *val, size_t length, timestamp_t ts);

  DbStatus alloc(uint64_t key, size_t length, timestamp_t ts);

  DbStatus update(uint64_t key, void *val, size_t length, timestamp_t ts);

  LockReply try_lock(uint64_t key, timestamp_t ts, Mode mode);

 private:
  using Hash_Table = std::unordered_map<uint64_t, std::shared_ptr<object>>;

  Hash_Table shardings_[kSharding];
  SharedSpinLock locks_[kSharding];
};

extern KVEngine *global_db;
extern KVEngine *c_to_d_db;
extern KVEngine *d_to_w_db;
extern KVEngine *stock_db;
extern KVEngine *new_order_db;

typedef uint64_t DB_INDEX;
extern const DB_INDEX c_to_d_db_index;
extern const DB_INDEX d_to_w_db_index;
extern const DB_INDEX stock_db_index;
extern const DB_INDEX new_order_db_index;
extern const DB_INDEX db_index_mask;