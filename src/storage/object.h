#pragma once

#include <atomic>
#include <cstdint>

#include "common.h"
#include "dtxn/takeout_lock.h"
#include "util/lock.h"

constexpr uint16_t kMaxObjSize = ~0;

struct undo_log {
  timestamp_t version{0};
  undo_log *older{nullptr};  // 旧版本
  volatile bool visiable{false};
  data_t data[0];
};

struct ReadResult {
  size_t buf_size;
  void *buf;
  timestamp_t version;  // data version
  TakeoutLock lock_info;
  // ---- for OCC validate
  uint64_t obj_addr;
  uint64_t ptr_val;
};

struct object {
  TakeoutLock lock{};
  undo_log *latest{nullptr};  // 旧版本
  size_t obj_size{0};

  object(uint16_t size) : obj_size(size) {}
  ~object();
  DbStatus get(timestamp_t ts, ReadResult &res, bool check_lock);

  DbStatus alloc_insert(timestamp_t ts, void *val);

  DbStatus alloc(timestamp_t ts, undo_log *&ptr);
};
