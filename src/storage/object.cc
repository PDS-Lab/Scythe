#include "object.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>

#include "common.h"
#include "util/common.h"
#include "util/mem_pool.h"

DbStatus object::get(timestamp_t ts, ReadResult &res, bool check_lock) {
  if (unlikely(latest == nullptr)) {
    return DbStatus::NOT_EXIST;
  }

  if (check_lock && lock.lower != lock.upper) {
    // early abort
    return DbStatus::LOCKED;
  }

  assert(res.buf_size >= obj_size);
  auto cur = latest;
  while (cur != nullptr && (ts < cur->version || !cur->visiable)) {
    cur = cur->older;
  }
  if (unlikely(cur == nullptr)) {
    return DbStatus::OBSOLETE;
  }
  memcpy(res.buf, cur->data, obj_size);
  res.version = cur->version;
  // for read new
  res.lock_info = lock;
  res.obj_addr = reinterpret_cast<uint64_t>(this);
  res.ptr_val = reinterpret_cast<uint64_t>(cur);
  return DbStatus::OK;
}

DbStatus object::alloc_insert(timestamp_t ts, void *val) {
  void *buf = malloc(sizeof(undo_log) + obj_size);
  if (unlikely(buf == nullptr)) {
    return DbStatus::ALLOC_FAILED;
  }

  undo_log *log = new (buf) undo_log();
  log->version = ts;
  log->older = latest;
  log->visiable = true;
  memcpy(log->data, val, obj_size);

  auto expect = latest;
  while (!__atomic_compare_exchange_n(&latest, &expect, log, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    ;
  return DbStatus::OK;
}

DbStatus object::alloc(timestamp_t ts, undo_log *&ptr) {
  void *buf = malloc(sizeof(undo_log) + obj_size);
  if (unlikely(buf == nullptr)) {
    return DbStatus::ALLOC_FAILED;
  }

  undo_log *log = new (buf) undo_log();
  log->version = ts;
  log->older = latest;
  log->visiable = false;
  ptr = log;
  auto expect = latest;
  while (!__atomic_compare_exchange_n(&latest, &expect, log, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    ;
  return DbStatus::OK;
}

object::~object() {
  auto cur = latest;
  while (cur != nullptr) {
    auto tmp = cur;
    cur = cur->older;
    free(tmp);
  }
}
