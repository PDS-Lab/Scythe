#include "db.h"

#include <memory>
#include <mutex>
#include <utility>

#include "common.h"
#include "dtxn/takeout_lock.h"
#include "storage/object.h"
#include "util/common.h"
#include "util/lock.h"
#include "util/logging.h"
#include "util/mem_pool.h"

KVEngine *global_db;

// implement
DbStatus KVEngine::get(uint64_t key, ReadResult &res, timestamp_t ts, bool check_lock) {
  auto h = key % kSharding;
  Hash_Table &table = shardings_[h];
  auto &lock = locks_[h];

  lock.RLock();
  if (unlikely(!table.count(key))) {
    lock.RUnlock();
    return DbStatus::NOT_EXIST;
  }

  auto obj = table[key];
  lock.RUnlock();
  return obj->get(ts, res, check_lock);
}

DbStatus KVEngine::put(uint64_t key, void *val, size_t length, timestamp_t ts) {
  auto h = key % kSharding;
  Hash_Table &table = shardings_[h];
  auto &lock = locks_[h];
  lock.WLock();
  if (!table.count(key)) {
    auto buf = BuddyThreadHeap::get_instance()->alloc(sizeof(object));
    if (unlikely(buf == nullptr)) {
      lock.WUnlock();
      return DbStatus::ALLOC_FAILED;
    }
    object *new_obj = new (buf) object(length);
    auto heap = BuddyThreadHeap::get_instance();
    auto deleter = [heap](object *p) {
      p->~object();
      if (heap == BuddyThreadHeap::get_instance()) {
        BuddyThreadHeap::get_instance()->free_local(p);
      }
    };
    table.insert(std::make_pair(key, std::shared_ptr<object>(new_obj, deleter)));
    lock.WUnlock();

    return new_obj->alloc_insert(ts, val);
  }
  auto obj = table[key];
  lock.WUnlock();

  return obj->alloc_insert(ts, val);
}

DbStatus KVEngine::update(uint64_t key, void *val, size_t length, timestamp_t ts) {
  auto h = key % kSharding;
  Hash_Table &table = shardings_[h];
  auto &lock = locks_[h];
  lock.RLock();
  if (unlikely(!table.count(key))) {
    lock.RUnlock();
    return DbStatus::NOT_EXIST;
  }
  auto obj = table[key];
  lock.RUnlock();

  return obj->alloc_insert(ts, val);
}

DbStatus KVEngine::alloc(uint64_t key, size_t length, timestamp_t ts) {
  auto h = key % kSharding;
  Hash_Table &table = shardings_[h];
  auto &lock = locks_[h];

  auto buf = BuddyThreadHeap::get_instance()->alloc(sizeof(object));
  if (unlikely(buf == nullptr)) {
    return DbStatus::ALLOC_FAILED;
  }
  object *new_obj = new (buf) object(length);
  auto heap = BuddyThreadHeap::get_instance();
  auto deleter = [heap](object *p) {
    p->~object();
    if (heap == BuddyThreadHeap::get_instance()) {
      BuddyThreadHeap::get_instance()->free_local(p);
    }
  };

  lock.WLock();
  if (unlikely(table.count(key) != 0)) {
    lock.WUnlock();
    BuddyThreadHeap::get_instance()->free_local(new_obj);
    return DbStatus::DUPLICATE;
  }
  table.insert(std::make_pair(key, std::shared_ptr<object>(new_obj, deleter)));
  lock.WUnlock();
  return DbStatus::OK;
}

LockReply KVEngine::try_lock(uint64_t key, timestamp_t ts, Mode mode) {
  auto h = key % kSharding;
  Hash_Table &table = shardings_[h];
  auto &lock = locks_[h];

  lock.RLock();
  if (unlikely(!table.count(key))) {
    lock.RUnlock();
    return {};
  }

  auto obj = table[key];
  lock.RUnlock();
  auto res = obj->lock.Lock(ts, mode);
  return res;
};