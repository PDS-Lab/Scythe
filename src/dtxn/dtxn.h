#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "common.h"
#include "coroutine_pool/scheduler.h"
#include "takeout_lock.h"
#include "tso.h"
#include "util/logging.h"
#include "util/mem_pool.h"
enum class TxnStatus {
  OK = 1,
  VALIDATE_FAIL,
  RETRY,      // OCC 重试
  SWITCH,     // OCC 切换
  INTERNAL,   // 系统内部故障
  TOC_RETRY,  // TOC 叫号乱序
};

// TxnObj 的生命周期从TxnBegin 到 Commit之后就结束了，每次事务重试都需要创建新的TxnObj
class TxnObj {
  using Op = uint8_t;
  static constexpr Op INIT = 0;
  static constexpr Op READ = 1;
  static constexpr Op WRITE = 2;
  static constexpr Op RW = 3;

  friend class OCC;
  friend class TOC;
  friend void read_service_cb(void *_reply, void *_arg);
  friend void queuing_service_cb(void *_reply, void *_arg);
  friend void queuing_read_service_cb(void *_reply, void *_arg);
  friend void poll_lock_cb(void *_ctx);

 public:
  TxnObj(uint64_t obj_id, size_t size)
      : id_(obj_id),
        size_(size),
        table_id_(0),
        buf_(operator new(size)),
        lock_proxy(std::make_shared<TakeoutLockProxy>(this_coroutine::current(), this)),
        hold_lock(false),
        own_buf_(true) {
    // LOG_INFO("[%d] Make TxnObj: [%lu]", this_coroutine::current()->id(), obj_id);
  }
  TxnObj(uint64_t obj_id, size_t size,uint32_t table_id)
      : id_(obj_id),
        size_(size),
        table_id_(table_id),
        buf_(operator new(size)),
        lock_proxy(std::make_shared<TakeoutLockProxy>(this_coroutine::current(), this)),
        hold_lock(false),
        own_buf_(true) {
    // LOG_INFO("[%d] Make TxnObj: [%lu]", this_coroutine::current()->id(), obj_id);
  }
  TxnObj(uint64_t obj_id, size_t size, void *buf, uint32_t table_id = 0)
      : id_(obj_id),
        size_(size),
        table_id_(table_id),
        buf_(buf),
        lock_proxy(std::make_shared<TakeoutLockProxy>(this_coroutine::current(), this)),
        hold_lock(false),
        own_buf_(false) {}

  ~TxnObj() {
    if (own_buf_) {
      operator delete(buf_);
      own_buf_ = false;
    }
    buf_ = nullptr;
    lock_proxy->txn_coro = nullptr;
    lock_proxy->obj = nullptr;
  }

  void *data() { return buf_; }

  template <typename T>
  T *get_as() {
    return reinterpret_cast<T *>(buf_);
  }

  uint32_t size() const { return size_; }
  uint64_t id() const { return id_; }
  uint32_t table_id() const {return table_id_;}

  void set_new() {
    put_new_ = true;
  }
  void set_read() { op_ |= READ; }
  void set_write() { op_ |= WRITE; };
  Op op() { return op_; }

 private:
  TLP lock_proxy;
  bool hold_lock;
  

  // ----------- used for OCC validate -------
  uint32_t rkey;
  uint64_t ptr_val;
  uint64_t obj_addr;

  uint64_t id_;
  uint32_t table_id_;
  uint32_t size_;
  void *buf_;
  bool own_buf_;
  bool put_new_{false};

  Op op_{INIT};
};
using TxnObjPtr = std::shared_ptr<TxnObj>;

class Transaction {
 public:
  Transaction() { begin_ts_ = TSO::get_ts(); }

  virtual TxnStatus Read(TxnObjPtr obj) = 0;
  // read serval objs in batch
  virtual TxnStatus Read(const std::vector<TxnObjPtr> &objs) = 0;

  virtual TxnStatus Write(TxnObjPtr obj) = 0;

  virtual TxnStatus Write(const std::vector<TxnObjPtr> &objs) = 0;

  virtual TxnStatus Commit() = 0;

  virtual TxnStatus Commit(struct timeval& lock_start_tv,struct timeval& lock_end_tv,struct timeval& validation_start_tv,struct timeval& validation_end_tv,struct timeval& write_start_tv,struct timeval& write_end_tv,struct timeval& commit_start_tv,struct timeval& commit_end_tv) = 0;

  virtual TxnStatus Rollback() = 0;

  template <typename... Args>
  TxnObjPtr GetObject(uint64_t obj_id, Args &&...__args);

 protected:
  timestamp_t begin_ts_;
  timestamp_t commit_ts_;
  std::unordered_map<uint64_t, TxnObjPtr> obj_cache_;
};

template <typename... Args>
TxnObjPtr Transaction::GetObject(uint64_t obj_id, Args &&...__args) {
  if (obj_cache_.count(obj_id)) {
    return obj_cache_.at(obj_id);
  }
  auto new_obj = std::make_shared<TxnObj>(obj_id, std::forward<Args>(__args)...);
  obj_cache_.emplace(obj_id, new_obj);
  return new_obj;
}

class TransactionFactory {
 public:
  static std::shared_ptr<Transaction> TxnBegin(Mode mode = Mode::COLD);
};
