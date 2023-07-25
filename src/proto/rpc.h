#pragma once

#include <cstdint>

#include "common.h"
#include "coroutine_pool/coroutine.h"
#include "dtxn/dtxn.h"
#include "dtxn/takeout_lock.h"
#include "rrpc/rrpc.h"
#include "rrpc/rrpc_callbacks.h"

/**
 * @brief 低热度模式下数据读取
 *
 */
struct ReadReq {
  timestamp_t ts;
  uint64_t obj_id;
  uint32_t size;
  Mode mode;
};

struct ReadReqCtx {
  TxnStatus rc;
  TxnObj* obj;
  Coroutine* self;
};

struct ReadReply {
  DbStatus rc;
  TakeoutLock lock_info;
  timestamp_t version;
  uint32_t rkey;
  uint64_t obj_addr;
  uint64_t ptr_val;
  data_t data[0];
};

void read_service(Rocket::BatchIter* iter, Rocket* rkt);
void read_service_cb(void* _reply, void* _arg);

/**
 * @brief 主动排队
 *
 */
struct QueuingReq {
  Mode mode;
  timestamp_t ts;
  uint64_t obj_id;
};

struct QueuingCtx {
  bool is_queued;
  TxnObj* obj;
  Coroutine* self;
};

struct QueuingReply {
  LockReply lock_reply;
  uint32_t rkey;
};
void queuing_service(Rocket::BatchIter* iter, Rocket* rkt);
void queuing_service_cb(void* _reply, void* _arg);

/**
 * @brief 提交数据
 *
 */
struct WriteReq {
  bool create;
  uint64_t obj_id;
  timestamp_t ts;
  size_t size;
  data_t data[0];
};

struct WriteCtx {
  DbStatus status;
  Coroutine* self;
};

struct WriteReply {
  DbStatus status;
};
void write_service(Rocket::BatchIter* iter, Rocket* rkt);
void write_service_cb(void* _reply, void* _arg);

/**
 * @brief 高热度模式下读取数据，同时排队
 *
 */
struct QueuingReadReq {
  timestamp_t ts;
  uint64_t obj_id;
  uint32_t size;
};

struct QueuingReadCtx {
  TxnStatus rc;
  TLP tlp;
  Coroutine* self;
};

struct QueuingReadReply {
  LockReply lock_info;
  uint32_t rkey;
  timestamp_t version;
  data_t data[0];
};
void queuing_read_service(Rocket::BatchIter* iter, Rocket* rkt);
void queuing_read_service_cb(void* _reply, void* _arg);

// -------------------- Debug -------------------
struct DebugRead {
  uint64_t id;
  uint32_t sz;
  timestamp_t ts;
};

struct DebugReadReply {
  DbStatus rc;
  size_t sz;
  data_t raw[0];
};

struct DebugReadCtx {
  DbStatus rc;
  void *buf;
};

void debug_read_service(Rocket::BatchIter* iter, Rocket* rkt);
void debug_read_service_cb(void* _reply, void* _arg);

enum RpcId {
  READ = 123,
  QUEUING,
  WRITE,
  QUEUING_READ,

  DEBUG_READ,
};

static inline void RegisterService() {
  reg_rpc_service(READ, read_service);
  reg_rpc_service(QUEUING, queuing_service);
  reg_rpc_service(WRITE, write_service);
  reg_rpc_service(QUEUING_READ, queuing_read_service);
  reg_rpc_service(DEBUG_READ, debug_read_service);
};