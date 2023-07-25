#include "rpc.h"

#include <cstring>

#include "common.h"
#include "rrpc/rdma_cm.h"
#include "storage/db.h"
#include "storage/object.h"
#include "util/logging.h"

void read_service(Rocket::BatchIter* iter, Rocket* rkt) {
  auto req = iter->get_request<ReadReq>();
  auto reply = rkt->gen_reply<ReadReply>(sizeof(ReadReply) + req->size, iter);

  ReadResult res;
  res.buf_size = req->size;
  res.buf = reply->data;

  auto rc = global_db->get(req->obj_id, res, req->ts, req->mode == Mode::COLD);
  reply->rc = rc;
  reply->version = res.version;
  reply->lock_info = res.lock_info;
  reply->rkey = global_cm->get_rkey();
  reply->obj_addr = res.obj_addr;
  reply->ptr_val = res.ptr_val;
}

void read_service_cb(void* _reply, void* _arg) {
  auto reply = reinterpret_cast<ReadReply*>(_reply);
  auto ctx = reinterpret_cast<ReadReqCtx*>(_arg);
  switch (reply->rc) {
    case DbStatus::OK: {
      memcpy(ctx->obj->data(), reply->data, ctx->obj->size());
      ctx->obj->rkey = reply->rkey;
      ctx->obj->obj_addr = reply->obj_addr;
      ctx->obj->ptr_val = reply->ptr_val;
      ctx->rc = TxnStatus::OK;
      break;
    }
    case DbStatus::LOCKED: {
      auto queued = reply->lock_info.queued_num();
      ctx->rc = queued > kColdWatermark ? TxnStatus::SWITCH : TxnStatus::RETRY;
      break;
    }
    default:
      LOG_FATAL("read_service failed, %s", Status2Str(reply->rc).c_str());
  }
  ctx->self->wakeup_once();
};

void queuing_service(Rocket::BatchIter* iter, Rocket* rkt) {
  auto req = iter->get_request<QueuingReq>();
  auto reply = rkt->gen_reply<QueuingReply>(sizeof(QueuingReply), iter);

  auto lock_reply = global_db->try_lock(req->obj_id, req->ts, Mode::COLD);
  reply->lock_reply = lock_reply;
  reply->rkey = global_cm->get_rkey();
}

void queuing_service_cb(void* _reply, void* _arg) {
  auto reply = reinterpret_cast<QueuingReply*>(_reply);
  auto ctx = reinterpret_cast<QueuingCtx*>(_arg);
  ctx->is_queued = reply->lock_reply.is_queued;
  ctx->obj->lock_proxy->rkey = reply->rkey;
  ctx->obj->lock_proxy->lock_addr = reply->lock_reply.lock_addr;
  ctx->obj->lock_proxy->tl = reply->lock_reply.lock;
  ctx->self->wakeup_once();
};

void write_service(Rocket::BatchIter* iter, Rocket* rkt) {
  auto req = iter->get_request<WriteReq>();
  auto reply = rkt->gen_reply<WriteReply>(sizeof(WriteReply), iter);
  if (req->create) {
    reply->status = global_db->put(req->obj_id, req->data, req->size, req->ts);
  } else {
    reply->status = global_db->update(req->obj_id, req->data, req->size, req->ts);
  }
};

void write_service_cb(void* _reply, void* _arg) {
  auto ctx = reinterpret_cast<WriteCtx*>(_arg);
  auto reply = reinterpret_cast<WriteReply*>(_reply);
  ctx->status = reply->status;
  ctx->self->wakeup_once();
};

void queuing_read_service(Rocket::BatchIter* iter, Rocket* rkt) {
  auto req = iter->get_request<QueuingReadReq>();
  QueuingReadReply* reply = nullptr;

  auto lock_reply = global_db->try_lock(req->obj_id, req->ts, Mode::HOT);
  if (lock_reply.is_queued && lock_reply.lock.ready()) {
    // lock & read
    reply = rkt->gen_reply<QueuingReadReply>(sizeof(QueuingReadReply) + req->size, iter);
    ReadResult res;
    res.buf = reply->data;
    res.buf_size = req->size;
    auto rc = global_db->get(req->obj_id, res, req->ts, false);
    LOG_ASSERT(rc == DbStatus::OK, "read failed");
    reply->lock_info = lock_reply;
    reply->rkey = global_cm->get_rkey();
    reply->version = res.version;
    return;
  }
  // lock fail
  reply = rkt->gen_reply<QueuingReadReply>(sizeof(QueuingReadReply), iter);
  reply->lock_info = lock_reply;
  reply->rkey = global_cm->get_rkey();
};

void queuing_read_service_cb(void* _reply, void* _arg) {
  auto ctx = reinterpret_cast<QueuingReadCtx*>(_arg);
  auto reply = reinterpret_cast<QueuingReadReply*>(_reply);
  if (reply->lock_info.is_queued) {
    ctx->rc = TxnStatus::OK;
    ctx->tlp->tl = reply->lock_info.lock;
    ctx->tlp->lock_addr = reply->lock_info.lock_addr;
    ctx->tlp->rkey = reply->rkey;
    if (reply->lock_info.lock.ready()) {
      // ready now
      auto obj = ctx->tlp->obj;
      memcpy(obj->buf_, reply->data, obj->size_);
      obj->hold_lock = true;
      LOG_DEBUG("Ready %ld", obj->id());
    } else {
      // delegate lock
      ctx->tlp->us_since_poll = RdtscTimer::instance().us();
      ctx->tlp->hangout = ctx->tlp->tl.queued_num() * RTT;
      this_coroutine::scheduler_delegate(ctx->tlp);
      LOG_DEBUG("delegate {lower:%ld, upper:%ld}", ctx->tlp->tl.lower, ctx->tlp->tl.upper);
    }
  } else {
    // queuing failed
    ctx->rc = TxnStatus::TOC_RETRY;
  }
  ctx->self->wakeup_once();
};

void debug_read_service(Rocket::BatchIter* iter, Rocket* rkt) {
  auto req = iter->get_request<DebugRead>();
  auto reply = rkt->gen_reply<DebugReadReply>(sizeof(DebugReadReply) + req->sz, iter);
  ReadResult res;
  res.buf = reply->raw;
  res.buf_size = req->sz;

  reply->rc = global_db->get(req->id, res, LATEST, true);
  reply->sz = req->sz;
};

void debug_read_service_cb(void* _reply, void* _arg) {
  auto ctx = reinterpret_cast<DebugReadCtx*>(_arg);
  auto reply = reinterpret_cast<DebugReadReply*>(_reply);
  ctx->rc = reply->rc;
  if (reply->rc == DbStatus::OK) {
    memcpy(ctx->buf, reply->raw, reply->sz);
  } else {
    LOG_INFO("Failed %s", Status2Str(ctx->rc).c_str());
  }
};