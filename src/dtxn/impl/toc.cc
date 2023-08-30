#include "toc.h"

#include "proto/rpc.h"
#include "rrpc/rdma_cm.h"
#include "util/logging.h"

TOC::TOC(uint32_t table_num){
  obj_cache_.resize(table_num);
}

TxnStatus TOC::Read(TxnObjPtr obj) {
  if (obj->op() & TxnObj::READ) {
    return TxnStatus::OK;
  }
  obj->set_read();
  read_set_.push_back(obj);
  // issue RDMA read
  auto ctx = QueuingReadCtx{TxnStatus::INTERNAL, obj->lock_proxy, this_coroutine::current()};
  auto rkt = GetRocket(0);
  auto req = rkt->gen_request<QueuingReadReq>(sizeof(QueuingReadReq), QUEUING_READ, queuing_read_service_cb, &ctx);
  req->obj_id = obj->id();
  req->size = obj->size();
  req->table_id = obj->table_id();
  req->ts = begin_ts_;
  rkt->batching();
  this_coroutine::co_wait();
  if (ctx.rc != TxnStatus::OK) {
    return ctx.rc;
  }
  // LOG_DEBUG("[%d] obj [%ld] Read lock_info {lower:%ld, upper:%ld}", this_coroutine::current()->id(), obj->id(),
  //           obj->lock_proxy->tl.lower, obj->lock_proxy->tl.upper);
  if (!obj->lock_proxy->tl.ready()) {
    this_coroutine::co_wait();
    auto ctx = ReadReqCtx{.rc = TxnStatus::INTERNAL, .obj = obj.get(), .self = this_coroutine::current()};
    auto req = rkt->gen_request<ReadReq>(sizeof(ReadReq), READ, read_service_cb, &ctx);
    req->obj_id = obj->id();
    req->size = obj->size();
    req->table_id = obj->table_id();
    req->ts = begin_ts_;
    req->mode = Mode::HOT;
    rkt->batching();
    this_coroutine::co_wait();
  }
  return TxnStatus::OK;
};

TxnStatus TOC::Read(const std::vector<TxnObjPtr> &objs) {
  auto rkt = GetRocket(0);
  auto num = objs.size();
  QueuingReadCtx ctxs[num];
  int cnt = 0;
  for (size_t i = 0; i < num; i++) {
    auto obj = objs[i];
    if (obj->op() & TxnObj::READ) {
      continue;
    }
    obj->set_read();
    read_set_.push_back(obj);
    // issue RDMA read
    ctxs[cnt] = QueuingReadCtx{TxnStatus::INTERNAL, obj->lock_proxy, this_coroutine::current()};
    auto req =
        rkt->gen_request<QueuingReadReq>(sizeof(QueuingReadReq), QUEUING_READ, queuing_read_service_cb, &ctxs[cnt]);
    req->obj_id = obj->id();
    req->size = obj->size();
    req->table_id = obj->table_id();
    req->ts = begin_ts_;
    rkt->batching();
    cnt++;
  }
  this_coroutine::co_wait(cnt);

  int need_wait = 0;
  for (size_t i = 0; i < cnt; i++) {
    if (ctxs[i].rc == TxnStatus::TOC_RETRY) {
      return TxnStatus::TOC_RETRY;
    }
    // LOG_DEBUG("[%d] obj [%ld] Read lock_info {lower:%ld, upper:%ld}", this_coroutine::current()->id(),
    //           ctxs[i].tlp->obj->id(), ctxs[i].tlp->obj->lock_proxy->tl.lower, ctxs[i].tlp->obj->lock_proxy->tl.upper);
    if (!ctxs[i].tlp->tl.ready()) {
      need_wait++;
    }
  }
  this_coroutine::co_wait(need_wait);
  ReadReqCtx r_ctxs[num];
  for (int i = 0; i < need_wait; i++) {
    r_ctxs[i] = ReadReqCtx{.rc = TxnStatus::INTERNAL, .obj = objs[i].get(), .self = this_coroutine::current()};
    auto req = rkt->gen_request<ReadReq>(sizeof(ReadReq), READ, read_service_cb, &r_ctxs[i]);
    req->obj_id = objs[i]->id();
    req->size = objs[i]->size();
    req->table_id = objs[i]->table_id();
    req->ts = begin_ts_;
    req->mode = Mode::HOT;
    rkt->batching();
  }
  this_coroutine::co_wait(need_wait);
  return TxnStatus::OK;
};

TxnStatus TOC::Write(TxnObjPtr obj) {
  switch (obj->op()) {
    case TxnObj::INIT:
      obj->set_write();
      write_set_.push_back(obj);
      if (!obj->put_new_) lock_set_.push_back(obj);
      break;
    case TxnObj::READ:
      obj->set_write();
      write_set_.push_back(obj);
      break;
    default:
      break;
  }
  return TxnStatus::OK;
};

TxnStatus TOC::Write(const std::vector<TxnObjPtr> &objs) {
  for (auto &obj : objs) {
    Write(obj);
  }
  return TxnStatus::OK;
};

TxnStatus TOC::Commit() {
  auto rc = TxnStatus::OK;
  rc = lock();
  if (rc != TxnStatus::OK) {
    Rollback();
    return rc;
  }

  commit_ts_ = TSO::get_ts();
  rc = issue_write();
  ENSURE(rc == TxnStatus::OK, "issue write failed");

  rc = unlock();
  ENSURE(rc == TxnStatus::OK, "unlock failed");

  return TxnStatus::OK;
};

TxnStatus TOC::Commit(struct timeval& lock_start_tv,struct timeval& lock_end_tv,struct timeval& validation_start_tv,struct timeval& validation_end_tv,struct timeval& write_start_tv,struct timeval& write_end_tv,struct timeval& commit_start_tv,struct timeval& commit_end_tv)
{
  auto rc = TxnStatus::OK;
  gettimeofday(&lock_start_tv,NULL);
  rc = lock();
  gettimeofday(&lock_end_tv,NULL);
  if (rc != TxnStatus::OK) {
    Rollback();
    return rc;
  }

  commit_ts_ = TSO::get_ts();
  gettimeofday(&write_start_tv,NULL);
  rc = issue_write();
  gettimeofday(&write_end_tv,NULL);
  ENSURE(rc == TxnStatus::OK, "issue write failed");

  gettimeofday(&commit_start_tv,NULL);
  rc = unlock();
  gettimeofday(&commit_end_tv,NULL);
  ENSURE(rc == TxnStatus::OK, "unlock failed");

  return TxnStatus::OK;
}


TxnStatus TOC::lock() {
  auto rkt = GetRocket(0);
  if (!lock_set_.empty()) {
    QueuingCtx ctxs[lock_set_.size()];
    int sz = 0;
    // queueing
    for (auto obj : lock_set_) {
      ctxs[sz].obj = obj.get();
      ctxs[sz].self = this_coroutine::current();

      auto req = rkt->gen_request<QueuingReq>(sizeof(QueuingReq), QUEUING, queuing_service_cb, &ctxs[sz]);
      req->obj_id = obj->id();
      req->table_id = obj->table_id();
      req->ts = begin_ts_;
      req->mode = Mode::HOT;
      rkt->batching();
      sz++;
    }

    this_coroutine::co_wait(sz);
    // handle lock
    //int delegate_num = 0;
    std::vector<TLP> delegates;
    delegates.reserve(sz);
    bool early_abort = false;
    for (int i = 0; i < sz; i++) {
      LOG_DEBUG("[%d] obj [%ld] Read lock_info {lower:%ld, upper:%ld}", this_coroutine::current()->id(),
                ctxs[i].obj->id(), ctxs[i].obj->lock_proxy->tl.lower, ctxs[i].obj->lock_proxy->tl.upper);
      if (ctxs[i].is_queued && !ctxs[i].obj->lock_proxy->tl.ready()) {
        auto &lock_proxy = ctxs[i].obj->lock_proxy;
        lock_proxy->us_since_poll = RdtscTimer::instance().us();
        lock_proxy->hangout = RTT * lock_proxy->tl.queued_num();
        this_coroutine::scheduler_delegate(lock_proxy);
        delegates.push_back(lock_proxy);
      } else if (!ctxs[i].is_queued) {
        early_abort = true;
      } else {
        ctxs[i].obj->hold_lock = true;
      }
    }

    if (early_abort) {
      for (auto &tlp : delegates) {
        tlp->txn_coro = nullptr;
      }
      return TxnStatus::TOC_RETRY;
    }

    this_coroutine::co_wait(delegates.size());
  }
  // lock success
  return TxnStatus::OK;
};

TxnStatus TOC::issue_write() {
  auto rkt = GetRocket(0);
  auto ctxs = std::unique_ptr<WriteCtx[]>(new WriteCtx[write_set_.size()]);

  int sz = 0;
  for (auto obj : write_set_) {
    ctxs[sz] = {DbStatus::UNEXPECTED_ERROR, this_coroutine::current()};
    auto req = rkt->gen_request<WriteReq>(sizeof(WriteReq) + obj->size(), WRITE, write_service_cb, &ctxs[sz]);
    req->obj_id = obj->id();
    req->size = obj->size();
    req->table_id = obj->table_id();
    req->ts = commit_ts_;
    req->create = obj->put_new_;
    memcpy(req->data, obj->data(), obj->size());
    rkt->batching();
    sz++;
  }
  this_coroutine::co_wait(sz);

  for (int i = 0; i < sz; i++) {
    if (ctxs[i].status != DbStatus::OK) {
      return TxnStatus::INTERNAL;
    }
  }
  return TxnStatus::OK;
};

TxnStatus TOC::unlock() {
  // one side unlock
  int sz = 0;
  for (auto obj : read_set_) {
    if (obj->hold_lock) {
      obj->lock_proxy->unlock();
      sz++;
    }
  }
  for (auto obj : lock_set_) {
    if (obj->hold_lock) {
      obj->lock_proxy->unlock();
      sz++;
    }
  }
  this_coroutine::co_wait(sz);
  return TxnStatus::OK;
};

TxnStatus TOC::Rollback() { return unlock(); };