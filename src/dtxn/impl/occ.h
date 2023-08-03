#pragma once
#include "common.h"
#include "dtxn/dtxn.h"
#include "dtxn/tso.h"

class OCC : public Transaction {
  friend class TransactionFactory;

 public:
  TxnStatus Read(TxnObjPtr obj) override;
  TxnStatus Read(const std::vector<TxnObjPtr> &objs) override;
  TxnStatus Write(TxnObjPtr obj) override;
  TxnStatus Write(const std::vector<TxnObjPtr> &objs) override;

  TxnStatus Commit() override;
  TxnStatus Commit(struct timeval& lock_start_tv,struct timeval& lock_end_tv,struct timeval& validation_start_tv,struct timeval& validation_end_tv,struct timeval& write_start_tv,struct timeval& write_end_tv,struct timeval& commit_start_tv,struct timeval& commit_end_tv) override;
  TxnStatus Rollback() override;

 private:
  OCC(uint32_t table_num);
  TxnStatus validate();
  TxnStatus lock();
  TxnStatus issue_write();
  TxnStatus unlock();

 private:
  std::list<TxnObjPtr> read_set_;
  std::list<TxnObjPtr> write_set_;
};