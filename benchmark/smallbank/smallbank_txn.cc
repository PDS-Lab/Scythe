#include "smallbank/smallbank_txn.h"
#include "coroutine_pool/coroutine_pool.h"
#include <string>
using std::string;

TxnStatus TxAmalgamate(SmallBank* smallbank, Mode mode, PhasedLatency* phased_lat){
    auto txn = TransactionFactory::TxnBegin(mode, (uint32_t)SmallBankTableType::TableNum);
    uint64_t acct_id_0, acct_id_1;
    smallbank->get_two_accounts(&acct_id_0, &acct_id_1);
    //LOG_INFO("acct0:%lu, acct1:%lu",acct_id_0,acct_id_1);
    std::vector<TxnObjPtr> read_set;

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
    }
    
    smallbank_savings_key_t sav_key_0;
    sav_key_0.acct_id = acct_id_0;
    auto sav_obj_0 = txn->GetObject(sav_key_0.item_key,(uint32_t)SmallBankTableType::kSavingsTable,sizeof(smallbank_savings_val_t));
    read_set.push_back(sav_obj_0);

    smallbank_checking_key_t chk_key_0;
    chk_key_0.acct_id = acct_id_0;
    auto chk_obj_0 = txn->GetObject(chk_key_0.item_key,(uint32_t)SmallBankTableType::kCheckingTable,sizeof(smallbank_checking_val_t));
    read_set.push_back(chk_obj_0);

    /* Read from checking account for acct_id_1 */
    smallbank_checking_key_t chk_key_1;
    chk_key_1.acct_id = acct_id_1;
    auto chk_obj_1 = txn->GetObject(chk_key_1.item_key,(uint32_t)SmallBankTableType::kCheckingTable,sizeof(smallbank_checking_val_t));
    read_set.push_back(chk_obj_1);

    txn->Read(read_set);

    //if (!dtx->TxExe(yield)) return false;
    smallbank_savings_val_t* sav_val_0 = sav_obj_0->get_as<smallbank_savings_val_t>();
    smallbank_checking_val_t* chk_val_0 = chk_obj_0->get_as<smallbank_checking_val_t>();
    smallbank_checking_val_t* chk_val_1 = chk_obj_1->get_as<smallbank_checking_val_t>();
    
    chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

    sav_val_0->bal = 0;
    chk_val_0->bal = 0;
    txn->Write(sav_obj_0);
    txn->Write(chk_obj_0);
    txn->Write(chk_obj_1);
    if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
    }
    return txn->Commit();
}
/* Calculate the sum of saving and checking kBalance */
TxnStatus TxBalance(SmallBank* smallbank, Mode mode, PhasedLatency* phased_lat){
    auto txn = TransactionFactory::TxnBegin(mode, (uint32_t)SmallBankTableType::TableNum);
    uint64_t acct_id;
    //LOG_INFO("acct0:%lu",acct_id);
    if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
    }
    smallbank->get_account(&acct_id);
    smallbank_savings_key_t sav_key;
    sav_key.acct_id = acct_id;
    smallbank_checking_key_t chk_key;
    chk_key.acct_id = acct_id;
    std::vector<TxnObjPtr> read_set;

    auto sav_obj = txn->GetObject(sav_key.item_key,(uint32_t)SmallBankTableType::kSavingsTable,sizeof(smallbank_savings_val_t));
    read_set.push_back(sav_obj);
    auto chk_obj = txn->GetObject(chk_key.item_key,(uint32_t)SmallBankTableType::kCheckingTable,sizeof(smallbank_checking_val_t));
    read_set.push_back(chk_obj);
    txn->Read(read_set);
    
    //if (!dtx->TxExe(yield)) return false;
    smallbank_savings_val_t* sav_val = sav_obj->get_as<smallbank_savings_val_t>();
    smallbank_checking_val_t* chk_val = chk_obj->get_as<smallbank_checking_val_t>();

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
    }

    return txn->Commit();
}
/* Add $1.3 to acct_id's checking account */
TxnStatus TxDepositChecking(SmallBank* smallbank, Mode mode,  PhasedLatency* phased_lat){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)SmallBankTableType::TableNum);
    uint64_t acct_id;
    smallbank->get_account(&acct_id);
    //LOG_INFO("acct0:%lu",acct_id);
    float amount = 1.3;

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
    }

    /* Read from checking table */
    smallbank_checking_key_t chk_key;
    chk_key.acct_id = acct_id;
    auto chk_obj = txn->GetObject(chk_key.item_key,(uint32_t)SmallBankTableType::kCheckingTable,sizeof(smallbank_checking_val_t));
    txn->Read(chk_obj);

    //if (!dtx->TxExe(yield)) return false;

    /* If we are here, execution succeeded and we have a lock*/
    smallbank_checking_val_t* chk_val = chk_obj->get_as<smallbank_checking_val_t>();
    // if (chk_val->magic != smallbank_checking_magic) {
    //     RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    // }
    // assert(chk_val->magic == smallbank_checking_magic);

    chk_val->bal += amount; /* Update checking kBalance */
    txn->Write(chk_obj);

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
    }

    return txn->Commit();
}
/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
TxnStatus TxSendPayment(SmallBank* smallbank, Mode mode, PhasedLatency* phased_lat){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)SmallBankTableType::TableNum);
    uint64_t acct_id_0, acct_id_1;
    smallbank->get_two_accounts(&acct_id_0, &acct_id_1);
    //LOG_INFO("acct0:%lu, acct1:%lu",acct_id_0,acct_id_1);
    float amount = 5.0;
    std::vector<TxnObjPtr> read_set;

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
    }

    /* Read from checking table */
    smallbank_checking_key_t chk_key_0;
    chk_key_0.acct_id = acct_id_0;
    auto chk_obj_0 = txn->GetObject(chk_key_0.item_key,(uint32_t)SmallBankTableType::kCheckingTable,sizeof(smallbank_checking_val_t));
    read_set.push_back(chk_obj_0);

    smallbank_checking_key_t chk_key_1;
    chk_key_1.acct_id = acct_id_1;
    auto chk_obj_1 = txn->GetObject(chk_key_1.item_key,(uint32_t)SmallBankTableType::kCheckingTable,sizeof(smallbank_checking_val_t));
    read_set.push_back(chk_obj_1);
    txn->Read(read_set);

    //if (!dtx->TxExe(yield)) return false;
    smallbank_checking_val_t* chk_val_0 = chk_obj_0->get_as<smallbank_checking_val_t>();
    smallbank_checking_val_t* chk_val_1 = chk_obj_1->get_as<smallbank_checking_val_t>();
    // if (chk_val_0->magic != smallbank_checking_magic) {
    //     RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    // }
    // if (chk_val_1->magic != smallbank_checking_magic) {
    //     RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    // }
    if (chk_val_0->bal < amount) {
        return txn->Rollback();
    }

    chk_val_0->bal -= amount; /* Debit */
    chk_val_1->bal += amount; /* Credit */
    txn->Write(chk_obj_0);
    txn->Write(chk_obj_1);

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
    }

    return txn->Commit();
}
/* Add $20 to acct_id's saving's account */
TxnStatus TxTransactSaving(SmallBank* smallbank, Mode mode, PhasedLatency* phased_lat){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)SmallBankTableType::TableNum);
    uint64_t acct_id;
    smallbank->get_account(&acct_id);
    //LOG_INFO("acct0:%lu",acct_id);

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
    }

    float amount = 20.20;
    smallbank_savings_key_t sav_key;
    sav_key.acct_id = acct_id;
    auto sav_obj = txn->GetObject(sav_key.item_key,(uint32_t)SmallBankTableType::kSavingsTable,sizeof(smallbank_savings_val_t));
    txn->Read(sav_obj);
    //if (!dtx->TxExe(yield)) return false;
    /* If we are here, execution succeeded and we have a lock */
    smallbank_savings_val_t* sav_val = sav_obj->get_as<smallbank_savings_val_t>();
    // if (sav_val->magic != smallbank_savings_magic) {
    //     RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    // }
    // assert(sav_val->magic == smallbank_savings_magic);

    sav_val->bal += amount; /* Update saving kBalance */
    txn->Write(sav_obj);

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
    }

    return txn->Commit();
}
/* Read saving and checking kBalance + update checking kBalance unconditionally */
TxnStatus TxWriteCheck(SmallBank* smallbank, Mode mode, PhasedLatency* phased_lat){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)SmallBankTableType::TableNum);
    uint64_t acct_id;
    smallbank->get_account(&acct_id);
    //LOG_INFO("acct0:%lu",acct_id);

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_start_tv),nullptr);
    }

    float amount = 5.0;
    smallbank_savings_key_t sav_key;
    sav_key.acct_id = acct_id;
    auto sav_obj = txn->GetObject(sav_key.item_key,(uint32_t)SmallBankTableType::kSavingsTable,sizeof(smallbank_savings_val_t));
    txn->Read(sav_obj);

    smallbank_checking_key_t chk_key;
    chk_key.acct_id = acct_id;
    auto chk_obj = txn->GetObject(chk_key.item_key,(uint32_t)SmallBankTableType::kCheckingTable,sizeof(smallbank_checking_val_t));
    txn->Read(chk_obj);

    //if (!dtx->TxExe(yield)) return false;
    smallbank_savings_val_t* sav_val = sav_obj->get_as<smallbank_savings_val_t>();
    smallbank_checking_val_t* chk_val = chk_obj->get_as<smallbank_checking_val_t>();
    // if (sav_val->magic != smallbank_savings_magic) {
    //     RDMA_LOG(INFO) << "read value: " << sav_val;
    //     RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    // }
    // if (chk_val->magic != smallbank_checking_magic) {
    //     RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    // }
    if (sav_val->bal + chk_val->bal < amount) {
        chk_val->bal -= (amount + 1);
    } else {
        chk_val->bal -= amount;
    }
    txn->Write(chk_obj);

    if(phased_lat){
        gettimeofday(&(phased_lat->exe_end_tv),nullptr);
        return txn->Commit(phased_lat->lock_start_tv,phased_lat->lock_end_tv,phased_lat->vali_start_tv,
                            phased_lat->vali_end_tv,phased_lat->write_start_tv,phased_lat->write_end_tv,
                            phased_lat->commit_start_tv,phased_lat->commit_end_tv
        );
    }

    return txn->Commit();
}

TxnStatus SbTxTestReadWrite(){
  TxnStatus rc = TxnStatus::OK;
  do{
    auto txn = TransactionFactory::TxnBegin(Mode::COLD,(uint32_t)SmallBankTableType::TableNum);
    int32_t acct_id = 1;
    smallbank_savings_key_t saving_key;
    saving_key.acct_id = acct_id;
    auto saving_obj = txn->GetObject(saving_key.item_key,static_cast<uint32_t>(SmallBankTableType::kSavingsTable),sizeof(smallbank_savings_val_t));
    txn->Read(saving_obj);
    auto saving_val = saving_obj->get_as<smallbank_savings_val_t>();
    auto check(saving_val->magic);
    //LOG_INFO("magic: %u",check);
    LOG_ASSERT(check==SmallBank_MAGIC,"unexpected magic, %u", check);
    saving_val->magic = 255;
    txn->Write(saving_obj);
    rc = txn->Commit();
    if(rc!=TxnStatus::OK){
      LOG_DEBUG("txn retry");
    }
  }while(rc!=TxnStatus::OK);
  auto new_txn = TransactionFactory::TxnBegin();
  int32_t new_acct_id = 1;
  smallbank_savings_key_t new_saving_key;
  new_saving_key.acct_id = new_acct_id;
  auto new_saving_obj = new_txn->GetObject(new_saving_key.item_key,static_cast<uint32_t>(SmallBankTableType::kSavingsTable),sizeof(smallbank_savings_val_t));
  new_txn->Read(new_saving_obj);
  auto new_saving_val = new_saving_obj->get_as<smallbank_savings_val_t>();
  LOG_DEBUG("now magic:%d",new_saving_val->magic);
  rc = new_txn->Commit();
  return rc;
}