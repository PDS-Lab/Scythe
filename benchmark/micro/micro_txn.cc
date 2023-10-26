#include "micro/micro_txn.h"
#include "coroutine_pool/coroutine_pool.h"
#include <string>
using std::string;

TxnStatus TxRead(MICRO* micro, Mode mode, bool is_skewed, int index){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)MicroTableType::TableNum);
    auto op = micro->workload_arr_[index];
    LOG_ASSERT(op.TxType == MICROTxType::kRead,"Unexpected txn type");
    micro_key_t micro_key;
    micro_key.micro_id = op.micro_id;
    auto micro_obj = txn->GetObject(micro_key.item_key,(uint32_t)MicroTableType::kMicroTable,sizeof(micro_val_t));
    auto rc = txn->Read(micro_obj);
    if(rc!=TxnStatus::OK){
      txn->Rollback();
      return rc;
    }
    return txn->Commit();
}

TxnStatus TxInsert(MICRO* micro, Mode mode, bool is_skewed, int index){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)MicroTableType::TableNum);
    auto op = micro->workload_arr_[index];
    LOG_ASSERT(op.TxType == MICROTxType::kInsert,"Unexpected txn type");
    micro_key_t micro_key;
    micro->get_test_key(&micro_key.item_key,is_skewed);
    auto micro_obj = txn->GetObject(micro_key.item_key,(uint32_t)MicroTableType::kMicroTable,sizeof(micro_val_t));
    micro_obj->set_new();
    auto micro_val = micro_obj->get_as<micro_val_t>();
    for(int i=0;i<MICRO_VAL_SIZE;i++){
        micro_val->magic[i] = (Micro_MAGIC + i); 
    }
    txn->Write(micro_obj);
    return txn->Commit();
}

//ReadModifyWrite
TxnStatus TxUpdate(MICRO* micro, Mode mode, bool is_skewed, int index){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)MicroTableType::TableNum);
    auto op = micro->workload_arr_[index];
    LOG_ASSERT(op.TxType == MICROTxType::kUpdate,"Unexpected txn type");
    micro_key_t micro_key;
    micro_key.micro_id = op.micro_id;
    auto micro_obj = txn->GetObject(micro_key.item_key,(uint32_t)MicroTableType::kMicroTable,sizeof(micro_val_t));
    auto rc = txn->Read(micro_obj);
    if(rc!=TxnStatus::OK){
      txn->Rollback();
      return rc;
    }
    auto micro_val = micro_obj->get_as<micro_val_t>();
    for(int i=0;i<MICRO_VAL_SIZE;i++){
        micro_val->magic[i] += 1; 
    }
    txn->Write(micro_obj);
    return txn->Commit();
}

TxnStatus TxDelete(MICRO* micro, Mode mode, bool is_skewed, int index){
    auto txn = TransactionFactory::TxnBegin(mode,(uint32_t)MicroTableType::TableNum);
    auto op = micro->workload_arr_[index];
    LOG_ASSERT(op.TxType == MICROTxType::kDelete,"Unexpected txn type");
    micro_key_t micro_key;
    micro_key.micro_id = op.micro_id;
    auto micro_obj = txn->GetObject(micro_key.item_key,(uint32_t)MicroTableType::kMicroTable,sizeof(micro_val_t));
    auto micro_val = micro_obj->get_as<micro_val_t>();
    for(int i=0;i<MICRO_VAL_SIZE;i++){
        micro_val->magic[i] = 0; 
    }
    txn->Write(micro_obj);
    return txn->Commit();
}
