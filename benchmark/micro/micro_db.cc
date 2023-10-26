#include "micro/micro_db.h"
#include "dtxn/dtxn.h"
#include "unistd.h"
std::random_device micro_rd;
std::mt19937 micro_gen(micro_rd());

void MICRO::CreateWorkgenArray(int read_ratio, int write_ratio,int update_ratio, int delete_ratio){
    LOG_ASSERT(read_ratio+write_ratio+update_ratio+delete_ratio==100,"Sum of txn ratio is unequal to 100");
    int i=0,j=0;
    j+=read_ratio;
    for (; i < j; i++) workgen_arr_[i]=MICROTxType::kRead;
    j+=write_ratio;
    for (; i < j; i++) workgen_arr_[i]=MICROTxType::kInsert;
    j+=update_ratio;
    for (; i < j; i++) workgen_arr_[i]=MICROTxType::kUpdate;
    j+=delete_ratio;
    for (; i < j; i++) workgen_arr_[i]=MICROTxType::kDelete;
    assert(i == 100 && j == 100);
}

void MICRO::get_test_key(uint64_t* test_key, bool is_skewed){
    if(is_skewed){
        *test_key = (*zipf)(micro_gen) - 1;
    }else{
        *test_key = FastRand(test_key) % num_keys_global_;
    }
}

void MICRO::CreateWorkLoad(bool is_skewed, int op_num, uint64_t range, double exponent){
    workload_arr_ = new MICROTxTypeWithOp[op_num];
    uint64_t seed;
    if(is_skewed){
        zipf = new zipf_table_distribution<>(range,exponent);
    }
    int cnt = 0;
    for(cnt = 0;cnt < op_num;cnt++){
        MICROTxType tx_type = workgen_arr_[FastRand(&seed)%100];
        uint64_t test_key;
        get_test_key(&test_key,is_skewed);
        workload_arr_[cnt].TxType = tx_type;
        workload_arr_[cnt].micro_id = test_key;
    }
    assert(cnt == op_num);
}

/* Called by main. Only initialize here. The worker threads will populate. */
void MICRO::LoadTable() {
  // Initiate + Populate table for primary role
  LOG_INFO("Begin to populate micro table");
  micro_table_ = new KVEngine();
  dbs[(size_t)MicroTableType::kMicroTable] = micro_table_;
  for (uint64_t id = 0; id < num_keys_global_; id++) {
    micro_key_t micro_key;
    micro_key.micro_id = (uint64_t)id;
    micro_val_t micro_val;
    for (int i = 0; i < MICRO_VAL_SIZE; i++) {
      micro_val.magic[i] = micro_magic + i;
    }

    micro_table_->put(micro_key.item_key,&micro_val,sizeof(micro_val),TSO::get_ts());
  }
  LOG_INFO("Finish to populate micro table");
}
