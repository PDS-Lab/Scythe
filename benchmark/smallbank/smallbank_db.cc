#include "smallbank/smallbank_db.h"
#include "dtxn/dtxn.h"
#include "unistd.h"

std::random_device rd;
std::mt19937 gen(rd());

void SmallBank::get_account(uint64_t* acct_id){
  LOG_ASSERT(zipf!=nullptr,"zipf hasn't been initialized");
  *acct_id = (*zipf)(gen)-1;
}

void SmallBank::get_two_accounts(uint64_t* acct_id_0, uint64_t* acct_id_1){
    *acct_id_0 = (*zipf)(gen)-1;
    *acct_id_1 = (*zipf)(gen)-1;
    while (*acct_id_1 == *acct_id_0) {
        *acct_id_1 = (*zipf)(gen)-1;
    }
}

void SmallBank::CreateWorkgenArray(int write_ratio){
    LOG_ASSERT(write_ratio >= 0&&write_ratio <=100&&write_ratio%5==0,"write_ratio should be [0-100] and divisible by 5.");
    int i=0,j=0;
    j += write_ratio/5;
    for (; i < j; i++) workgen_arr_[i] = SmallBankTxType::kAmalgamate;

    j += write_ratio/5;
    for (; i < j; i++) workgen_arr_[i] = SmallBankTxType::kDepositChecking;

    j += write_ratio/5;
    for (; i < j; i++) workgen_arr_[i] = SmallBankTxType::kSendPayment;

    j += write_ratio/5;
    for (; i < j; i++) workgen_arr_[i] = SmallBankTxType::kTransactSaving;

    j += write_ratio/5;
    for (; i < j; i++) workgen_arr_[i] = SmallBankTxType::kWriteCheck;

    //read-only txn
    j += 100 - write_ratio;
    for (; i < j; i++) workgen_arr_[i] = SmallBankTxType::kBalance;

    assert(i == 100 && j == 100);
}

void SmallBank::CreateWorkLoad(int op_num, uint64_t range, double exponent){
    //should call CreateWorkGenArr first
    workload_arr_ = new SmallBankTxTypeWithOp[op_num];
    uint64_t seed;
    zipf = new zipf_table_distribution<>(range,exponent);
    int cnt = 0;
    for(cnt = 0;cnt < op_num; cnt++){
        SmallBankTxType tx_type = workgen_arr_[FastRand(&seed)%100];
        uint64_t acct_id_0=0, acct_id_1=0;
        get_two_accounts(&acct_id_0,&acct_id_1);
        switch(tx_type){
            case SmallBankTxType::kAmalgamate:
                //2 accounts
                workload_arr_[cnt] = SmallBankTxTypeWithOp{
                    .TxType = SmallBankTxType::kAmalgamate,
                    .acct_id_0 = acct_id_0,
                    .acct_id_1 = acct_id_1,
                };
                break;
            case SmallBankTxType::kBalance:
                //1 account
                workload_arr_[cnt] = SmallBankTxTypeWithOp{
                    .TxType = SmallBankTxType::kBalance,
                    .acct_id_0 = acct_id_0,
                    .acct_id_1 = (unsigned long)-1,
                };
                break;
            case SmallBankTxType::kDepositChecking:
                //1 account
                workload_arr_[cnt] = SmallBankTxTypeWithOp{
                    .TxType = SmallBankTxType::kDepositChecking,
                    .acct_id_0 = acct_id_0,
                    .acct_id_1 = (unsigned long)-1,
                };
                break;
            case SmallBankTxType::kSendPayment:
                //2 accounts
                workload_arr_[cnt] = SmallBankTxTypeWithOp{
                    .TxType = SmallBankTxType::kSendPayment,
                    .acct_id_0 = acct_id_0,
                    .acct_id_1 = acct_id_1,
                };
                break;
            case SmallBankTxType::kTransactSaving:
                //1 account
                workload_arr_[cnt] = SmallBankTxTypeWithOp{
                    .TxType = SmallBankTxType::kTransactSaving,
                    .acct_id_0 = acct_id_0,
                    .acct_id_1 = (unsigned long)-1,
                };
                break;
            case SmallBankTxType::kWriteCheck:
                //1 account
                workload_arr_[cnt] = SmallBankTxTypeWithOp{
                    .TxType = SmallBankTxType::kWriteCheck,
                    .acct_id_0 = acct_id_0,
                    .acct_id_1 = (unsigned long)-1,
                };
                break;
            default:
                LOG_FATAL("generate workload, Txn type doesn't exist");
                exit(-1);
        }
    }
    assert(cnt == op_num);
    
}

void SmallBank::LoadTable(){
    LOG_INFO("Begin to populate savings table");
    savings_table_ = new KVEngine();
    dbs[(size_t)SmallBankTableType::kSavingsTable] = savings_table_;
    PopulateSavingsTable();
    LOG_INFO("Finish to populate savings table");

    LOG_INFO("Begin to populate checking table");
    checking_table_ = new KVEngine();
    dbs[(size_t)SmallBankTableType::kCheckingTable] = checking_table_;
    PopulateCheckingTable();
    LOG_INFO("Finish to populate checking table");
}

void SmallBank::PopulateSavingsTable(){
    /* Populate the tables */
    for (uint32_t acct_id = 0; acct_id < num_accounts_global_; acct_id++) {
        // Savings
        smallbank_savings_key_t savings_key;
        savings_key.acct_id = (uint64_t)acct_id;

        smallbank_savings_val_t savings_val;
        savings_val.magic = smallbank_savings_magic;
        savings_val.bal = 1000ull;

        savings_table_->put(savings_key.item_key,&savings_val,sizeof(savings_val),TSO::get_ts());
  }
}

void SmallBank::PopulateCheckingTable(){
    for (uint32_t acct_id = 0; acct_id < num_accounts_global_; acct_id++) {
        // Checking
        smallbank_checking_key_t checking_key;
        checking_key.acct_id = (uint64_t)acct_id;

        smallbank_checking_val_t checking_val;
        checking_val.magic = smallbank_checking_magic;
        checking_val.bal = 1000ull;

        checking_table_->put(checking_key.item_key,&checking_val,sizeof(checking_val),TSO::get_ts());
  }
}