/**
Smallbank根据FORD修改而来
**/

#pragma once

#include <gtest/gtest.h>
#include <cassert>
#include <cstdint>
#include <vector>
#include "storage/db.h"

#include "util/logging.h"
#include "config/benchmark_randomer.h"

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK 15

#define TX_HOT 90 /* Percentage of txns that use accounts from hotspot */
using benchmark::FastRand;
using benchmark::zipf_table_distribution;
union smallbank_savings_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_savings_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_savings_key_t) == sizeof(uint64_t), "");

struct smallbank_savings_val_t {
  uint32_t magic;
  float bal;
};

static_assert(sizeof(smallbank_savings_val_t) == sizeof(uint64_t), "");

union smallbank_checking_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_checking_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_checking_key_t) == sizeof(uint64_t), "");

struct smallbank_checking_val_t {
  uint32_t magic;
  float bal;
};
static_assert(sizeof(smallbank_checking_val_t) == sizeof(uint64_t), "");


// Magic numbers for debugging. These are unused in the spec.
#define SmallBank_MAGIC 97 /* Some magic number <= 255 */
#define smallbank_savings_magic (SmallBank_MAGIC)
#define smallbank_checking_magic (SmallBank_MAGIC + 1)

// Helpers for generating workload
#define SmallBank_TX_TYPES 6
enum class SmallBankTxType : int {
  kAmalgamate,
  kBalance,
  kDepositChecking,
  kSendPayment,
  kTransactSaving,
  kWriteCheck,
};

struct TxTypeWithOp{
  SmallBankTxType TxType;
  uint64_t acct_id_0;
  uint64_t acct_id_1;
};

const std::string SmallBank_TX_NAME[SmallBank_TX_TYPES] = {"Amalgamate", "Balance", "DepositChecking", \
"SendPayment", "TransactSaving", "WriteCheck"};

// Table id
enum class SmallBankTableType : uint64_t {
  kSavingsTable = 1,
  kCheckingTable,
  TableNum
};

class SmallBank{
public:
    std::string bench_name_;

    uint32_t total_thread_num_;

    uint32_t num_accounts_global_=100000;

    KVEngine* savings_table_=nullptr;

    KVEngine* checking_table_=nullptr;

    SmallBankTxType workgen_arr_[100];

    TxTypeWithOp* workload_arr_=nullptr;

    zipf_table_distribution<>* zipf = nullptr;

    SmallBank(){
        bench_name_ = "SmallBank";
        // num_accounts_global = conf.get("num_accounts").get_uint64();
        // num_hot_global = conf.get("num_hot_accounts").get_uint64();

        // /* Up to 2 billion accounts */
        // assert(num_accounts_global <= 2ull * 1024 * 1024 * 1024);
    }
    ~SmallBank() {
        if(savings_table_) delete savings_table_;
        if(checking_table_) delete checking_table_;
        if(workload_arr_) delete[] workload_arr_;
    }
    /*
   * Generators for new account IDs. Called once per transaction because
   * we need to decide hot-or-not per transaction, not per account.
   */
    
    void CreateWorkgenArray(int write_ratio);
    void CreateWorkLoad(int op_num, uint64_t range, double exponent);
    void LoadTable();
    void PopulateSavingsTable();
    void PopulateCheckingTable();
    void get_account(uint64_t* acct_id);
    void get_two_accounts(uint64_t* acct_id_0, uint64_t* acct_id_1);
};