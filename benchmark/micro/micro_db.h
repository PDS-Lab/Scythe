// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include "storage/db.h"
#include "util/logging.h"
#include "common/benchmark_randomer.h"
#include "common/types.h"

using namespace benchmark;

union micro_key_t {
  uint64_t micro_id;
  uint64_t item_key;

  micro_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(micro_key_t) == sizeof(uint64_t), "");

#define MICRO_VAL_SIZE 5
struct micro_val_t {
  // 40 bytes, consistent with FaSST
  uint64_t magic[MICRO_VAL_SIZE];
};
static_assert(sizeof(micro_val_t) == 8*MICRO_VAL_SIZE, "");

// Magic numbers for debugging. These are unused in the spec.
#define Micro_MAGIC 97 /* Some magic number <= 255 */
#define micro_magic (Micro_MAGIC)

// Table id
enum class MicroTableType : uint64_t {
  kMicroTable = 1,
  TableNum,
};

#define MICRO_TX_TYPES 4
enum class MICROTxType : int {
    kRead=0,
    kInsert,
    kUpdate,
    kDelete,
};

struct MICROTxTypeWithOp{
    MICROTxType TxType;
    uint64_t micro_id;
};

const std::string MICRO_TX_NAME[MICRO_TX_TYPES] = {"Read", "Write", "Scan", "Delete"};

class MICRO {
public:
    std::string bench_name_;
    uint64_t num_keys_global_;
    /* Tables */
    KVEngine* micro_table_;

    MICROTxType workgen_arr_[100];

    MICROTxTypeWithOp* workload_arr_=nullptr;

    zipf_table_distribution<>* zipf = nullptr;

    // For server usage: Provide interfaces to servers for loading tables
    // Also for client usage: Provide interfaces to clients for generating ids during tests
    MICRO(uint64_t num_keys=1000000) {
        bench_name_ = "MICRO";
        num_keys_global_ = num_keys;
        micro_table_ = nullptr;
    }

    ~MICRO() {
        if (micro_table_) delete micro_table_;
        if (workload_arr_) delete[] workload_arr_;
    }

    void LoadTable();
    void get_test_key(uint64_t* test_key, bool is_skewed);
    void CreateWorkgenArray(int read_ratio, int write_ratio,int scan_ratio, int delete_ratio);
    void CreateWorkLoad(bool is_skewed, int op_num, uint64_t range, double exponent);
};
