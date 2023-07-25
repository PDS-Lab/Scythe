#include <gtest/gtest.h>

#include "cmdline.h"
#include "coroutine_pool/coroutine_pool.h"
#include "benchmark_randomer.h"
#include "dtxn/dtxn.h"
#include "proto/rpc.h"
#include "rrpc/rrpc.h"
#include "storage/db.h"
#include "util/logging.h"
#include "util/rand.h"
#include "util/waitgroup.h"
using namespace std;
using benchmark::zipf_table_distribution;

struct Compare{
  int operator()(const uint64_t& a, const uint64_t& b) const { return a - b; }
};

struct TPCC_OP{
    uint64_t customer_id;
    uint64_t item_id;
    uint32_t item_num;
};

struct OpAndModeWithStatistics {
    TPCC_OP op;
    Mode mode;
    uint64_t latency;
};

enum DBType {
    TPCC_CUSTOMER_TO_DISTRICT = 0,
    TPCC_DISTRICT_TO_WAREHOUSE,
    TPCC_STOCK,
    TPCC_NEW_ORDER,
};

// key: uint64_t c_id
struct Customer {
    uint64_t d_id;   // district
};

// key: uint64_t d_id
struct District {
    uint64_t w_id;
    uint64_t next_order_id;
};

// key: uint64_t w_id(warehouse) + uint64_t i_id(item)
struct Stock {
    uint64_t item_num;
};

// key: string type c_id _ i_id _ next_order_id
struct Order {
    uint64_t item_num;
};

const uint64_t warehouse_num_ = 10;
const uint64_t item_types_    = 1000;
const uint64_t district_per_w_ = 10;
const uint64_t customer_per_d_ = 100;

KVEngine* c_to_d_db;
KVEngine* d_to_w_db;
KVEngine* stock_db;
KVEngine* new_order_db;

thread_local std::mt19937      gen_;
std::uniform_int_distribution<>* customer_id_randomer_;
zipf_table_distribution<>* item_id_randomer_;
std::uniform_int_distribution<>* item_num_randomer_;

std::string ip = "192.168.1.88";
int port = 10123;
int thread_num;
int task_num_per_thread;
int cort_per_thread;
vector<OpAndModeWithStatistics> tasks;

vector<OpAndModeWithStatistics> op_with_stat;
struct timeval starttv, endtv;

template <typename T>
std::shared_ptr<T> read_from_db(KVEngine* db, uint64_t oid) {
  T* t = new T();
  ReadResult res;
  res.buf = t;
  res.buf_size = sizeof(T);

  auto rc = db->get(oid, res, LATEST, false);
  EXPECT_EQ(rc, DbStatus::OK);
  return std::shared_ptr<T>(t);
}


template <typename T>
std::shared_ptr<T> read_from_db(uint64_t oid) {
  T* t = new T();
  ReadResult res;
  res.buf = t;
  res.buf_size = sizeof(T);

  auto rc = global_db->get(oid, res, LATEST, false);
  EXPECT_EQ(rc, DbStatus::OK);
  return std::shared_ptr<T>(t);
}

double pcalc(double rate, std::vector<uint64_t>& data) {
    size_t start_idx = (size_t)(data.size() * rate) - 1;
    // int cnt = 0;
    // uint64_t sum = 0;
    // for (int i = start_idx; i < data.size(); ++i) {
    //     sum += data[i];
    //     cnt++;
    // }
    return data[start_idx];
}

void set_random_op(TPCC_OP* op){
    op->customer_id = (*customer_id_randomer_)(gen_);
    op->item_id = (*item_id_randomer_)(gen_);
    op->item_num = (*item_num_randomer_)(gen_);
}

void TestTask(){
  auto txn = TransactionFactory::TxnBegin();
  auto list_obj = txn->GetObject(1000000, sizeof(uint64_t));
  auto rc = txn->Read(list_obj);
  EXPECT_EQ(rc, TxnStatus::OK);
  LOG_INFO("ok");
}

//./tpcc_benchmark_test -r s -a 192.168.1.88 -t 1 -c 1 -n 100
//./tpcc_benchmark_test -r c -a 192.168.1.88 -t 1 -c 1 -n 100
int main(int argc, char** argv) {
  cmdline::parser cmd;
  cmd.add<string>("role", 'r', "the role of process", true, "", cmdline::oneof<string>("c", "s"));
  cmd.add<string>("ip", 'a', "server ip address", true);
  cmd.add<int>("thread", 't', "thread num", false, 1);
  cmd.add<int>("coro", 'c', "coroutine per thread", false, 1);
  cmd.add<int>("task", 'n', "task per thread",false,100000);
  cmd.parse_check(argc, argv);

  bool server = cmd.get<string>("role") == "s";
  if (server) {
    ip = cmd.get<string>("ip");
    thread_num = cmd.get<int>("thread");

    RrpcRte::Options rte_opt;
    rte_opt.tcp_port_ = port;

    RrpcRte rte(rte_opt);
    global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size(), thread_num);
    InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
    global_db = new KVEngine();
    RegisterService();
    uint64_t val;
    global_db->put(1000000, &val, sizeof(uint64_t), TSO::get_ts());

    while (true)
      ;
  } else {
    ip = cmd.get<string>("ip");
    thread_num = cmd.get<int>("thread");
    cort_per_thread = cmd.get<int>("coro");

    RrpcRte::Options rte_opt;
    RrpcRte rte(rte_opt);
    global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size());
    std::unordered_map<node_id, RdmaCM::Iport> config;
    config[0] = {ip, port};
    global_cm->manual_set_network_config(config);

    InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());

    CoroutinePool pool(thread_num, cort_per_thread);
    pool.start();

    gettimeofday(&starttv, NULL);

    {
      WaitGroup wg(1);
      pool.enqueue([&wg]() {
        TestTask();
        wg.Done();
      });
      wg.Wait();
    }

    gettimeofday(&endtv, NULL);
    LOG_INFO("PASS");
    LOG_INFO("tasks: %zu",op_with_stat.size());
    uint64_t tot = ((endtv.tv_sec - starttv.tv_sec) * 1000000 + endtv.tv_usec - starttv.tv_usec) >> 1;
        printf("============================ Throughput:%lf MOPS =========================\n", 
                1.0 / tot);
    DestroyMemPool();
    }
}