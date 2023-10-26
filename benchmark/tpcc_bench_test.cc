// #include <gtest/gtest.h>

// #include "cmdline.h"
// #include "coroutine_pool/coroutine_pool.h"
// #include "common/benchmark_randomer.h"
// #include "dtxn/dtxn.h"
// #include "proto/rpc.h"
// #include "rrpc/rrpc.h"
// #include "storage/db.h"
// #include "util/logging.h"
// #include "util/rand.h"
// #include "util/waitgroup.h"

// #define COROUTINE_SIZE 8

// using namespace std;
// using benchmark::zipf_table_distribution;

// struct Compare{
//   int operator()(const uint64_t& a, const uint64_t& b) const { return a - b; }
// };

// struct TPCC_OP{
//     uint64_t customer_id;
//     uint64_t item_id;
//     uint64_t item_num;
// };

// struct OpAndModeWithStatistics {
//     TPCC_OP op;
//     Mode mode;
//     uint64_t latency;
// };

// enum DBType {
//     TPCC_CUSTOMER_TO_DISTRICT = 0,
//     TPCC_DISTRICT_TO_WAREHOUSE,
//     TPCC_STOCK,
//     TPCC_NEW_ORDER,
// };

// // key: uint64_t c_id
// struct Customer {
//     uint64_t d_id;   // district
// };

// // key: uint64_t d_id
// struct District {
//     uint64_t w_id;
//     uint64_t next_order_id;
// };

// // key: uint64_t w_id(warehouse) + uint64_t i_id(item)
// struct Stock {
//     uint64_t item_num;
// };

// // key: string type c_id _ i_id _ next_order_id
// struct Order {
//     uint64_t item_num;
// };

// const uint64_t warehouse_num_ = 10;
// const uint64_t item_types_    = 100000;
// const uint64_t district_per_w_ = 10;
// const uint64_t customer_per_d_ = 300;

// typedef uint64_t db_index;

// const db_index TestKey = ~0;


// thread_local std::mt19937_64      gen_;
// std::uniform_int_distribution<>* customer_id_randomer_;
// zipf_table_distribution<>* item_id_randomer_;
// std::uniform_int_distribution<>* item_num_randomer_;

// std::string ip = "192.168.1.11";
// int port = 10123;
// int thread_num;
// int task_num;
// int cort_per_thread;
// vector<OpAndModeWithStatistics> tasks;

// vector<OpAndModeWithStatistics> op_with_stat;
// struct timeval starttv, endtv;

// template <typename T>
// std::shared_ptr<T> read_from_db(KVEngine* db, uint64_t oid) {
//   T* t = new T();
//   ReadResult res;
//   res.buf = t;
//   res.buf_size = sizeof(T);

//   auto rc = db->get(oid, res, LATEST, false);
//   EXPECT_EQ(rc, DbStatus::OK);
//   return std::shared_ptr<T>(t);
// }

// inline uint64_t c_to_d_wrapper(uint64_t c_id){
//   return c_to_d_db_index | c_id;
// }

// inline uint64_t d_to_w_wrapper(uint64_t d_id){
//   return d_to_w_db_index | d_id;
// }

// inline uint64_t stock_wrapper(uint64_t w_id,uint64_t i_id){
//   return stock_db_index | (w_id << (uint64_t)32) | i_id;
// }

// inline uint64_t new_order_wrapper(uint64_t c_id, uint64_t i_id, uint64_t next_order_id){
//   return new_order_db_index | (c_id << (uint64_t)40) | (i_id << (uint64_t)20) | (next_order_id & 0xfffffffff);
// }

// double pcalc(double rate, std::vector<uint64_t>& data) {
//     size_t start_idx = (size_t)(data.size() * rate) - 1;
//     // int cnt = 0;
//     // uint64_t sum = 0;
//     // for (int i = start_idx; i < data.size(); ++i) {
//     //     sum += data[i];
//     //     cnt++;
//     // }
//     return data[start_idx];
// }

// void set_random_op(TPCC_OP* op){
//     op->customer_id = (*customer_id_randomer_)(gen_);
//     op->item_id = (*item_id_randomer_)(gen_);
//     op->item_num = (*item_num_randomer_)(gen_);
// }

// void TPCCtask(OpAndModeWithStatistics& op){
//     struct timeval starttv, endtv;
//     uint64_t c_id =  op.op.customer_id;
//     uint64_t i_id =  op.op.item_id;
//     uint64_t i_num = op.op.item_num;
//     LOG_DEBUG("[%d] op_cid:%lx, op_iid:%lx,op_i_num:%ld ", this_coroutine::current()->id(),c_id,i_id,i_num);
//     TxnStatus rc = TxnStatus::OK;
//     Mode mode = Mode::COLD;
//     gettimeofday(&starttv, NULL);
//     do{
//       rc = TxnStatus::OK;
//       mode = (rc == TxnStatus::SWITCH) ? Mode::HOT : Mode::COLD;
//       auto txn = TransactionFactory::TxnBegin(mode);
//       // 1. 通过c_id(Customer)拿到d_id(District)
//       auto c_id_obj = txn->GetObject(c_to_d_wrapper(c_id),0, sizeof(Customer));
//       rc = txn->Read(c_id_obj);
//       if(rc!=TxnStatus::OK){
//           LOG_DEBUG("[%d] Read d_id with c_id failed, c_id_key:%lx, rc %d",this_coroutine::current()->id(),c_to_d_wrapper(c_id),(int)rc);
//           txn->Rollback();
//           continue;
//       }
//       Customer* customer = c_id_obj->get_as<Customer>();
//       ASSERT_NE(customer, nullptr);
//       uint64_t d_id = customer->d_id;
//       LOG_DEBUG("[%d] Read d_id with c_id succeeded, c_id_key:%lx, d_id %lx",this_coroutine::current()->id(),c_to_d_wrapper(c_id),customer->d_id);
//       // 2. 通过d_id拿到供货的w_id(Warehouse)
//       auto d_id_obj = txn->GetObject(d_to_w_wrapper(d_id),0, sizeof(District));
//       rc = txn->Read(d_id_obj);
//       if(rc!=TxnStatus::OK){
//           LOG_DEBUG("[%d] Read w_id with d_id failed, d_id_key:%lx, rc %d",this_coroutine::current()->id(),d_to_w_wrapper(d_id),(int)rc);
//           txn->Rollback();
//           continue;
//       }
//       District* district = d_id_obj->get_as<District>();
//       uint64_t w_id = district->w_id;
//       LOG_DEBUG("[%d] Read w_id with d_id succeeded, d_id_key:%lx, w_id %lu, next_order_id %lu",this_coroutine::current()->id(),d_to_w_wrapper(d_id),district->w_id,district->next_order_id);
//       // 3. 通过w_id与i_id联合查找Stock，得到商品库存item_num
//       auto stock_obj = txn->GetObject(stock_wrapper(w_id,i_id), sizeof(Stock));
//       rc = txn->Read(stock_obj);
//       if(rc!=TxnStatus::OK){
//           LOG_DEBUG("[%d] Read stock with stock_key failed, w_id_i_id:%lu, rc %d",this_coroutine::current()->id(),stock_wrapper(w_id,i_id),(int)rc);
//           txn->Rollback();
//           continue;
//       }
//       Stock* stock = stock_obj->get_as<Stock>();
//       LOG_DEBUG("[%d] Read item_num with w_id&i_id succeeded,w_id_i_id:%lx, cur_item_num %lu",this_coroutine::current()->id(),stock_wrapper(w_id,i_id),stock->item_num);
//       uint64_t cur_item_num = stock->item_num;
//       // 4. 如果库存不足，结束
//       if (cur_item_num < i_num) {
//           txn->Commit();
//           break;
//       }
//       // 5. 库存充足，从district拿到next_order_id并更新next_order_id （锁定订单）
//       uint64_t cur_order_id = district->next_order_id;
//       district->next_order_id++;
//       // 6. 更新库存
//       stock->item_num -= i_num;
//       // 7. 将c_id + i_id + next_order_id作为key，购买数量作为value写到order表中
//       uint64_t new_order_key = new_order_wrapper(c_id,i_id,cur_order_id);
//       auto new_order_obj = txn->GetObject(new_order_key,sizeof(Order));
//       auto new_order = new_order_obj->get_as<Order>();
//       new_order->item_num = i_num;
//       new_order_obj->set_new();
      
//       //LOG_DEBUG("after change, new order:%lu",new_order->item_num);
//       rc = txn->Write(d_id_obj);
//       EXPECT_EQ(rc,TxnStatus::OK);
//       rc = txn->Write(stock_obj);
//       EXPECT_EQ(rc,TxnStatus::OK);
//       rc = txn->Write(new_order_obj);
//       EXPECT_EQ(rc,TxnStatus::OK);
//       rc = txn->Commit();
//       EXPECT_EQ(rc,TxnStatus::OK);
//     }while(rc!=TxnStatus::OK);
//     gettimeofday(&endtv, NULL);
// }

// void TestReadAfterWriteTask(){
//   auto txn = TransactionFactory::TxnBegin();
//   LOG_INFO("[client] test Read&Write");
//   uint64_t* p_value;
//   auto obj = txn->GetObject(TestKey, sizeof(uint64_t));
//   auto rc = txn->Read(obj);
//   EXPECT_EQ(rc, TxnStatus::OK);
//   p_value = obj->get_as<uint64_t>();
//   LOG_DEBUG("[client] At first value:%lx",*p_value);
  
//   *p_value = ~0;
//   rc = txn->Write(obj);
//   EXPECT_EQ(rc,TxnStatus::OK);
//   rc = txn->Commit();
//   EXPECT_EQ(rc,TxnStatus::OK);

//   auto new_txn = TransactionFactory::TxnBegin();
//   auto new_obj = txn->GetObject(TestKey, sizeof(uint64_t));
//   rc = new_txn->Read(new_obj);
//   EXPECT_EQ(rc, TxnStatus::OK);
//   p_value = obj->get_as<uint64_t>();
//   LOG_DEBUG("[client] After modification value:%lx",*p_value);
//   new_txn->Commit();
// }

// void ValidateTask(){
//   auto txn = TransactionFactory::TxnBegin();
//   LOG_INFO("[client] validate c_to_d table start");
//   for(int d_id = 0; d_id < warehouse_num_ * district_per_w_; ++d_id){
//       int c_id = d_id * customer_per_d_; int tmp;
//       Customer* c_value;
//       for (int i = 0; i < customer_per_d_; ++i) {
//         tmp = c_id + i;
//         uint64_t c_id_key = c_to_d_wrapper((uint64_t)tmp);
//         auto obj = txn->GetObject(c_id_key, sizeof(Customer));
//         auto rc = txn->Read(obj);
//         EXPECT_EQ(rc, TxnStatus::OK);
//         c_value = obj->get_as<Customer>();
//         LOG_DEBUG("c_id_key:%lx, d_id:%lu", c_id_key, c_value->d_id);
//       }
//   }
//   LOG_INFO("[client] validate c_to_d table finish");
//   LOG_INFO("[client] validate d_to_w table start!");
//   for (int w_id = 0; w_id < warehouse_num_; ++w_id) {
//     int d_id = w_id * district_per_w_; int tmp;
//     District* d_value;
//     for (int i = 0; i < district_per_w_; ++i) {
//       tmp = d_id + i;
//       uint64_t d_id_key = d_to_w_wrapper((uint64_t)tmp);
//       auto obj = txn->GetObject(d_id_key, sizeof(District));
//       auto rc = txn->Read(obj);
//       EXPECT_EQ(rc, TxnStatus::OK);
//       d_value = obj->get_as<District>();
//       LOG_DEBUG("d_id_key:%lx, w_id:%lu, next_order:%lu", d_id_key, d_value->w_id, d_value->next_order_id);
//     }
//   }
//   LOG_INFO("[client] validate d_to_w table finish!");
//   LOG_INFO("[client] validate stock table start!");
//   for (int w_id = 0; w_id < warehouse_num_; ++w_id) {
//       Stock* stock_value;
//       for (int i_id = 0; i_id < item_types_; ++i_id) {
//         uint64_t stock_key = stock_wrapper((uint64_t)(w_id),(uint64_t)(i_id));
//         auto obj = txn->GetObject(stock_key, sizeof(District));
//         auto rc = txn->Read(obj);
//         EXPECT_EQ(rc, TxnStatus::OK);
//         stock_value = obj->get_as<Stock>();
//         LOG_DEBUG("stock_key:%lx, item num:%lu", stock_key, stock_value->item_num);
//       }
//   }
//   LOG_INFO("[client] validate stock table finish!");
//   LOG_INFO("ok");
// }

// void InsertTask(){
//   auto txn = TransactionFactory::TxnBegin();
//   auto obj  = txn->GetObject(TestKey-1, sizeof(uint64_t));
//   obj->set_new();
//   *obj->get_as<uint64_t>() = 0xbeefdeaddeadbeef;
//   txn->Write(obj);
//   txn->Commit();

//   auto new_txn = TransactionFactory::TxnBegin();
//   auto new_obj  = txn->GetObject(TestKey-1, sizeof(uint64_t));
//   new_txn->Read(new_obj);
//   uint64_t* pval = obj->get_as<uint64_t>();
//   LOG_DEBUG("read result %lx",*pval);
//   new_txn->Commit();
// }

// //./tpcc_bench_test -r s -a 192.168.1.11 -t 10 -c 1 -n 100000
// //./tpcc_bench_test -r c -a 192.168.1.11 -t 8 -c 8 -n 100000
// int main(int argc, char** argv) {
//   cmdline::parser cmd;
//   cmd.add<string>("role", 'r', "the role of process", true, "", cmdline::oneof<string>("c", "s"));
//   cmd.add<string>("ip", 'a', "server ip address", true);
//   cmd.add<int>("thread", 't', "thread num", false, 1);
//   cmd.add<int>("coro", 'c', "coroutine per thread", false, 1);
//   cmd.add<int>("task", 'n', "task per thread",false,100000);
//   cmd.parse_check(argc, argv);

//   bool server = cmd.get<string>("role") == "s";
//   if (server) {
//     ip = cmd.get<string>("ip");
//     thread_num = cmd.get<int>("thread");

//     RrpcRte::Options rte_opt;
//     rte_opt.tcp_port_ = port;

//     RrpcRte rte(rte_opt);
//     global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size(), thread_num);
//     InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
//     global_db = new KVEngine();
//     c_to_d_db = new KVEngine();
//     d_to_w_db = new KVEngine();
//     stock_db = new KVEngine();
//     new_order_db = new KVEngine();
//     dbs.reserve(10);
//     dbs[0] = global_db;
//     for(int i=1; i<10; i++){
//       dbs[i] = new KVEngine();
//     }
//     RegisterService();

//     uint64_t val = 0xDEADBEEFBEEFDEAD;
//     global_db->put(TestKey, &val, sizeof(uint64_t), TSO::get_ts());

//     LOG_INFO("[server] load c_to_d table start");
//     for(uint64_t d_id = 0; d_id < warehouse_num_ * district_per_w_; ++d_id){
//         uint64_t c_id = d_id * customer_per_d_; uint64_t tmp;
//         Customer c_value;
//         for (uint64_t i = 0; i < customer_per_d_; ++i) {
//           tmp = c_id + i;
//           uint64_t c_id_key = c_to_d_wrapper((uint64_t)tmp);
//           c_value.d_id = d_id;
//           c_to_d_db->put(c_id_key, &c_value, sizeof(Customer), TSO::get_ts());
//           //LOG_DEBUG("[server] insert to c_to_d db, key:%lx, value:%lu",c_id_key, d_id);
//         }
//     }
//     LOG_INFO("[server] load c_to_d table finish");
//     LOG_INFO("[Server] load d_to_w table start!");
//     for (uint64_t w_id = 0; w_id < warehouse_num_; ++w_id) {
//       uint64_t d_id = w_id * district_per_w_; uint64_t tmp;
//       District d_value;
//       for (uint64_t i = 0; i < district_per_w_; ++i) {
//         tmp = d_id + i;
//         uint64_t d_id_key = d_to_w_wrapper((uint64_t)tmp);
//         d_value.w_id = w_id; d_value.next_order_id = 0;
//         d_to_w_db->put(d_id_key, &d_value, sizeof(d_value), TSO::get_ts());
//         //LOG_DEBUG("[server] insert to d_to_w db, key:%lx, value:[w_id:%lu, next_id:%lu]",d_id_key, w_id,d_value.next_order_id = 0);
//       }
//     }
//     LOG_INFO("[Server] load d_to_w table finish!");
//     LOG_INFO("[Server] load stock table start!");
//     for (uint64_t w_id = 0; w_id < warehouse_num_; ++w_id) {
//         Stock stock_value;
//         for (uint64_t i_id = 0; i_id < item_types_; ++i_id) {
//           uint64_t stock_key = stock_wrapper((uint64_t)(w_id),(uint64_t)(i_id));
//           stock_value.item_num = 100000;
//           stock_db->put(stock_key, &stock_value, sizeof(stock_value), TSO::get_ts());
//           //LOG_DEBUG("[server] insert to stock db, key:%lx, value:%lu",stock_key, i_id);
//         }
//     }
//     LOG_INFO("[Server] load stock table finish!");

//     while (true)
//       ;
//   } else {
//     ip = cmd.get<string>("ip");
//     thread_num = cmd.get<int>("thread");
//     cort_per_thread = cmd.get<int>("coro");
//     task_num = cmd.get<int>("task");

//     RrpcRte::Options rte_opt;
//     RrpcRte rte(rte_opt);
//     global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size());
//     std::unordered_map<node_id, RdmaCM::Iport> config;
//     config[0] = {ip, port};
//     global_cm->manual_set_network_config(config);

//     InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());

//     customer_id_randomer_ = new std::uniform_int_distribution<>(0, warehouse_num_ * district_per_w_ * customer_per_d_ - 1);
//     item_id_randomer_ = new zipf_table_distribution<>(item_types_ - 1, 1.0);
//     item_num_randomer_ = new std::uniform_int_distribution<>(1, 10);

//     tasks.resize(task_num);
//     for(int i=0;i<task_num;i++){
//         set_random_op(&tasks[i].op);
//     }

//     CoroutinePool pool(thread_num, cort_per_thread);
//     pool.start();
//     LOG_INFO("tasks: %zu",op_with_stat.size());
//     gettimeofday(&starttv, NULL);
//     {
//       WaitGroup wg(task_num);
//       for(int i=0;i<task_num;i++){
//         pool.enqueue([&wg,i]() {
//           TPCCtask(tasks[i]);
//           wg.Done();
//         });
//       }
//       wg.Wait();
//     }
//     gettimeofday(&endtv, NULL);
//     LOG_INFO("PASS");
//     uint64_t tot = ((endtv.tv_sec - starttv.tv_sec) * 1000000 + endtv.tv_usec - starttv.tv_usec) >> 1;
//         printf("============================ Throughput:%lf MOPS =========================\n", 
//                 1.0 * task_num  / tot);
//       DestroyMemPool();
//     }
// }