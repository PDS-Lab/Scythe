// #include <gtest/gtest.h>
// #include <unordered_map>

// #include "coroutine_pool/coroutine_pool.h"
// #include "dtxn/dtxn.h"
// #include "proto/rpc.h"
// #include "rrpc/rrpc.h"
// #include "storage/db.h"
// #include "util/logging.h"
// #include "util/waitgroup.h"
// #include "third_party/cmdline.h"

// constexpr int record_num = 16;
// std::string ip = "192.168.1.11";
// int port = 10789;

// int thread_num=4;
// size_t op_num;
// int cort_per_thread = 4;
// std::unordered_map<std::string, std::shared_ptr<object>> table;

// void initdb() {
//   for(char ch = 'a';ch!='z';ch++){
//     std::string key;
//     table.emplace({key,nullptr});
//   }
// }


// void txn(uint64_t a, uint64_t b) {
//   vtuple_a _a;
//   vtuple_b _b;
//   Mode mode = Mode::COLD;
//   TxnStatus rc = TxnStatus::OK;
//   // start txn;
//   do {
//     mode = (rc == TxnStatus::SWITCH ? Mode::HOT : mode);
//     auto txn = TransactionFactory::TxnBegin(mode);
//     auto A = std::make_shared<TxnObj>(a, 0, sizeof(vtuple_a), &_a);
//     auto B = std::make_shared<TxnObj>(b, 0, sizeof(vtuple_b), &_b);

//     rc = txn->Read({A, B});
//     ASSERT_NE(rc, TxnStatus::INTERNAL);
//     if (rc != TxnStatus::OK) {
//       LOG_INFO("[%d] Read Abort %d", this_coroutine::current()->id(), (int)rc);
//       txn->Rollback();
//       continue;
//     }

//     LOG_DEBUG("[%d] Read A B success.", this_coroutine::current()->id());
//     _a.Debug();
//     _b.Debug();

//     _b.b = _a.a + _b.b;
//     _a.a = 0;
//     txn->Write(A);
//     txn->Write(B);
//     rc = txn->Commit();
//     ASSERT_NE(rc, TxnStatus::INTERNAL);
//     if (rc != TxnStatus::OK) {
//       LOG_INFO("[%d] Commit Abort %d", this_coroutine::current()->id(), (int)rc);
//     }
//   } while (rc != TxnStatus::OK);
//   LOG_DEBUG("[%d] After B = A + B, A = 0", this_coroutine::current()->id());
//   _a.Debug();
//   _b.Debug();
// }

// void validate() {
//   {
//     vtuple_a _a;
//     DebugReadCtx ctx{DbStatus::UNEXPECTED_ERROR, &_a};
//     auto rkt = GetRocket(0);
//     for (int i = 0; i < record_num; i++) {
//       auto req = rkt->gen_request<DebugRead>(sizeof(DebugRead), DEBUG_READ, debug_read_service_cb, &ctx);
//       req->id = i * 2;
//       req->sz = sizeof(vtuple_a);

//       rkt->send();
//       rkt->poll_reply_msg();

//       EXPECT_EQ(ctx.rc, DbStatus::OK);
//       _a.Debug();
//     }
//   }
//   {
//     vtuple_b _b;
//     DebugReadCtx ctx{DbStatus::UNEXPECTED_ERROR, &_b};
//     auto rkt = GetRocket(0);
//     for (int i = 0; i < record_num; i++) {
//       auto req = rkt->gen_request<DebugRead>(sizeof(DebugRead), DEBUG_READ, debug_read_service_cb, &ctx);
//       req->id = i * 2 + 1;
//       req->sz = sizeof(vtuple_b);

//       rkt->send();
//       rkt->poll_reply_msg();

//       EXPECT_EQ(ctx.rc, DbStatus::OK);
//       _b.Debug();
//     }
//   }
// }

// int main(int argc, char **argv) {
//   Rocket::Options opt;
//   Rocket::ConnectOptions connect_opt;

//   bool server = argv[1][0] == 'c' ? false : true;
//   if (server) {
//     // ip = std::string(argv[2]);
//     // thread_num = ::atoi(argv[3]);
//     RrpcRte::Options rte_opt;
//     rte_opt.tcp_port_ = port;

//     RrpcRte rte(rte_opt);
//     global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size(), thread_num);
//     InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
//     global_db = new KVEngine();
//     initdb();
//     RegisterService();
//     while (true)
//       ;
//   } else {
//     // ip = std::string(argv[2]);
//     // thread_num = ::atoi(argv[3]);

//     RrpcRte::Options rte_opt;
//     RrpcRte rte(rte_opt);
//     global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size());
//     std::unordered_map<node_id, RdmaCM::Iport> config;
//     config[0] = {ip, port};
//     global_cm->manual_set_network_config(config);

//     InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
//     CoroutinePool pool(thread_num, cort_per_thread);

//     WaitGroup wg(thread_num * cort_per_thread);
//     auto fn = [&]() {
//       for (int i = 0; i < record_num; i++) {
//         txn(i * 2, i * 2 + 1);
//       }
//       wg.Done();
//     };
//     validate();
//     for (int i = 0; i < thread_num * cort_per_thread; i++) {
//       pool.enqueue(fn);
//     }

//     pool.start();
//     wg.Wait();
//     validate();
//   }
//   DestroyMemPool();
// }
