// #include <gtest/gtest.h>

// #include "cmdline.h"
// #include "coroutine_pool/coroutine_pool.h"
// #include "dtxn/dtxn.h"
// #include "proto/rpc.h"
// #include "rrpc/rrpc.h"
// #include "storage/db.h"
// #include "util/logging.h"
// #include "util/rand.h"
// #include "util/waitgroup.h"
// using namespace std;

// struct Compare {
//   int operator()(const uint64_t& a, const uint64_t& b) const { return a - b; }
// };

// enum op_type{OP_INSERT, OP_READ};

// struct OpAndModeWithStatistics {
//     op_type op;
//     Mode mode;
//     uint64_t exe_latency;
//     uint64_t lock_latency;
//     uint64_t write_latency;
//     uint64_t validate_latency;
//     uint64_t commit_latency;
//     uint64_t tot_latency;
// };

// std::atomic<int> oid{10001};
// constexpr int kHeadOid = 10000;
// constexpr int kSkipListOid = 1000;
// thread_local FastRand rnd;


// std::string ip = "192.168.1.11";
// int port = 10123;
// int thread_num;
// size_t op_num;
// int cort_per_thread;
// int write_rate;
// vector<OpAndModeWithStatistics> op_with_stat;
// struct timeval starttv, endtv;

// template <typename T>
// std::shared_ptr<T> read_from_db(uint64_t oid) {
//   T* t = new T();
//   ReadResult res;
//   res.buf = t;
//   res.buf_size = sizeof(T);

//   auto rc = global_db->get(oid, res, LATEST, false);
//   EXPECT_EQ(rc, DbStatus::OK);
//   return std::shared_ptr<T>(t);
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


// class SkipList {
//   enum { kMaxHeight = 12 };

//  private:
//   struct Node {
//     explicit Node(uint64_t k) : key(k) {}
//     Node() = default;
//     uint64_t key;
//     // Accessors/mutators for links.  Wrapped in methods so we can
//     // add the appropriate barriers as necessary.
//     uint64_t Next(int n) {
//       assert(n >= 0);
//       // Use an 'acquire load' so that we observe a fully initialized
//       // version of the returned Node.
//       return next_[n];
//     }
//     void SetNext(int n, uint64_t x) {
//       assert(n >= 0);
//       // Use a 'release store' so that anybody who reads through this
//       // pointer observes a fully initialized version of the inserted node.
//       next_[n] = x;
//     }

//    private:
//     // Array of length equal to the node height.  next_[0] is lowest level link.
//     uint64_t next_[kMaxHeight]{};
//   };

//  public:
//   // Create a new SkipList object that will use "cmp" for comparing keys,
//   // and will allocate memory using "*arena".  Objects allocated in the arena
//   // must remain allocated for the lifetime of the skiplist object.
//   SkipList() : compare_(), head_(kHeadOid), max_height_(1) {
//     Node head(0);
//     global_db->put(head_, &head, sizeof(Node), TSO::get_ts());
//   }

//   // Insert key into the list.
//   // REQUIRES: nothing that compares equal to key is currently in the list.
//   TxnStatus Insert(std::shared_ptr<Transaction> txn, uint64_t key) {
//     uint64_t prev[kMaxHeight];

//     uint64_t _tmp;
//     auto rc = FindGreaterOrEqual(txn, key, prev, _tmp);
//     if (rc != TxnStatus::OK) {
//       LOG_DEBUG("1");
//       txn->Rollback();
//       return rc;
//     }

//     int height = RandomHeight();
//     if (height > GetMaxHeight()) {
//       for (int i = GetMaxHeight(); i < height; i++) {
//         prev[i] = head_;
//       }
//       // It is ok to mutate max_height_ without any synchronization
//       // with concurrent readers.  A concurrent reader that observes
//       // the new value of max_height_ will see either the old value of
//       // new level pointers from head_ (nullptr), or a new value set in
//       // the loop below.  In the former case the reader will
//       // immediately drop to the next level since nullptr sorts after all
//       // keys.  In the latter case the reader will use the new node.
//       max_height_ = height;
//     }

//     auto x_obj = NewNode(txn, key);
//     uint64_t x_id = x_obj->id();
//     auto x = x_obj->get_as<Node>();

//     std::vector<TxnObjPtr> prev_objs;
//     prev_objs.reserve(height);
//     for (int i = 0; i < height; i++) {
//       prev_objs.emplace_back(txn->GetObject(prev[i], sizeof(Node)));
//     }
//     rc = txn->Read(prev_objs);
//     if (rc != TxnStatus::OK) {
//       LOG_DEBUG("2");
//       txn->Rollback();
//       return rc;
//     }

//     for (int i = 0; i < height; i++) {
//       auto previ = prev_objs[i]->get_as<Node>();
//       x->SetNext(i, previ->Next(i));
//       previ->SetNext(i, x_id);
//     }
//     txn->Write(x_obj);
//     txn->Write(prev_objs);
//     return TxnStatus::OK;
//   }

//   // Returns true iff an entry that compares equal to key is in the list.
//   bool Contains(std::shared_ptr<Transaction> txn, uint64_t key) {
//     uint64_t x_id;
//     auto rc = FindGreaterOrEqual(txn, key, nullptr, x_id);
//     assert(rc == TxnStatus::OK);
//     if (x_id != 0) {
//       auto obj = txn->GetObject(x_id, sizeof(Node));
//       auto rc = txn->Read(obj);
//       assert(rc == TxnStatus::OK);
//       auto got = obj->get_as<Node>();
//       return Equal(key, got->key);
//     } else {
//       return false;
//     }
//   }

//   inline int GetMaxHeight() const { return max_height_; }

//  private:
//   TxnObjPtr NewNode(std::shared_ptr<Transaction> txn, uint64_t key) {
//     // // char* const node_memory = arena_->AllocateAligned(
//     // //     sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
//     // // return new (node_memory) Node(key);
//     auto obj = txn->GetObject(oid.fetch_add(1), sizeof(Node));
//     obj->set_new();
//     obj->get_as<Node>()->key = key;
//     LOG_INFO("[%d] obj [%ld] key [%ld]", this_coroutine::current()->id(), obj->id(), key);
//     txn->Write(obj);
//     return obj;
//   }

//   int RandomHeight() {
//     // Increase height with probability 1 in kBranching
//     static const unsigned int kBranching = 4;
//     int height = 1;
//     while (height < kMaxHeight && rnd.one_in(kBranching)) {
//       height++;
//     }
//     assert(height > 0);
//     assert(height <= kMaxHeight);
//     return height;
//   }

//   bool Equal(uint64_t a, uint64_t b) { return (compare_(a, b) == 0); }

//   // Return true if key is greater than the data stored in "n"
//   bool KeyIsAfterNode(uint64_t key, Node* n) {
//     // null n is considered infinite
//     return compare_(n->key, key) < 0;
//   }

//   // Return the earliest node that comes at or after key.
//   // Return nullptr if there is no such node.
//   //
//   // If prev is non-null, fills prev[level] with pointer to previous
//   // node at "level" for every level in [0..max_height_-1].
//   TxnStatus FindGreaterOrEqual(std::shared_ptr<Transaction> txn, uint64_t key, uint64_t* prev, uint64_t& out) {
//     uint64_t x_id = head_;
//     int level = GetMaxHeight() - 1;
//     while (true) {
//       auto obj = txn->GetObject(x_id, sizeof(Node));
//       Node* x = obj->get_as<Node>();
//       auto rc = txn->Read(obj);
//       if (rc != TxnStatus::OK) {
//         LOG_DEBUG("[%d] 3 read %ld", this_coroutine::current()->id(), x_id);
//         return rc;
//       }
//       uint64_t next_id = x->Next(level);

//       bool descend_flag = false;
//       if (next_id != 0) {
//         auto obj = txn->GetObject(next_id, sizeof(Node));
//         auto rc = txn->Read(obj);
//         if (rc != TxnStatus::OK) {
//           LOG_DEBUG("[%d] 4 read %ld", this_coroutine::current()->id(), next_id);
//           return rc;
//         }
//         auto next = obj->get_as<Node>();
//         if (KeyIsAfterNode(key, next))
//           // Keep searching in this list
//           x_id = next_id;
//         else
//           descend_flag = true;
//       } else {
//         descend_flag = true;
//       }

//       if (descend_flag) {
//         if (prev != nullptr) prev[level] = x_id;
//         if (level == 0) {
//           out = next_id;
//           return TxnStatus::OK;
//         } else {
//           // Switch to next list
//           level--;
//         }
//       }
//     }
//   }
//   // Immutable after construction
//   Compare const compare_;

//   uint64_t head_;

//   // Modified only by Insert().  Read racily by readers, but stale
//   // values are ok.
//   int max_height_;  // Height of the entire list
// };

// void InsertTask(uint64_t key, int index) {
//   Mode mode = Mode::COLD;
//   TxnStatus rc = TxnStatus::OK;
//   do {
//     rc = TxnStatus::OK;
//     mode = (rc == TxnStatus::SWITCH) ? Mode::HOT : Mode::COLD;
//     auto txn = TransactionFactory::TxnBegin(mode);

//     struct timeval exe_start_tv, exe_end_tv;
//     struct timeval lock_start_tv, lock_end_tv;
//     struct timeval validate_start_tv, validate_end_tv;
//     struct timeval write_start_tv, write_end_tv;
//     struct timeval commit_start_tv, commit_end_tv;

//     gettimeofday(&exe_start_tv,NULL);

//     auto list_obj = txn->GetObject(kSkipListOid, sizeof(SkipList));
//     rc = txn->Read(list_obj);
//     LOG_DEBUG("[%d] read skiplist", this_coroutine::current()->id());
//     if (rc != TxnStatus::OK) {
//       LOG_DEBUG("[%d] read skiplist failed, rc %d", this_coroutine::current()->id(), (int)rc);
//       txn->Rollback();
//       continue;
//     }
//     SkipList* list = list_obj->get_as<SkipList>();
//     int height = list->GetMaxHeight();
//     rc = list->Insert(txn, key);
//     LOG_DEBUG("[%d] insert %lu", this_coroutine::current()->id(), key);
//     if (rc != TxnStatus::OK) {
//       LOG_DEBUG("[%d] insert failed, rc %d", this_coroutine::current()->id(), (int)rc);
//       txn->Rollback();
//       continue;
//     }
//     if (list->GetMaxHeight() != height) {
//       txn->Write(list_obj);
//     }

//     gettimeofday(&exe_end_tv,NULL);
//     //gettimeofday(&commit_start_tv,NULL);

//     rc = txn->Commit(lock_start_tv,lock_end_tv,validate_start_tv,validate_end_tv,write_start_tv,write_end_tv,commit_start_tv,commit_end_tv);
//     if (rc != TxnStatus::OK) {
//       LOG_DEBUG("[%d] insert commit failed, rc %d", this_coroutine::current()->id(), (int)rc);
//     }

//     //gettimeofday(&commit_end_tv,NULL);
//     if(index!=-1)
//     {
//       op_with_stat[index] = 
//       OpAndModeWithStatistics{
//         .op = OP_INSERT,
//         .mode = mode,
//         .exe_latency = (((uint64_t)exe_end_tv.tv_sec - exe_start_tv.tv_sec) * 1000000 + exe_end_tv.tv_usec - exe_start_tv.tv_usec),
//         .lock_latency = (((uint64_t)lock_end_tv.tv_sec - lock_start_tv.tv_sec) * 1000000 + lock_end_tv.tv_usec - lock_start_tv.tv_usec),
//         .write_latency = (((uint64_t)write_end_tv.tv_sec - write_start_tv.tv_sec) * 1000000 + write_end_tv.tv_usec - write_start_tv.tv_usec),        
//         .commit_latency = (((uint64_t)commit_end_tv.tv_sec - commit_start_tv.tv_sec) * 1000000 + commit_end_tv.tv_usec - commit_start_tv.tv_usec),
//       };
//       if(mode == Mode::COLD){
//         op_with_stat[index].validate_latency = (((uint64_t)validate_end_tv.tv_sec - validate_start_tv.tv_sec) * 1000000 + validate_end_tv.tv_usec - validate_start_tv.tv_usec);
//       }
//     }
//   } while (rc != TxnStatus::OK);
//   LOG_INFO("[%d] Finish Insert %ld", this_coroutine::current()->id(), key);
// }

// void ReadTask(uint64_t key,int index){
//     LOG_DEBUG("read task with key:%lu",key);
//     auto txn = TransactionFactory::TxnBegin();

//     struct timeval exe_start_tv, exe_end_tv;
//     struct timeval commit_start_tv, commit_end_tv;
//     gettimeofday(&exe_start_tv,NULL);

//     auto list_obj = txn->GetObject(kSkipListOid, sizeof(SkipList));
//     SkipList* list = list_obj->get_as<SkipList>();
//     auto rc = txn->Read(list_obj);
//     EXPECT_EQ(rc, TxnStatus::OK);
//     //EXPECT_TRUE(list->Contains(txn, key)) << "key :" << key;
//     gettimeofday(&exe_end_tv,NULL);
//     gettimeofday(&commit_start_tv,NULL);
//     ASSERT_EQ(txn->Commit(), TxnStatus::OK);
//     LOG_DEBUG("read task done with key:%lu",key);
//     gettimeofday(&commit_end_tv,NULL);
//     if(index!=-1)
//     {op_with_stat[index]=
//       OpAndModeWithStatistics{
//         .op = OP_READ,
//         .exe_latency = (((uint64_t)exe_end_tv.tv_sec - exe_start_tv.tv_sec) * 1000000 + exe_end_tv.tv_usec - exe_start_tv.tv_usec),
//         .commit_latency = (((uint64_t)commit_end_tv.tv_sec - commit_start_tv.tv_sec) * 1000000 + commit_end_tv.tv_usec - commit_start_tv.tv_usec),
//       };}
// }

// void ValidateTask(uint64_t key_start, uint64_t key_end) {
//   auto txn = TransactionFactory::TxnBegin();
//   auto list_obj = txn->GetObject(kSkipListOid, sizeof(SkipList));
//   SkipList* list = list_obj->get_as<SkipList>();
//   auto rc = txn->Read(list_obj);
//   EXPECT_EQ(rc, TxnStatus::OK);
//   for (uint64_t key = key_start; key < key_end; key++) {
//     EXPECT_TRUE(list->Contains(txn, key)) << "key :" << key;
//   }
//   ASSERT_EQ(txn->Commit(), TxnStatus::OK);
// }

// void Task(op_type op, uint64_t key,int index){
//     switch(op){
//         case OP_INSERT:
//             InsertTask(key,index);
//             break;
//         case OP_READ:
//             ReadTask(key,index);
//             break;
//         default:
//             LOG_ERROR("should not happen");
//     }
// }

// void TestTask(){
//   auto txn = TransactionFactory::TxnBegin();
//   auto list_obj = txn->GetObject(1000000, sizeof(uint64_t));
//   auto rc = txn->Read(list_obj);
//   EXPECT_EQ(rc, TxnStatus::OK);
//   LOG_INFO("ok");
// }

// //  ./skiplist_benchmark_test -r s -a 192.168.1.11 -t 4
// //  ./skiplist_benchmark_test -r c -a 192.168.1.11 -t 8 --coro 8 --op 100 -w 50
// int main(int argc, char** argv) {
//   cmdline::parser cmd;
//   cmd.add<string>("role", 'r', "the role of process", true, "", cmdline::oneof<string>("c", "s"));
//   cmd.add<string>("ip", 'a', "server ip address", true);
//   cmd.add<int>("thread", 't', "thread num", false, 1);
//   cmd.add<int>("coro", 0, "coroutine per thread", false, 1);
//   cmd.add<int>("op", 0, "transaction num per thread", false, 1000);
//   cmd.add<int>("rate",'w',"write ratio",false,50);
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
//     RegisterService();
//     SkipList list;
//     global_db->put(1000, &list, sizeof(SkipList), TSO::get_ts());
//     uint64_t val;
//     global_db->put(1000000, &val, sizeof(uint64_t), TSO::get_ts());

//     while (true)
//       ;
//   } else {
//     ip = cmd.get<string>("ip");
//     thread_num = cmd.get<int>("thread");
//     cort_per_thread = cmd.get<int>("coro");
//     op_num = cmd.get<int>("op");
//     write_rate = cmd.get<int>("rate");
//     int write_num = (int)((double)(op_num)*write_rate/100);
//     int read_num = op_num - write_num;

//     RrpcRte::Options rte_opt;
//     RrpcRte rte(rte_opt);
//     global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size());
//     std::unordered_map<node_id, RdmaCM::Iport> config;
//     config[0] = {ip, port};
//     global_cm->manual_set_network_config(config);

//     InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());

//     CoroutinePool pool(thread_num, cort_per_thread);
//     pool.start();

//     // {
//     //   WaitGroup wg(1);
//     //   pool.enqueue([&wg]() {
//     //     TestTask();
//     //     wg.Done();
//     //   });
//     //   wg.Wait();
//     // }

//     LOG_INFO("Init Skiplist for read");
//     {
//       WaitGroup wg(read_num);
//       for (int i = 0; i < read_num; i++) {
//         pool.enqueue([&wg, i]() {
//           InsertTask(i+100000,-1);
//           wg.Done();
//         });
//       }
//       wg.Wait();
//     }

//     LOG_INFO("Validate init");
//     {
//         WaitGroup wg(1);
//         pool.enqueue([&wg, read_num]() {
//             ValidateTask(100000, 100000 + read_num);
//             wg.Done();
//         });
//         wg.Wait();
//     }

//     LOG_INFO("Start Test Task .....");
//     op_with_stat.resize(op_num);
//     gettimeofday(&starttv, NULL);
//     {
//       WaitGroup wg(op_num);
//       for (int i = 0; i < op_num; i++) {
//         int insert_cnt = write_num, read_cnt = read_num;
//         if(insert_cnt&&read_num){
//             op_type op = rnd.next_u32()%2?OP_INSERT:OP_READ;
//             if(op==OP_INSERT){
//                 uint64_t key = rnd.next_u32()%write_num + 100000 + read_num;
//                 pool.enqueue([&wg, key,i]() {
//                     Task(OP_INSERT, key,i);
//                     wg.Done();
//                 });
//                 --insert_cnt;
//             }else{
//                 //wg.Done();
//                 uint64_t key = rnd.next_u32()%read_num + 100000;
//                 pool.enqueue([&wg, key,i]() {
//                     Task(OP_READ, key,i);
//                     wg.Done();
//                 });
//                 --read_cnt;
//             }
//         }
//         else if(insert_cnt){
//             uint64_t key = rnd.next_u32()%write_num + 100000 + read_num;
//             pool.enqueue([&wg, key,i]() {
//                 Task(OP_INSERT, key,i);
//                 wg.Done();
//             });
//             --insert_cnt;
//         }else if(read_cnt){
//             uint64_t key = rnd.next_u32()%read_num + 100000;
//             pool.enqueue([&wg, key,i]() {
//                 Task(OP_READ, key,i);
//                 wg.Done();
//             });
//             --read_cnt;
//         }else{
//             LOG_ERROR("should not happen");
//         }
//       }
//       wg.Wait();
//     }
//     gettimeofday(&endtv, NULL);
//     LOG_INFO("PASS");
//     LOG_INFO("tasks: %zu",op_with_stat.size());
//     uint64_t tot = ((endtv.tv_sec - starttv.tv_sec) * 1000000 + endtv.tv_usec - starttv.tv_usec) >> 1;
//         printf("============================ Throughput:%lf MOPS =========================\n", 
//                 op_num * 1.0 / tot);
//     std::vector<uint64_t> exe_lats, lock_lats, write_lats, validate_lats, commit_lats;
//     for(int i=0;i<op_with_stat.size();i++){
//       if(op_with_stat[i].op == OP_INSERT){
//         exe_lats.push_back(op_with_stat[i].exe_latency);
//         lock_lats.push_back(op_with_stat[i].lock_latency);
//         write_lats.push_back(op_with_stat[i].write_latency);
//         commit_lats.push_back(op_with_stat[i].commit_latency);
//       }
//       if(op_with_stat[i].mode == Mode::COLD){
//         validate_lats.push_back(op_with_stat[i].validate_latency);
//       }
//     }
//     std::sort(exe_lats.begin(), exe_lats.end());
//     std::sort(lock_lats.begin(), lock_lats.end());
//     std::sort(write_lats.begin(), write_lats.end());
//     std::sort(validate_lats.begin(),validate_lats.end());
//     std::sort(commit_lats.begin(), commit_lats.end());
//     printf("============================\n"
//                "P50 exe_Latency: %lf us, P99 exe_Latency: %lf us, P999 exe_Latency: %lf us, P9999 exe_Latency: %lf us\n"
//                "P50 lock_Latency: %lf us, P99 lock_Latency: %lf us, P999 lock_Latency: %lf us, P9999 lock_Latency: %lf us\n"
//                "P50 validate_Latency: %lf us, P99 validate_Latency: %lf us, P999 validate_Latency: %lf us, P9999 validate_Latency: %lf us\n"
//                "P50 write_Latency: %lf us, P99 write_Latency: %lf us, P999 write_Latency: %lf us, P9999 write_Latency: %lf us\n"
//                "P50 commit_Latency: %lf us, P99 commit_Latency: %lf us, P999 commit_Latency: %lf us, P9999 commit_Latency: %lf us\n",
//                pcalc(0.5, exe_lats), pcalc(0.99, exe_lats), pcalc(0.999, exe_lats), pcalc(0.9999, exe_lats),
//                pcalc(0.5, lock_lats), pcalc(0.99, lock_lats), pcalc(0.999, lock_lats), pcalc(0.9999, lock_lats),
//                pcalc(0.5, validate_lats), pcalc(0.99, validate_lats), pcalc(0.999, validate_lats), pcalc(0.9999, validate_lats),
//                pcalc(0.5, write_lats), pcalc(0.99, write_lats), pcalc(0.999, write_lats), pcalc(0.9999, write_lats),
//                pcalc(0.5, commit_lats), pcalc(0.99, commit_lats), pcalc(0.999, commit_lats), pcalc(0.9999, commit_lats));
//   }
//   DestroyMemPool();
// }