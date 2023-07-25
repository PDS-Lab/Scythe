// #ifndef BENCHMARK_SKIPLIST_SCHEMA_H
// #define BENCHMARK_SKIPLIST_SCHEMA_H

// #include <gtest/gtest.h>
// #include <cstdint>
// #include <cstddef>
// #include <cassert>
// #include <random>
// #include <vector>
// #include <algorithm>

// #include "util/logging.h"
// #include "util/rand.h"

// std::atomic<int> oid{10001};
// constexpr int kHeadOid = 10000;
// constexpr int kSkipListOid = 1000;
// thread_local FastRand rnd;

// namespace benchmark{

// struct Compare {
//   int operator()(const uint64_t& a, const uint64_t& b) const { return a - b; }
// };

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


// class SkipListBench{
//     // key: uint64_t key
// public:

//     enum OpType {
//         OPTYPE_SEARCH = 0,
//         OPTYPE_ADD,
//         TOTAL_OPTYPE_NUM
//     };

//     struct OpSearch {
//         uint64_t key;
//     };

//     struct OpAdd {
//         uint64_t key;
//     };

//     struct SkipListOp {
//         OpType op_type;
//         union OpArgs {
//             OpSearch search_op;
//             OpAdd    add_op;
//         } op_args;
//     };

//     SkipListBench(const std::vector<int>& op_rate, 
//         uint64_t max_rand_key, bool is_server)
//         :op_rate_(op_rate), gen_(std::random_device()()){
//         assert(check_op_rate_valid());
//         assert(max_rand_key>=1);
//         if(is_server){
//             SkipList list;
//             //根节点
//             global_db->put(1000, &list, sizeof(SkipList), TSO::get_ts());
//         }else{
//             skiplist_key_randomer_ = new std::uniform_int_distribution<>(1, max_rand_key);
//             op_randomer_ = new std::uniform_int_distribution<>(1, 100);
//         }
//     }

//     void set_random_op(SkipListOp* op) {
//         int op_rand_num = (*op_randomer_)(gen_);
//         if (op_rate_[0] && op_rand_num <= op_rate_[0]) {
//             op->op_type = OPTYPE_ADD;
//         } else {
//             op->op_type = OPTYPE_SEARCH;
//         }

//         switch (op->op_type)
//         {
//         case OPTYPE_ADD:
//             op->op_args.add_op.key = (*skiplist_key_randomer_)(gen_);
//             break;
//         case OPTYPE_SEARCH:
//             op->op_args.search_op.key = (*skiplist_key_randomer_)(gen_);
//             break;
//         default:
//             break;
//         }
//     }

// private:
//     bool check_op_rate_valid() {
//         if (op_rate_.size() != TOTAL_OPTYPE_NUM) return false; 
//         int sum = 0;
//         for (auto rate : op_rate_) {
//             if (rate < 0 || rate > 100) return false;
//             sum += rate;
//         }
//         return sum == 100;
//     }

//     std::vector<int>                 op_rate_;
//     std::mt19937                     gen_;
//     std::uniform_int_distribution<>* skiplist_key_randomer_;
//     std::uniform_int_distribution<>* op_randomer_;
// };


// }

// #endif