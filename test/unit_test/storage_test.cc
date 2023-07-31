#include <gtest/gtest.h>

#include <cstdio>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "storage/db.h"
#include "storage/object.h"
#include "util/logging.h"
#include "util/mem_pool.h"
#include "util/rand.h"
#include "util/timer.h"

struct Foo {
  int a;
  int b;
  int c;
  char tmp[50];
  bool operator==(const Foo &f) const { return a == f.a && b == f.b && c == f.c; }
  void Debug() { printf("Foo : <%d, %d, %d>\n", a, b, c); }
};

void *pool_addr;
class KVEngineTest : public ::testing::Test {
 public:
  // Sets up the stuff shared by all tests in this test suite.
  //
  // Google Test will call Foo::SetUpTestSuite() before running the first
  // test in test suite Foo.  Hence a sub-class can define its own
  // SetUpTestSuite() method to shadow the one defined in the super
  // class.
  static void SetUpTestSuite() {
    size_t pool_size = GB(2);
    pool_addr = malloc(sizeof(char) * pool_size);
    InitMemPool(pool_addr, pool_size);
  }

  // Tears down the stuff shared by all tests in this test suite.
  //
  // Google Test will call Foo::TearDownTestSuite() after running the last
  // test in test suite Foo.  Hence a sub-class can define its own
  // TearDownTestSuite() method to shadow the one defined in the super
  // class.
  static void TearDownTestSuite() {
    DestroyMemPool();
    free(pool_addr);
  }

 protected:
  // Sets up the test fixture.
  virtual void SetUp() override { kv = new KVEngine(); }

  // Tears down the test fixture.
  virtual void TearDown() override { delete kv; }

  KVEngine *kv;
  FastRand rand;
  std::unordered_map<uint64_t, std::vector<Foo>> modify;
};

TEST_F(KVEngineTest, Simple) {
  int cnt = 10;
  std::vector<uint64_t> keys;
  DbStatus rc;
  // put 10 KV
  for (int i = 0; i < cnt; i++) {
    auto key = rand.next_u32();
    Foo foo = {i, (int)key, (int)(i + key)};

    rc = kv->put(key, &foo, sizeof(Foo), rdtsc());
    EXPECT_EQ(DbStatus::OK, rc);
    keys.push_back(key);
  }

  // get 10 KV
  ReadResult res;
  res.buf = malloc(sizeof(Foo));
  res.buf_size = sizeof(Foo);

  for (int i = 0; i < cnt; i++) {
    rc = kv->get(keys[i], res, LATEST, false);
    EXPECT_EQ(DbStatus::OK, rc);

    Foo *got = reinterpret_cast<Foo *>(res.buf);
    got->Debug();
    EXPECT_EQ(got->a, i);
    EXPECT_EQ(got->b, (int)keys[i]);
    EXPECT_EQ(got->c, (int)(keys[i] + i));
  }

  // update
  printf("Update...\n");
  for (int i = 0; i < 10; i++) {
    int idx = rand.next_u32() % cnt;
    modify[keys[idx]].push_back({idx, i, idx + i});
    modify[keys[idx]].back().Debug();
    rc = kv->put(keys[idx], &modify[keys[idx]].back(), sizeof(Foo), rdtsc());
    EXPECT_EQ(DbStatus::OK, rc);
  }

  // validate
  printf("Validate...\n");
  for (auto &mdf : modify) {
    auto key = mdf.first;
    auto latest = mdf.second.back();
    rc = kv->get(key, res, rdtsc(), false);
    EXPECT_EQ(DbStatus::OK, rc);

    Foo *got = reinterpret_cast<Foo *>(res.buf);
    got->Debug();
    EXPECT_EQ(*got, latest);
  }

  free(res.buf);
}

TEST_F(KVEngineTest, Concurrent) {
  size_t op = MB(4);
  int thread_num = 32;
  std::vector<std::thread> threads;
  auto func_put = [this](size_t start, size_t num) {
    auto seed = rand.next_u32();
    Foo foo = {1, 2, 3};
    for (size_t i = 0; i < num; i++) {
      auto ts = rdtsc();
      foo.a = i + start;
      foo.b = ts;
      foo.c = seed + i;
      auto rc = kv->put(start + i, &foo, sizeof(Foo), ts);
      EXPECT_EQ(rc, DbStatus::OK);
    }
  };

  auto func_get = [this](size_t start, size_t num) {
    ReadResult res;
    res.buf = malloc(sizeof(Foo));
    res.buf_size = sizeof(Foo);

    for (size_t i = 0; i < num; i++) {
      auto ts = rdtsc();
      auto rc = kv->get(start + i, res, ts, false);
      EXPECT_EQ(rc, DbStatus::OK);
      auto got = reinterpret_cast<Foo *>(res.buf);
      EXPECT_EQ(got->a, start + i);
    }

    free(res.buf);
  };

  ChronoTimer timer;
  auto op_per_thread = op / thread_num;

  {
    threads.clear();
    auto st = timer.get_us();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(func_put, op_per_thread * i, op_per_thread);
    }

    for (auto &thr : threads) {
      thr.join();
    }
    auto ed = timer.get_us();

    auto mops = 1.0 * op / (ed - st);
    LOG_INFO("KVEngine Put New : %lf MOps", mops);
  }
  {
    threads.clear();
    auto st = timer.get_us();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(func_put, op_per_thread * i, op_per_thread);
    }

    for (auto &thr : threads) {
      thr.join();
    }
    auto ed = timer.get_us();

    auto mops = 1.0 * op / (ed - st);
    LOG_INFO("KVEngine Update : %lf MOps", mops);
  }
  {
    threads.clear();
    auto st = timer.get_us();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(func_get, op_per_thread * i, op_per_thread);
    }

    for (auto &thr : threads) {
      thr.join();
    }
    auto ed = timer.get_us();

    auto mops = 1.0 * op / (ed - st);
    LOG_INFO("KVEngine Get : %lf MOps", mops);
  }
}

TEST_F(KVEngineTest, Concurrent2) {
  size_t op = MB(4);
  int thread_num = 32;
  std::vector<std::thread> threads;

  auto func_alloc = [this](size_t start, size_t num) {
    for (size_t i = 0; i < num; i++) {
      auto ts = rdtsc();
      auto rc = kv->alloc(start + i, sizeof(Foo), ts);
      EXPECT_EQ(rc, DbStatus::OK);
    }
  };

  auto func_update = [this](size_t start, size_t num) {
    auto seed = rand.next_u32();
    Foo foo = {1, 2, 3};
    for (size_t i = 0; i < num; i++) {
      auto ts = rdtsc();
      foo.a = i + start;
      foo.b = ts;
      foo.c = seed + i;
      auto rc = kv->update(start + i, &foo, sizeof(Foo), ts);
      EXPECT_EQ(rc, DbStatus::OK);
    }
  };

  auto func_get = [this](size_t start, size_t num) {
    ReadResult res;
    res.buf = malloc(sizeof(Foo));
    res.buf_size = sizeof(Foo);

    for (size_t i = 0; i < num; i++) {
      auto ts = rdtsc();
      auto rc = kv->get(start + i, res, ts, false);
      EXPECT_EQ(rc, DbStatus::OK);
      auto got = reinterpret_cast<Foo *>(res.buf);
      EXPECT_EQ(got->a, start + i);
    }

    free(res.buf);
  };

  ChronoTimer timer;
  auto op_per_thread = op / thread_num;

  {
    threads.clear();
    auto st = timer.get_us();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(func_alloc, op_per_thread * i, op_per_thread);
    }

    for (auto &thr : threads) {
      thr.join();
    }
    auto ed = timer.get_us();

    auto mops = 1.0 * op / (ed - st);
    LOG_INFO("KVEngine Alloc : %lf MOps", mops);
  }
  {
    threads.clear();
    auto st = timer.get_us();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(func_update, op_per_thread * i, op_per_thread);
    }

    for (auto &thr : threads) {
      thr.join();
    }
    auto ed = timer.get_us();

    auto mops = 1.0 * op / (ed - st);
    LOG_INFO("KVEngine Update : %lf MOps", mops);
  }
  {
    threads.clear();
    auto st = timer.get_us();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(func_get, op_per_thread * i, op_per_thread);
    }

    for (auto &thr : threads) {
      thr.join();
    }
    auto ed = timer.get_us();

    auto mops = 1.0 * op / (ed - st);
    LOG_INFO("KVEngine Get : %lf MOps", mops);
  }
}