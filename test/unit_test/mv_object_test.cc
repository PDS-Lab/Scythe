#include <gtest/gtest.h>

#include <cstring>

#include "common.h"
#include "storage/object.h"
#include "util/mem_pool.h"

struct Foo {
  int a;
  int b;
  int c;
  bool operator==(const Foo &f) { return a == f.a && b == f.b && c == f.c; }
  void Debug() { printf("Foo : <%d, %d, %d>\n", a, b, c); }
};

TEST(Object, Simple) {
  void *pool_addr = malloc(sizeof(char) * (2ULL * 1024 * 1024 * 1024));
  // printf("[mem_pool_test] pool_addr:%lu\n",
  // (uint64_t)(uintptr_t)(pool_addr));
  assert(pool_addr);

  InitMemPool(pool_addr, (2ULL * 1024 * 1024 * 1024));
  Foo foo = {1, 2, 3};

  object obj(sizeof(Foo));
  for (int i = 1; i <= 10; i++) {
    foo.a = i;
    auto rc = obj.alloc_insert(i, reinterpret_cast<data_t *>(&foo));
    EXPECT_EQ(DbStatus::OK, rc);
    foo.Debug();
  }

  for (int i = 1; i <= 10; i++) {
    ReadResult read;
    read.buf = reinterpret_cast<data_t *>(new Foo());
    read.buf_size = sizeof(Foo);

    auto rc = obj.get(i, read, false);
    EXPECT_EQ(DbStatus::OK, rc);

    Foo expected = {i, 2, 3};
    EXPECT_TRUE(expected == *reinterpret_cast<Foo *>(read.buf));
    reinterpret_cast<Foo *>(read.buf)->Debug();
    delete reinterpret_cast<Foo *>(read.buf);
  }

  ReadResult read;
  read.buf = reinterpret_cast<data_t *>(new Foo());
  read.buf_size = sizeof(Foo);
  auto rc = obj.get(LATEST, read, false);
  EXPECT_EQ(DbStatus::OK, rc);

  EXPECT_TRUE(foo == *reinterpret_cast<Foo *>(read.buf));
  reinterpret_cast<Foo *>(read.buf)->Debug();

  DestroyMemPool();
}

TEST(Object, Lock) {
  void *pool_addr = malloc(sizeof(char) * (2ULL * 1024 * 1024 * 1024));
  // printf("[mem_pool_test] pool_addr:%lu\n",
  // (uint64_t)(uintptr_t)(pool_addr));
  assert(pool_addr);

  InitMemPool(pool_addr, (2ULL * 1024 * 1024 * 1024));
  Foo foo = {1, 2, 3};

  object obj(sizeof(Foo));
  for (int i = 1; i <= 10; i++) {
    foo.a = i;
    auto rc = obj.alloc_insert(i, reinterpret_cast<data_t *>(&foo));
    EXPECT_EQ(DbStatus::OK, rc);

    foo.Debug();
  }

  ReadResult read;
  read.buf = reinterpret_cast<data_t *>(new Foo());
  read.buf_size = sizeof(Foo);
  for (int i = 1; i <= 10; i++) {
    auto rc = obj.get(i, read, false);
    EXPECT_EQ(DbStatus::OK, rc);

    Foo expected = {i, 2, 3};
    EXPECT_TRUE(expected == *reinterpret_cast<Foo *>(read.buf));
    reinterpret_cast<Foo *>(read.buf)->Debug();
    printf("Version : %lu\n", read.version);
  }

  obj.lock.Lock(1, Mode::HOT);

  auto rc = obj.get(LATEST, read, true);
  EXPECT_EQ(DbStatus::LOCKED, rc);

  // EXPECT_TRUE(foo == *reinterpret_cast<Foo *>(read.buf));
  reinterpret_cast<Foo *>(read.buf)->Debug();
  printf("Version : %lu\n", read.version);

  delete reinterpret_cast<Foo *>(read.buf);
  DestroyMemPool();
}

TEST(Object, Visable) {
  void *pool_addr = malloc(sizeof(char) * (2ULL * 1024 * 1024 * 1024));
  // printf("[mem_pool_test] pool_addr:%lu\n",
  // (uint64_t)(uintptr_t)(pool_addr));
  assert(pool_addr);

  InitMemPool(pool_addr, (2ULL * 1024 * 1024 * 1024));
  Foo foo = {1, 2, 3};

  object obj(sizeof(Foo));
  for (int i = 1; i <= 10; i++) {
    foo.a = i;
    auto rc = obj.alloc_insert(i, reinterpret_cast<data_t *>(&foo));
    EXPECT_EQ(DbStatus::OK, rc);
    foo.Debug();
  }
  undo_log *ptr;
  obj.alloc(101, ptr);

  ReadResult read;
  read.buf = reinterpret_cast<data_t *>(new Foo());
  read.buf_size = sizeof(Foo);

  auto rc = obj.get(LATEST, read, false);
  EXPECT_EQ(DbStatus::OK, rc);
  EXPECT_TRUE(foo == *reinterpret_cast<Foo *>(read.buf));
  reinterpret_cast<Foo *>(read.buf)->Debug();
  printf("Version : %lu\n", read.version);

  Foo tmp{3, 45, 5};
  memcpy(ptr->data, &tmp, sizeof(tmp));
  ptr->visiable = true;

  rc = obj.get(LATEST, read, false);
  EXPECT_EQ(DbStatus::OK, rc);
  EXPECT_TRUE(tmp == *reinterpret_cast<Foo *>(read.buf));
  reinterpret_cast<Foo *>(read.buf)->Debug();
  printf("Version : %lu\n", read.version);

  DestroyMemPool();
}