#include <functional>

#include "coroutine_pool/coroutine.h"
#include "coroutine_pool/coroutine_pool.h"
#include "coroutine_pool/scheduler.h"
#include "rrpc/rdma_cm.h"
#include "rrpc/rrpc.h"
#include "rrpc/rrpc_config.h"
#include "util/lock.h"
#include "util/logging.h"
#include "util/mem_pool.h"
#include "util/timer.h"
#include "util/waitgroup.h"

std::string ip = "192.168.1.88";
int port = 10123;

int dev_id = 0;

bool server;
int thread_num;
size_t msg_size;
size_t op_num;
int cort_per_thread;
int test;

int rpc_qp;
int one_side_qp;
int op_per_task = 10;
int batch_num;

constexpr int RPC_A_PLUS_B = 123;
constexpr int RPC_ACCESS = 101;
constexpr int RPC_KILL = 199;
constexpr int MR = 111;
constexpr size_t BUF_SIZE = 64 * 1024 * 1024;

struct RpcContext {
  int a;
  int b;
  Coroutine* self = nullptr;
};
struct DummyRequest {
  RpcContext ctx;
  size_t data_len;
  char data[];
};

struct DummyReply {
  int result;
};
// a + b;
void a_plus_b(Rocket::BatchIter* iter, Rocket* rkt) {
  auto req = iter->get_request<DummyRequest>();
  auto reply = rkt->gen_reply<DummyReply>(msg_size, iter);
  reply->result = req->ctx.a + req->ctx.b;
  LOG_DEBUG("get request %d + %d", req->ctx.a, req->ctx.b);
}

void a_plus_b_cb(void* reply, void* arg) {
  auto rep = reinterpret_cast<DummyReply*>(reply);
  auto ctx = reinterpret_cast<RpcContext*>(arg);
  ENSURE(rep->result == ctx->a + ctx->b, "%d + %d != %d", ctx->a, ctx->b, rep->result);
  if (ctx->self) {
    ctx->self->wakeup_once();
  }
}

struct OnesideContext {
  int idx;
  size_t data_len;
  char* data;
  bool read;
  Coroutine* self = nullptr;
};

void one_side_cb(void* arg) {
  auto ctx = reinterpret_cast<OnesideContext*>(arg);
  if (ctx->read) {
    for (int i = 0; i < ctx->data_len; i++) {
      ENSURE(ctx->idx == (int)ctx->data[i], "expected %d, got %d, in %d", ctx->idx, ctx->data[i], i);
    }
  }
  if (ctx->self) {
    ctx->self->wakeup_once();
  }
};

struct Access {
  uintptr_t addr;
  uint32_t rkey;
};

struct AccessRequest {};

void mr_access(Rocket::BatchIter* iter, Rocket* rkt) {
  auto reply = rkt->gen_reply<Access>(sizeof(Access), iter);
  reply->addr = global_cm->get_mr_info(MR).ptr;
  reply->rkey = global_cm->get_mr_info(MR).rkey;
}

void access_cb(void* reply, void* arg) {
  auto access = reinterpret_cast<Access*>(arg);
  auto rep = reinterpret_cast<Access*>(reply);
  *access = *rep;
}

volatile bool running = true;

struct KillRequest {};
struct KillReply {};

void kill_server(Rocket::BatchIter* iter, Rocket* rkt) {
  running = false;
  auto reply = rkt->gen_reply<KillReply>(sizeof(KillReply), iter);
  LOG_INFO("Kill Server");
}

void TestBody();

int main(int argc, char** argv) {
  Rocket::Options opt;
  opt.pref_dev_id = dev_id;

  Rocket::ConnectOptions connect_opt;
  connect_opt.pref_rdma_dev_id = dev_id;
  connect_opt.pref_remote_dev_id = dev_id;

  server = argv[1][0] == 'c' ? false : true;
  if (server) {
    ip = std::string(argv[2]);
    thread_num = ::atoi(argv[3]);
    msg_size = ::atoi(argv[4]);

    RrpcRte::Options rte_opt;
    rte_opt.tcp_port_ = port;

    RrpcRte rte(rte_opt);
    global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size(), thread_num);
    InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());

    reg_rpc_service(RPC_A_PLUS_B, a_plus_b);
    reg_rpc_service(RPC_ACCESS, mr_access);
    reg_rpc_service(RPC_KILL, kill_server);

    global_cm->DEFAULT_ROCKET_OPT = opt;
    global_cm->DEFAULT_CONNECTION_OPT = connect_opt;

    char* raw_buf = new char[BUF_SIZE];
    global_cm->register_memory(MR, raw_buf, BUF_SIZE, global_cm->DEFAULT_ROCKET_OPT.pref_dev_id);
    ENSURE(BUF_SIZE % msg_size == 0, "msg_size %% BUF_SIZE != 0");
    for (size_t i = 0; i < BUF_SIZE / msg_size; i++) {
      ::memset(raw_buf + i * msg_size, i % 8, msg_size);
    }

    LOG_INFO("============ Server Config ============:");
    LOG_INFO("==== Server IP : %s", ip.c_str());
    LOG_INFO("==== thread_num : %d", thread_num);
    LOG_INFO("==== msg size : %zu", msg_size);

    while (running) {
      cpu_relax();
    }
    delete global_cm;
    delete[] raw_buf;

  } else {
    //./rrpc_coroutine_test c 192.168.1.88 4 64 1024 4 4 1 4 4 
    ENSURE(argc == 11,
           "arguments: 'c' <ip> <thread> <msg_size> <op_num> <cort_num> <batch_size> "
           "<type> <qp> <one_side_qp>");
    ip = std::string(argv[2]);
    thread_num = ::atoi(argv[3]);
    msg_size = ::atoi(argv[4]);
    op_num = ::atoi(argv[5]);
    cort_per_thread = ::atoi(argv[6]);
    batch_num = ::atoi(argv[7]);
    test = ::atoi(argv[8]);
    rpc_qp = ::atoi(argv[9]);
    one_side_qp = ::atoi(argv[10]);

    RrpcRte::Options rte_opt;
    RrpcRte rte(rte_opt);
    global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(), rte.get_buffer_size());

    global_cm->DEFAULT_ROCKET_OPT = opt;
    global_cm->DEFAULT_CONNECTION_OPT = connect_opt;

    InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
    LOG_INFO("============ Client Config ============:");
    LOG_INFO("==== Server IP : %s", ip.c_str());
    LOG_INFO("==== thread_num : %d", thread_num);
    LOG_INFO("==== msg_size : %zu", msg_size);
    LOG_INFO("==== op_num per thread: %zu", op_num);
    LOG_INFO("==== cort_num: %d", cort_per_thread);
    LOG_INFO("==== msg_batch_num: %d", batch_num);
    LOG_INFO("==== rpc_qp: %d", rpc_qp);
    LOG_INFO("==== one_side_qp: %d", one_side_qp);
    TestBody();
    delete global_cm;
  }
  return 0;
}

struct TestStat {
  uint64_t time[5];
};

void read_test(Access access, bool zero_cp) {
  Rocket rkt((global_cm->DEFAULT_ROCKET_OPT));
  ENSURE(rkt.connect(ip, port, global_cm->DEFAULT_CONNECTION_OPT) == RDMA_CM_ERROR_CODE::CM_SUCCESS,
         "connect to server failed.");
  ENSURE(BUF_SIZE % (msg_size * 8) == 0, "BUF_SIZE %% (msg_size * 8) != 0");
  char* data = (char*)BuddyThreadHeap::get_instance()->alloc(msg_size);
  uint32_t lkey;
  global_cm->get_mr_rkey_by_dev_id(global_cm->DEFAULT_ROCKET_OPT.pref_dev_id, &lkey);
  for (int i = 0; i < op_num; i++) {
    OnesideContext ctx{
        .idx = i % 8,
        .data_len = msg_size,
        .data = data,
    };
    auto offset = (i * msg_size) % BUF_SIZE;
    uintptr_t addr = access.addr + offset;
    if (zero_cp) {
      rkt.remote_read_zero_cp(ctx.data, ctx.data_len, lkey, addr, access.rkey, one_side_cb, &ctx);
    } else {
      rkt.remote_read(ctx.data, ctx.data_len, addr, access.rkey, one_side_cb, &ctx);
    }
    rkt.rdma_burst();
    rkt.poll_one_side(1);
  }
  BuddyThreadHeap::get_instance()->free_local(data);
};

void write_test(Access access, bool zero_cp) {
  Rocket rkt((global_cm->DEFAULT_ROCKET_OPT));
  ENSURE(rkt.connect(ip, port, global_cm->DEFAULT_CONNECTION_OPT) == RDMA_CM_ERROR_CODE::CM_SUCCESS,
         "connect to server failed.");
  char* data = (char*)BuddyThreadHeap::get_instance()->alloc(msg_size);
  ::memset(data, 1, msg_size);
  uint32_t lkey;
  global_cm->get_mr_rkey_by_dev_id(global_cm->DEFAULT_ROCKET_OPT.pref_dev_id, &lkey);
  for (int i = 0; i < op_num; i++) {
    OnesideContext ctx{
        .idx = i % 8,
        .data_len = msg_size,
        .data = data,
    };
    auto offset = (i * msg_size) % BUF_SIZE;
    uintptr_t addr = access.addr + offset;
    if (zero_cp) {
      rkt.remote_write_zero_cp(ctx.data, ctx.data_len, lkey, addr, access.rkey, nullptr, nullptr);
    } else {
      rkt.remote_write(ctx.data, ctx.data_len, addr, access.rkey, nullptr, nullptr);
    }
    rkt.rdma_burst();
    rkt.poll_one_side(1);
  }
  BuddyThreadHeap::get_instance()->free_local(data);
};

void LatencyTest(Access access) {
  LOG_INFO("Latency Test =====================>");
  auto rpc_test = []() {
    Rocket rkt((global_cm->DEFAULT_ROCKET_OPT));

    ENSURE(rkt.connect(ip, port, global_cm->DEFAULT_CONNECTION_OPT) == RDMA_CM_ERROR_CODE::CM_SUCCESS,
           "connect to server failed.");
    RpcContext ctx;
    for (int i = 0; i < op_num; i++) {
      ctx = {.a = i, .b = i + 1};
      auto req = rkt.gen_request<DummyRequest>(msg_size, RPC_A_PLUS_B, a_plus_b_cb, &ctx);
      req->ctx = ctx;
      rkt.send();
      rkt.poll_reply_msg();
    }
  };

  TestStat stat;
  std::function<void()> fns[] = {rpc_test, std::bind(read_test, access, false), std::bind(read_test, access, true),
                                 std::bind(write_test, access, false), std::bind(write_test, access, true)};
  ChronoTimer timer;
  for (size_t task = 0; task < 5; task++) {
    auto st = timer.get_us();
    fns[task]();
    auto ed = timer.get_us();
    stat.time[task] = ed - st;
  }

  uint64_t avg_time;
  double latency;

  avg_time = stat.time[0];
  latency = 1.0 * avg_time / op_num;
  LOG_INFO("RPC Latency: %f us", latency);

  avg_time = stat.time[1];
  latency = 1.0 * avg_time / op_num;
  LOG_INFO("Read Latency: %f us", latency);

  avg_time = stat.time[2];
  latency = 1.0 * avg_time / op_num;
  LOG_INFO("Read Zero Copy Latency: %f us", latency);

  avg_time = stat.time[3];
  latency = 1.0 * avg_time / op_num;
  LOG_INFO("Write Latency: %f us", latency);

  avg_time = stat.time[4];
  latency = 1.0 * avg_time / op_num;
  LOG_INFO("Write Zero Copy Latency: %f us", latency);
}

void RpcTask(WaitGroup* counter) {
  auto rkt = GetRocket(0);
  RpcContext ctx[op_per_task];
  for (int i = 0; i < op_per_task; i++) {
    ctx[i] = {
        .a = i,
        .b = i + 1,
        .self = this_coroutine::current(),
    };
    auto req = rkt->gen_request<DummyRequest>(msg_size, RPC_A_PLUS_B, a_plus_b_cb, &ctx[i]);
    req->ctx = ctx[i];
    rkt->batching();
    this_coroutine::co_wait();
  }
  counter->Done();
};

void ReadTask(WaitGroup* counter, Access access, bool zero_cp, int j) {
  auto rkt = GetRocket(0);
  ENSURE(BUF_SIZE % (msg_size * 8) == 0, "BUF_SIZE %% (msg_size * 8) != 0");
  char* data = (char*)BuddyThreadHeap::get_instance()->alloc(msg_size * op_per_task);
  ::memset(data, 0, msg_size * op_per_task);
  uint32_t lkey;
  global_cm->get_mr_rkey_by_dev_id(global_cm->DEFAULT_ROCKET_OPT.pref_dev_id, &lkey);
  OnesideContext ctx[op_per_task];
  for (int i = 0; i < op_per_task; i++) {
    ctx[i] = {
        .idx = j % 8,
        .data_len = msg_size,
        .data = data + i * msg_size,
        .read = true,
        .self = this_coroutine::current(),
    };
    auto offset = (j * msg_size) % BUF_SIZE;
    uintptr_t addr = access.addr + offset;
    if (zero_cp) {
      rkt->remote_read_zero_cp(ctx[i].data, ctx[i].data_len, lkey, addr, access.rkey, one_side_cb, &ctx[i]);
    } else {
      rkt->remote_read(ctx[i].data, ctx[i].data_len, addr, access.rkey, one_side_cb, &ctx[i]);
    }
  }
  this_coroutine::co_wait(op_per_task);
  BuddyThreadHeap::get_instance()->free_local(data);
  counter->Done();
};

void WriteTask(WaitGroup* counter, Access access, bool zero_cp, int j) {
  auto rkt = GetRocket(0);
  char* data = (char*)BuddyThreadHeap::get_instance()->alloc(msg_size * op_per_task);
  ::memset(data, 1, msg_size * op_per_task);
  uint32_t lkey;
  global_cm->get_mr_rkey_by_dev_id(global_cm->DEFAULT_ROCKET_OPT.pref_dev_id, &lkey);
  OnesideContext ctx[op_per_task];
  for (int i = 0; i < op_per_task; i++) {
    ctx[i] = {
        .idx = j % 8,
        .data_len = msg_size,
        .data = data + i * msg_size,
        .read = false,
        .self = this_coroutine::current(),
    };
    auto offset = (j * msg_size) % BUF_SIZE;
    uintptr_t addr = access.addr + offset;
    if (zero_cp) {
      rkt->remote_write_zero_cp(ctx[i].data, ctx[i].data_len, lkey, addr, access.rkey, one_side_cb, &ctx[i]);
    } else {
      rkt->remote_write(ctx[i].data, ctx[i].data_len, addr, access.rkey, one_side_cb, &ctx[i]);
    }
  }
  this_coroutine::co_wait(op_per_task);

  BuddyThreadHeap::get_instance()->free_local(data);
  counter->Done();
};

void ThroughputTest(Access access) {
  LOG_INFO("Throughput1 Test =====================>");
  std::unordered_map<node_id, RdmaCM::Iport> config;
  config[0] = {ip, port};
  global_cm->manual_set_network_config(config);

  CoroutinePool cort_pool(thread_num, cort_per_thread);
  cort_pool.start();
  auto task_num = op_num * thread_num;
  ENSURE(op_num % op_per_task == 0, "op_num %zu %% op_per_task %d != 0", op_num, op_per_task);
  TestStat stat;
  ChronoTimer timer;
  {
    WaitGroup wg(task_num / op_per_task);
    auto st = timer.get_us();
    for (int i = 0; i < task_num / op_per_task; i++) {
      cort_pool.enqueue(std::bind(RpcTask, &wg));
    }
    wg.Wait();
    auto ed = timer.get_us();
    ENSURE(wg.Cnt() == 0, "wg.Cnt() : %d != 0", wg.Cnt());
    stat.time[0] = ed - st;
    LOG_INFO("Rpc Task Finish.");
  }
  {
    WaitGroup wg(task_num / op_per_task);
    auto st = timer.get_us();
    for (int i = 0; i < task_num / op_per_task; i++) {
      cort_pool.enqueue(std::bind(ReadTask, &wg, access, false, i));
    }
    wg.Wait();
    auto ed = timer.get_us();
    ENSURE(wg.Cnt() == 0, "wg.Cnt() : %d != 0", wg.Cnt());
    stat.time[1] = ed - st;
    LOG_INFO("Read Task Finish.");
  }
  {
    WaitGroup wg(task_num / op_per_task);
    auto st = timer.get_us();
    for (int i = 0; i < task_num / op_per_task; i++) {
      cort_pool.enqueue(std::bind(ReadTask, &wg, access, true, i));
    }
    wg.Wait();
    auto ed = timer.get_us();
    ENSURE(wg.Cnt() == 0, "wg.Cnt() : %d != 0", wg.Cnt());
    stat.time[2] = ed - st;
    LOG_INFO("Read Task Zero Copy Finish.");
  }
  {
    WaitGroup wg(task_num / op_per_task);
    auto st = timer.get_us();
    for (int i = 0; i < task_num / op_per_task; i++) {
      cort_pool.enqueue(std::bind(WriteTask, &wg, access, false, i));
    }
    wg.Wait();
    auto ed = timer.get_us();
    ENSURE(wg.Cnt() == 0, "wg.Cnt() : %d != 0", wg.Cnt());
    stat.time[3] = ed - st;
    LOG_INFO("Write Task Finish.");
  }
  {
    WaitGroup wg(task_num / op_per_task);
    auto st = timer.get_us();
    for (int i = 0; i < task_num / op_per_task; i++) {
      cort_pool.enqueue(std::bind(WriteTask, &wg, access, true, i));
    }
    wg.Wait();
    auto ed = timer.get_us();
    ENSURE(wg.Cnt() == 0, "wg.Cnt() : %d != 0", wg.Cnt());
    stat.time[4] = ed - st;
    LOG_INFO("Write Task Zero Copy Finish.");
  }

  uint64_t avg_time;
  double throughput;

  avg_time = stat.time[0];
  throughput = 1.0 * task_num / (1.0 * avg_time / 1000000);
  LOG_INFO("RPC Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[1];
  throughput = 1.0 * task_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Read Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[2];
  throughput = 1.0 * task_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Read Zero Copy Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[3];
  throughput = 1.0 * task_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Write Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[4];
  throughput = 1.0 * task_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Write Zero Copy Throughput: %f MOPS", throughput / 1000000);
}

void ThroughputTest2(Access access) {
  LOG_INFO("Throughput2 Test =====================>");
  auto rpc_test = []() {
    Rocket rkt((global_cm->DEFAULT_ROCKET_OPT));

    ENSURE(rkt.connect(ip, port, global_cm->DEFAULT_CONNECTION_OPT) == RDMA_CM_ERROR_CODE::CM_SUCCESS,
           "connect to server failed.");
    void* msg_buf = nullptr;
    RpcContext ctx[op_per_task];
    for (int j = 0; j < op_num; j += op_per_task) {
      for (int i = 0; i < op_per_task; i++) {
        ctx[i] = {.a = i, .b = i + 1};
        auto req = rkt.gen_request<DummyRequest>(msg_size, RPC_A_PLUS_B, a_plus_b_cb, &ctx[i]);
        req->ctx = ctx[i];
      }
      rkt.send();
      rkt.poll_reply_msg();
    }
  };

  TestStat stat;
  std::vector<std::thread> threads;

  std::function<void()> fns[] = {rpc_test, std::bind(read_test, access, false), std::bind(read_test, access, true),
                                 std::bind(write_test, access, false), std::bind(write_test, access, true)};
  ChronoTimer timer;
  for (size_t task = 0; task < 5; task++) {
    threads.clear();
    auto st = timer.get_us();
    for (int i = 0; i < thread_num; i++) {
      threads.emplace_back(fns[task]);
    }
    for (auto& thr : threads) {
      thr.join();
    }
    auto ed = timer.get_us();
    stat.time[task] = ed - st;
    LOG_INFO("Finish Task %zu", task);
  }

  uint64_t avg_time;
  double throughput;

  avg_time = stat.time[0];
  throughput = 1.0 * op_num * thread_num / (1.0 * avg_time / 1000000);
  LOG_INFO("RPC Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[1];
  throughput = 1.0 * op_num * thread_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Read Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[2];
  throughput = 1.0 * op_num * thread_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Read Zero Copy Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[3];
  throughput = 1.0 * op_num * thread_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Write Throughput: %f MOPS", throughput / 1000000);

  avg_time = stat.time[4];
  throughput = 1.0 * op_num * thread_num / (1.0 * avg_time / 1000000);
  LOG_INFO("Write Zero Copy Throughput: %f MOPS", throughput / 1000000);
}

void TestBody() {
  LOG_INFO("Start Test....");
  ENSURE(msg_size >= sizeof(DummyRequest), "too small rpc request");
  global_cm->DEFAULT_ROCKET_OPT.kMaxBatchNum = batch_num;
  Rocket rkt((global_cm->DEFAULT_ROCKET_OPT));
  
  global_cm->DEFAULT_CONNECTION_OPT.qp_num = rpc_qp;
  global_cm->DEFAULT_CONNECTION_OPT.one_side_qp_num = one_side_qp;
  ENSURE(rkt.connect(ip, port, global_cm->DEFAULT_CONNECTION_OPT) == RDMA_CM_ERROR_CODE::CM_SUCCESS,
         "connect to server failed.");
  Access access;
  rkt.gen_request<AccessRequest>(sizeof(AccessRequest), RPC_ACCESS, access_cb, &access);
  rkt.send();
  rkt.poll_reply_msg();

  // test
  if (test == 1) {
    LatencyTest(access);
  }
  if (test == 2) {
    ThroughputTest(access);
  }
  if (test == 3) {
    ThroughputTest2(access);
  }
  LOG_INFO("Finish Test <=====================");
  rkt.gen_request<KillRequest>(sizeof(KillRequest), RPC_KILL, nullptr, nullptr);
  rkt.send();
  rkt.poll_reply_msg();
}