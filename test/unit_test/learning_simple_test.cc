#include<functional>
#include<iostream>

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
int port = 10456;

int dev_id = 0;
bool server;
int thread_num;
size_t msg_size;
size_t op_num;
int cort_per_thread;
int test;

int qp_num;
int one_side_qp_num;
int op_per_task;
int batch_num;

constexpr int RPC_MY_RPC = 123;
constexpr int RPC_ACCESS = 101;
constexpr int RPC_KILL = 199;
constexpr int MR = 111;
constexpr size_t BUF_SIZE = 64 * 1024 * 1024;

struct RpcContext {
    int val;
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

void my_rpc(Rocket::BatchIter* iter, Rocket* rkt){
    auto req = iter->get_request<DummyRequest>();
    auto reply = rkt->gen_reply<DummyReply>(msg_size,iter);
    reply->result = req->ctx.val;
    LOG_DEBUG("get request %d", req->ctx.val);
}

void my_rpc_cb(void* reply, void* args){
    auto rep = reinterpret_cast<DummyReply*>(reply);
    auto ctx = reinterpret_cast<RpcContext*>(args);
    ENSURE(rep->result == ctx->val, "%d!=%d", rep->result, ctx->val);
    if(ctx->self){
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

int main(int argc, char** argv){
    Rocket::Options opt;
    opt.pref_dev_id = dev_id;

    Rocket::ConnectOptions connect_opt;
    connect_opt.pref_rdma_dev_id = dev_id;
    connect_opt.pref_remote_dev_id = dev_id;

    

    server = argv[1][0] == 's';
    if(server){
        ip = "192.168.1.88";
        thread_num = 4;//::atoi()
        msg_size = 64;//

        RrpcRte::Options rte_opt;
        rte_opt.tcp_port_ = port;

        RrpcRte rte(rte_opt);
        global_cm = new RdmaCM(&rte, ip, port, rte.get_rdma_buffer(),rte.get_buffer_size(),thread_num);
        InitMemPool(rte.get_rdma_buffer(),rte.get_buffer_size());

        reg_rpc_service(RPC_MY_RPC,my_rpc);
        reg_rpc_service(RPC_ACCESS,mr_access);
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
    }else{
    
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
    qp_num = ::atoi(argv[9]);
    one_side_qp_num = ::atoi(argv[10]);

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
    LOG_INFO("==== rpc_qp: %d", qp_num);
    LOG_INFO("==== one_side_qp: %d", one_side_qp_num);
    TestBody();
    delete global_cm;
    }
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
      ctx = {.val = i};
      auto req = rkt.gen_request<DummyRequest>(msg_size, RPC_MY_RPC, my_rpc_cb, &ctx);
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


void TestBody(){
    LOG_INFO("Start Test....");
  ENSURE(msg_size >= sizeof(DummyRequest), "too small rpc request");
  global_cm->DEFAULT_ROCKET_OPT.kMaxBatchNum = batch_num;
  Rocket rkt((global_cm->DEFAULT_ROCKET_OPT));
  
  global_cm->DEFAULT_CONNECTION_OPT.qp_num = qp_num;
  global_cm->DEFAULT_CONNECTION_OPT.one_side_qp_num = one_side_qp_num;
  ENSURE(rkt.connect(ip, port, global_cm->DEFAULT_CONNECTION_OPT) == RDMA_CM_ERROR_CODE::CM_SUCCESS,
         "connect to server failed.");
  Access access;
  rkt.gen_request<AccessRequest>(sizeof(AccessRequest), RPC_ACCESS, access_cb, &access);
  rkt.send();
  rkt.poll_reply_msg();

  // test
  LatencyTest(access);

  LOG_INFO("Finish Test <=====================");
  rkt.gen_request<KillRequest>(sizeof(KillRequest), RPC_KILL, nullptr, nullptr);
  rkt.send();
  rkt.poll_reply_msg();
}