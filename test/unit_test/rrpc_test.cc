#include <sys/time.h>
#include <unistd.h>
//#include<string>

#include "rrpc/rdma_cm.h"
#include "rrpc/rrpc_callbacks.h"
#include "rrpc/rrpc_config.h"
#include "rrpc/rrpc_rte.h"

#define ROCKET_MAX_THREAD 1024
#define BUF_SIZE 64*1024*1024

const int port = 12345;
const int BATCH_SIZE = 10;
const int BATCHES = 100;
int str_l;
bool enable_doorbell;
int thread_num;
int qp_num;
pthread_t tids[ROCKET_MAX_THREAD];
volatile bool running = true;

int dev_id = 0;
size_t msg_size = 0;

constexpr int MR = 111;
constexpr int RPC_SIMPLE_RPC = 123;

struct FooCtx {
  int reply_cnt{0};
};

void simple_rpc(Rocket::BatchIter* iter, Rocket* rkt){
  auto req = iter->get_request<RdmaSimpleReq>();
  auto reply = rkt->gen_reply<RdmaSimpleReply>(msg_size,iter);
   
}

void simple_cb(void* reply, void* ctx) {
  static int reply_num = 0;
  reply_num++;
  RdmaSimpleReply* rp = (RdmaSimpleReply*)(reply);
  FooCtx* rpc_ctx = (FooCtx*)ctx;
  rpc_ctx->reply_cnt++;
  // printf("[client] get reply %d\n", reply_num);
}

void* single_thread_job(void* args) {
  Rocket::Options opt;
  Rocket rocket(opt);
  Rocket::ConnectOptions copt;
  copt.qp_num = qp_num;
  RDMA_CM_ERROR_CODE rc = rocket.connect(0, copt);
  // printf("[thread-%lu] rc: %d\n", pthread_self(), (int)rc);

  struct timeval starttv, endtv;
  void* msg_buf = nullptr;
  int N = BATCH_SIZE;  // batch size
  int T = BATCHES;     // batches

  while (T--) {
    // get buffer & prepare args
    for (int i = 0; i < N; ++i) {
      rocket.get_msg_buf(&msg_buf, sizeof(RdmaSimpleReq) + str_l + 1, RDMA_CALLBACKS::RDMA_SIMPLE_TEST, nullptr);
      RdmaSimpleReq* simple_req = (RdmaSimpleReq*)(msg_buf);
      simple_req->size = str_l;
      std::string str(str_l, (char)('A' + i));
      snprintf(simple_req->data, str_l + 1, "%s", str.c_str());
      simple_req->data[str_l] = '\0';
    }

    // send
    rc = rocket.send(enable_doorbell);
    if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
      printf("rocket send failed: %d\n", (int)rc);
    }
    // get reply
    rocket.poll_reply_msg();
  }
  return NULL;
}

int main(int argc, char* argv[]) {
  assert(argc >= 2);
  Rocket::Options opt;
  opt.pref_dev_id = dev_id;

  Rocket::ConnectOptions connect_opt;
  connect_opt.pref_rdma_dev_id = dev_id;
  connect_opt.pref_remote_dev_id = dev_id;

  auto server = argv[1][0] == 's';

  if (server) {
    RrpcRte::Options rte_opt;
    rte_opt.tcp_port_ = port;

    RrpcRte rte(rte_opt);
    global_cm = new RdmaCM(&rte, "localhost", port, rte.get_rdma_buffer(), rte.get_buffer_size(), 4, 1024);
    InitMemPool(rte.get_rdma_buffer(),rte.get_buffer_size());

    reg_rpc_service(RPC_SIMPLE_RPC,simple_rpc);

    global_cm->DEFAULT_ROCKET_OPT = opt;
    global_cm->DEFAULT_CONNECTION_OPT = connect_opt;

    char* raw_buf = new char[BUF_SIZE];
    global_cm->register_memory(MR, raw_buf, BUF_SIZE, global_cm->DEFAULT_ROCKET_OPT.pref_dev_id);
    for (size_t i = 0; i < BUF_SIZE / msg_size; i++) {
      ::memset(raw_buf + i * msg_size, i % 8, msg_size);
    }

    while (running) {
      sleep(1);
    }
    delete global_cm;
    delete[] raw_buf;

  } else {
    //                        <data size> <enable_doorbell> <thread_num> <qp_num>
    // ./rocket_connect_test c 2048 1 1 1
    assert(argc >= 6);
    str_l = atoi(argv[2]);
    enable_doorbell = atoi(argv[3]);
    thread_num = atoi(argv[4]);
    qp_num = atoi(argv[5]);
    printf("OPTIONS: ============= str_l: %d enable_doorbell: %d ============\n", str_l, enable_doorbell);

    RrpcRte::Options rte_opt;
    RrpcRte rte(rte_opt);
    global_cm = new RdmaCM(&rte, "localhost", port, rte.get_rdma_buffer(), rte.get_buffer_size());
    global_cm->DEFAULT_CONNECTION_OPT = connect_opt;
    global_cm->DEFAULT_ROCKET_OPT = opt;
    InitMemPool(rte.get_rdma_buffer(),rte.get_buffer_size());
    

    if (thread_num > 1) {
      struct timeval starttv, endtv;
      gettimeofday(&starttv, NULL);
      for (int i = 0; i < thread_num; ++i) {
        pthread_create(&(tids[i]), NULL, single_thread_job, NULL);
      }
      for (int i = 0; i < thread_num; ++i) {
        pthread_join(tids[i], NULL);
      }
      gettimeofday(&endtv, NULL);
      uint64_t duration = ((endtv.tv_sec - starttv.tv_sec) * 1000000 + endtv.tv_usec - starttv.tv_usec);
      printf("time cost : %lf us throughput: %lf MOPS\n", duration * 1.0 / (thread_num * BATCH_SIZE * BATCHES),
             (thread_num * BATCH_SIZE * BATCHES) * 1.0 / duration);
    } else {
      Rocket::Options opt;
      Rocket rocket(opt);
      Rocket::ConnectOptions copt;
      copt.qp_num = qp_num;
      RDMA_CM_ERROR_CODE rc = rocket.connect(0, copt);
      // printf("[thread-%lu] rc: %d\n", pthread_self(), (int)rc);
      struct timeval starttv, endtv;
      gettimeofday(&starttv, NULL);
      void* msg_buf = nullptr;
      int N = BATCH_SIZE;  // batch size
      int T = BATCHES;     // batches

      while (T--) {
        // get buffer & prepare args
        FooCtx ctx;
        for (int i = 0; i < N; ++i) {
          rocket.get_msg_buf(&msg_buf, sizeof(RdmaSimpleReq) + str_l + 1, RDMA_CALLBACKS::RDMA_SIMPLE_TEST, simple_cb,
                             (void*)&ctx);
          RdmaSimpleReq* simple_req = (RdmaSimpleReq*)(msg_buf);
          simple_req->size = str_l;
          std::string str(str_l, (char)('A' + i));
          snprintf(simple_req->data, str_l + 1, "%s", str.c_str());
          simple_req->data[str_l] = '\0';
        }

        // send
        rc = rocket.send(enable_doorbell);
        if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
          printf("rocket send failed: %d\n", (int)rc);
        }
        // get reply
        while (ctx.reply_cnt != N) {
          rocket.try_poll_reply_msg();
        }
      }
      gettimeofday(&endtv, NULL);
      uint64_t duration = ((endtv.tv_sec - starttv.tv_sec) * 1000000 + endtv.tv_usec - starttv.tv_usec);
      printf("time cost : %lf us throughput: %lf MOPS\n", duration * 1.0 / (BATCH_SIZE * BATCHES),
             (BATCH_SIZE * BATCHES) * 1.0 / duration);
    }
  }
  return 0;
}