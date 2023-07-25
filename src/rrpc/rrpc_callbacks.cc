#include "rrpc_callbacks.h"

#include "rdma_cm.h"

#define MAX_RRPC_SERVICE_NUM 1024

rpc_service_func_t rrpc_service_func[MAX_RRPC_SERVICE_NUM] = {
    rdma_simple_test_callback,
};

void reg_rpc_service(size_t idx, rpc_service_func_t func) {
  assert(idx < MAX_RRPC_SERVICE_NUM);
  rrpc_service_func[idx] = func;
}

void rdma_simple_test_callback(Rocket::BatchIter* batch_iter, Rocket* rkt) {
  static int recv_num = 0;
  recv_num++;
  auto* req = batch_iter->get_request<RdmaSimpleReq>();

  uint32_t cstr_len = req->size + 1;
  // printf("[RRPC][SIMPLE_TEST] get req msg: %c(%d) cb_addr:%lu\n",
  //         req->data[0], cstr_len, batch_iter->get_callback_func_addr());
  // Hello,

  auto simple_reply = rkt->gen_reply<RdmaSimpleReply>(sizeof(RdmaSimpleReply) + cstr_len + 7, batch_iter);
  // Rocket::Msg reply(rkt, sizeof(RdmaSimpleReply) + cstr_len + 7);
  assert(simple_reply);
  simple_reply->size = cstr_len + 6;
  snprintf(simple_reply->data, cstr_len + 7, "Hello, %s", req->data);
  simple_reply->data[cstr_len + 6] = '\0';
  // printf("receive msg %d\n", recv_num);
}