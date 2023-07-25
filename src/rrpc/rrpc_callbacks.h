#pragma once

#include <cstdint>
#include <functional>

#include "rrpc/rocket.h"

struct RdmaSimpleReq {
  uint32_t size;
  char data[0];
};

struct RdmaSimpleReply {
  uint32_t size;
  char data[0];
};

typedef std::function<void(Rocket::BatchIter*, Rocket*)> rpc_service_func_t;

enum RDMA_CALLBACKS {
  RDMA_SIMPLE_TEST = 0,
};

void rdma_simple_test_callback(Rocket::BatchIter* batch_iter, Rocket* rkt);
void reg_rpc_service(std::size_t idx, rpc_service_func_t func);

extern rpc_service_func_t rrpc_service_func[];
