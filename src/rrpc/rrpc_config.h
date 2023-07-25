#pragma once

#include <infiniband/verbs.h>
#include <sys/time.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

typedef uint32_t rrpc_opcode_t;
using node_id = uint64_t;

const int DEFUALT_MSG_RING_BUFFER_MR_ID = 0;
const int DEFUALT_OBJ_DATA_MR_ID = 1;
const int REGION_MD_MR_ID = 2;
const int DEFUALT_MSG_RING_BUF_START_ID = 10;
const uint64_t READ_BY_ONE_SIDE_THRESHOLD = 512;
const int MAX_ONE_SIDE_BAG_NUM = 512;
const int ROCKETS_POOL_SIZE = 4;

enum class NodeType {
  kNodeInvalid = 0,
  kNodeMaster = 1,
  kNodeDN = 2,
  kNodeClient = 3,
};


enum class RDMA_CM_ERROR_CODE : uint16_t {
  CM_SUCCESS = 0,
  CM_OPEN_DEV_FAILED,
  CM_DEV_NOT_INITIALIZED,
  CM_ALLOC_PD_FAILED,
  CM_REG_MR_FAILED,
  CM_OBJ_INITIALIZED,
  CM_INACTIVE_RDMA_PORT_IDX,
  CM_CREATE_CQ_FAILED,
  CM_CREATE_QP_FAILED,
  CM_QP_READY_TO_INIT_FAILED,
  CM_QP_INIT_TO_RTR_FAILED,
  CM_QP_RTR_TO_RTS_FAILED,
  CM_POLL_CQ_FAILED,
  CM_WC_STATUS_FAILED,
  CM_STALE_RDMA_STRAWS,
  CM_POST_INVALID_WR,
  CM_POST_NO_MEM,
  CM_POST_INVALID_QP,  // qp broken (maybe disconnection)
  CM_POST_BAD_WR,
  CM_TCP_CONNECTION_FAILED,
  CM_INVALID_NODE_ID,
  CM_ALLOC_RING_BUF_FAILED,
  CM_REG_ROCKET_NUM_EXCEED_LIMIT,
};