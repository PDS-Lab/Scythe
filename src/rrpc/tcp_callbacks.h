#pragma once

#include <functional>
#include <zmq.hpp>

struct RawReq {
  int req_type;
  char msg[0];
};

struct RdmaQpAttr {
  uint16_t lid;
  uint64_t qpn;
  volatile uint64_t subnet_prefix;
  volatile uint64_t interface_id;
  volatile uint64_t local_id;
  RdmaQpAttr() {}
} __attribute__((aligned(64)));

struct QPConnectReq {
  uint16_t for_one_side;
  int remote_dev_id;
  int remote_port_idx;
  RdmaQpAttr qp_attr;
  int port;
  char ip[64];
};

struct QPConnectReply {
  int reply_code;
  RdmaQpAttr qp_attr;
};

struct RocketConnectReq {
  uint64_t send_ring_addr;
  uint32_t send_ring_key;
  uint64_t recv_ring_addr;
  uint32_t recv_ring_key;
  int port;
  char ip[64];
};

struct RocketConnectReply {
  int reply_code;
  uint64_t send_ring_addr;
  uint32_t send_ring_key;
  uint64_t recv_ring_addr;
  uint32_t recv_ring_key;
};

typedef std::function<void(void*, int, zmq::socket_t*)> tcp_msg_func_t;

enum TCP_CALLBACK {
  SIMPLE_TEST = 0,
  QP_CONNECT,
  ROCKET_CONNECT,
  QP_CONNECT_WITH_ROCKET,
  // add new tcp callbacks here
  TCP_CALLBACK_NUM
};

void simple_test_callback(void* msg, int msg_sz, zmq::socket_t* socket);
void qp_connect_callback(void* msg, int msg_sz, zmq::socket_t* socket);
void rocket_connect_callback(void* msg, int msg_sz, zmq::socket_t* socket);

extern tcp_msg_func_t tcp_msg_callbacks_[];
