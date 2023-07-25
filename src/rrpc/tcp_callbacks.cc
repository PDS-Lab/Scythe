#include "tcp_callbacks.h"

#include <zmq.hpp>

#include "rdma_cm.h"

tcp_msg_func_t tcp_msg_callbacks_[] = {simple_test_callback, qp_connect_callback, rocket_connect_callback,
                                       simple_test_callback};

void simple_test_callback(void* msg, int msg_sz, zmq::socket_t* socket) {
  char* msg_c = (char*)msg;
  printf("[RRPC][TcpAdaptor] get client msg: %s msg_sz:%u\n", msg_c, msg_sz);

  zmq::message_t reply(msg_sz + 8);
  snprintf((char*)(reply.data()), msg_sz + 8, "Hello, %s", msg_c);
  printf("[RRPC][TcpAdaptor] server send reply: %s\n", (char*)reply.data());
   socket->send(reply, zmq::send_flags::none);
}

void qp_connect_callback(void* msg, int msg_sz, zmq::socket_t* socket) {
  QPConnectReq* req = (QPConnectReq*)msg;
  std::string ip(req->ip);
  // printf("[RRPC][TcpAdaptor] get qp connect req: lid:%u qpn:%lu "
  //        "subnet_prefix:%lu interface_id:%lu local_id:%lu from ip:%s\n",
  //        req->qp_attr.lid, req->qp_attr.qpn,
  //        req->qp_attr.subnet_prefix, req->qp_attr.interface_id,
  //        req->qp_attr.local_id, ip.c_str());

  zmq::message_t reply(sizeof(QPConnectReply));
  QPConnectReply* qp_conn_reply = (QPConnectReply*)(reply.data());
  qp_conn_reply->reply_code = (int)(RDMA_CM_ERROR_CODE::CM_SUCCESS);

  int dev_id = req->remote_dev_id;
  RdmaDev* dev = nullptr;
  RDMA_CM_ERROR_CODE rc = global_cm->open_device(dev_id, &dev);

  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS && rc != RDMA_CM_ERROR_CODE::CM_OBJ_INITIALIZED) {
    qp_conn_reply->reply_code = (int)rc;
    socket->send(reply, zmq::send_flags::none);
    return;
  }

  int valid_port_cnt = dev->valid_port_ids.size();
  bool active_port = false;
  for (int i = 0; i < valid_port_cnt; ++i) {
    if (req->remote_port_idx == dev->valid_port_ids[i]) {
      active_port = true;
      break;
    }
  }
  if (!active_port) {
    qp_conn_reply->reply_code = (int)(RDMA_CM_ERROR_CODE::CM_INACTIVE_RDMA_PORT_IDX);
    socket->send(reply, zmq::send_flags::none);
    return;
  }

  RdmaQp::Options opt;
  RdmaQp rdma_qp(opt);
  rdma_qp.init_rc(dev, req->remote_port_idx, &rc);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
    qp_conn_reply->reply_code = (int)rc;
    socket->send(reply, zmq::send_flags::none);
    return;
  }

  struct ibv_port_attr port_attr;
  int res = ibv_query_port(dev->ctx, req->remote_port_idx, &port_attr);
  assert(!res);
  qp_conn_reply->qp_attr.lid = port_attr.lid;
  qp_conn_reply->qp_attr.qpn = rdma_qp.get_qp_num();
  ibv_gid gid;
  res = ibv_query_gid(dev->ctx, req->remote_port_idx, 1, &gid);
  assert(!res);
  qp_conn_reply->qp_attr.subnet_prefix = gid.global.subnet_prefix;
  qp_conn_reply->qp_attr.interface_id = gid.global.interface_id;
  qp_conn_reply->qp_attr.local_id = 1;

  rdma_qp.rc_init2rtr(req->qp_attr.qpn, req->qp_attr.lid, req->qp_attr.subnet_prefix, req->qp_attr.interface_id,
                      req->qp_attr.local_id, &rc);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
    printf("[RRPC][TcpAdaptor] qp change status to RTR failed\n");
    qp_conn_reply->reply_code = (int)rc;
    socket->send(reply, zmq::send_flags::none);
    return;
  }
  rdma_qp.rc_rtr2rts(&rc);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
    printf("[RRPC][TcpAdaptor] qp change status to RTS failed\n");
    qp_conn_reply->reply_code = (int)rc;
    socket->send(reply, zmq::send_flags::none);
    return;
  }

  std::shared_ptr<RdmaStraws> straws = global_cm->get_rdma_straws(ip, req->port);
  assert(straws);
  if (req->for_one_side) {
    printf("[qp_connect_callback] *********** one_side_qp add ***********\n");
    straws->add_one_side_qp(&rdma_qp);
  } else {
    printf("[qp_connect_callback] *********** rpc qp add ***********\n");
    straws->add_rdma_qp(&rdma_qp);
  }
  socket->send(reply, zmq::send_flags::none);
}

void rocket_connect_callback(void* msg, int msg_sz, zmq::socket_t* socket) {
  RocketConnectReq* req = (RocketConnectReq*)msg;
  std::string ip(req->ip);
  // printf("[RRPC][TcpAdaptor] get rocket connect req: send_ring_addr:%lu send_ring_key:%u "
  //        "recv_ring_addr:%lu recv_ring_key:%u from ip:%s\n",
  //        req->send_ring_addr, req->send_ring_key,
  //        req->recv_ring_addr, req->recv_ring_key,
  //        ip.c_str());

  Rocket* rocket = new Rocket(global_cm->DEFAULT_ROCKET_OPT);
  rocket->init_ring(req->send_ring_addr, req->send_ring_key, req->recv_ring_addr, req->recv_ring_key);
  rocket->set_connector(global_cm->get_rdma_straws(ip, req->port));

  zmq::message_t reply(sizeof(RocketConnectReply));
  RocketConnectReply* rocket_conn_reply = (RocketConnectReply*)(reply.data());
  rocket_conn_reply->reply_code = 0;

  RDMA_CM_ERROR_CODE rc = global_cm->reg_rocket_to_poll_worker(rocket);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS) {
    rocket_conn_reply->reply_code = (int)rc;
    socket->send(reply, zmq::send_flags::none);
    delete rocket;
    return;
  }

  rocket_conn_reply->send_ring_addr = rocket->get_send_ring_addr();
  rocket_conn_reply->send_ring_key = rocket->get_send_ring_key();
  rocket_conn_reply->recv_ring_addr = rocket->get_recv_ring_addr();
  rocket_conn_reply->recv_ring_key = rocket->get_recv_ring_key();
  socket->send(reply, zmq::send_flags::none);
}