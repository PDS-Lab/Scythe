#include "tcp_adaptor.h"

#include <unistd.h>

#include <zmq.hpp>
zmq::context_t zmq_ctxt(1);

void TcpAdaptor::server_main_loop(void* args) {
  (void)args;
  pthread_detach(pthread_self());

  zmq::socket_t socket(zmq_ctxt, ZMQ_REP);
  char address[30] = "";
  sprintf(address, "tcp://*:%d", server_port_);
  printf("[RRPC][TcpAdaptor] : listener binding: %s\n", address);
  try {
    socket.bind(address);
  } catch (...) {
    return;
  }

  while (running_) {
    zmq::message_t request;
    zmq::recv_result_t ret;
    try {
      ret = socket.recv(request, zmq::recv_flags::dontwait);
    } catch (...) {
      printf("zmq Unexpected error, exit\n");
      return;
    }
    if (ret.has_value()) {
      RawReq* req = (RawReq*)(request.data());
      int req_type = req->req_type;
      assert(req_type >= 0 && req_type < TCP_CALLBACK::TCP_CALLBACK_NUM);
      tcp_msg_callbacks_[req_type]((void*)(req->msg), request.size() - offsetof(RawReq, msg), &socket);
    }
    usleep(1000);
  }
}

bool TcpAdaptor::send_to(zmq::socket_t* socket, const char* ip, const int port, void* args, size_t msg_sz,
                         TCP_CALLBACK type) {
  char address[30];
  snprintf(address, 30, "tcp://%s:%d", ip, port);
  // printf("[RRPC][TcpAdaptor] conn to %s\n", address);
  socket->connect(address);  // block
  try {
    zmq::message_t request(sizeof(RawReq) + msg_sz);
    RawReq* req = (RawReq*)(request.data());
    req->req_type = type;
    memcpy(req->msg, args, msg_sz);
    socket->setsockopt(ZMQ_SNDTIMEO, 1000);
    socket->send(request, zmq::send_flags::none);
  } catch (...) {
    printf("[RRPC][TcpAdaptor] send failed\n");
    return false;
  }
  return true;
}