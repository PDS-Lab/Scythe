#pragma once

#include <pthread.h>

#include "tcp_callbacks.h"

extern zmq::context_t zmq_ctxt;

class TcpAdaptor {
 public:
  typedef void* server_loop(void*);

  static const int DEFAULT_SERVER_PORT = 12345;
  TcpAdaptor(int tcp_port) : server_port_(tcp_port) { running_ = false; }

  ~TcpAdaptor() {
    if (running_) {
      running_ = false;
      pthread_join(server_tid_, NULL);
    }
  }

  void run() {
    if (server_port_) {
      running_ = true;
      // spawn
      pthread_create(&server_tid_, NULL, static_server_main_loop, this);
    }
  }

  void server_main_loop(void* args);
  static void* static_server_main_loop(void* args) {
    TcpAdaptor* adaptor = (TcpAdaptor*)args;
    adaptor->server_main_loop(NULL);
    return NULL;
  }
  static bool send_to(zmq::socket_t* socket, const char* ip, const int port, void* args, size_t msg_sz,
                      TCP_CALLBACK type);

 private:
  volatile bool running_ = false;
  int server_port_;
  pthread_t server_tid_;
};