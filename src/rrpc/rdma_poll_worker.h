#pragma once

#include "rocket.h"
#include "util/lock.h"

class PollWorker {
 public:
  static const size_t DEFAULT_MAX_ROCKET_NUM_PER_WORKER = 1024;
  static constexpr double VALID_ROCKET_RATE_LOW_WATERMARK = 0.2;

  PollWorker(size_t max_rocket_num = DEFAULT_MAX_ROCKET_NUM_PER_WORKER) : max_rocket_num_(max_rocket_num) {
    rockets_ = (Rocket**)(malloc(sizeof(Rocket*) * max_rocket_num_));
    memset(rockets_, 0, sizeof(Rocket*) * max_rocket_num_);
    current_ = 0;
    valid_rocket_num_ = 0;
    running_ = true;
  }

  ~PollWorker() {
    running_ = false;
    pthread_join(worker_id_, NULL);
    for (int i = 0; i < max_rocket_num_; ++i) {
      if (rockets_[i]) {
        delete rockets_[i];
      }
    }
    free(rockets_);
  }

  void run() { pthread_create(&worker_id_, NULL, static_worker_job, (void*)this); }
  bool ctrl_add_rocket(Rocket* rocket);

 private:
  void worker_job();

  static void* static_worker_job(void* args) {
    printf("[RDMA_POLL_WORKER] thread-%lu start\n", pthread_self());
    PollWorker* worker = (PollWorker*)args;
    worker->worker_job();
    return NULL;
  }

  volatile bool running_;
  pthread_t worker_id_;
  size_t max_rocket_num_;

  size_t valid_rocket_num_;
  SpinLock lock_;
  volatile size_t current_;
  Rocket** rockets_;
};
