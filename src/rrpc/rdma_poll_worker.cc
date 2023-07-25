#include "rdma_cm.h"
#include "rrpc_callbacks.h"

bool PollWorker::ctrl_add_rocket(Rocket* rocket) {
  lock_.Lock();
  if (current_ >= max_rocket_num_) {
    lock_.Unlock();
    return false;
  }
  // LOG_INFO("[%lu] Current %zu, add %p",worker_id_, current_, rocket);
  rockets_[current_] = rocket;
  asm volatile("" : : : "memory");
  current_++;
  lock_.Unlock();
  return true;
}

void PollWorker::worker_job() {
  while (running_) {
    if (current_ == 0) {
      asm volatile("pause\n" : : : "memory");
      continue;
    }
    // printf("[PollWorker] thread-%lu start\n", pthread_self());
    int cur = current_;
    Rocket* rkt = nullptr;
    for (int i = 0; i < cur; ++i) {
      rkt = rockets_[i];
      if (!rkt) continue;
      Rocket::BatchIter iter;
      if (rkt->try_poll_msg(&iter)) {
        uint32_t send_tail;
        do {
          auto service = rrpc_service_func[iter.get_rpc_id()];
          service(&iter, rkt);
          send_tail = iter.get_ack_id();
        } while (iter.Next());
        RDMA_CM_ERROR_CODE rc = rkt->send();
        assert(rc == RDMA_CM_ERROR_CODE::CM_SUCCESS);
        // recv tail + 1
        rkt->advance_recv_ring_tail();
        rkt->set_send_ring_tail(send_tail);
      }
    }
  }
}