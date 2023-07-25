#pragma once

#include <pthread.h>
#include <sys/mman.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "tcp_adaptor.h"
#include "util/mem_pool.h"

/*
 *   RrpcRte manages runtime environment for RRPC
 * 1. create tcp connector thread
 * 2. init huge page environment
 * 3. init mem pool environment
 *
 */
class RrpcRte {
 public:
  static const uint64_t HUGE_PAGE_SIZE = (2ULL * 1024 * 1024);

  struct Options {
    uint64_t rdma_buffer_sz_ = 2UL * 1024 * 1024 * 1024;
    uint16_t tcp_port_ = 0;  // if tcp_port_ != 0, a tcp server thread will be spawned.
    bool use_huge_page_ = true;
  };

  RrpcRte(const Options& opt) : tcp_adaptor_(opt.tcp_port_) {
    void* ptr = nullptr;
    // 1. alloc rdma buffer with huge pages.
#define ALIGN_TO_PAGE_SIZE(x) (((x) + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE * HUGE_PAGE_SIZE)
    real_rdma_buffer_sz_ = ALIGN_TO_PAGE_SIZE(opt.rdma_buffer_sz_);
    if (opt.use_huge_page_) {
      ptr =
          mmap(NULL, real_rdma_buffer_sz_, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE, -1, 0);
      if (ptr == MAP_FAILED) {
        perror("[RrpcRte] init without hugepages.");
        ptr = malloc(real_rdma_buffer_sz_);
        enable_huge_page_ = false;
        if (ptr == nullptr) {
          assert(false);
        }
      } else {
        enable_huge_page_ = true;
      }
    } else {
      ptr = malloc(real_rdma_buffer_sz_);
      enable_huge_page_ = false;
      if (ptr == nullptr) {
        assert(false);
      }
    }
    rdma_buffer_ = ptr;
    printf("[RrpcRte] rdma_buffer: %lu size: %lu\n", (uintptr_t)(rdma_buffer_), real_rdma_buffer_sz_);
  }

  ~RrpcRte() {
    if (enable_huge_page_) {
      munmap(rdma_buffer_, real_rdma_buffer_sz_);
    } else {
      free(rdma_buffer_);
    }
  }

  void run_tcp_adaptor() { tcp_adaptor_.run(); }

  void* get_rdma_buffer() { return rdma_buffer_; }
  uint64_t get_buffer_size() { return real_rdma_buffer_sz_; }

 private:
  uint64_t real_rdma_buffer_sz_;
  bool enable_huge_page_;
  void* rdma_buffer_;
  TcpAdaptor tcp_adaptor_;
};
