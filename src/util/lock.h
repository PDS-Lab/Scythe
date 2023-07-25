#pragma once

#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#define cpu_relax() asm volatile("pause\n" : : : "memory")
/**
 * Reader-Writer latch backed by pthread.h
 */
class SharedLock {
 public:
  SharedLock() { pthread_rwlock_init(&rwlock_, nullptr); };
  ~SharedLock() { pthread_rwlock_destroy(&rwlock_); }

  /**
   * Acquire a write latch.
   */
  int WLock() { return pthread_rwlock_wrlock(&rwlock_); }

  /**
   * Release a write latch.
   */
  int WUnlock() { return pthread_rwlock_unlock(&rwlock_); }

  /**
   * Acquire a read latch.
   */
  int RLock() { return pthread_rwlock_rdlock(&rwlock_); }

  /**
   * Release a read latch.
   */
  int RUnlock() { return pthread_rwlock_unlock(&rwlock_); }

 private:
  pthread_rwlock_t rwlock_;
};

static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("isb");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#endif
  // it's okay for other platforms to be no-ops
}

class alignas(64) SharedSpinLock {
 public:
  void WLock() {
    int8_t lock = 0;
    while (!lock_.compare_exchange_weak(lock, 1, std::memory_order_acquire)) {
      lock = 0;
      AsmVolatilePause();
    }
  }

  bool TryWLock() {
    int8_t lock = 0;
    return lock_.compare_exchange_weak(lock, 1, std::memory_order_acquire);
  }

  void WUnlock() { lock_.fetch_xor(1, std::memory_order_release); }

  void RLock() {
    int8_t lock = lock_.fetch_add(2, std::memory_order_acquire);
    while (lock & 1) {
      lock = lock_.load(std::memory_order_relaxed);
    }
  }

  bool TryRLock() {
    int8_t lock = lock_.fetch_add(2, std::memory_order_acquire);
    auto succ = !(lock & 1);
    if (!succ) {
      lock_.fetch_add(-2, std::memory_order_release);
    }
    return succ;
  }

  void RUnlock() { lock_.fetch_add(-2, std::memory_order_release); }

 private:
  std::atomic_int8_t lock_{0};
};

class alignas(64) SpinLock {
 public:
  void Lock() {
    while (lock_.test_and_set(std::memory_order_acquire))
      ;
  }

  void Unlock() { lock_.clear(std::memory_order_release); }

 private:
  std::atomic_flag lock_{0};
};