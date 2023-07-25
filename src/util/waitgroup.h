#pragma once
// C++ WaitGroup like golang sync.WaitGroup
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
class WaitGroup {
   public:
    WaitGroup(int init = 0) : counter(init), done_flag(false) {}

    void Add(int incr = 1) {
        assert(!done_flag && "'Add' after 'Done' is not allowed");
        counter += incr;
    }

    void Done() {
        if (!done_flag) done_flag = true;
        if (--counter <= 0) cond.notify_all();
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait(lock, [&] { return counter <= 0; });
    }

    int Cnt() const { return counter.load(std::memory_order_relaxed); }

   private:
    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<int> counter;
    volatile bool done_flag;
};