#include <unistd.h>

#include <atomic>
#include <iostream>
#include <thread>

#include "coroutine_pool/coroutine_pool.h"
#include "coroutine_pool/scheduler.h"

int main() {
  CoroutinePool coro_pool(1, 4);
  std::atomic_int wg{0};
  auto func = [&wg](int task) {
    std::cout << "(1) This is task " << task << std::endl;
    this_coroutine::yield();
    std::cout << "(2) This is task " << task << std::endl;
    this_coroutine::yield();
    std::cout << "(3) This is task " << task << std::endl;
    this_coroutine::yield();
    wg.fetch_add(1);
  };

  coro_pool.enqueue(std::bind(func, 1));
  coro_pool.enqueue(std::bind(func, 2));
  coro_pool.enqueue(std::bind(func, 3));
  coro_pool.enqueue(std::bind(func, 4));
  coro_pool.enqueue(std::bind(func, 5));
  coro_pool.enqueue(std::bind(func, 6));

  coro_pool.start();
  while (wg.load() != 6)
    ;
  return 0;
}