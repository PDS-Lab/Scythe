#pragma once
#include "common.h"
#include "util/timer.h"

// 还没实现TSO，这里用rdtsc代替
class TSO {
 public:
  static timestamp_t get_ts() { return rdtsc(); }

 private:
};