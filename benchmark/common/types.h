#pragma once

#include <cstdint>
#include <cstddef>

using itemkey_t = uint64_t; //Data item key type, used in DB tables

struct PhasedLatency{
  Mode mode;
  struct timeval exe_start_tv;    struct timeval exe_end_tv;    double exe_latency;
  struct timeval lock_start_tv;   struct timeval lock_end_tv;   double lock_latency;
  struct timeval vali_start_tv;   struct timeval vali_end_tv;   double vali_latency;
  struct timeval write_start_tv;  struct timeval write_end_tv;  double write_latency;
  struct timeval commit_start_tv; struct timeval commit_end_tv; double commit_latency;
  struct timeval txn_start_tv;    struct timeval txn_end_tv;    double txn_latency;
};