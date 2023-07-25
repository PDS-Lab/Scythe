#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

using timestamp_t = uint64_t;
using data_t = char;

#define KB(x) ((x)*1024ul)
#define MB(x) ((x)*1024 * 1024ul)
#define GB(x) ((x)*1024 * 1024 * 1024ul)
#define OFFSET(TYPE, MEMBER) ((unsigned long)(&(((TYPE *)0)->MEMBER)))

constexpr timestamp_t LATEST = ~0ull;
constexpr uint16_t kColdWatermark = 3;
constexpr uint64_t kHotWatermark = 10;
constexpr size_t kMaxObjectSize = 1024;

// transaction execution mode
enum class Mode : uint8_t { COLD, HOT };

enum class DbStatus : uint8_t {
  OK = 0,
  NOT_EXIST,
  OBSOLETE,
  LOCKED,
  DUPLICATE,
  ALLOC_FAILED,
  UNEXPECTED_ERROR,
};

static inline std::string Status2Str(DbStatus s) {
  switch (s) {
    case DbStatus::OK:
      return "OK";
    case DbStatus::NOT_EXIST:
      return "NOT_EXIST";
    case DbStatus::LOCKED:
      return "LOCKED";
    case DbStatus::OBSOLETE:
      return "OBSOLETE";
    case DbStatus::ALLOC_FAILED:
      return "ALLOC_FAILED";
    case DbStatus::UNEXPECTED_ERROR:
      return "UNEXPECTED_ERROR";
    case DbStatus::DUPLICATE:
      return "DUPLICATE";
  }
  return "<Unkonwn>";
}

constexpr uint64_t RTT = 6;  // 偷个懒，先固定RTT为6us，实际上需要实时计算滑动平均值