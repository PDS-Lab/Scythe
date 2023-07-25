#pragma once
#include <sys/time.h>
#include <time.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "common.h"

#define GET_TIME                                                                                                       \
  struct timeval tv;                                                                                                   \
  gettimeofday(&tv, NULL);                                                                                             \
  static const int MAX_BUFFER_SIZE = 128;                                                                              \
  char timestamp_str[MAX_BUFFER_SIZE];                                                                                 \
  time_t sec = static_cast<time_t>(tv.tv_sec);                                                                         \
  int us = static_cast<int>(tv.tv_usec);                                                                               \
  struct tm tm_time;                                                                                                   \
  localtime_r(&sec, &tm_time);                                                                                         \
  static const char *formater = "%02d:%02d:%02d.%06d";                                                                 \
  int wsize = snprintf(timestamp_str, MAX_BUFFER_SIZE, formater, tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec, us); \
  timestamp_str[std::min(wsize, MAX_BUFFER_SIZE - 1)] = '\0';

#define TIME timestamp_str

#define DEBUG(format, ...)       \
  printf(format, ##__VA_ARGS__); \
  fflush(stdout)

#define __FILENAME__ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1) : __FILE__)

// 定义日志级别

#define LOG_LEVEL_OFF 0
#define LOG_LEVEL_FATAL 1
#define LOG_LEVEL_ERROR 2
#define LOG_LEVEL_INFO 3
#define LOG_LEVEL_DEBUG 4

#define LOG_LEVEL LOG_LEVEL_DEBUG

#if (LOG_LEVEL >= 1)
#define LOG_FATAL(format, ...)                                                                          \
  do {                                                                                                  \
    GET_TIME                                                                                            \
    if (LOG_LEVEL >= LOG_LEVEL_FATAL)                                                                   \
      DEBUG("\033[;31m[FATAL] %s %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
    fflush(stdout);                                                                                     \
    abort();                                                                                            \
  } while (0)
#else
#define LOG_FATAL(format, ...)
#endif

#if (LOG_LEVEL >= 2)
#define LOG_ERROR(format, ...)                                                                          \
  do {                                                                                                  \
    GET_TIME                                                                                            \
    if (LOG_LEVEL >= LOG_LEVEL_ERROR)                                                                   \
      DEBUG("\033[;31m[ERROR] %s %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
  } while (0)
#else
#define LOG_ERROR(format, ...)
#endif

#if (LOG_LEVEL >= 3)
#define LOG_INFO(format, ...)                                                                               \
  do {                                                                                                      \
    GET_TIME                                                                                                \
    if (LOG_LEVEL >= LOG_LEVEL_INFO)                                                                        \
      DEBUG("\033[;34m[INFO]  %s %s:%d: " format "\n\033[0m", TIME, __FILENAME__, __LINE__, ##__VA_ARGS__); \
  } while (0)
#else
#define LOG_INFO(format, ...)
#endif

#if (LOG_LEVEL >= 4)
#define LOG_DEBUG(format, ...)                                                                              \
  do {                                                                                                      \
    GET_TIME                                                                                                \
    if (LOG_LEVEL >= LOG_LEVEL_DEBUG)                                                                       \
      DEBUG("\033[;33m[DEBUG] %s %s:%d: " format "\n\033[0m", TIME, __FILENAME__, __LINE__, ##__VA_ARGS__); \
  } while (0)
#else
#define LOG_DEBUG(format, ...)
#endif

#ifndef NDEBUG
#define LOG_ASSERT(condition, format, ...)                                                                       \
  if (unlikely(!(condition))) {                                                                                  \
    GET_TIME                                                                                                     \
    DEBUG("\033[;31m %s Assertion Failed! %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
    abort();                                                                                                     \
  }
#else
#define LOG_ASSERT(condition, format, ...)
#endif

#define ENSURE(condition, format, ...)                                                                           \
  if (unlikely(!(condition))) {                                                                                  \
    GET_TIME                                                                                                     \
    DEBUG("\033[;31m %s Failed to ensure '%s' %s:%d: " format "\n\033[0m", TIME, #condition, __FILE__, __LINE__, \
          ##__VA_ARGS__);                                                                                        \
    fflush(stdout);                                                                                              \
    abort();                                                                                                     \
  }

#define INFO_IF(condition, format, ...)                                                        \
  if ((condition)) {                                                                           \
    GET_TIME                                                                                   \
    DEBUG("\033[;33m %s %s:%d: " format "\n\033[0m", TIME, __FILE__, __LINE__, ##__VA_ARGS__); \
  }
