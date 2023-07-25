#pragma once
#include "coroutine_pool/coroutine.h"
#include "dtxn/takeout_lock.h"

/**
 * @brief OCC单边验证
 *
 */
struct ValidateCtx {
  Coroutine* self;
};

void validate_cb(void* _ctx);

/**
 * @brief 单边解锁
 *
 */
struct UnlockCtx {
  Coroutine* self;
};

void unlock_cb(void* _ctx);

/**
 * @brief 单边轮询takelock是否准备好
 *
 */

struct PollLockCtx {
  TLP tlp;
};

void poll_lock_cb(void* _ctx);