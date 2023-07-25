#pragma once

#include <infiniband/verbs.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>

struct MemoryAttr {
    uintptr_t ptr{0};
    uint32_t lkey{0};
    uint32_t rkey{0};
};
// 负责创建 mr，记录 mr 信息
class MemoryRegion {
   public:
    static const int DEFAULT_PROTECTION_FLAG = (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                                IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);

    MemoryRegion(uintptr_t addr, uint64_t len, ibv_pd* pd, int flag = DEFAULT_PROTECTION_FLAG)
        : addr_(addr), len_(len), mr_(ibv_reg_mr(pd, (void*)addr, len, flag)) {
        // perror("ibv_reg_mr");
        assert(mr_ != nullptr);
        mattr_.ptr = addr;
        mattr_.rkey = mr_->rkey;
        mattr_.lkey = mr_->lkey;
    }

    ~MemoryRegion() {
        assert(mr_ != nullptr);
        int rc = ibv_dereg_mr(mr_);
        assert(rc == 0);
    }

    bool valid() { return mr_ != nullptr; }

    MemoryAttr getMemoryAttr() const { return mattr_; }

   public:
    uintptr_t addr_;
    uint64_t len_;
    MemoryAttr mattr_;
    struct ibv_mr* mr_ = nullptr;
};
