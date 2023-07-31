#include "util/mem_pool.h"
#include <cstring>
#include <cstdio>
#include <cstdlib>

int main() {
    void* pool_addr = malloc(sizeof(char) * (2ULL * 1024 * 1024 * 1024));
    printf("[mem_pool_test] pool_addr:%lu\n", (uint64_t)(uintptr_t)(pool_addr));
    assert(pool_addr);

    InitMemPool(pool_addr, (2ULL * 1024 * 1024 * 1024));
    
    void* obj1 = BuddyThreadHeap::get_instance()->alloc(32);
    printf("[mem_pool_test] obj1:%lu\n", (uint64_t)(uintptr_t)(obj1));

    void* obj2 = BuddyThreadHeap::get_instance()->alloc(32);
    printf("[mem_pool_test] obj2:%lu\n", (uint64_t)(uintptr_t)(obj2));

    BuddyThreadHeap::get_instance()->free_local(obj2);
    obj2 = BuddyThreadHeap::get_instance()->alloc(32);
    printf("[mem_pool_test] obj2:%lu\n", (uint64_t)(uintptr_t)(obj2));

    BuddyThreadHeap::get_instance()->free_local(obj1);
    BuddyThreadHeap::get_instance()->free_local(obj2);
    obj1 = BuddyThreadHeap::get_instance()->alloc(100);
    printf("[mem_pool_test] obj1:%lu\n", (uint64_t)(uintptr_t)(obj1));
    // void* c1 = pool.alloc_chunk();
    // printf("[mem_pool_test] c1:%lx\n", (uint64_t)(uintptr_t)(c1));
    // void* c2 = pool.alloc_chunk();
    // printf("[mem_pool_test] c1:%lx\n", (uint64_t)(uintptr_t)(c2));
    // pool.free_chunk(c2);
    // pool.free_chunk(c1);

    // c1 = pool.alloc_chunk();
    // printf("[mem_pool_test] c1:%lx\n", (uint64_t)(uintptr_t)(c1));
    // c2 = pool.alloc_chunk();
    // printf("[mem_pool_test] c1:%lx\n", (uint64_t)(uintptr_t)(c2));
    DestroyMemPool();
    return 0;
}