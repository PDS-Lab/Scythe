#ifndef BENCHMARK_SKIPLIST_WORKER_H
#define BENCHMARK_SKIPLIST_WORKER_H

#include "skiplist_schema.h"
#include "coroutine_pool/coroutine_pool.h"
#include "dtxn/dtxn.h"
#include "proto/rpc.h"
#include "rrpc/rrpc.h"
#include "storage/db.h"
#include "util/logging.h"
#include "util/rand.h"
#include "rrpc/rocket.h"
#include "util/waitgroup.h"


namespace benchmark{
class SkipListWorker{
public:
    const int MAX_DISTRIBUTED_NODE_NUM = 10;

    struct OpAndModeWithStatistics {
        SkipListBench::SkipListOp op;
        uint64_t exe_latency;
        uint64_t lock_latency;
        uint64_t write_latency;
        uint64_t commit_latency;
        uint64_t tot_latency;
    };

    SkipListWorker(int id, SkipListBench* skiplist,  
                   int remote_node_num, uint32_t task_num) 
    : id_(id), skiplist_(skiplist), remote_node_num_(remote_node_num), task_num_(task_num) {
        // memset(rkts_, 0, sizeof(rkts_));
        // Rocket::Options opt;
        // Rocket::ConnectOptions copt;
        // RDMA_CM_ERROR_CODE rc;
        // for (int i = 0; i < remote_node_num_; ++i) {
        //     rkts_[i] = new Rocket(opt);
        //     rc = rkts_[i]->connect(i, copt);
        //     assert(rc == RDMA_CM_ERROR_CODE::CM_SUCCESS);
        // }
        OpAndModeWithStatistics op_with_stat;
        for (int i = 0; i < task_num_; ++i) {
            skiplist_->set_random_op(&(op_with_stat.op));
            op_with_stats_.push_back(op_with_stat);
        }
    }
    ~SkipListWorker() = default;

    void run(){
        pthread_create(&worker_id_, NULL, static_run_wrapper, this);
    }

    void join(){
        pthread_join(worker_id_,NULL);
    }

    const std::vector<OpAndModeWithStatistics>& get_statistics() {
        return op_with_stats_;
    };
private:

    void skiplist_worker_job(){
        
    }

    static void* static_run_wrapper(void* args) {
        SkipListWorker* worker = (SkipListWorker*)args;
        worker->skiplist_worker_job();
        return NULL;
    }

    uint64_t key_;
    int                          id_;
    SkipListBench*               skiplist_;
    //dtxn::TimestampProxy*        ts_proxy_;
    int                          remote_node_num_;
    //Rocket*                      rkts_[MAX_DISTRIBUTED_NODE_NUM];
    uint32_t                     task_num_;
    pthread_t                    worker_id_;
    std::vector<OpAndModeWithStatistics> op_with_stats_;
};
};

#endif