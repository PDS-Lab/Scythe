#include <gtest/gtest.h>
#include "util/logging.h"
#include "cmdline.h"
#include "tpcc/tpcc_db.h"
#include "rrpc/rrpc.h"
#include "proto/rpc.h"
using std::string;
// ./bench_runner -r s -a 192.168.1.88 -t 10 -b tpcc
int main(int argc, char** argv){
    cmdline::parser cmd;
    cmd.add<string>("role", 'r', "the role of process", true, "", cmdline::oneof<string>("c", "s"));
    cmd.add<string>("ip", 'a', "server ip address", true);
    cmd.add<int>("thread", 't', "thread num", false, 1);
    cmd.add<int>("coro", 'c', "coroutine per thread", false, 1);
    cmd.add<int>("task", 'n', "total task",false,100000);
    cmd.add<string>("benchmark", 'b', "benchmark type", true);
    cmd.parse_check(argc,argv);
    bool server = cmd.get<string>("role") == "s";
    string bench = cmd.get<string>("benchmark");
    if(server){
        string ip = cmd.get<string>("ip");
        int thread_num = cmd.get<int>("thread");

        RrpcRte::Options rte_opt;
        rte_opt.tcp_port_ = 10456;

        RrpcRte rte(rte_opt);
        global_cm = new RdmaCM(&rte, ip, 10456, rte.get_rdma_buffer(), rte.get_buffer_size(), 4);
        InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
        RegisterService();
        if(bench == "tpcc"){
            dbs.reserve((size_t)TPCCTableType::TableNum);
            global_db = new KVEngine();
            dbs[0] = global_db;
            TPCC_SCHEMA tpcc;
            tpcc.LoadTable();
        }else if(bench == "smallbank"){
            //TODO
        }else if(bench == "YCSB"){
            //TODO
        }else if(bench == "micro"){
            //TODO
        }
        else {
            LOG_FATAL("Unexpected benchmark name, should not reach here");
        }
        while(true)
            ;
    }else{
        string ip = cmd.get<string>("ip");
        int thread_num = cmd.get<int>("thread");
        int cort_per_thread = cmd.get<int>("coro");
        int task_num = cmd.get<int>("task");

        RrpcRte::Options rte_opt;
        RrpcRte rte(rte_opt);
        global_cm = new RdmaCM(&rte, ip, 10456, rte.get_rdma_buffer(), rte.get_buffer_size());
        std::unordered_map<node_id, RdmaCM::Iport> config;
        config[0] = {ip, 10456};
        global_cm->manual_set_network_config(config);

        InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
        TPCC_SCHEMA tpcc;
        auto work_gen = tpcc.CreateWorkgenArray();
        
        delete work_gen;
        DestroyMemPool();
    }
    return 0;
}