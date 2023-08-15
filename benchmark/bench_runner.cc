#include <gtest/gtest.h>
#include "util/logging.h"
#include "cmdline.h"
#include "tpcc/tpcc_txn.h"
#include "smallbank/smallbank_txn.h"
#include "coroutine_pool/coroutine_pool.h"
#include "util/waitgroup.h"
using std::string;
using std::vector;
using std::cout;
using std::endl;
using benchmark::zipf_table_distribution;
thread_local zipf_table_distribution<>* zipf;

// ./bench_runner -r s -a 192.168.1.88 -t 8 -c 8 -b tpcc
// ./bench_runner -r c -a 192.168.1.88 -t 2 -c 8 -b tpcc

int main(int argc, char** argv){
    cmdline::parser cmd;
    cmd.add<string>("role", 'r', "the role of process", true, "", cmdline::oneof<string>("c", "s"));
    cmd.add<string>("ip", 'a', "server ip address", true);
    cmd.add<int>("thread", 't', "thread num", false, 1);
    cmd.add<int>("coro", 'c', "coroutine per thread", false, 1);
    cmd.add<int>("task", 'n', "total task",false,40000);
    cmd.add<string>("benchmark", 'b', "benchmark type", true);
    cmd.add<int>("obj_num",'o',"acct num for smallbank",false,100000);
    cmd.add<double>("exponent",'e',"exponent parameter for zipf",false,0.5);
    cmd.add<int>("write_ratio",0,"write ratio",false,50,cmdline::oneof(0,30,50,70,100));
    cmd.parse_check(argc,argv);
    bool server = cmd.get<string>("role") == "s";
    string bench = cmd.get<string>("benchmark");
    if(server){
        string ip = cmd.get<string>("ip");
        int thread_num = cmd.get<int>("thread");

        RrpcRte::Options rte_opt;
        rte_opt.tcp_port_ = 10456;

        RrpcRte rte(rte_opt);
        global_cm = new RdmaCM(&rte, ip, 10456, rte.get_rdma_buffer(), rte.get_buffer_size(), thread_num);
        InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
        RegisterService();
        TPCC_SCHEMA* tpcc;
        SmallBank* smallbank;
        if(bench == "tpcc"){
            dbs.resize((size_t)TPCCTableType::TableNum);
            //global_db is for debug
            global_db = new KVEngine();
            dbs[0] = global_db;
            tpcc = new TPCC_SCHEMA();
            tpcc->LoadTable();
            
            auto db = dbs[(size_t)TPCCTableType::kWarehouseTable];
            int32_t warehouse_id = 1;
            tpcc_warehouse_key_t ware_key;
            ware_key.w_id = warehouse_id;
            ReadResult res;
            res.buf_size = sizeof(tpcc_warehouse_val_t);
            res.buf = (void*)new(tpcc_warehouse_val_t);
            db->get(ware_key.item_key,res,TSO::get_ts(),false);
            tpcc_warehouse_val_t* val = (tpcc_warehouse_val_t*)res.buf;
            LOG_INFO("%s",val->w_zip);
            
        }else if(bench == "smallbank"){
            dbs.resize((size_t)SmallBankTableType::TableNum);
            global_db = new KVEngine();
            dbs[0] = global_db;
            smallbank = new SmallBank();
            smallbank->LoadTable();
            LOG_INFO("Loadtable finish");

            auto db = dbs[(size_t)SmallBankTableType::kSavingsTable];
            int32_t acct_id = 6;
            smallbank_savings_key_t saving_key;
            smallbank_checking_key_t checking_key;
            saving_key.item_key = acct_id;
            checking_key.item_key = acct_id;

            ReadResult res;
            res.buf_size = sizeof(smallbank_savings_val_t);
            res.buf = (void*)new(smallbank_savings_val_t);
            db->get(saving_key.item_key,res,TSO::get_ts(),false);
            smallbank_savings_val_t* saving_val = (smallbank_savings_val_t*)res.buf;
            LOG_INFO("saving %u",saving_val->magic);

            db = dbs[(size_t)SmallBankTableType::kCheckingTable];
            res.buf_size = sizeof(smallbank_checking_val_t);
            res.buf = (void *)new (smallbank_checking_val_t);
            db->get(checking_key.item_key,res,TSO::get_ts(),false);
            smallbank_checking_val_t* checking_val = (smallbank_checking_val_t*)res.buf;
            LOG_INFO("checking %u",checking_val->magic);

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
        string bench = cmd.get<string>("benchmark");
        RrpcRte::Options rte_opt;
        RrpcRte rte(rte_opt);
        global_cm = new RdmaCM(&rte, ip, 10456, rte.get_rdma_buffer(), rte.get_buffer_size());
        std::unordered_map<node_id, RdmaCM::Iport> config;
        config[0] = {ip, 10456};
        global_cm->manual_set_network_config(config);

        InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
        if(bench == "tpcc"){
            CoroutinePool pool(thread_num, cort_per_thread);
            TPCC_SCHEMA tpcc;
            pool.start();
            {
                WaitGroup wg(task_num);
                pool.enqueue([&wg,task_num,&tpcc](){
                    for(int i=0;i<task_num;i++){
                        TxnStatus rc=TxnStatus::OK;
                        Mode mode = Mode::COLD;
                        do{
                            mode = Mode::COLD;
                            if(rc == TxnStatus::SWITCH){
                                mode = Mode::HOT;
                            }
                            rc = TxNewOrder(&tpcc,mode);
                        }while(rc!=TxnStatus::OK);
                    }
                });
                wg.Wait();
            }
        }else if(bench == "smallbank"){
            // ./bench_runner -r s -a 192.168.1.88 -t 8 -c 8 -b smallbank
            // ./bench_runner -r c -a 192.168.1.88 -t 4 -c 8 -b smallbank --write_ratio 70 --obj_num 100000 --exponent 0.5 --task 100000 > log/sbtest.log
            int write_ratio = cmd.get<int>("write_ratio");
            int range = cmd.get<int>("obj_num");
            double exponent = cmd.get<double>("exponent");
            
            SmallBank smallbank;
            smallbank.CreateWorkgenArray(write_ratio);
            smallbank.CreateWorkLoad(task_num,range,exponent);
            CoroutinePool pool(thread_num,cort_per_thread);
            struct timeval start_tv, end_tv;
            vector<double> latency(task_num,0);
            vector<int> retry_time(task_num,0);
            pool.start();
            // for(int i=0;i<100;i++){
            //     std::cout <<i<<": "<<SmallBank_TX_NAME[(int)smallbank.workgen_arr_[i]]<<std::endl;
            // }
            // std::cout<<"workload"<<std::endl;
            // for(int i=0;i<task_num;i++){
            //     std::cout <<i<<": "<<SmallBank_TX_NAME[(int)smallbank.workload_arr_[i].TxType];
            //     if(!(i%5)) std::cout << std::endl;
            // }
            // std::cout << std::endl;
            {
                WaitGroup wg(task_num);
                gettimeofday(&start_tv,nullptr);
                for(int i=0;i<task_num;i++){
                    auto op = smallbank.workload_arr_[i];
                    pool.enqueue([&wg,&smallbank,&op,&latency,&retry_time,i,range,exponent](){
                        if(zipf == nullptr){
                            //Every thread should init its own zipf first.
                            LOG_DEBUG("zipf==nullptr");
                            zipf = new zipf_table_distribution<>(range,exponent);
                        }
                        smallbank.zipf = zipf;
                        TxnStatus rc = TxnStatus::OK;
                        Mode mode = Mode::COLD;
                        std::function<TxnStatus(SmallBank*,Mode)> TxnFunc;
                        struct timeval txn_start_tv, txn_end_tv;
                        switch(op.TxType){
                            case SmallBankTxType::kAmalgamate:
                                TxnFunc = TxAmalgamate;break;
                            case SmallBankTxType::kBalance:
                                TxnFunc = TxBalance;break;
                            case SmallBankTxType::kDepositChecking:
                                TxnFunc = TxDepositChecking;break;
                            case SmallBankTxType::kSendPayment:
                                TxnFunc = TxSendPayment;break;
                            case SmallBankTxType::kTransactSaving:
                                TxnFunc = TxTransactSaving;break;
                            case SmallBankTxType::kWriteCheck:
                                TxnFunc = TxWriteCheck;break;
                            default:
                                LOG_FATAL("Unexpected txn type, %d",(int)op.TxType);
                                break;
                        }
                        int cnt = 0;
                        gettimeofday(&txn_start_tv,nullptr);
                        do{
                            //mode = Mode::COLD;
                            if(rc == TxnStatus::SWITCH){
                                //mode = Mode::HOT;
                            }
                            rc = TxnFunc(&smallbank,mode);
                            cnt ++;
                        }while(rc!=TxnStatus::OK);
                        gettimeofday(&txn_end_tv,nullptr);
                        uint64_t txn_tot = ((txn_end_tv.tv_sec - txn_start_tv.tv_sec) * 1000000 + txn_end_tv.tv_usec - txn_start_tv.tv_usec);
                        latency[i] = txn_tot;
                        retry_time[i] = cnt;
                        wg.Done();
                    });
                }
                wg.Wait();
                gettimeofday(&end_tv,nullptr);
                uint64_t tot = ((end_tv.tv_sec - start_tv.tv_sec) * 1000000 + end_tv.tv_usec - start_tv.tv_usec);
                printf("============================ Throughput:%lf MOPS =========================\n", 
                 task_num * 1.0 / tot);
                std::sort(latency.begin(),latency.end());
                printf("p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",latency[(latency.size())/2-1],latency[(latency.size())*99/100-1],latency[(latency.size())*999/1000-1]);

                // WaitGroup wg(1);
                // pool.enqueue([&wg](){
                //     SmallBank sb;
                //     SbTxTestReadWrite();
                //     wg.Done();
                // });
                // wg.Wait();
            }
            
        }else if(bench == "YCSB"){
            //TODO
        }else if(bench == "micro"){
            //TODO
        }
        else {
            LOG_FATAL("Unexpected benchmark name, should not reach here");
        }
        DestroyMemPool();
    }
    return 0;
}