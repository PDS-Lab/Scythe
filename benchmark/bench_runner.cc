#define TEST_PHASED_LATENCY

#include <gtest/gtest.h>
#include "util/logging.h"
#include "cmdline.h"
#include "micro/micro_txn.h"
#include "tpcc/tpcc_txn.h"
#include "smallbank/smallbank_txn.h"
#include "coroutine_pool/coroutine_pool.h"
#include "util/waitgroup.h"
using std::string;
using std::vector;
using std::cout;
using std::endl;
using benchmark::zipf_table_distribution;
thread_local zipf_table_distribution<>* zipf=nullptr;
thread_local FastRandom** f_rand=nullptr;

auto cmp_txn = [](const PhasedLatency& a,const PhasedLatency& b){
    return a.txn_latency < b.txn_latency;
};
auto cmp_exe = [](const PhasedLatency& a,const PhasedLatency& b){
    return a.exe_latency < b.exe_latency;
};
auto cmp_lock = [](const PhasedLatency& a,const PhasedLatency& b){
    return a.lock_latency < b.lock_latency;
};
auto cmp_vali = [](const PhasedLatency& a,const PhasedLatency& b){
    return a.vali_latency < b.vali_latency;
};
auto cmp_write = [](const PhasedLatency& a,const PhasedLatency& b){
    return a.write_latency < b.write_latency;
};
auto cmp_commit = [](const PhasedLatency& a,const PhasedLatency& b){
    return a.commit_latency < b.commit_latency;
};

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
    //for micro
    cmd.add<int>("read_ratio",0,"read_ratio",false,0);
    cmd.add<int>("insert_ratio",0,"insert ratio",false,0);
    cmd.add<int>("update_ratio",0,"scan ratio",false,0);
    cmd.add<int>("delete_ratio",0,"delete ratio",false,0);
    cmd.add<bool>("skew",0,"is skewed?",false);
    //for smallbank
    cmd.add<int>("write_ratio",0,"write ratio",false,50,cmdline::oneof(0,30,50,70,100));

    cmd.parse_check(argc,argv);
    bool server = cmd.get<string>("role") == "s";
    string bench = cmd.get<string>("benchmark");
    if(server){
        string ip = cmd.get<string>("ip");
        int thread_num = cmd.get<int>("thread");
        int obj_num = cmd.get<int>("obj_num");

        RrpcRte::Options rte_opt;
        rte_opt.tcp_port_ = 10456;

        RrpcRte rte(rte_opt);
        global_cm = new RdmaCM(&rte, ip, 10456, rte.get_rdma_buffer(), rte.get_buffer_size(), thread_num);
        InitMemPool(rte.get_rdma_buffer(), rte.get_buffer_size());
        RegisterService();
        MICRO* micro;
        TPCC_SCHEMA* tpcc;
        SmallBank* smallbank;
        if(bench == "tpcc"){
            dbs.resize((size_t)TPCCTableType::TableNum);
            //global_db is for debug
            global_db = new KVEngine();
            dbs[0] = global_db;
            tpcc = new TPCC_SCHEMA();
            tpcc->LoadTable();
            
            //check load
            auto db = dbs[(size_t)TPCCTableType::kDistrictTable];
            for(uint32_t w_id = 1; w_id <= tpcc->num_warehouse_; w_id++){
                for(uint32_t d_id = 1; d_id <= tpcc->num_district_per_warehouse_; d_id++){
                    tpcc_district_key_t dist_key;
                    dist_key.d_id = tpcc->MakeDistrictKey(w_id, d_id);
                    ReadResult res;
                    res.buf_size = sizeof(tpcc_district_val_t);
                    res.buf = (void*)new(tpcc_district_val_t);
                    db->get(dist_key.item_key,res,TSO::get_ts(),false);
                    tpcc_district_val_t* val = (tpcc_district_val_t*)res.buf;
                    LOG_ASSERT(strcmp(val->d_zip,"123456789")==0,"wrong zip:%s",val->d_zip);
                }
            }
            LOG_INFO("check ok");
            int32_t district_id = 1;
            tpcc_district_key_t dist_key;
            dist_key.d_id = tpcc->MakeDistrictKey(1,district_id);
            ReadResult res;
            res.buf_size = sizeof(tpcc_district_val_t);
            res.buf = (void*)new(tpcc_district_val_t);
            db->get(dist_key.item_key,res,TSO::get_ts(),false);
            tpcc_district_val_t* val = (tpcc_district_val_t*)res.buf;
            LOG_INFO("%s",val->d_zip);
            
        }else if(bench == "smallbank"){
            dbs.resize((size_t)SmallBankTableType::TableNum);
            global_db = new KVEngine();
            dbs[0] = global_db;
            smallbank = new SmallBank(obj_num);
            smallbank->LoadTable();
            LOG_INFO("Loadtable finish");

            //check load
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

        }else if(bench == "micro"){
            dbs.resize((size_t)MicroTableType::TableNum);
            global_db = new KVEngine();
            dbs[0] = global_db;
            micro = new MICRO();
            micro->LoadTable();
            //check
            auto db = dbs[(size_t)MicroTableType::kMicroTable];
            uint64_t test_seed = 114514;
            uint64_t test_id = FastRand(&test_seed);
            micro_key_t test_key;
            test_key.item_key = test_id;
            ReadResult res;
            res.buf_size = sizeof(micro_val_t);
            res.buf = (void*)new(micro_val_t);
            db->get(test_key.item_key,res,TSO::get_ts(),false);
            micro_val_t* val = (micro_val_t*)res.buf;
            for(int i=0;i<5;i++){
                LOG_ASSERT(val->magic[i]==Micro_MAGIC+i,"Unexpected magic, i:%d, magic:%ld",i,val->magic[i]);
            }
            LOG_INFO("check ok");
            //TODO
        }else {
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
            // ./bench_runner -r s -a 192.168.1.11 -t 8 -c 8 -b tpcc
            // ./bench_runner -r c -a 192.168.1.11 -t 16 -c 8 -b tpcc --task 100000
            TPCC_SCHEMA tpcc;
            tpcc.CreateWorkgenArray();
            tpcc.CreateWorkLoad(task_num);
            CoroutinePool pool(thread_num, cort_per_thread);
            struct timeval start_tv, end_tv;
            vector<PhasedLatency> latency(task_num);
            vector<int> retry_time(task_num,0);
            pool.start();
            {
                WaitGroup wg(task_num);
                gettimeofday(&start_tv,nullptr);
                for(int i=0;i<task_num;i++){
                    pool.enqueue([&wg,&tpcc,&latency,&retry_time,i](){
                        if(f_rand==nullptr){
                            f_rand = new FastRandom*[8];
                            for(int i=0;i<8;i++){
                                f_rand[i] = new FastRandom((time(nullptr) + this_coroutine::current()->id() * 114514));
                            }
                         }
                        tpcc.f_rand_ = f_rand;
                        TxnStatus rc = TxnStatus::OK;
                        Mode mode = Mode::COLD;
                        std::function<TxnStatus(TPCC_SCHEMA*,Mode,PhasedLatency*)> TxnFunc;
                        TPCCTxType op = tpcc.workload_arr_[i];
                        switch (op)
                        {
                        case TPCCTxType::kNewOrder:
                            TxnFunc = TxNewOrder; break;
                        case TPCCTxType::kDelivery:
                            TxnFunc = TxDelivery; break;
                        case TPCCTxType::kOrderStatus:
                            TxnFunc = TxOrderStatus;break;
                        case TPCCTxType::kPayment:
                            TxnFunc = TxPayment;break;
                        case TPCCTxType::kStockLevel:
                            TxnFunc = TxStockLevel;break;
                        default:
                            LOG_FATAL("Unexpected txn type for tpcc, %s",TPCC_TX_NAME[(int)op].c_str() );  
                            break;
                        }
                        int cnt = 0;
                        gettimeofday(&latency[i].txn_start_tv,nullptr);
                        //为了测试TOC热度切换，将测试指标的new order事务进行不断重试
                        if(op == TPCCTxType::kNewOrder){
                            mode = Mode::COLD;
                            gettimeofday(&latency[i].txn_start_tv,nullptr);
                            do{
                                if(rc == TxnStatus::SWITCH){
                                    mode = Mode::HOT;
                                }
#ifdef TEST_PHASED_LATENCY
                                rc = TxnFunc(&tpcc,mode,&latency[i]);
#else
                                rc = TxnFunc(&tpcc,mode,nullptr);
#endif
                                cnt ++;
                            }while(rc!=TxnStatus::OK);
                            gettimeofday(&latency[i].txn_end_tv,nullptr);
                        }
                        else {
                            mode = Mode::COLD;
                            rc = TxnFunc(&tpcc,mode,nullptr);
                        }
                        latency[i].mode = mode;
                        retry_time[i] = cnt;
                        wg.Done();
                    });
                }
                wg.Wait();
                gettimeofday(&end_tv,nullptr);
                uint64_t tot = ((end_tv.tv_sec - start_tv.tv_sec) * 1000000 + end_tv.tv_usec - start_tv.tv_usec);
                printf("============================ Throughput:%lf MOPS =========================\n", 
                task_num *FREQUENCY_NEW_ORDER /100 * 1.0 / tot);

                for(int i=0;i<latency.size();i++){
                    if(tpcc.workload_arr_[i] == TPCCTxType::kNewOrder){
                        latency[i].txn_latency = ((latency[i].txn_end_tv.tv_sec - latency[i].txn_start_tv.tv_sec) * 1000000 + latency[i].txn_end_tv.tv_usec - latency[i].txn_start_tv.tv_usec);
                        latency[i].exe_latency = ((latency[i].exe_end_tv.tv_sec - latency[i].exe_start_tv.tv_sec) * 1000000 + latency[i].exe_end_tv.tv_usec - latency[i].exe_start_tv.tv_usec);
                        latency[i].lock_latency = ((latency[i].lock_end_tv.tv_sec - latency[i].lock_start_tv.tv_sec) * 1000000 + latency[i].lock_end_tv.tv_usec - latency[i].lock_start_tv.tv_usec);
                        if(latency[i].mode == Mode::COLD)latency[i].vali_latency = ((latency[i].vali_end_tv.tv_sec - latency[i].vali_start_tv.tv_sec) * 1000000 + latency[i].vali_end_tv.tv_usec - latency[i].vali_start_tv.tv_usec);
                        latency[i].write_latency = ((latency[i].write_end_tv.tv_sec - latency[i].write_start_tv.tv_sec) * 1000000 + latency[i].write_end_tv.tv_usec - latency[i].write_start_tv.tv_usec);
                        latency[i].commit_latency = ((latency[i].commit_end_tv.tv_sec - latency[i].commit_start_tv.tv_sec) * 1000000 + latency[i].commit_end_tv.tv_usec - latency[i].commit_start_tv.tv_usec);
                    }
                }
                vector<int> neworder_retrytime;
                vector<PhasedLatency> neworder_latency;
                vector<PhasedLatency> occ_latency;
                vector<PhasedLatency> toc_latency;
                for(int i=0;i<task_num;i++){
                    if(tpcc.workload_arr_[i] == TPCCTxType::kNewOrder){
                        if(latency[i].mode == Mode::COLD){
                            occ_latency.emplace_back(latency[i]);
                        }else{
                            toc_latency.emplace_back(latency[i]);
                        }
                        neworder_latency.emplace_back(latency[i]);
                        neworder_retrytime.emplace_back(retry_time[i]);
                    }
                }
                std::sort(neworder_latency.begin(),neworder_latency.end(),cmp_txn);
                printf("p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",neworder_latency[(neworder_latency.size())/2-1].txn_latency,neworder_latency[(neworder_latency.size())*99/100-1].txn_latency,neworder_latency[(neworder_latency.size())*999/1000-1].txn_latency);
                printf("============================ OCC:%zu =========================\n",occ_latency.size());
                
                printf("============================ TOC:%zu =========================\n",toc_latency.size());

                std::sort(neworder_retrytime.begin(),neworder_retrytime.end());
                int sum = std::accumulate(neworder_retrytime.begin(),neworder_retrytime.end(),0);
                printf("retry time: mid-val:%d, avg:%f\n",neworder_retrytime[neworder_retrytime.size()/2-1],sum*1.0/neworder_retrytime.size());
#ifdef TEST_PHASED_LATENCY
                std::sort(neworder_latency.begin(),neworder_latency.end(),cmp_exe);
                printf("exe: p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",neworder_latency[(neworder_latency.size())/2-1].exe_latency,neworder_latency[(neworder_latency.size())*99/100-1].exe_latency,neworder_latency[(neworder_latency.size())*999/1000-1].exe_latency);

                std::sort(neworder_latency.begin(),neworder_latency.end(),cmp_lock);
                printf("lock: p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",neworder_latency[(neworder_latency.size())/2-1].lock_latency,neworder_latency[(neworder_latency.size())*99/100-1].lock_latency,neworder_latency[(neworder_latency.size())*999/1000-1].lock_latency);

                std::sort(neworder_latency.begin(),neworder_latency.end(),cmp_write);
                printf("write: p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",neworder_latency[(neworder_latency.size())/2-1].write_latency,neworder_latency[(neworder_latency.size())*99/100-1].write_latency,neworder_latency[(neworder_latency.size())*999/1000-1].write_latency);

                std::sort(neworder_latency.begin(),neworder_latency.end(),cmp_commit);
                printf("commit: p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",neworder_latency[(neworder_latency.size())/2-1].commit_latency,neworder_latency[(neworder_latency.size())*99/100-1].commit_latency,neworder_latency[(neworder_latency.size())*999/1000-1].commit_latency);

                std::sort(occ_latency.begin(),occ_latency.end(),cmp_vali);
                printf("vali: p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",occ_latency[(occ_latency.size())/2-1].vali_latency,occ_latency[(occ_latency.size())*99/100-1].vali_latency,occ_latency[(occ_latency.size())*999/1000-1].vali_latency);
#endif                
            }
        }else if(bench == "smallbank"){
            // ./bench_runner -r s -a 192.168.1.11 -t 8 -c 8 -b smallbank
            // ./bench_runner -r c -a 192.168.1.11 -t 16 -c 8 -b smallbank --write_ratio 100 --obj_num 100000 --exponent 0.9 --task 100000 > log/sbDEBUGtest1.log
            int write_ratio = cmd.get<int>("write_ratio");
            int range = cmd.get<int>("obj_num");
            double exponent = cmd.get<double>("exponent");
            
            SmallBank smallbank(range);
            smallbank.CreateWorkgenArray(write_ratio);
            smallbank.CreateWorkLoad(task_num,range,exponent);
            CoroutinePool pool(thread_num,cort_per_thread);
            struct timeval start_tv, end_tv;
            vector<PhasedLatency> latency(task_num);
            vector<int> retry_time(task_num,0);
            
            pool.start();
            {
                WaitGroup wg(task_num);
                gettimeofday(&start_tv,nullptr);
                for(int i=0;i<task_num;i++){
                    pool.enqueue([&wg,&smallbank,&latency,&retry_time,i,range,exponent](){
                        if(zipf == nullptr){
                            //Every thread should init its own zipf first.
                            LOG_DEBUG("zipf==nullptr");
                            zipf = new zipf_table_distribution<>(range,exponent);
                            smallbank.zipf = zipf;
                        }
                        auto op = smallbank.workload_arr_[i];
                        smallbank.zipf = zipf;
                        TxnStatus rc = TxnStatus::OK;
                        Mode mode = Mode::COLD;
                        std::function<TxnStatus(SmallBank*,Mode,PhasedLatency*)> TxnFunc;
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
                                LOG_FATAL("Unexpected txn type for smallbank, %d",(int)op.TxType);
                                break;
                        }
                        int cnt = 0;
                        gettimeofday(&latency[i].txn_start_tv,nullptr);
                        do{
                            //mode = Mode::COLD;
                            if(rc == TxnStatus::SWITCH){
                                mode = Mode::HOT;
                            }
                            rc = TxnFunc(&smallbank,mode,&latency[i]);
                            cnt ++;
                        }while(rc!=TxnStatus::OK);
                        gettimeofday(&latency[i].txn_end_tv,nullptr);
                        latency[i].mode = mode;
                        retry_time[i] = cnt;
                        wg.Done();
                    });
                }
                wg.Wait();
                gettimeofday(&end_tv,nullptr);
                uint64_t tot = ((end_tv.tv_sec - start_tv.tv_sec) * 1000000 + end_tv.tv_usec - start_tv.tv_usec);
                printf("============================ Throughput:%lf MOPS =========================\n", 
                task_num * 1.0 / tot);
                //phased perf

                for(int i=0;i<latency.size();i++){
                    latency[i].txn_latency = ((latency[i].txn_end_tv.tv_sec - latency[i].txn_start_tv.tv_sec) * 1000000 + latency[i].txn_end_tv.tv_usec - latency[i].txn_start_tv.tv_usec);
                    latency[i].exe_latency = ((latency[i].exe_end_tv.tv_sec - latency[i].exe_start_tv.tv_sec) * 1000000 + latency[i].exe_end_tv.tv_usec - latency[i].exe_start_tv.tv_usec);
                    latency[i].lock_latency = ((latency[i].lock_end_tv.tv_sec - latency[i].lock_start_tv.tv_sec) * 1000000 + latency[i].lock_end_tv.tv_usec - latency[i].lock_start_tv.tv_usec);
                    if(latency[i].mode == Mode::COLD)latency[i].vali_latency = ((latency[i].vali_end_tv.tv_sec - latency[i].vali_start_tv.tv_sec) * 1000000 + latency[i].vali_end_tv.tv_usec - latency[i].vali_start_tv.tv_usec);
                    latency[i].write_latency = ((latency[i].write_end_tv.tv_sec - latency[i].write_start_tv.tv_sec) * 1000000 + latency[i].write_end_tv.tv_usec - latency[i].write_start_tv.tv_usec);
                    latency[i].commit_latency = ((latency[i].commit_end_tv.tv_sec - latency[i].commit_start_tv.tv_sec) * 1000000 + latency[i].commit_end_tv.tv_usec - latency[i].commit_start_tv.tv_usec);
                }
                
                std::sort(latency.begin(),latency.end(),cmp_txn);
                printf("p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",latency[(latency.size())/2-1].txn_latency,latency[(latency.size())*99/100-1].txn_latency,latency[(latency.size())*999/1000-1].txn_latency);

                vector<PhasedLatency> occ_latency;
                vector<PhasedLatency> toc_latency;
                for(auto lat : latency){
                    if(lat.mode == Mode::COLD){
                        occ_latency.push_back(lat);
                    }else{
                        toc_latency.push_back(lat);
                    }
                }
                printf("============================ OCC:%zu =========================\n",occ_latency.size());
                printf("============================ TOC:%zu =========================\n",toc_latency.size());
            }
        }else if(bench == "micro"){
            // ./bench_runner -r s -a 192.168.1.11 -t 8 -c 8 -b micro
            // ./bench_runner -r c -a 192.168.1.11 -t 16 -c 8 -b micro --read_ratio 50 --insert_ratio 50 --obj_num 100000 --exponent 0.9 --task 100000 --skew 1 > log/micro1.log
            int read_ratio = cmd.get<int>("read_ratio");
            int insert_ratio = cmd.get<int>("insert_ratio");
            int update_ratio = cmd.get<int>("update_ratio");
            int delete_ratio = cmd.get<int>("delete_ratio");
            int obj_num = cmd.get<int>("obj_num");
            double exponent = cmd.get<double>("exponent");
            bool is_skewed = cmd.get<bool>("skew");

            MICRO micro(obj_num);
            micro.CreateWorkgenArray(read_ratio,insert_ratio,update_ratio,delete_ratio);
            micro.CreateWorkLoad(is_skewed,task_num,obj_num,exponent);
            CoroutinePool pool(thread_num,cort_per_thread);
            vector<double> latency(task_num);
            struct timeval start_tv, end_tv;
            pool.start();
            {
                WaitGroup wg(task_num);
                gettimeofday(&start_tv,nullptr);
                for(int i=0;i<task_num;i++){
                    pool.enqueue([&wg,&micro,&latency,i,obj_num,exponent,is_skewed](){
                        if(zipf == nullptr){
                            //Every thread should init its own zipf first.
                            LOG_DEBUG("zipf==nullptr");
                            zipf = new zipf_table_distribution<>(obj_num,exponent);
                            micro.zipf = zipf;
                        }
                        auto op = micro.workload_arr_[i];
                        micro.zipf = zipf;
                        TxnStatus rc = TxnStatus::OK;
                        Mode mode = Mode::COLD;
                        std::function<TxnStatus(MICRO*, Mode, bool, int)>func;
                        switch (op.TxType)
                        {
                        case MICROTxType::kRead:
                            func = TxRead;break;
                        case MICROTxType::kInsert:
                            func = TxInsert;break;
                        case MICROTxType::kUpdate:
                            func = TxUpdate;break;
                        case MICROTxType::kDelete:
                            func = TxDelete;break;
                        default:
                            LOG_FATAL("Unexpected txn type");
                            break;
                        }
                        struct timeval txn_start_tv, txn_end_tv;
                        gettimeofday(&txn_start_tv,nullptr);
                        do{
                            if(rc == TxnStatus::SWITCH){
                                mode = Mode::HOT;
                            }
                            rc = func(&micro,mode,is_skewed,i);
                        }while(rc!=TxnStatus::OK);
                        gettimeofday(&txn_end_tv,nullptr);
                        latency[i] = ((txn_end_tv.tv_sec-txn_start_tv.tv_sec)*1000000 + (txn_end_tv.tv_usec-txn_start_tv.tv_usec));
                        wg.Done();
                    });
                }
                wg.Wait();
                gettimeofday(&end_tv,nullptr);
                uint64_t tot = ((end_tv.tv_sec - start_tv.tv_sec) * 1000000 + end_tv.tv_usec - start_tv.tv_usec);
                printf("============================ Throughput:%lf MOPS =========================\n", 
                task_num * 1.0 / tot);
            }
            std::sort(latency.begin(),latency.end());
            printf("p50 latency:%lf, p99 latency:%lf, p999 latency:%lf\n",latency[(latency.size())/2-1],latency[(latency.size())*99/100-1],latency[(latency.size())*999/1000-1]);
        }
        else {
            LOG_FATAL("Unexpected benchmark name, should not reach here");
        }
        DestroyMemPool();
    }
    return 0;
}