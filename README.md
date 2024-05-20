# Scythe: Low-latency RDMA-enabled Distributed Transactions for Disaggregated Memory

## Brief Introduction

Scythe is a novel low-latency RDMA-enabled distributed transaction system for disaggregated memory. Scythe optimizes the latency of high-contention transactions in three approaches: 

1. Scythe proposes a hot-aware concurrency control policy that uses optimistic concurrency control (OCC) to improve transaction processing efficiency in low-conflict scenarios. Under high conflicts, Scythe designs a timestamp-ordered OCC (TOCC) strategy based on fair locking to reduce the number of retries and cross-node communication overhead. 
2. Scythe presents an RDMA-friendly timestamp service for improved timestamp management. 
3. Scythe designs an RDMA-optimized RPC framework to improve RDMA bandwidth utilization. The evaluation results show that, compared to state-of-the-art distributed transaction systems, Scythe achieves more than 2.5× lower latency with 1.8× higher throughput under high-contention workloads

## Dependencies

- Hardware
  - Mellanox RoCE NIC (e.g., ConnectX-5) that supports RDMA
  - At least 2 machines
- Software
  - Operating System: Ubuntu 18.04 LTS
  - Programming Language: C++ 11
  - Compiler: g++ 9.4.0
  - Libraries: zmq ibverbs boost_coroutine gtest gmock
  - Other：Huge page with 2GB granularity is recommended

## How to use

* Fetch Code

  ```sh
  git clone #url
  ```

* Build

  ``` shell
  mkdir build && cd build
  cmake ..
  make -j
  ```

After the build is complete, the transaction system is generated in the ``build`` folder as a static library ``libbase.a``, while the unit tests and benchmarks are generated in the ``build/test`` and ``build/benchmark`` directories

## Benchmarks

Benchmark tests are under build/benchmark,  requiring two machines as server or client respectively.

* Args Manual

  | Args            | Implication                           | Comment                                                      |
  | --------------- | ------------------------------------- | ------------------------------------------------------------ |
  | -r, --role      | the role of process(server or client) |                                                              |
  | -a, --ip        | server ip address                     |                                                              |
  | -t, --thread    | thread num                            | default:1 maximum:16                                         |
  | -c, --coro      | coroutine num of each thread          | default:1 maximum:8                                          |
  | -n, --task      | total task                            | default:40000                                                |
  | -b, --benchmark | benchmark type                        | micro/smallbank/tpcc                                         |
  | -o, --obj_num   | account num                           | default:100000                                               |
  | -e, --exponent  | exponent parameter for zipf           | default:0.5                                                  |
  | --read_ratio    | read ratio of all ops                 | only used in micro benchmark                                 |
  | --write_ratio   | write ratio of all ops                | only used in micro benchmark                                 |
  | --update_ratio  | update ratio of all ops               | only used in micro benchmark                                 |
  | --delete_ratio  | delete ratio of all ops               | only used in micro benchmark                                 |
  | --skew          | whether test key is skewed            | default:false, only used in micro benchmark                  |
  | --write_ratio   | write ratio of all ops                | only used in smallbank benchmark, one of {0, 30, 50, 70, 100}|
  
* Micro

  * Introduction

    In the Micro benchmark, we performed basic functional simulations, providing insert, delete, read and update operations, and  the degree of data skew could be configured  to test hotspot data performance

  * Running
  
    * server:

      ``` shell
    ./bench_runner -r s -a 192.168.1.51 -t 8 -b micro
      ```

    * client:

      ``` shell
    ./bench_runner -r c -a 192.168.1.51 -t 16 -c 8 -b micro --read_ratio 50 --insert_ratio 50 --obj_num 100000 --exponent 0.9 --task 10000 > log/micro.log
      ```
  
* Smallbank

  * Introduction

    Smallbank benchmark is a benchmark test designed to evaluate the performance of database management systems (DBMS). It is a simulated banking application that models common financial transaction operations such as deposits, withdrawals, and transfers. The Smallbank benchmark aims to measure the performance of a DBMS in handling concurrent transactions and workloads.

  * Running

    * server:

      ``` shell
      ./bench_runner -r s -a 192.168.1.11 -t 8 -c 8 -b smallbank
      ```

    * client:

      ``` shell
      ./bench_runner -r c -a 192.168.1.11 -t 16 -c 8 -b smallbank --write_ratio 100 --obj_num 100000 --exponent 0.9 --task 100000
      ```

* TPCC

  * Introduction

    The TPC-C (Transaction Processing Performance Council - Benchmark C) benchmark is a widely recognized and standardized benchmark used to evaluate the performance of database systems in online transaction processing (OLTP) workloads. It simulates a complex order-entry system and measures the system's ability to process transactions in a multi-user, concurrent environment. 

    Our smallbank and tpcc implementation references [FORD](https://www.usenix.org/conference/fast22/presentation/zhang-ming).

  * Running

    * server:

      ``` shell
      ./bench_runner -r s -a 192.168.1.51 -t 8 -c 8 -b tpcc
      ```

    * client:

      ``` shell
      ./bench_runner -r c -a 192.168.1.51 -t 1 -c 8 -b tpcc --task 100000
      ```

## Paper

Kai Lu, Siqi Zhao, Haikang Shan, Qiang Wei, Guankuan Li*, Jiguang Wan\*, Ting Yao, Huatao Wu, Daohui Wang. Scythe: A Low-latency RDMA-enabled Distributed Transaction System for Disaggregated Memory. ACM Transactions on Architecture and Code Optimization (TACO) 2024. (Just Accepted)