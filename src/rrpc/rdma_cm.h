#pragma once

#include <unordered_map>
#include <vector>

#include "rdma_poll_worker.h"
#include "rrpc/mr.h"
#include "rrpc/rocket.h"
#include "rrpc/rrpc_config.h"
#include "rrpc_rte.h"
#include "util/lock.h"

struct RdmaDev {
  int dev_id;
  struct ibv_context* ctx = nullptr;
  struct ibv_pd* pd = nullptr;
  struct ibv_mr* mr = nullptr;
  bool inited;  // if inited, RdmaDev is thread-safe.
  std::vector<int> valid_port_ids;
  SpinLock dev_lock_;

  RdmaDev(int dev_idx) : dev_id(dev_idx) {
    ctx = nullptr;
    pd = nullptr;
    inited = false;
  }
};

/*
 *   A simple rdma connection manager (RdmaCtrl in RRPC)
 * singular class
 */
class RdmaCM {
 public:
  struct Iport {
    std::string ip;
    int port;
  };

  static int get_local_ip(char* outip);
  RdmaCM(RrpcRte* rte, std::string master_ip, int master_port, void* rdma_buffer, uint64_t rdma_buffer_size,
         int poll_worker_num = 0, int max_rocket_num_per_worker = PollWorker::DEFAULT_MAX_ROCKET_NUM_PER_WORKER);

  ~RdmaCM();

  void query_devinfo();
  RDMA_CM_ERROR_CODE get_iport(node_id node_id, Iport* iport) {
    network_config_lock_.Lock();
    if (network_config_.find(node_id) == network_config_.end()) {
      return RDMA_CM_ERROR_CODE::CM_INVALID_NODE_ID;
    }
    *iport = network_config_[node_id];
    network_config_lock_.Unlock();
    return RDMA_CM_ERROR_CODE::CM_SUCCESS;
  }
  RDMA_CM_ERROR_CODE open_device(int dev_id, RdmaDev** res);
  RDMA_CM_ERROR_CODE get_mr_rkey_by_dev_id(int dev_id, uint32_t* rkey);
  // RDMA_CM_ERROR_CODE create_rc_qp(int dev_id, int port_idx, const std::string& remote_ip);
  std::shared_ptr<RdmaStraws> get_rdma_straws(const std::string& ip, int port);

  RDMA_CM_ERROR_CODE disconnect_straws(const std::string& ip, int port);
  RDMA_CM_ERROR_CODE reg_rocket_to_poll_worker(Rocket* rocket);

  // Note: for simple test, don't use.
  void manual_set_network_config(std::unordered_map<node_id, Iport>& config);

  void add_node(node_id node, std::string ip, int port);
  RDMA_CM_ERROR_CODE remove_node(node_id node, std::string ip, int port);
  // for user.
  bool register_memory(int mr_id, char* buf, uint64_t size, int dev_id,
                       int flag = MemoryRegion::DEFAULT_PROTECTION_FLAG);
  // NOT thread-safe with register_memory
  MemoryAttr get_mr_info(int mr_id) const;

  node_id master_id() const { return master_id_; }
  void set_master_id(node_id id) { master_id_ = id; }

  std::string master_ip() const { return master_ip_; };
  int master_port() const { return master_port_; }

  node_id id() const { return node_id_; }
  void set_id(node_id id) { node_id_ = id; }

  std::string ip() const { return ip_; };
  int port() const { return port_; };
  void set_ip(std::string ip) { ip_ = ip; };
  void set_port(int port) { port_ = port; };

  // 1:client, 2:dn
  RDMA_CM_ERROR_CODE connect_master(NodeType type);
  RDMA_CM_ERROR_CODE pull_config();
  Rocket::Options DEFAULT_ROCKET_OPT;
  Rocket::ConnectOptions DEFAULT_CONNECTION_OPT;

  uint32_t get_rkey() const { return rkey_; }

 private:
  std::string ip_;
  int port_;
  node_id node_id_;

  std::string master_ip_;
  int master_port_;
  node_id master_id_;
  // node_id -> <ip, port>
  // master_node_id == 0
  SpinLock network_config_lock_;
  std::unordered_map<node_id, Iport> network_config_;  // node id -> ip

  // retrieve user registered  mr
  std::unordered_map<int, std::shared_ptr<MemoryRegion>> mrs_;

  RdmaDev** rdma_devs_;
  SpinLock ip_straws_lock_;
  // ip:port -> RdmaStraws
  std::unordered_map<std::string, std::shared_ptr<RdmaStraws>> ip_straws_;

  int poll_worker_num_;
  int max_rocket_num_per_worker_;
  int rdrb_idx_;  // Note: tcp adaptor is a single thread. Lock or Atomic is not necessary.
  PollWorker** workers_;

  // ----- immutable members after RdmaCM initialized -----
  int num_devices_;
  struct ibv_device** dev_list_;

  volatile void* rdma_buffer_;
  uint64_t rdma_buffer_size_;

  // RrpcRte* rte_;
  uint32_t rkey_;
};

extern RdmaCM* global_cm;

// used with coroutine
Rocket* GetRocket(node_id srv);
void PollRockets();

void Issue();