#include "rdma_cm.h"

#include <arpa/inet.h>
#include <linux/if.h>
#include <sys/ioctl.h>

#include <thread>
#include <vector>

#include "coroutine_pool/coroutine.h"
#include "rrpc/rocket.h"
#include "rrpc/rrpc_config.h"
#include "util/common.h"
#include "util/logging.h"

RdmaCM* global_cm = nullptr;

int RdmaCM::get_local_ip(char* outip) {
  int sockfd;
  struct ifconf ifconf;
  char buf[512];
  struct ifreq* ifreq;
  char* ip;

  ifconf.ifc_len = 512;
  ifconf.ifc_buf = buf;

  if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return -1;
  }

  ioctl(sockfd, SIOCGIFCONF, &ifconf);
  close(sockfd);

  ifreq = (struct ifreq*)buf;
  for (int i = (ifconf.ifc_len / sizeof(struct ifreq)); i > 0; i--) {
    ip = inet_ntoa(((struct sockaddr_in*)&(ifreq->ifr_addr))->sin_addr);
    if (strcmp(ip, "127.0.0.1") == 0) {
      ifreq++;
      continue;
    }

    strcpy(outip, ip);
    return 0;
  }

  return -1;
}

RdmaCM::RdmaCM(RrpcRte* rte, std::string master_ip, int master_port, void* rdma_buffer, uint64_t rdma_buffer_size,
               int poll_worker_num, int max_rocket_num_per_worker) {
  rdma_buffer_ = rdma_buffer;
  rdma_buffer_size_ = rdma_buffer_size;
  poll_worker_num_ = poll_worker_num;
  max_rocket_num_per_worker_ = max_rocket_num_per_worker;
  master_ip_ = master_ip;
  master_port_ = master_port;
  query_devinfo();

  rte->run_tcp_adaptor();

  rdrb_idx_ = 0;
  if (poll_worker_num) {
    workers_ = (PollWorker**)(malloc(sizeof(PollWorker*) * poll_worker_num));
    for (int i = 0; i < poll_worker_num; ++i) {
      workers_[i] = new PollWorker(max_rocket_num_per_worker);
      workers_[i]->run();
    }
  }
}

RdmaCM::~RdmaCM() {
  for (int i = 0; i < poll_worker_num_; ++i) {
    delete workers_[i];
  }
  for (int i = 0; i < num_devices_; ++i) {
    if (rdma_devs_[i]) delete rdma_devs_[i];
  }
  free(rdma_devs_);
}

void RdmaCM::query_devinfo() {
  int rc;
  dev_list_ = ibv_get_device_list(&num_devices_);
  rdma_devs_ = (RdmaDev**)malloc(num_devices_ * sizeof(RdmaDev*));
  for (int dev_id = 0; dev_id < num_devices_; ++dev_id) {
    struct ibv_context* ib_ctx = ibv_open_device(dev_list_[dev_id]);
    if (!ib_ctx) {
      rdma_devs_[dev_id] = nullptr;
      continue;
    }

    struct ibv_device_attr device_attr;
    memset(&device_attr, 0, sizeof(device_attr));
    rc = ibv_query_device(ib_ctx, &device_attr);
    if (rc) {
      rdma_devs_[dev_id] = nullptr;
      continue;
    }

    rdma_devs_[dev_id] = new RdmaDev(dev_id);
    int port_num = device_attr.phys_port_cnt;
    for (int port_id = 1; port_id <= port_num; ++port_id) {
      struct ibv_port_attr port_attr;
      rc = ibv_query_port(ib_ctx, port_id, &port_attr);
      if (rc) continue;
      if (port_attr.phys_state != IBV_PORT_ACTIVE && port_attr.phys_state != IBV_PORT_ACTIVE_DEFER) continue;
      rdma_devs_[dev_id]->valid_port_ids.push_back(port_id);
    }
    ibv_close_device(ib_ctx);
  }
}

RDMA_CM_ERROR_CODE RdmaCM::open_device(int dev_id, RdmaDev** res) {
  RdmaDev* dev = rdma_devs_[dev_id];
  if (!dev) {
    return RDMA_CM_ERROR_CODE::CM_OPEN_DEV_FAILED;
  }
  dev->dev_lock_.Lock();
  if (dev->inited) {
    dev->dev_lock_.Unlock();
    *res = dev;
    return RDMA_CM_ERROR_CODE::CM_OBJ_INITIALIZED;
  }
  dev->ctx = ibv_open_device(dev_list_[dev_id]);
  dev->pd = ibv_alloc_pd(dev->ctx);
  if (!(dev->pd)) {
    ibv_close_device(dev->ctx);
    dev->ctx = nullptr;
    dev->dev_lock_.Unlock();
    return RDMA_CM_ERROR_CODE::CM_ALLOC_PD_FAILED;
  }

  dev->mr =
      ibv_reg_mr(dev->pd, (char*)rdma_buffer_, rdma_buffer_size_,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  printf("[RdmaCM][open_device] reg mr addr:%lu size:%lu lkey:%u rkey:%u\n", (uintptr_t)rdma_buffer_, rdma_buffer_size_,
         dev->mr->lkey, dev->mr->rkey);
  rkey_ = dev->mr->rkey;
  if (!(dev->mr)) {
    ibv_dealloc_pd(dev->pd);
    ibv_close_device(dev->ctx);
    dev->ctx = nullptr;
    dev->pd = nullptr;
    dev->dev_lock_.Unlock();
    return RDMA_CM_ERROR_CODE::CM_REG_MR_FAILED;
  }

  dev->inited = true;
  dev->dev_lock_.Unlock();
  *res = dev;
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

RDMA_CM_ERROR_CODE RdmaCM::get_mr_rkey_by_dev_id(int dev_id, uint32_t* rkey) {
  RdmaDev* dev = nullptr;
  RDMA_CM_ERROR_CODE rc;
  rc = open_device(dev_id, &dev);
  assert(dev);
  if (rc != RDMA_CM_ERROR_CODE::CM_SUCCESS && rc != RDMA_CM_ERROR_CODE::CM_OBJ_INITIALIZED) return rc;
  *rkey = dev->mr->rkey;
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

std::shared_ptr<RdmaStraws> RdmaCM::get_rdma_straws(const std::string& ip, int port) {
  std::shared_ptr<RdmaStraws> straws_ptr;
  std::string iport_str = ip + ':' + std::to_string(port);
  ip_straws_lock_.Lock();
  if (ip_straws_.find(iport_str) == ip_straws_.end() || !ip_straws_[iport_str]->valid()) {
    // RdmaCM need a new RdmaStraws with a new qp
    straws_ptr = std::make_shared<RdmaStraws>(ip, port, this);
    ip_straws_[iport_str] = straws_ptr;
  } else {
    straws_ptr = ip_straws_[iport_str];
  }
  ip_straws_lock_.Unlock();
  return straws_ptr;
}

RDMA_CM_ERROR_CODE RdmaCM::reg_rocket_to_poll_worker(Rocket* rocket) {
  for (int i = rdrb_idx_; i < poll_worker_num_; ++i) {
    if (workers_[i]->ctrl_add_rocket(rocket)) {
      rdrb_idx_ = (i + 1) % poll_worker_num_;
      return RDMA_CM_ERROR_CODE::CM_SUCCESS;
    }
  }
  return RDMA_CM_ERROR_CODE::CM_REG_ROCKET_NUM_EXCEED_LIMIT;
}

void RdmaCM::manual_set_network_config(std::unordered_map<node_id, Iport>& config) {
  network_config_lock_.Lock();
  network_config_ = config;
  network_config_lock_.Unlock();
}

MemoryAttr RdmaCM::get_mr_info(int mr_id) const {
  auto iter = mrs_.find(mr_id);
  if (iter == mrs_.end()) {
    LOG_FATAL("cannot find mr");
    return {};
  }
  return iter->second->getMemoryAttr();
}

bool RdmaCM::register_memory(int mr_id, char* buf, uint64_t size, int dev_id, int flag) {
  RdmaDev* dev;
  // get dev
  auto rc = this->open_device(dev_id, &dev);
  assert(rc == RDMA_CM_ERROR_CODE::CM_OBJ_INITIALIZED || rc == RDMA_CM_ERROR_CODE::CM_SUCCESS);

  MemoryRegion* mr = new MemoryRegion((uintptr_t)buf, size, dev->pd, flag);
  if (!mr->valid()) {
    delete mr;
    return false;
  }

  auto iter = mrs_.find(mr_id);
  if (iter != mrs_.end()) {
    assert(false);
    LOG_ERROR("duplicated mr");
  }
  mrs_[mr_id] = std::shared_ptr<MemoryRegion>(mr);
  return true;
}

void RdmaCM::add_node(node_id node, std::string ip, int port) {
  network_config_lock_.Lock();
  network_config_[node] = {ip, port};
  network_config_lock_.Unlock();
}

RDMA_CM_ERROR_CODE RdmaCM::remove_node(node_id node, std::string ip, int port) {
  network_config_lock_.Lock();
  auto iter = network_config_.find(node);
  if (iter == network_config_.end()) {
    network_config_lock_.Unlock();
    return RDMA_CM_ERROR_CODE::CM_INVALID_NODE_ID;
  }
  network_config_.erase(iter);
  network_config_lock_.Unlock();
  return RDMA_CM_ERROR_CODE::CM_SUCCESS;
}

thread_local std::unordered_map<node_id, std::shared_ptr<Rocket>> rkt_map;
Rocket* GetRocket(node_id srv) {
  // coroutine local
  auto iter = rkt_map.find(srv);
  if (iter == rkt_map.end()) {
    // create dn rocket
    auto rkt = std::make_shared<Rocket>(global_cm->DEFAULT_ROCKET_OPT);
    auto rc = rkt->connect(srv, global_cm->DEFAULT_CONNECTION_OPT);
    // LOG_INFO("cort %p connect", cort);
    rkt_map[srv] = rkt;
    assert(rc == RDMA_CM_ERROR_CODE::CM_SUCCESS);
    return rkt.get();
  }
  return iter->second.get();
}

// for coroutine scheduler
void PollRockets() {
  for (auto& rkt : rkt_map) {
    rkt.second->poll_reply_msg();
    for (int i = 0; i < 100; i++) {
      rkt.second->try_poll_one_side();
    }
  }
}

void Issue() {
  for (auto& rkt : rkt_map) {
    rkt.second->send();
    rkt.second->rdma_burst();
  }
};
