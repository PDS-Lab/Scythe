#pragma once

#include <infiniband/verbs.h>

#include <atomic>

#include "tcp_adaptor.h"
#include "util/lock.h"

struct RdmaDev;
enum class RDMA_CM_ERROR_CODE : uint16_t;
class RdmaCM;

class RdmaQp {
 public:
  static const uint32_t DEFAULT_PSN = 3158;

  struct Options {
    int mx_scq_size;
    int mx_shared_rcq_size;
    int mx_sq_size;
    int mx_rq_size;
    int mx_send_sge;
    int mx_recv_sge;
    int mx_inline_size;
    int cq_signal_watermark;  // how many wrs post from last cq signal.
    int poll_cq_watermark;    // how many wrs post from last polling.

    Options() {
      mx_scq_size = 1024;
      mx_shared_rcq_size = 1024;
      mx_sq_size = 1024;
      mx_rq_size = 1;
      mx_send_sge = 4;
      mx_recv_sge = 4;
      mx_inline_size = 64;
      cq_signal_watermark = 256;
      poll_cq_watermark = 768;
    }
  };

  RdmaQp(const Options& opt)
      : mx_send_cq_size_(opt.mx_scq_size),
        mx_shared_rcq_size_(opt.mx_shared_rcq_size),
        mx_sq_size_(opt.mx_sq_size),
        mx_rq_size_(opt.mx_rq_size),
        mx_send_sge_(opt.mx_send_sge),
        mx_recv_sge_(opt.mx_recv_sge),
        mx_inline_size_(opt.mx_inline_size),
        cq_signal_watermark_(opt.cq_signal_watermark),
        poll_cq_watermark_(opt.poll_cq_watermark) {
    assert(cq_signal_watermark_ <= mx_sq_size_ && poll_cq_watermark_ <= mx_sq_size_ &&
           cq_signal_watermark_ <= poll_cq_watermark_);
    wrs_posted_from_last_signal_ = 0;
    wrs_in_sq_ = 0;

    max_wc_num_ = mx_sq_size_ / cq_signal_watermark_;
    wcs_ = (ibv_wc*)malloc(sizeof(ibv_wc) * max_wc_num_);
  }

  RdmaQp(RdmaQp& lv) {
    mx_send_cq_size_ = lv.mx_send_cq_size_;
    mx_shared_rcq_size_ = lv.mx_shared_rcq_size_;
    mx_sq_size_ = lv.mx_sq_size_;
    mx_rq_size_ = lv.mx_rq_size_;
    mx_send_sge_ = lv.mx_send_sge_;
    mx_recv_sge_ = lv.mx_recv_sge_;
    mx_inline_size_ = lv.mx_inline_size_;

    dev_ = lv.dev_;
    port_idx_ = lv.port_idx_;
    qp_ = lv.qp_;
    send_cq_ = lv.send_cq_;
    recv_cq_ = lv.recv_cq_;
    cq_signal_watermark_ = lv.cq_signal_watermark_;
    poll_cq_watermark_ = lv.poll_cq_watermark_;
    wrs_posted_from_last_signal_ = lv.wrs_posted_from_last_signal_;
    wrs_in_sq_.store(lv.wrs_in_sq_);
    max_wc_num_ = lv.max_wc_num_;
    wcs_ = lv.wcs_;
  }

  RdmaQp(RdmaQp&& rv) noexcept : RdmaQp(rv) {
    rv.qp_ = nullptr;
    rv.send_cq_ = nullptr;
    rv.recv_cq_ = nullptr;
    rv.wcs_ = nullptr;
  }

  ~RdmaQp();

  inline ibv_qp* get_qp() { return qp_; }
  inline ibv_cq* get_cq() { return send_cq_; }
  void init_rc(RdmaDev* dev, int port_idx, RDMA_CM_ERROR_CODE* rc);
  void rc_init2rtr(int qpn, int dlid, uint64_t subnet_prefix, uint64_t interface_id, int local_id,
                   RDMA_CM_ERROR_CODE* rc);
  void rc_rtr2rts(RDMA_CM_ERROR_CODE* rc);

  uint32_t get_qp_num() {
    assert(qp_);
    return qp_->qp_num;
  }

  RDMA_CM_ERROR_CODE post_send(ibv_send_wr* srs, ibv_send_wr** bad_wrs, size_t wr_num, bool enable_doorbell = true);
  RDMA_CM_ERROR_CODE post_one_side(ibv_send_wr* srs, ibv_send_wr** bad_wrs, size_t wr_num, bool enable_doorbell = true);
  RDMA_CM_ERROR_CODE poll_comp(bool one_side = false, bool noblock = false, bool rocket_direct = false);

 private:
  void rc_ready2init(RDMA_CM_ERROR_CODE* rc);

  int mx_send_cq_size_;
  int mx_shared_rcq_size_;
  int mx_sq_size_;
  int mx_rq_size_;  // set to 1 if one-side rdma
  int mx_send_sge_;
  int mx_recv_sge_;
  int mx_inline_size_;

  RdmaDev* dev_;
  int port_idx_;
  struct ibv_qp* qp_ = nullptr;
  struct ibv_cq* send_cq_ = nullptr;
  struct ibv_cq* recv_cq_ = nullptr;
  // RdmaQpAttr       remote_attr_;
  int max_wc_num_;
  struct ibv_wc* wcs_ = nullptr;

  SpinLock lock_;
  size_t wrs_posted_from_last_signal_;
  std::atomic_size_t wrs_in_sq_;

  size_t cq_signal_watermark_;
  size_t poll_cq_watermark_;
};

/*
 * RdmaStraws is a set of RdmaQps which connect the same remote node.
 */
class RdmaStraws {
 public:
  static const size_t MAX_QP_PER_STRAWS = 1024;

  RdmaStraws(const std::string& dest_ip, int port, RdmaCM* cm) : dest_ip_(dest_ip), port_(port) {
    cm_ = cm;
    valid_ = true;
    qps_.reserve(MAX_QP_PER_STRAWS);
    one_side_qps_.reserve(MAX_QP_PER_STRAWS);
  }

  RDMA_CM_ERROR_CODE init_connection(int pref_rdma_dev_id = 0, int pref_rdma_port_idx = 1, int pref_remote_dev_id = 0,
                                     int pref_remote_port_idx = 1, bool create_new_qp = false, int qp_num = 1,
                                     int one_side_qp_num = 1, RdmaQp::Options opt = RdmaQp::Options());
  RDMA_CM_ERROR_CODE post_send(ibv_send_wr* wr, ibv_send_wr** bad_wr, size_t wr_num, bool enable_doorbell = true);
  RDMA_CM_ERROR_CODE post_one_side(ibv_send_wr* wr, ibv_send_wr** bad_wr, size_t wr_num, bool enable_doorbell = true,
                                   bool server_mode = false);
  RDMA_CM_ERROR_CODE try_poll_one_side();
  // RDMA_CM_ERROR_CODE
  void add_rdma_qp(RdmaQp* qp) {
    lock_.Lock();
    assert(qps_.size() < MAX_QP_PER_STRAWS);
    qps_.push_back(std::move(*qp));
    lock_.Unlock();
  }

  void add_one_side_qp(RdmaQp* qp) {
    lock_.Lock();
    assert(one_side_qps_.size() < MAX_QP_PER_STRAWS);
    one_side_qps_.push_back(std::move(*qp));
    lock_.Unlock();
  }

  inline void destory_rdma_qps() {
    lock_.Lock();
    valid_ = false;
    // TODO: wait util all qps are empty.
    qps_.clear();
    one_side_qps_.clear();
    lock_.Unlock();
  }

  inline bool valid() {
    bool v = true;
    lock_.Lock();
    v = valid_;
    lock_.Unlock();
    return v;
  }

  // for debug
  ibv_qp* get_first_qp() {
    lock_.Lock();
    ibv_qp* qp = qps_[0].get_qp();
    lock_.Unlock();
    return qp;
  }

  size_t one_side_qp_size() const { return one_side_qps_.size(); }

 private:
  // immutable member
  RdmaCM* cm_;
  std::string dest_ip_;
  int port_;

  SpinLock lock_;
  bool valid_;
  std::vector<RdmaQp> qps_;
  std::vector<RdmaQp> one_side_qps_;
};

extern std::atomic_uint32_t _global_thread_id;
uint32_t self_thread_id();