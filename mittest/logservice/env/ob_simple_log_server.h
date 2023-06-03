/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "lib/hash/ob_hashset.h"
#include "lib/ob_errno.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/signal/ob_signal_worker.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/function/ob_function.h"           // ObFunction
#include "observer/ob_signal_handle.h"
#include "lib/utility/ob_defer.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/ob_request.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/frame/ob_req_transport.h"
#include "logservice/logrpc/ob_log_rpc_processor.h"
#include "logservice/palf/log_rpc_macros.h"
#include "logservice/palf/log_rpc_processor.h"
#include "logservice/palf/palf_env.h"
#include "logservice/ob_arbitration_service.h"
#include "mock_ob_locality_manager.h"
#include "mock_ob_meta_reporter.h"
#include "lib/net/ob_addr.h"
#include "share/ob_rpc_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "share/ob_local_device.h"
#include "share/ob_occam_timer.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "logservice/ob_net_keepalive_adapter.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include <memory>
#include <map>

namespace oceanbase
{
namespace unittest
{
class ObLogDeliver;
}

namespace unittest
{
using namespace oceanbase;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::palf;
using namespace oceanbase::share;

class MockNetKeepAliveAdapter : public logservice::IObNetKeepAliveAdapter
{
public:
  MockNetKeepAliveAdapter() : log_deliver_(NULL) {}
  ~MockNetKeepAliveAdapter() { log_deliver_ = NULL; }
  int init(unittest::ObLogDeliver *log_deliver);
  bool in_black_or_stopped(const common::ObAddr &server) override final;
  bool is_server_stopped(const common::ObAddr &server) override final;
private:
  unittest::ObLogDeliver *log_deliver_;
};
uint32_t get_local_addr(const char *dev_name);
std::string get_local_ip();

struct LossConfig
{
  ObAddr src_;
  int loss_rate_;
  LossConfig()
    : src_(), loss_rate_(0)
  {}
  LossConfig(const ObAddr &src, const int loss_rate)
    : src_(src), loss_rate_(loss_rate)
  {}
  TO_STRING_KV(K_(src), K_(loss_rate));
};

class ObMittestBlacklist {
public:
  int init(const common::ObAddr &self);
  void block_net(const ObAddr &src);
  void unblock_net(const ObAddr &src);
  bool need_filter_packet_by_blacklist(const ObAddr &address);
	void set_need_drop_packet(const bool need_drop_packet) { need_drop_packet_ = need_drop_packet; }
  void set_rpc_loss(const ObAddr &src, const int loss_rate);
  void reset_rpc_loss(const ObAddr &src);
  bool need_drop_by_loss_config(const ObAddr &addr);
  void get_loss_config(const ObAddr &src, bool &exist, LossConfig &loss_config);
  TO_STRING_KV(K_(blacklist), K_(rpc_loss_config));
protected:
  hash::ObHashSet<ObAddr> blacklist_;
	bool need_drop_packet_;
  common::ObSEArray<LossConfig, 4> rpc_loss_config_;
  common::ObAddr self_;
};

class ObLogDeliver : public rpc::frame::ObReqDeliver, public lib::TGTaskHandler, public ObMittestBlacklist
{
public:
	ObLogDeliver()
      : rpc::frame::ObReqDeliver(),
        palf_env_impl_(NULL),
        tg_id_(0),
        is_stopped_(true) {}
  ~ObLogDeliver() { destroy(true); }
  int init() override final {return OB_SUCCESS;}
  int init(const common::ObAddr &self, const bool is_bootstrap);
  void destroy(const bool is_shutdown);
  int deliver(rpc::ObRequest &req);
  int start();
  void stop();
  int wait();
  void handle(void *task);

private:
  void init_all_propocessor_();
  typedef std::function<int(ObReqProcessor *&)> Func;

  Func funcs_[MAX_PCODE];
  template <typename PROCESSOR>
  void register_rpc_propocessor_(int pcode)
  {
    auto func = [](ObReqProcessor *&ptr) -> int {
      int ret = OB_SUCCESS;
      if (NULL == (ptr = OB_NEW(PROCESSOR, "SimpleLogSvr"))) {
        SERVER_LOG(WARN, "allocate memory failed");
      } else if (OB_FAIL(ptr->init())) {
      } else {
      }
      return ret;
    };
    funcs_[pcode] = func;
    SERVER_LOG(INFO, "register_rpc_propocessor_ success", K(pcode));
  }
  int handle_req_(rpc::ObRequest &req);
private:
  mutable common::RWLock lock_;
  bool is_inited_;
	PalfEnvImpl *palf_env_impl_;
  int tg_id_;
  bool is_stopped_;
  int64_t node_id_;
};

class ObISimpleLogServer
{
public:
  ObISimpleLogServer() {}
  virtual ~ObISimpleLogServer() {}
  virtual bool is_valid() const = 0;
  virtual IPalfEnvImpl *get_palf_env() = 0;
  virtual void revert_palf_env(IPalfEnvImpl *palf_env) = 0;
  virtual const std::string& get_clog_dir() const = 0;
  virtual common::ObAddr get_addr() const = 0;
  virtual ObTenantBase *get_tenant_base() const = 0;
  virtual logservice::ObLogFlashbackService *get_flashback_service() = 0;
	virtual void set_need_drop_packet(const bool need_drop_packet) = 0;
  virtual void block_net(const ObAddr &src) = 0;
  virtual void unblock_net(const ObAddr &src) = 0;
  virtual void set_rpc_loss(const ObAddr &src, const int loss_rate) = 0;
  virtual void reset_rpc_loss(const ObAddr &src) = 0;
  virtual int simple_init(const std::string &cluster_name,
                          const common::ObAddr &addr,
                          const int64_t node_id,
                          const bool is_bootstrap) = 0;
  virtual int simple_start(const bool is_bootstrap) = 0;
  virtual int simple_close(const bool is_shutdown) = 0;
  virtual int simple_restart(const std::string &cluster_name, const int64_t node_idx) = 0;
  virtual ILogBlockPool *get_block_pool() = 0;
  virtual ObILogAllocator *get_allocator() = 0;
  virtual int update_disk_opts(const PalfDiskOptions &opts) = 0;
  virtual int get_palf_env(PalfEnv *&palf_env) = 0;
  virtual bool is_arb_server() const {return false;};
  virtual int64_t get_node_id() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObSimpleLogServer : public ObISimpleLogServer
{
public:
  ObSimpleLogServer()
    : handler_(deliver_),
      transport_(NULL)
  {
  }
  ~ObSimpleLogServer()
  {
    if (OB_NOT_NULL(allocator_)) {
      ob_delete(allocator_);
    }
    if (OB_NOT_NULL(io_device_)) {
      ob_delete(io_device_);
    }
  }
  int simple_init(const std::string &cluster_name,
                  const common::ObAddr &addr,
                  const int64_t node_id,
                  const bool is_bootstrap) override final;
  int simple_start(const bool is_bootstrap) override final;
  int simple_close(const bool is_shutdown) override final;
  int simple_restart(const std::string &cluster_name, const int64_t node_idx) override final;
public:
  int64_t get_node_id() {return node_id_;}
  ILogBlockPool *get_block_pool() override final
  {return &log_block_pool_;};
  ObILogAllocator *get_allocator() override final
  { return allocator_; }
  virtual int update_disk_opts(const PalfDiskOptions &opts) override final;
  virtual int get_palf_env(PalfEnv *&palf_env)
  { palf_env = palf_env_; return OB_SUCCESS;}
  virtual void revert_palf_env(IPalfEnvImpl *palf_env) { UNUSED(palf_env); }
  bool is_valid() const override final {return NULL != palf_env_;}
  IPalfEnvImpl *get_palf_env() override final
  { return palf_env_->get_palf_env_impl(); }
  const std::string& get_clog_dir() const override final
  { return clog_dir_; }
  common::ObAddr get_addr() const override final
  { return addr_; }
  ObTenantBase *get_tenant_base() const override final
  { return tenant_base_; }
  logservice::ObLogFlashbackService *get_flashback_service() override final
  { return log_service_.get_flashback_service(); }
	// Nowdat, not support drop packet from specificed address
	void set_need_drop_packet(const bool need_drop_packet) override final
  { deliver_.set_need_drop_packet(need_drop_packet);}
  void block_net(const ObAddr &src) override final
  { deliver_.block_net(src); }
  void unblock_net(const ObAddr &src) override final
  { deliver_.unblock_net(src); }
  void set_rpc_loss(const ObAddr &src, const int loss_rate) override final
  { deliver_.set_rpc_loss(src, loss_rate); }
  void reset_rpc_loss(const ObAddr &src) override final
  { deliver_.reset_rpc_loss(src); }
  TO_STRING_KV(K_(node_id), K_(addr), KP(palf_env_));

protected:
  int init_io_(const std::string &cluster_name);
  int init_network_(const common::ObAddr &addr, const bool is_bootstrap);
  int init_log_service_();
  int init_memory_dump_timer_();

private:
  int64_t node_id_;
  common::ObAddr addr_;
  rpc::frame::ObNetEasy net_;
  obrpc::ObRpcHandler handler_;
  ObLogDeliver deliver_;
  ObRandom rand_;
  PalfEnv *palf_env_;
  ObTenantBase *tenant_base_;
  ObMalloc malloc_mgr_;
  ObLocalDevice *io_device_;
  static const int64_t MAX_IOD_OPT_CNT = 5;
  ObIODOpt iod_opt_array_[MAX_IOD_OPT_CNT];
  ObIODOpts iod_opts_;
  std::string clog_dir_;
  ObOccamTimer timer_;
  ObOccamTimerTaskRAIIHandle timer_handle_;
  logservice::ObLogService log_service_;
  ObTenantMutilAllocator *allocator_;
  rpc::frame::ObReqTransport *transport_;
  ObLSService ls_service_;
  ObLocationService location_service_;
  MockMetaReporter reporter_;
  logservice::ObServerLogBlockMgr log_block_pool_;
  common::ObMySQLProxy sql_proxy_;
  MockNetKeepAliveAdapter *net_keepalive_;
  ObSrvRpcProxy srv_proxy_;
  logservice::coordinator::ObFailureDetector detector_;
};

} // end unittest
} // oceanbase
