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

#include "ob_simple_log_cluster_testbase.h"
#include "logservice/arbserver/ob_arb_srv_network_frame.h"
#include "logservice/arbserver/palf_env_lite_mgr.h"
#include "logservice/arbserver/ob_arb_server_timer.h"
#include "logservice/arbserver/palf_handle_lite.h"

namespace oceanbase
{
namespace unittest
{
struct PalfEnvLiteGuard
{
  PalfEnvLiteGuard() : palf_env_lite_(NULL), palf_env_mgr_(NULL) {}
  ~PalfEnvLiteGuard()
  {
    if (NULL != palf_env_lite_ && NULL != palf_env_mgr_) {
      palf_env_mgr_->revert_palf_env_lite(palf_env_lite_);
    }
    palf_env_lite_ = NULL;
    palf_env_mgr_ = NULL;
  }
  palflite::PalfEnvLite *palf_env_lite_;
  palflite::PalfEnvLiteMgr *palf_env_mgr_;
};

class DummyBlockPool : public palf::ILogBlockPool {
public:
  virtual int create_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path,
                              const int64_t block_size)
  {
    int fd = -1;
    if (-1 == (fd = ::openat(dir_fd, block_path, palf::LOG_WRITE_FLAG | O_CREAT, 0664))) {
      CLOG_LOG_RET(WARN, common::OB_ERR_SYS, "openat failed", K(block_path), K(errno));
      return OB_IO_ERROR;
    } else if (-1 == ::fallocate(fd, 0, 0, block_size)) {
      CLOG_LOG_RET(WARN, common::OB_ERR_SYS, "fallocate failed", K(block_path), K(errno));
      return OB_IO_ERROR;
    } else {
      CLOG_LOG(INFO, "create_block_at success", K(block_path));
    }
    if (-1 != fd)  {
      ::close(fd);
    }
    return OB_SUCCESS;
  }
  virtual int remove_block_at(const palf::FileDesc &dir_fd,
                              const char *block_path)
  {
    if (-1 == ::unlinkat(dir_fd, block_path, 0)) {
      return OB_IO_ERROR;
    }
    return OB_SUCCESS;
  }
};

class ObSimpleArbServer : public ObISimpleLogServer
{
public:
  static const int64_t cluster_id_ = 1;
  ObSimpleArbServer();
  ~ObSimpleArbServer() override;
  ILogBlockPool *get_block_pool() override final
  {return &dummy_block_pool_;};
  ObILogAllocator *get_allocator() override final
  {return &allocator_;}
  virtual int update_disk_opts(const PalfDiskOptions &opts) override final
  {return OB_NOT_SUPPORTED;};
  virtual int get_palf_env(PalfEnv *&palf_env)
  {return OB_NOT_SUPPORTED;};
  bool is_valid() const override final
  {return true == is_inited_;}
  IPalfEnvImpl *get_palf_env() override final
  {
    palflite::PalfEnvLite *palf_env_lite = NULL;
    palflite::PalfEnvKey palf_env_key(cluster_id_, OB_SERVER_TENANT_ID);
    palf_env_mgr_.get_palf_env_lite(palf_env_key, palf_env_lite);
    PALF_LOG(INFO, "yunlong trace get_palf_env", KP(palf_env_lite));
    return palf_env_lite;
  }
  void revert_palf_env(IPalfEnvImpl *palf_env) override final
  {
    palflite::PalfEnvLite *palf_env_lite = dynamic_cast<palflite::PalfEnvLite*>(palf_env);
    palf_env_mgr_.revert_palf_env_lite(palf_env_lite);
  }
  const std::string& get_clog_dir() const override final
  {return base_dir_;}
  common::ObAddr get_addr() const override final
  {return self_;}
  ObTenantBase *get_tenant_base() const override final
  {return tenant_base_;}
  logservice::ObLogFlashbackService *get_flashback_service() override final
  {return NULL;}
	void set_need_drop_packet(const bool need_drop_packet) override final
  {blacklist_.set_need_drop_packet(need_drop_packet);}
  void block_net(const ObAddr &src) override final
  {blacklist_.block_net(src);}
  void unblock_net(const ObAddr &src) override final
  {blacklist_.unblock_net(src);}
  void set_rpc_loss(const ObAddr &src, const int loss_rate) override final
  {blacklist_.set_rpc_loss(src, loss_rate);}
  void reset_rpc_loss(const ObAddr &src) override final
  {blacklist_.reset_rpc_loss(src);}
  bool is_arb_server() const override final {return true;}
  int64_t get_node_id() {return node_id_;}
public:
  int simple_init(const std::string &cluster_name,
                  const common::ObAddr &addr,
                  const int64_t node_id,
                  bool is_bootstrap) override final;
  int simple_start(const bool is_bootstrap) override final;
  int simple_close(const bool is_shutdown) override final;
  int simple_restart(const std::string &cluster_name,
                     const int64_t node_idx) override final;
  void destroy();
  int create_palf_env_lite(const int64_t tenant_id);
  int remove_palf_env_lite(const int64_t tenant_id);
  int get_palf_env_lite(const int64_t tenant_id, PalfEnvLiteGuard &guard);
  TO_STRING_KV(K_(palf_env_mgr), K_(self), K_(node_id));
private:
  palflite::PalfEnvLiteMgr palf_env_mgr_;
  arbserver::ObArbSrvNetworkFrame srv_network_frame_;
  arbserver::ObArbServerTimer timer_;
  DummyBlockPool dummy_block_pool_;
  ObTenantMutilAllocator allocator_;
  std::string base_dir_;
  common::ObAddr self_;
  ObTenantBase *tenant_base_;
  int64_t node_id_;
  ObMittestBlacklist blacklist_;
  ObFunction<bool(const ObAddr &src)> filter_;
  bool is_inited_;
};
}
}
