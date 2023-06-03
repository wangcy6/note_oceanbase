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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_SERVICE_
#define OCEANBASE_LOGSERVICE_OB_LOG_SERVICE_

#include "common/ob_role.h"
#include "lib/ob_define.h"
#include "share/ob_tenant_info_proxy.h"                // ObTenantRole
#include "applyservice/ob_log_apply_service.h"
#include "cdcservice/ob_cdc_service.h"
#include "logrpc/ob_log_rpc_req.h"
#include "logrpc/ob_log_rpc_proxy.h"
#include "palf/log_block_pool_interface.h"             // ILogBlockPool
#include "palf/log_define.h"
#include "rcservice/ob_role_change_service.h"
#include "restoreservice/ob_log_restore_service.h"     // ObLogRestoreService
#include "replayservice/ob_log_replay_service.h"
#include "ob_net_keepalive_adapter.h"
#include "ob_reporter_adapter.h"
#include "ob_ls_adapter.h"
#include "ob_location_adapter.h"
#include "ob_log_flashback_service.h"                  // ObLogFlashbackService
#include "ob_log_handler.h"
#include "ob_log_monitor.h"

namespace oceanbase
{
namespace commom
{
class ObAddr;
class ObILogAllocator;
class ObMySQLProxy;
}

namespace obrpc
{
class ObNetKeepAlive;
}
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}

namespace share
{
class ObLSID;
class ObLocationService;
class SCN;
}

namespace palf
{
class PalfHandleGuard;
class PalfRoleChangeCb;
class PalfDiskOptions;
class PalfEnv;
}
namespace storage
{
class ObLSService;
}

namespace logservice
{

class ObLogService
{
public:
  ObLogService();
  virtual ~ObLogService();
  static int mtl_init(ObLogService* &logservice);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  static palf::AccessMode get_palf_access_mode(const share::ObTenantRole &tenant_role);
  int init(const palf::PalfOptions &options,
           const char *base_dir,
           const common::ObAddr &self,
           common::ObILogAllocator *alloc_mgr,
           rpc::frame::ObReqTransport *transport,
           storage::ObLSService *ls_service,
           share::ObLocationService *location_service,
           observer::ObIMetaReport *reporter,
<<<<<<< HEAD
           palf::ILogBlockPool *log_block_pool);
  //--日志流相关接口--
  //新建日志流接口，该接口会创建日志流对应的目录，新建一个空白日志流。其中包括生成并初始化对应的ObReplayStatus结构
  // @param [in] id，日志流标识符
  // @param [in] replica_type，日志流的副本类型
  // @param [in] tenant_role, 租户角色, 以此决定Palf使用模式(APPEND/RAW_WRITE)
  // @param [in] create_scn, 日志流创建scn, 保证日志流产生的日志都大于该scn
  // @param [in] allow_log_sync, 是否允许同步日志, 迁移支持先加 learner 后移除
  // @param [out] log_handler，新建日志流以ObLogHandler形式返回，保证上层使用日志流时的生命周期
  // @param [out] restore_handler，新建日志流以ObLogRestoreHandler形式返回，用于备库同步日志
  int create_ls(const share::ObLSID &id,
                const common::ObReplicaType &replica_type,
                const share::ObTenantRole &tenant_role,
                const share::SCN &create_scn,
                const bool allow_log_sync,
                ObLogHandler &log_handler,
                ObLogRestoreHandler &restore_handler);

=======
           palf::ILogBlockPool *log_block_pool,
           common::ObMySQLProxy *sql_proxy,
           IObNetKeepAliveAdapter *net_keepalive_adapter);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  //--日志流相关接口--
  //新建日志流接口，该接口会创建日志流对应的目录，新建一个以PalfBaeInfo为日志基点的日志流。
  //其中包括生成并初始化对应的ObReplayStatus结构
  // @param [in] id，日志流标识符
  // @param [in] replica_type，日志流的副本类型
  // @param [in] tenant_role, 租户角色, 以此决定Palf使用模式(APPEND/RAW_WRITE)
  // @param [in] palf_base_info, 日志同步基点信息
  // @param [out] log_handler，新建日志流以ObLogHandler形式返回，保证上层使用日志流时的生命周期
  // @param [out] restore_handler，新建日志流以ObLogRestoreHandler形式返回，用于备库同步日志
  int create_ls(const share::ObLSID &id,
                const common::ObReplicaType &replica_type,
                const share::ObTenantRole &tenant_role,
                const palf::PalfBaseInfo &palf_base_info,
                const bool allow_log_sync,
                ObLogHandler &log_handler,
                ObLogRestoreHandler &restore_handler);

  //删除日志流接口:外层调用create_ls()之后，后续流程失败，需要调用remove_ls()
  int remove_ls(const share::ObLSID &id,
                ObLogHandler &log_handler,
                ObLogRestoreHandler &restore_handler);

  int check_palf_exist(const share::ObLSID &id, bool &exist) const;
  //宕机重启恢复日志流接口，包括生成并初始化对应的ObReplayStatus结构
  // @param [in] id，日志流标识符
  // @param [out] log_handler，新建日志流以ObLogHandler形式返回，保证上层使用日志流时的生命周期
  // @param [out] restore_handler，新建日志流以ObLogRestoreHandler形式返回，用于备库同步日志
  int add_ls(const share::ObLSID &id,
             ObLogHandler &log_handler,
             ObLogRestoreHandler &restore_handler);

  int open_palf(const share::ObLSID &id,
                palf::PalfHandleGuard &palf_handle);

  // get role of current palf replica.
  // NB: distinguish the difference from get_role of log_handler
  // In general, get the replica role to do migration/blance/report, use this interface,
  // to write log, use get_role of log_handler
  int get_palf_role(const share::ObLSID &id,
                    common::ObRole &role,
                    int64_t &proposal_id);

  int update_replayable_point(const share::SCN &replayable_point);
<<<<<<< HEAD
=======
  int get_replayable_point(share::SCN &replayable_point);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  int get_palf_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte);
  // why we need update 'log_disk_size_' and 'log_disk_util_threshold' separately.
  //
  // 'log_disk_size' is a member of unit config.
  // 'log_disk_util_threshold' and 'log_disk_util_limit_threshold' are members of tenant parameters.
  // If we just only provide 'update_disk_options', the correctness of PalfDiskOptions can not be guaranteed.
  // for example, original PalfDiskOptions is that
  // {
  //   log_disk_size = 100G,
  //   log_disk_util_limit_threshold = 95,
  //   log_disk_util_threshold = 80
  // }
  //
  // 1. thread1 update 'log_disk_size' with 50G, and it will used 'update_disk_options' with PalfDiskOptions
  // {
  //   log_disk_size = 50G,
  //   log_disk_util_limit_threshold = 95,
  //   log_disk_util_threshold = 80
  // }
  // 2. thread2 updaet 'log_disk_util_limit_threshold' with 85, and it will used 'update_disk_options' with PalfDiskOptions
  // {
  //   log_disk_size = 100G,
  //   log_disk_util_limit_threshold = 85,
  //   log_disk_util_threshold = 80
  // }.
  int update_palf_options_except_disk_usage_limit_size();
  int update_log_disk_usage_limit_size(const int64_t log_disk_usage_limit_size);
  int get_palf_options(palf::PalfOptions &options);
  int iterate_palf(const ObFunction<int(const palf::PalfHandle&)> &func);
  int iterate_apply(const ObFunction<int(const ObApplyStatus&)> &func);
  int iterate_replay(const ObFunction<int(const ObReplayStatus&)> &func);

  // @desc: flashback all log_stream's redo log of tenant 'tenant_id'
  // @params [in] const uint64_t tenant_id: id of tenant which should be flashbacked
  // @params [in] const SCN &flashback_scn: flashback point
  // @params [in] const int64_t timeout_us: timeout time (us)
  // @return
  //   - OB_SUCCESS
  //   - OB_INVALID_ARGUEMENT: invalid tenant_id or flashback_scn
  //   - OB_NOT_SUPPORTED: meta tenant or sys tenant can't be flashbacked
  //   - OB_EAGAIN: another flashback operation is doing
  //   - OB_TIMEOUT: timeout
  int flashback(const uint64_t tenant_id, const share::SCN &flashback_scn, const int64_t timeout_us);

  int diagnose_role_change(RCDiagnoseInfo &diagnose_info);
  int diagnose_replay(const share::ObLSID &id, ReplayDiagnoseInfo &diagnose_info);
  int diagnose_apply(const share::ObLSID &id, ApplyDiagnoseInfo &diagnose_info);
  int get_io_start_time(int64_t &last_working_time);
  int check_disk_space_enough(bool &is_disk_enough);

  palf::PalfEnv *get_palf_env() { return palf_env_; }
  // TODO by yunlong: temp solution, will by removed after Reporter be added in MTL
  ObLogReporterAdapter *get_reporter() { return &reporter_; }
  cdc::ObCdcService *get_cdc_service() { return &cdc_service_; }
  ObLogRestoreService *get_log_restore_service() { return &restore_service_; }
  ObLogReplayService *get_log_replay_service()  { return &replay_service_; }
  obrpc::ObLogServiceRpcProxy *get_rpc_proxy() { return &rpc_proxy_; }
  ObLogFlashbackService *get_flashback_service() { return &flashback_service_; }
private:
  int create_ls_(const share::ObLSID &id,
                 const common::ObReplicaType &replica_type,
                 const share::ObTenantRole &tenant_role,
                 const palf::PalfBaseInfo &palf_base_info,
                 const bool allow_log_sync,
                 ObLogHandler &log_handler,
                 ObLogRestoreHandler &restore_handler);
private:
  bool is_inited_;
  bool is_running_;

  common::ObAddr self_;
  palf::PalfEnv *palf_env_;
  IObNetKeepAliveAdapter *net_keepalive_adapter_;

  ObLogApplyService apply_service_;
  ObLogReplayService replay_service_;
  ObRoleChangeService role_change_service_;
  ObLocationAdapter location_adapter_;
  ObLSAdapter ls_adapter_;
  obrpc::ObLogServiceRpcProxy rpc_proxy_;
  ObLogReporterAdapter reporter_;
  cdc::ObCdcService cdc_service_;
  ObLogRestoreService restore_service_;
  ObLogFlashbackService flashback_service_;
  ObLogMonitor monitor_;
  ObSpinLock update_palf_opts_lock_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogService);
};
} // end namespace logservice
} // end namespace oceanbase
#endif // OCEANBASE_LOGSERVICE_OB_LOG_SERVICE_
