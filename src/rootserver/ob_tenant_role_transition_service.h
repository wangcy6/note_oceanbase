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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_ROLE_TRANSITION_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_ROLE_TRANSITION_SERVICE_H

#include "share/ob_rpc_struct.h"// ObSwitchTenantArg
#include "logservice/palf/palf_options.h"//access mode
#include "logservice/palf/log_define.h"//INVALID_PROPOSAL_ID
#include "share/ob_tenant_info_proxy.h"
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusOperator

namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
class SCN;
struct ObAllTenantInfo;
}

namespace rootserver 
{

using namespace share;


class ObTenantRoleTransitionConstants
{
public:
  static constexpr int64_t PRIMARY_UPDATE_LS_RECOVERY_STAT_TIME_US = 1000 * 1000;  // 1s
  static constexpr int64_t STANDBY_UPDATE_LS_RECOVERY_STAT_TIME_US = 100 * 1000;  // 100ms
  static constexpr int64_t STS_TENANT_INFO_REFRESH_TIME_US = 100 * 1000;  // 100ms
  static constexpr int64_t DEFAULT_TENANT_INFO_REFRESH_TIME_US = 1000 * 1000;  // 1s
  static constexpr int64_t TENANT_INFO_LEASE_TIME_US = 2 * DEFAULT_TENANT_INFO_REFRESH_TIME_US;  // 2s
  static const char* const SWITCH_TO_PRIMARY_LOG_MOD_STR;
  static const char* const SWITCH_TO_STANDBY_LOG_MOD_STR;
  static const char* const RESTORE_TO_STANDBY_LOG_MOD_STR;
};

/*description:
 * for primary to standby and standby to primary
 */
class ObTenantRoleTransitionService
{
public:
struct LSAccessModeInfo
{
  LSAccessModeInfo(): tenant_id_(OB_INVALID_TENANT_ID), ls_id_(),
                    leader_addr_(),
                    mode_version_(palf::INVALID_PROPOSAL_ID),
                    access_mode_(palf::AccessMode::INVALID_ACCESS_MODE) { }
  virtual ~LSAccessModeInfo() {}
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
         && ls_id_.is_valid()
         && leader_addr_.is_valid()
         && palf::INVALID_PROPOSAL_ID != mode_version_
         && palf::AccessMode::INVALID_ACCESS_MODE != access_mode_;

  }
  int init(uint64_t tenant_id, const share::ObLSID &ls_id, const ObAddr &addr,
           const int64_t mode_version, const palf::AccessMode &access_mode);
  int assign(const LSAccessModeInfo &other);
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(leader_addr),
               K_(mode_version), K_(access_mode));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObAddr leader_addr_;
  int64_t mode_version_;
  palf::AccessMode access_mode_;
};

public:
  ObTenantRoleTransitionService(const uint64_t tenant_id,
      common::ObMySQLProxy *sql_proxy,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      const obrpc::ObSwitchTenantArg::OpType &switch_optype)
    : tenant_id_(tenant_id), sql_proxy_(sql_proxy),
    rpc_proxy_(rpc_proxy), switchover_epoch_(OB_INVALID_VERSION),
    switch_optype_(switch_optype) {}
  virtual ~ObTenantRoleTransitionService() {}
  int failover_to_primary();
  int check_inner_stat();
  int do_switch_access_mode_to_append(const share::ObAllTenantInfo &tenant_info,
                             const share::ObTenantRole &target_tenant_role);
  int do_switch_access_mode_to_raw_rw(const share::ObAllTenantInfo &tenant_info);
  void set_switchover_epoch(const int64_t switchover_epoch)
  {
    switchover_epoch_ = switchover_epoch;
  }

  /**
   * @description:
   *    Update scn/tenant_role/switchover status when switchover is executed
   *    scn is the current sync point obtained through rpc
   * @param[in] tenant_id
   * @param[in] switch_to_primary switch_to_primary or switch_to_standby
   * @param[in] new_role new tenant role
   * @param[in] old_status current switchover status
   * @param[in] new_status new switchover status
   * @param[in] old_switchover_epoch current switchover epoch
   * @param[out] new_tenant_info return the updated tenant_info
   * @return return code
   */
  int switchover_update_tenant_status(
      const uint64_t tenant_id,
      const bool switch_to_primary,
      const ObTenantRole &new_role,
      const ObTenantSwitchoverStatus &old_status,
      const ObTenantSwitchoverStatus &new_status,
      const int64_t old_switchover_epoch,
      ObAllTenantInfo &new_tenant_info);

  /**
   * @description:
   *    wait tenant sync to switchover checkpoint until timeout
   * @param[in] tenant_id
   * @param[in] primary_checkpoints primary switchover checkpoint
   * @return return code
   */
  int wait_tenant_sync_to_latest_until_timeout_(const uint64_t tenant_id,
                                                        const ObAllTenantInfo &tenant_info);

  /**
   * @description:
   *    do the checking to see whether the standby tenant ls has synchronize to primary tenant checkpoints
   * @param[in] tenant_id the tenant id to check
   * @param[in] primary_checkpoints primary switchover checkpoint
   * @param[out] has_sync_to_checkpoint whether the standby tenant sync to primary tenant checkpoints
   * @return return code
   */
  int check_sync_to_restore_source_(const uint64_t tenant_id,
                                    const ObAllTenantInfo &tenant_info,
                                    bool &has_sync_to_checkpoint);

  void broadcast_tenant_info(const char* const log_mode);

private:
  int do_failover_to_primary_(const share::ObAllTenantInfo &tenant_info);
  int do_prepare_flashback_(share::ObAllTenantInfo &tenant_info);
  int do_flashback_(const share::ObAllTenantInfo &tenant_info);
  int change_ls_access_mode_(palf::AccessMode target_access_mode,
                             const share::SCN &ref_scn);
  int update_tenant_stat_info_();
  int get_ls_access_mode_(ObIArray<LSAccessModeInfo> &ls_access_info);
  int do_change_ls_access_mode_(const ObIArray<LSAccessModeInfo> &ls_access_info,
                                palf::AccessMode target_access_mode,
                                const share::SCN &ref_scn);
<<<<<<< HEAD
=======
  int do_switch_access_mode_to_flashback(
    const share::ObAllTenantInfo &tenant_info);

  /**
   * @description:
   *    get specified ls list sync_scn by rpc, which is named as checkpoint
   * @param[in] tenant_id the tenant to get switchover checkpoint
   * @param[in] status_info_array ls list to get sync scn
   * @param[in] get_latest_scn whether to get latest scn
   * @param[out] checkpoints switchover checkpoint
   * @return return code
   */
  int get_checkpoints_by_rpc_(
      const uint64_t tenant_id,
      const share::ObLSStatusInfoIArray &status_info_array,
      const bool get_latest_scn,
      ObIArray<obrpc::ObCheckpoint> &checkpoints
  );

  /**
   * @description:
   *    get max ls sync_scn across all ls in checkpoints array
   * @param[in] checkpoints switchover checkpoint
   * @param[out] max_checkpoint_scn
   * @return return code
   */
  int get_max_checkpoint_scn_(
      const ObIArray<obrpc::ObCheckpoint> &checkpoints,
      share::SCN &max_checkpoint_scn);

  /**
   * @description:
   *    get sys ls sync_snapshot from checkpoints array
   * @param[in] checkpoints checkpoint
   * @return return code
   */
  int get_sys_ls_sync_scn_(
      const ObIArray<obrpc::ObCheckpoint> &checkpoints,
      share::SCN &sys_ls_sync_scn,
      bool &is_sync_to_latest);

  /**
   * @description:
   *    when switch to primary, check all ls are sync to latest
   * @param[in] tenant_id the tenant id to check
   * @param[in] tenant_info
   * @param[out] has_sync_to_latest whether sync to latest
   * @return return code
   */
  int check_sync_to_latest_(const uint64_t tenant_id,
                            const ObAllTenantInfo &tenant_info,
                            bool &has_sync_to_latest);

  int do_prepare_flashback_for_switch_to_primary_(share::ObAllTenantInfo &tenant_info);
  int do_prepare_flashback_for_failover_to_primary_(share::ObAllTenantInfo &tenant_info);

private:
  const static int64_t SEC_UNIT = 1000L * 1000L;
  const static int64_t PRINT_INTERVAL = 10 * 1000 * 1000L;

>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
private:
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  int64_t switchover_epoch_;
  obrpc::ObSwitchTenantArg::OpType switch_optype_;
};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_ROLE_TRANSITION_SERVICE_H */
