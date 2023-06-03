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

#define USING_LOG_PREFIX STANDBY
#include "ob_tenant_role_transition_service.h"
#include "logservice/palf/log_define.h"
#include "share/scn.h"
#include "logservice/ob_log_service.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"//ObChangeLSAccessModeProxy
#include "lib/oblog/ob_log_module.h"// LOG_*
#include "lib/utility/ob_print_utils.h"// TO_STRING_KV
#include "rootserver/ob_cluster_event.h"// CLUSTER_EVENT_ADD_CONTROL
#include "rootserver/ob_rs_event_history_table_operator.h" // ROOTSERVICE_EVENT_ADD
#include "share/ob_rpc_struct.h"//ObLSAccessModeInfo
#include "observer/ob_server_struct.h"//GCTX
#include "share/location_cache/ob_location_service.h"//get ls leader
#include "share/ob_schema_status_proxy.h"//set_schema_status
#include "storage/tx/ob_timestamp_service.h"  // ObTimestampService
#include "share/ob_primary_standby_service.h" // ObPrimaryStandbyService

namespace oceanbase
{
using namespace share;
using namespace palf;
namespace rootserver
{

const char* const ObTenantRoleTransitionConstants::SWITCH_TO_PRIMARY_LOG_MOD_STR = "SWITCH_TO_PRIMARY";
const char* const ObTenantRoleTransitionConstants::SWITCH_TO_STANDBY_LOG_MOD_STR = "SWITCH_TO_STANDBY";
const char* const ObTenantRoleTransitionConstants::RESTORE_TO_STANDBY_LOG_MOD_STR = "RESTORE_TO_STANDBY";

////////////LSAccessModeInfo/////////////////
int ObTenantRoleTransitionService::LSAccessModeInfo::init(
    uint64_t tenant_id, const ObLSID &ls_id, const ObAddr &addr,
    const int64_t mode_version,
    const palf::AccessMode &access_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid() || !addr.is_valid()
                  || palf::INVALID_PROPOSAL_ID == mode_version
                  || palf::AccessMode::INVALID_ACCESS_MODE == access_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(addr),
    K(access_mode), K(mode_version));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    leader_addr_ = addr;
    mode_version_ = mode_version;
    access_mode_ = access_mode;
  }
  return ret;
}

int ObTenantRoleTransitionService::LSAccessModeInfo::assign(const LSAccessModeInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    leader_addr_ = other.leader_addr_;
    mode_version_ = other.mode_version_; 
    access_mode_ = other.access_mode_;
  }
  return ret;
}

void ObTenantRoleTransitionService::LSAccessModeInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  leader_addr_.reset();
  access_mode_ = palf::AccessMode::INVALID_ACCESS_MODE;
  mode_version_ = palf::INVALID_PROPOSAL_ID; 
} 
////////////ObTenantRoleTransitionService//////////////
int ObTenantRoleTransitionService::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(rpc_proxy_) ||
      OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  }
  return ret;
} 

int ObTenantRoleTransitionService::failover_to_primary()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[ROLE_TRANSITION] start to failover to primary", KR(ret), K(tenant_id_));
  const int64_t start_service_time = ObTimeUtility::current_time();
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                    false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (tenant_info.is_primary()) {
    LOG_INFO("is primary tenant, no need failover");
  } else if (OB_UNLIKELY(!tenant_info.get_recovery_until_scn().is_valid_and_not_min())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("invalid recovery_until_scn", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recovery_until_scn is invalid, switch to primary");
  } else if (FALSE_IT(switchover_epoch_ = tenant_info.get_switchover_epoch())) {
  } else if ((tenant_info.is_normal_status())) {
    //do failover to primary
    if (OB_FAIL(do_failover_to_primary_(tenant_info))) {
      LOG_WARN("failed to do failover to primary", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_prepare_flashback_for_failover_to_primary_status()
             || tenant_info.is_prepare_flashback_for_switch_to_primary_status()) {
    //prepare flashback
    if (OB_FAIL(do_prepare_flashback_(tenant_info))) {
      LOG_WARN("failed to prepare flashback", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_flashback_status()) {
    if (OB_FAIL(do_flashback_(tenant_info))) {
      LOG_WARN("failed to flashback", KR(ret), K(tenant_info));
    }
  } else if (tenant_info.is_switching_to_primary_status()) {
    if (OB_FAIL(do_switch_access_mode_to_append(tenant_info, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("failed to switch access mode", KR(ret), K(tenant_info));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match", KR(ret), K(tenant_info), K_(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "switchover status not match, switch to primary");
  }

  if (OB_FAIL(ret)) {
  } else {
    (void)broadcast_tenant_info(ObTenantRoleTransitionConstants::SWITCH_TO_PRIMARY_LOG_MOD_STR);
  }

  const int64_t cost = ObTimeUtility::current_time() - start_service_time;
  LOG_INFO("[ROLE_TRANSITION] finish failover to primary", KR(ret), K(tenant_info), K(cost));
  return ret;
}
  
int ObTenantRoleTransitionService::do_failover_to_primary_(const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo new_tenant_info;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY != switch_optype_
                         && obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY != switch_optype_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected switch tenant action", KR(ret),
             K_(switch_optype), K(tenant_info), K_(tenant_id));
  } else if (OB_UNLIKELY(!(tenant_info.is_normal_status())
                 || tenant_info.is_primary()
                 || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY == switch_optype_
             && OB_FAIL(wait_tenant_sync_to_latest_until_timeout_(tenant_id_, tenant_info))) {
    LOG_WARN("fail to wait_tenant_sync_to_latest_until_timeout_", KR(ret), K_(tenant_id), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
                 tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
                 share::STANDBY_TENANT_ROLE, tenant_info.get_switchover_status(),
                 obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY == switch_optype_
                                       ? share::PREPARE_FLASHBACK_FOR_SWITCH_TO_PRIMARY_SWITCHOVER_STATUS
                                       : share::PREPARE_FLASHBACK_FOR_FAILOVER_TO_PRIMARY_SWITCHOVER_STATUS,
                 switchover_epoch_))) {
    LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id_), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                    false, new_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(new_tenant_info.get_switchover_epoch() != switchover_epoch_)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover is concurrency", KR(ret), K(switchover_epoch_), K(new_tenant_info)); 
  } else if (OB_FAIL(do_prepare_flashback_(new_tenant_info))) {
    LOG_WARN("failed to prepare flashback", KR(ret), K(new_tenant_info));
  }
  return ret;
}

// operation
// 1. create abort all ls
// 2. take all ls to flashback
// 3. get all ls max sync point
int ObTenantRoleTransitionService::do_prepare_flashback_(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_PREPARE_FLASHBACK);

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!(tenant_info.is_prepare_flashback_for_failover_to_primary_status()
                           || tenant_info.is_prepare_flashback_for_switch_to_primary_status()))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match, switch tenant not allow", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "switchover status not match, switchover tenant");
  } else if (OB_UNLIKELY(switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (obrpc::ObSwitchTenantArg::OpType::SWITCH_TO_PRIMARY == switch_optype_) {
    if (OB_FAIL(do_prepare_flashback_for_switch_to_primary_(tenant_info))) {
      LOG_WARN("failed to do_prepare_flashback_for_switch_to_primary_", KR(ret), K(tenant_info));
    }
  } else if (obrpc::ObSwitchTenantArg::OpType::FAILOVER_TO_PRIMARY == switch_optype_) {
    if (OB_FAIL(do_prepare_flashback_for_failover_to_primary_(tenant_info))) {
      LOG_WARN("failed to do_prepare_flashback_for_failover_to_primary_", KR(ret), K(tenant_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected switch tenant action", KR(ret),
             K_(switch_optype), K(tenant_info), K_(tenant_id));
  }

  DEBUG_SYNC(BEFORE_DO_FLASHBACK);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_flashback_(tenant_info))) {
    LOG_WARN("failed to prepare flashback", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObTenantRoleTransitionService::do_prepare_flashback_for_switch_to_primary_(
    share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObLogRestoreSourceMgr restore_source_mgr;
  ObLogRestoreSourceItem item;

  DEBUG_SYNC(PREPARE_FLASHBACK_FOR_SWITCH_TO_PRIMARY);

  LOG_INFO("start to do_prepare_flashback_for_switch_to_primary_", KR(ret), K_(tenant_id));

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_prepare_flashback_for_switch_to_primary_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match, switch to primary not allow", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "switchover status not match, switchover to primary");
  } else if (OB_UNLIKELY(switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K_(switchover_epoch));
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id_, sql_proxy_))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), K_(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(restore_source_mgr.get_source(item))) {
    LOG_WARN("failed to get_source", KR(ret), K_(tenant_id), K_(tenant_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      // When restore_source fails, in order to proceed switchover. If no restore_source is set,
      // do not check sync with restore_source
      LOG_INFO("failed to get_source", KR(ret), K_(tenant_id));
      ret = OB_SUCCESS;
    }
  } else {
    bool has_sync_to_latest = false;
    if (OB_FAIL(check_sync_to_latest_(tenant_id_, tenant_info, has_sync_to_latest))) {
      LOG_WARN("fail to check_sync_to_latest_", KR(ret), K_(tenant_id), K(tenant_info));
    } else if (!has_sync_to_latest) {
      if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
                    tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
                    share::STANDBY_TENANT_ROLE, share::PREPARE_FLASHBACK_FOR_SWITCH_TO_PRIMARY_SWITCHOVER_STATUS,
                    share::NORMAL_SWITCHOVER_STATUS, switchover_epoch_))) {
        LOG_WARN("failed to update tenant role", KR(ret), K_(tenant_id), K_(switchover_epoch), K(tenant_info));
      }

      // Ignore the fallback switchover status error code and report not sync to latest.
      // A fallback switchover status command needs to be provided.
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("not sync to latest", KR(ret), K(has_sync_to_latest), K_(tenant_id),
               K_(switchover_epoch), K(tenant_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not sync to latest, switch to primary");
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(switchover_update_tenant_status(tenant_id_,
                                              true /* switch_to_primary */,
                                              tenant_info.get_tenant_role(),
                                              tenant_info.get_switchover_status(),
                                              share::FLASHBACK_SWITCHOVER_STATUS,
                                              tenant_info.get_switchover_epoch(),
                                              tenant_info))) {
    LOG_WARN("failed to switchover_update_tenant_status", KR(ret), K_(tenant_id), K(tenant_info));
  }

  return ret;
}

int ObTenantRoleTransitionService::do_prepare_flashback_for_failover_to_primary_(
    share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K_(tenant_id), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_prepare_flashback_for_failover_to_primary_status())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switchover status not match, failover to primary not allow", KR(ret), K(tenant_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "switchover status not match, failover to primary");
  } else if (OB_UNLIKELY(switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K_(switchover_epoch));
  } else if (OB_FAIL(update_tenant_stat_info_())) {
    LOG_WARN("failed to update tenant stat info", KR(ret), K(tenant_info), K_(switchover_epoch));
  } else if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.do_recover_tenant(tenant_id_,
                     share::PREPARE_FLASHBACK_FOR_FAILOVER_TO_PRIMARY_SWITCHOVER_STATUS,
                     obrpc::ObRecoverTenantArg::RecoverType::CANCEL,
                     SCN::min_scn()))) {
    LOG_WARN("failed to do_recover_tenant", KR(ret), K_(tenant_id));
    // reset error code and USER_ERROR to avoid print recover error log
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "can not do recover cancel for tenant, failed to failover to primary");
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_switchover_status(
                     tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
                     tenant_info.get_switchover_status(), share::FLASHBACK_SWITCHOVER_STATUS))) {
    LOG_WARN("failed to update tenant switchover status", KR(ret), K_(tenant_id), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
             tenant_id_, sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(tenant_info.get_switchover_epoch() != switchover_epoch_)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover is concurrency", KR(ret), K(tenant_info), K_(switchover_epoch));
  }

  return ret;
}

int ObTenantRoleTransitionService::do_switch_access_mode_to_flashback(
    const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_flashback_status()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (OB_FAIL(change_ls_access_mode_(palf::AccessMode::FLASHBACK, SCN::base_scn()))) {
    LOG_WARN("failed to get access mode", KR(ret), K(tenant_info));
  }
  return ret;
}

int ObTenantRoleTransitionService::do_flashback_(const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  logservice::ObLogService *log_service = NULL;
  ObLSStatusOperator status_op;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_flashback_status()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info), K(switchover_epoch_));
  } else if (OB_FAIL(status_op.create_abort_ls_in_switch_tenant(
                     tenant_id_, tenant_info.get_switchover_status(),
                     tenant_info.get_switchover_epoch(), *sql_proxy_))) {
    LOG_WARN("failed to create abort ls", KR(ret), K_(tenant_id), K(tenant_info));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get MTL log_service", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(log_service->flashback(tenant_id_, tenant_info.get_sync_scn(), ctx.get_timeout()))) {
    LOG_WARN("failed to flashback", KR(ret), K(tenant_id_), K(tenant_info));
  } else {
    CLUSTER_EVENT_ADD_LOG(ret, "flashback end",
                      "tenant id", tenant_id_,
                      "switchover#", tenant_info.get_switchover_epoch(),
                      "flashback_scn#", tenant_info.get_sync_scn());

    ObAllTenantInfo new_tenant_info;
    if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_switchover_status(
            tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
            tenant_info.get_switchover_status(), share::SWITCHING_TO_PRIMARY_SWITCHOVER_STATUS))) {
      LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id_), K(tenant_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
                                                      false, new_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(new_tenant_info.get_switchover_epoch() != tenant_info.get_switchover_epoch())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover is concurrency", KR(ret), K(tenant_info), K(new_tenant_info));
    } else if (OB_FAIL(do_switch_access_mode_to_append(new_tenant_info, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("failed to prepare flashback", KR(ret), K(new_tenant_info));
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_switch_access_mode_to_append(
    const share::ObAllTenantInfo &tenant_info,
    const share::ObTenantRole &target_tenant_role)
{
  int ret = OB_SUCCESS;
  palf::AccessMode access_mode = logservice::ObLogService::get_palf_access_mode(target_tenant_role);
<<<<<<< HEAD
  const SCN ref_scn = tenant_info.get_ref_scn();
=======
  const SCN &ref_scn = tenant_info.get_sync_scn();
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_switching_to_primary_status()
        || target_tenant_role == tenant_info.get_tenant_role()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info),
        K(target_tenant_role), K(switchover_epoch_));
<<<<<<< HEAD
  } else if (OB_FAIL(change_ls_access_mode_(access_mode, ref_scn))) {
    LOG_WARN("failed to get access mode", KR(ret), K(access_mode), K(ref_scn), K(tenant_info));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
          tenant_id_, sql_proxy_, tenant_info.get_switchover_epoch(),
          share::PRIMARY_TENANT_ROLE, share::NORMAL_SWITCHOVER_STATUS, switchover_epoch_))) {
    LOG_WARN("failed to update tenant switchover status", KR(ret), K(tenant_id_), K(tenant_info));
  }
  return ret;
}

int ObTenantRoleTransitionService::try_create_abort_ls_(const share::ObTenantSwitchoverStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("switchover stauts not valid", KR(ret), K(status));
=======
    //TODO(yaoying):xianming
  } else if (OB_FAIL(change_ls_access_mode_(access_mode, ref_scn))) {
    LOG_WARN("failed to get access mode", KR(ret), K(access_mode), K(ref_scn), K(tenant_info));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  } else {
    common::ObMySQLTransaction trans;
    share::ObAllTenantInfo cur_tenant_info;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id), K_(tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, &trans, true, cur_tenant_info))) {
      LOG_WARN("failed to load all tenant info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(tenant_info.get_switchover_status() != cur_tenant_info.get_switchover_status()
                           || tenant_info.get_switchover_epoch() != cur_tenant_info.get_switchover_epoch())) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("Tenant status changed by concurrent operation, switch to primary not allowed",
               KR(ret), K(tenant_info), K(cur_tenant_info));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Tenant status changed by concurrent operation, switch to primary");
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
            tenant_id_, &trans, tenant_info.get_switchover_epoch(),
            share::PRIMARY_TENANT_ROLE, tenant_info.get_switchover_status(),
            share::NORMAL_SWITCHOVER_STATUS, switchover_epoch_))) {
      LOG_WARN("failed to update tenant switchover status", KR(ret), K(tenant_id_), K(tenant_info), K(cur_tenant_info));
    } else if (cur_tenant_info.get_recovery_until_scn().is_max()) {
      LOG_INFO("recovery_until_scn already is max_scn", KR(ret), K_(tenant_id), K(cur_tenant_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_recovery_until_scn(
                  tenant_id_, trans, switchover_epoch_, SCN::max_scn()))) {
      LOG_WARN("failed to update_tenant_recovery_until_scn", KR(ret), K_(tenant_id), K(tenant_info), K(cur_tenant_info));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_switch_access_mode_to_raw_rw(
    const share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  palf::AccessMode access_mode = logservice::ObLogService::get_palf_access_mode(STANDBY_TENANT_ROLE);
  ObLSStatusOperator status_op;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(!tenant_info.is_switching_to_standby_status()
        || !tenant_info.is_standby()
        || switchover_epoch_ != tenant_info.get_switchover_epoch())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant switchover status not valid", KR(ret), K(tenant_info),
        K(switchover_epoch_));
  } else if (OB_FAIL(status_op.create_abort_ls_in_switch_tenant(
                     tenant_id_, tenant_info.get_switchover_status(),
                     tenant_info.get_switchover_epoch(), *sql_proxy_))) {
    LOG_WARN("failed to create abort ls", KR(ret), K(tenant_info));
  } else if (OB_FAIL(change_ls_access_mode_(access_mode, SCN::base_scn()))) {
    LOG_WARN("failed to get access mode", KR(ret), K(access_mode), K(tenant_info));
  }
  return ret;
}

int ObTenantRoleTransitionService::update_tenant_stat_info_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else {
    //TODO, get all ls sync_scn, update __all_tenant_info,
    //the new sync_scn cannot larger than recovery_scn and sync_scn of sys_ls
  }
  LOG_INFO("[ROLE_TRANSITION] finish update tenant stat info", KR(ret), K(tenant_id_));
  return ret;
}

int ObTenantRoleTransitionService::change_ls_access_mode_(palf::AccessMode target_access_mode,
                             const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(palf::AccessMode::INVALID_ACCESS_MODE == target_access_mode 
                         || OB_INVALID_VERSION == switchover_epoch_
<<<<<<< HEAD
                         || !ref_scn.is_valid())) {
=======
                         || !ref_scn.is_valid_and_not_min())) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_access_mode), K(switchover_epoch_), K(ref_scn));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    ObArray<LSAccessModeInfo> ls_mode_info;
    ObArray<LSAccessModeInfo> need_change_info;
    //ignore error, try until success
    bool need_retry = true;
    do {
      ls_mode_info.reset();
      need_change_info.reset();
      //1. get current access mode
      if (ctx.is_timeouted()) {
        need_retry = false;
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", KR(ret));
      } else if (OB_FAIL(get_ls_access_mode_(ls_mode_info))) {
        LOG_WARN("failed to get ls access mode", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_mode_info.count(); ++i) {
          const LSAccessModeInfo &info = ls_mode_info.at(i);
          if (info.access_mode_ == target_access_mode) {
            //nothing, no need change
          } else if (OB_FAIL(need_change_info.push_back(info))) {
            LOG_WARN("failed to assign", KR(ret), K(i), K(info));
          }
        }
      }//end for
      //2. check epoch not change
      if (OB_SUCC(ret) && need_change_info.count() > 0) {
        ObAllTenantInfo new_tenant_info;
        if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                tenant_id_, sql_proxy_, false, new_tenant_info))) {
          LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
        } else if (switchover_epoch_ != new_tenant_info.get_switchover_epoch()) {
          need_retry = false;
          ret = OB_NEED_RETRY;
          LOG_WARN("epoch change, no need change access mode", KR(ret),
              K(switchover_epoch_), K(new_tenant_info),
              K(target_access_mode));
        }
      }
      //3. change access mode
      if (OB_SUCC(ret) && need_change_info.count() > 0) {
        if (OB_FAIL(do_change_ls_access_mode_(need_change_info,
                target_access_mode, ref_scn))) {
          LOG_WARN("failed to change ls access mode", KR(ret), K(need_change_info),
              K(target_access_mode), K(ref_scn));
        }
      }
      if (OB_SUCC(ret)) {
        break;
      }
    } while (need_retry);

  }
  LOG_INFO("[ROLE_TRANSITION] finish change ls mode", KR(ret), K(tenant_id_),
      K(target_access_mode), K(ref_scn));
  return ret;

}

int ObTenantRoleTransitionService::get_ls_access_mode_(ObIArray<LSAccessModeInfo> &ls_access_info)
{
  int ret = OB_SUCCESS;
  ls_access_info.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else {
    share::ObLSStatusInfoArray status_info_array;
    ObLSStatusOperator status_op;
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret));
    } else if (OB_FAIL(status_op.get_all_ls_status_by_order_for_switch_tenant(tenant_id_,
                       false/* ignore_need_create_abort */, status_info_array, *sql_proxy_))) {
      LOG_WARN("fail to get_all_ls_status_by_order", KR(ret), K_(tenant_id), KP(sql_proxy_));
    } else {
      ObAddr leader;
      ObGetLSAccessModeProxy proxy(
          *rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_access_mode);
      obrpc::ObGetLSAccessModeInfoArg arg;
      int64_t rpc_count = 0;
      ObArray<int> return_code_array;
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
        return_code_array.reset();
        const ObLSStatusInfo &info = status_info_array.at(i);
        const int64_t timeout = ctx.get_timeout();
        if (OB_FAIL(GCTX.location_service_->get_leader(
          GCONF.cluster_id, tenant_id_, info.ls_id_, false, leader))) {
          LOG_WARN("failed to get leader", KR(ret), K(tenant_id_), K(info));
        } else if (OB_FAIL(arg.init(tenant_id_, info.ls_id_))) {
          LOG_WARN("failed to init arg", KR(ret), K(tenant_id_), K(info));
        // use meta rpc process thread
        } else if (OB_FAIL(proxy.call(leader, timeout, GCONF.cluster_id, gen_meta_tenant_id(tenant_id_), arg))) {
          //can not ignore error of each ls
          LOG_WARN("failed to send rpc", KR(ret), K(leader), K(timeout), K(tenant_id_), K(arg));
        } else {
          rpc_count++;
        }
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS !=(tmp_ret = GCTX.location_service_->nonblock_renew(
                  GCONF.cluster_id, tenant_id_, info.ls_id_))) {
            LOG_WARN("failed to renew location", KR(ret), K(tenant_id_), K(info));
          }
        }
      }//end for

      //get result
      if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
        LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(ret)) {
        //no need to process return code
      } else if (rpc_count != return_code_array.count() ||
                 rpc_count != proxy.get_args().count() ||
                 rpc_count != proxy.get_results().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc count not equal to result count", KR(ret),
                 K(rpc_count), K(return_code_array), "arg count",
                 proxy.get_args().count());
      } else {
        LSAccessModeInfo info;
        for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
          ret = return_code_array.at(i);
          const obrpc::ObGetLSAccessModeInfoArg &arg = proxy.get_args().at(i);
          if (OB_FAIL(ret)) {
            LOG_WARN("send rpc is failed", KR(ret), K(i));
          } else {
            const auto *result = proxy.get_results().at(i);
            const ObAddr &leader = proxy.get_dests().at(i);
            if (OB_ISNULL(result)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result is null", KR(ret), K(i));
            } else if (OB_FAIL(info.init(tenant_id_, result->get_ls_id(),
            leader, result->get_mode_version(), result->get_access_mode()))) {
              LOG_WARN("failed to init info", KR(ret), KPC(result), K(leader));
            } else if (OB_FAIL(ls_access_info.push_back(info))) {
              LOG_WARN("failed to push back info", KR(ret), K(info));
            }
          }
          LOG_INFO("[ROLE_TRANSITION] get ls access mode", KR(ret), K(arg));
       
          if (OB_FAIL(ret)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS !=(tmp_ret = GCTX.location_service_->nonblock_renew(
                     GCONF.cluster_id, tenant_id_, arg.get_ls_id()))) {
              LOG_WARN("failed to renew location", KR(ret), K(tenant_id_),
                       K(arg));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::do_change_ls_access_mode_(const ObIArray<LSAccessModeInfo> &ls_access_info,
                                palf::AccessMode target_access_mode, const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(0== ls_access_info.count()
                         || palf::AccessMode::INVALID_ACCESS_MODE == target_access_mode 
<<<<<<< HEAD
                         || !ref_scn.is_valid())) {
=======
                         || !ref_scn.is_valid_and_not_min())) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_access_mode), K(ls_access_info), K(ref_scn));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx,
                                                          GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else {
    ObChangeLSAccessModeProxy proxy(*rpc_proxy_, &obrpc::ObSrvRpcProxy::change_ls_access_mode);
    obrpc::ObLSAccessModeInfo arg;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_access_info.count(); ++i) {
      const LSAccessModeInfo &info = ls_access_info.at(i);
      const int64_t timeout = ctx.get_timeout();
      if (OB_FAIL(arg.init(tenant_id_, info.ls_id_, info.mode_version_, target_access_mode, ref_scn))) {
        LOG_WARN("failed to init arg", KR(ret), K(info), K(target_access_mode), K(ref_scn));
<<<<<<< HEAD
      } else if (OB_FAIL(proxy.call(info.leader_addr_, timeout, GCONF.cluster_id, tenant_id_, arg))) {
=======
      // use meta rpc process thread
      } else if (OB_FAIL(proxy.call(info.leader_addr_, timeout, GCONF.cluster_id, gen_meta_tenant_id(tenant_id_), arg))) {
        //can not ignore of each ls
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
        LOG_WARN("failed to send rpc", KR(ret), K(info), K(timeout), K(tenant_id_), K(arg));
      }
    }//end for
    //result
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    const int64_t rpc_count = ls_access_info.count();
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
    } else if (OB_FAIL(ret)) {
    } else if (rpc_count != return_code_array.count() ||
               rpc_count != proxy.get_args().count() ||
               rpc_count != proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc count not equal to result count", KR(ret), K(rpc_count),
               K(return_code_array), "arg count", proxy.get_args().count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        ret = return_code_array.at(i);
        const obrpc::ObLSAccessModeInfo &arg = proxy.get_args().at(i);
        const auto *result = proxy.get_results().at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("send rpc is failed", KR(ret), K(i));
        } else if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), K(i));
        } else if (OB_FAIL(result->get_result())) {
          LOG_WARN("failed to change ls mode", KR(ret), KPC(result));
        }

        LOG_INFO("[ROLE_TRANSITION] change ls access mode", KR(ret), K(arg), KPC(result),
            "leader", proxy.get_dests().at(i));
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS !=
              (tmp_ret = GCTX.location_service_->nonblock_renew(
                   GCONF.cluster_id, tenant_id_, arg.get_ls_id()))) {
            LOG_WARN("failed to renew location", KR(ret), K(tenant_id_),
                     K(arg));
          }
        }
      }// end for
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::switchover_update_tenant_status(
    const uint64_t tenant_id,
    const bool switch_to_primary,
    const ObTenantRole &new_role,
    const ObTenantSwitchoverStatus &old_status,
    const ObTenantSwitchoverStatus &new_status,
    const int64_t old_switchover_epoch,
    ObAllTenantInfo &new_tenant_info)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  SCN max_checkpoint_scn = SCN::min_scn();
  SCN max_sys_ls_sync_scn = SCN::min_scn();
  ObMySQLTransaction trans;
  ObLSStatusOperator status_op;
  share::ObLSStatusInfoArray status_info_array;
  common::ObArray<obrpc::ObCheckpoint> switchover_checkpoints;
  bool is_sync_to_latest = false;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
                  || !new_role.is_valid()
                  || !old_status.is_valid()
                  || !new_status.is_valid()
                  || OB_INVALID_VERSION == old_switchover_epoch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(new_role), K(old_status), K(new_status), K(old_switchover_epoch));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  // switch_to_primary: ignore_need_create_abort = true
  // switch_to_standby: ignore_need_create_abort = false. There should be no LS that needs to create abort
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order_for_switch_tenant(tenant_id,
                      switch_to_primary/* ignore_need_create_abort */, status_info_array, *sql_proxy_))) {
    LOG_WARN("failed to get_all_ls_status_by_order", KR(ret), K(tenant_id), KP(sql_proxy_));
  } else if (OB_UNLIKELY(0 >= status_info_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls list is null", KR(ret), K(tenant_id), K(status_info_array));
  } else if (OB_FAIL(get_checkpoints_by_rpc_(tenant_id, status_info_array, false/* check_sync_to_latest */,
                                             switchover_checkpoints))) {
    LOG_WARN("fail to get_checkpoints_by_rpc_", KR(ret), K(tenant_id), K(status_info_array));
  } else if (OB_FAIL(get_max_checkpoint_scn_(switchover_checkpoints, max_checkpoint_scn))) {
    LOG_WARN("fail to get_max_checkpoint_scn_", KR(ret), K(tenant_id), K(switchover_checkpoints));
  } else if (OB_FAIL(get_sys_ls_sync_scn_(switchover_checkpoints, max_sys_ls_sync_scn, is_sync_to_latest))) {
    LOG_WARN("failed to get_sys_ls_sync_scn_", KR(ret), K(switchover_checkpoints));
  } else if (OB_UNLIKELY(!max_checkpoint_scn.is_valid_and_not_min() || !max_sys_ls_sync_scn.is_valid_and_not_min())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid checkpoint_scn", KR(ret), K(tenant_id), K(max_checkpoint_scn),
                                       K(max_sys_ls_sync_scn), K(switchover_checkpoints));
  } else {
    ObAllTenantInfo tmp_tenant_info;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    /**
     * @description:
     * In order to make timestamp monotonically increasing before and after switch tenant role
     * set readable_scn to gts_upper_limit:
     *    MAX{MAX_SYS_LS_LOG_TS + 2 * preallocated_range}
     * set sync_snc to
     *    MAX{MAX{LS max_log_ts}, MAX_SYS_LS_LOG_TS + 2 * preallocated_range}
     * Because replayable point is not support, set replayable_scn = sync_scn
     */
    const SCN gts_upper_limit = transaction::ObTimestampService::get_sts_start_scn(max_sys_ls_sync_scn);

    const SCN final_sync_scn = MAX(max_checkpoint_scn, gts_upper_limit);
    const SCN final_replayable_scn = final_sync_scn;
    SCN final_readable_scn = SCN::min_scn();
    SCN final_recovery_until_scn = SCN::min_scn();
    if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                       tenant_id, &trans, true /* for update */, tmp_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(old_switchover_epoch != tmp_tenant_info.get_switchover_epoch()
                           || old_status != tmp_tenant_info.get_switchover_status())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("switchover may concurrency, need retry", KR(ret), K(old_switchover_epoch),
               K(old_status), K(tmp_tenant_info));
    } else {
      if (switch_to_primary) {
        // switch_to_primary
        // Does not change STS
        final_readable_scn = tmp_tenant_info.get_standby_scn();
        // To prevent unexpected sync log, set recovery_until_scn = sync_scn
        final_recovery_until_scn = final_sync_scn;
      } else {
        // switch_to_standby
        // STS >= GTS
        final_readable_scn = gts_upper_limit;
        // Does not change recovery_until_scn, it should be MAX_SCN,
        // also check it when just entering switch_to_standby
        final_recovery_until_scn = tmp_tenant_info.get_recovery_until_scn();
        if (OB_UNLIKELY(!final_recovery_until_scn.is_max())) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("recovery_until_scn is not max_scn ", KR(ret), K(tenant_id),
                   K(final_recovery_until_scn), K(tmp_tenant_info));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recovery_until_scn is not max_scn, switchover to standby");
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_status(tenant_id,
                                                                  trans,
                                                                  new_role,
                                                                  old_status,
                                                                  new_status,
                                                                  final_sync_scn,
                                                                  final_replayable_scn,
                                                                  final_readable_scn,
                                                                  final_recovery_until_scn,
                                                                  old_switchover_epoch))) {
      LOG_WARN("failed to update_tenant_status", KR(ret), K(tenant_id), K(new_role),
               K(old_status), K(new_status), K(final_sync_scn), K(final_replayable_scn),
               K(final_readable_scn), K(final_recovery_until_scn), K(old_switchover_epoch));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                    tenant_id, &trans, false /* for update */, new_tenant_info))) {
      LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
    }
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, KR(temp_ret));
      ret = OB_SUCC(ret) ? temp_ret : ret;
    }
  }

  CLUSTER_EVENT_ADD_LOG(ret, "update tenant status",
                  "tenant id", tenant_id,
                  "old switchover#", old_switchover_epoch,
                  "new switchover#", new_tenant_info.get_switchover_epoch(),
                  K(new_role), K(old_status), K(new_status));
  return ret;
}

int ObTenantRoleTransitionService::wait_tenant_sync_to_latest_until_timeout_(
                                            const uint64_t tenant_id,
                                            const ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  ObLogRestoreSourceMgr restore_source_mgr;
  ObLogRestoreSourceItem item;
  bool has_restore_source = true;
  int64_t begin_time = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id, sql_proxy_))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), K(tenant_id), KP(sql_proxy_));
  } else if (OB_FAIL(restore_source_mgr.get_source(item))) {
    LOG_WARN("failed to get_source", KR(ret), K(tenant_id), K(tenant_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      // When restore_source fails, in order to proceed switchover. If no restore_source is set,
      // do not check sync with restore_source
      LOG_INFO("failed to get_source", KR(ret), K(tenant_id), K(tenant_id));
      has_restore_source = false;
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret) || !has_restore_source) {
  } else {
    bool has_sync_to_latest = false;
    while (!THIS_WORKER.is_timeout() && !logservice::ObLogRestoreHandler::need_fail_when_switch_to_primary(ret)) {
      has_sync_to_latest = false;
      if (OB_FAIL(check_sync_to_latest_(tenant_id, tenant_info, has_sync_to_latest))) {
        LOG_WARN("fail to check_sync_to_latest_", KR(ret), K(tenant_id),
                                                      K(tenant_info), K(has_sync_to_latest));
      } else if (has_sync_to_latest) {
        LOG_INFO("sync to latest", K(has_sync_to_latest), K(tenant_id));
        break;
      } else {
        LOG_WARN("not sync to latest, wait a while", K(tenant_id));
      }
      usleep(10L * 1000L);
    }
    if (logservice::ObLogRestoreHandler::need_fail_when_switch_to_primary(ret)) {
    } else if (THIS_WORKER.is_timeout() || !has_sync_to_latest) {
      // return NOT_ALLOW instead of timeout
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("has not sync to latest, can not swithover to primary", KR(ret));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not sync to latest, switchover to primary");
    } else if (OB_SUCC(ret)) {
      LOG_INFO("finish check sync to latest", K(has_sync_to_latest));
    }

    int64_t cost = ObTimeUtility::current_time() - begin_time;
    LOG_INFO("check sync to latest", KR(ret), K(cost),
        "is_pass", has_sync_to_latest);
    CLUSTER_EVENT_ADD_LOG(ret, "wait sync to latest end",
                          "tenant id", tenant_id,
                          "switchover#", tenant_info.get_switchover_epoch(),
                          "finished", OB_SUCC(ret) ? "yes" : "no",
                          "cost sec", cost / SEC_UNIT);
  }
  return ret;
}

int ObTenantRoleTransitionService::check_sync_to_latest_(const uint64_t tenant_id,
                                                   const ObAllTenantInfo &tenant_info,
                                                   bool &has_sync_to_latest)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  has_sync_to_latest = false;
  ObLSStatusOperator ls_status_op;
  share::ObLSStatusInfoArray all_ls_status_array;
  share::ObLSStatusInfoArray sys_ls_status_array;
  common::ObArray<obrpc::ObCheckpoint> switchover_checkpoints;
  ObLSRecoveryStatOperator ls_recovery_operator;
  ObLSRecoveryStat sys_ls_recovery_stat;
  SCN sys_ls_sync_scn = SCN::min_scn();
  bool sys_ls_sync_to_latest = false;
  share::ObLSStatusInfo ls_status;

  LOG_INFO("start to check_sync_to_latest", KR(ret), K(tenant_id), K(tenant_info));

  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(ls_status_op.get_ls_status_info(tenant_id, SYS_LS, ls_status, *sql_proxy_))) {
    LOG_WARN("failed to get ls status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_ls_status_array.push_back(ls_status))) {
    LOG_WARN("fail to push back", KR(ret), K(ls_status), K(sys_ls_status_array));
  } else if (OB_FAIL(get_checkpoints_by_rpc_(tenant_id, sys_ls_status_array, true/* check_sync_to_latest */,
                                             switchover_checkpoints))) {
    LOG_WARN("fail to get_checkpoints_by_rpc_", KR(ret), K(tenant_id), K(sys_ls_status_array));
  } else if (1 != switchover_checkpoints.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("checkpoints count is not 1", KR(ret), K(switchover_checkpoints), K(tenant_id),
                                           K(tenant_info), K(sys_ls_status_array));
  } else if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, SYS_LS,
        false/*for_update*/, sys_ls_recovery_stat, *sql_proxy_))) {
    LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sys_ls_sync_scn_(switchover_checkpoints, sys_ls_sync_scn, sys_ls_sync_to_latest))) {
    LOG_WARN("failed to get_sys_ls_sync_scn_", KR(ret), K(switchover_checkpoints));
  } else if (!(sys_ls_sync_scn.is_valid_and_not_min() && sys_ls_sync_to_latest
             && sys_ls_recovery_stat.get_sync_scn() == sys_ls_sync_scn)) {
    LOG_WARN("sys ls not sync, keep waiting", KR(ret), K(sys_ls_sync_scn), K(sys_ls_sync_to_latest),
             K(sys_ls_recovery_stat), K(switchover_checkpoints));
  // SYS LS is sync, check other LS
  } else if (OB_FAIL(ls_status_op.get_all_ls_status_by_order_for_switch_tenant(tenant_id,
                      true/* ignore_need_create_abort */, all_ls_status_array, *sql_proxy_))) {
    LOG_WARN("failed to get_all_ls_status_by_order", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_checkpoints_by_rpc_(tenant_id, all_ls_status_array, true/* check_sync_to_latest */,
                                             switchover_checkpoints))) {
    LOG_WARN("fail to get_checkpoints_by_rpc_", KR(ret), K(tenant_id), K(all_ls_status_array));
  } else if (switchover_checkpoints.count() != all_ls_status_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect checkpoints count", KR(ret), K(switchover_checkpoints), K(tenant_id), K(tenant_info),
                                       K(all_ls_status_array));
  } else {
    has_sync_to_latest = true;
    for (int64_t i = 0; i < switchover_checkpoints.count() && OB_SUCC(ret) && has_sync_to_latest; i++) {
      const auto &checkpoint = switchover_checkpoints.at(i);
      if (checkpoint.is_sync_to_latest()) {
        // LS is sync
      } else {
        has_sync_to_latest = false;
        LOG_WARN("ls not sync, keep waiting", KR(ret), K(checkpoint));
      }
    }//end for
  }

  LOG_INFO("check sync to latest", KR(ret));
  int64_t cost = ObTimeUtility::current_time() - begin_time;
  if (REACH_TIME_INTERVAL(PRINT_INTERVAL) || has_sync_to_latest) {
    CLUSTER_EVENT_ADD_LOG(ret, "wait tenant sync from latest",
                      "tenant id", tenant_id,
                      "is sync", has_sync_to_latest ? "yes" : "no",
                      "switchover#", tenant_info.get_switchover_epoch(),
                      "finished", OB_SUCC(ret) ? "yes" : "no",
                      "checkpoint", switchover_checkpoints,
                      "cost sec", cost / SEC_UNIT);
  }
  return ret;
}

int ObTenantRoleTransitionService::get_checkpoints_by_rpc_(const uint64_t tenant_id,
                                                     const share::ObLSStatusInfoIArray &status_info_array,
                                                     const bool check_sync_to_latest,
                                                     ObIArray<obrpc::ObCheckpoint> &checkpoints)
{
  int ret = OB_SUCCESS;
  checkpoints.reset();

  LOG_INFO("start to get_checkpoints_by_rpc_", KR(ret), K(tenant_id), K(check_sync_to_latest), K(status_info_array));
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("error unexpected", KR(ret), KP(sql_proxy_));
  } else if (!is_user_tenant(tenant_id) || 0 >= status_info_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(status_info_array));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));
  } else {
    ObAddr leader;
    ObGetLSSyncScnProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::get_ls_sync_scn);
    obrpc::ObGetLSSyncScnArg arg;
    int64_t rpc_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const ObLSStatusInfo &info = status_info_array.at(i);
      const int64_t timeout_us = INT64_MAX == THIS_WORKER.get_timeout_remain() ?
        GCONF.rpc_timeout : THIS_WORKER.get_timeout_remain();
      if (OB_FAIL(GCTX.location_service_->get_leader(
        GCONF.cluster_id, tenant_id, info.ls_id_, false, leader))) {
        LOG_WARN("failed to get leader", KR(ret), K(tenant_id), K(info));
      } else if (OB_FAIL(arg.init(tenant_id, info.ls_id_, check_sync_to_latest))) {
        LOG_WARN("failed to init arg", KR(ret), K(tenant_id), K(info));
      // use meta rpc process thread
      } else if (OB_FAIL(proxy.call(leader, timeout_us, gen_meta_tenant_id(tenant_id), arg))) {
        LOG_WARN("failed to send rpc", KR(ret), K(leader), K(timeout_us), K(tenant_id), K(arg));
      } else {
        rpc_count++;
      }
    }//end for
    //get result
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (rpc_count != return_code_array.count() ||
                rpc_count != proxy.get_args().count() ||
                rpc_count != proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc count not equal to result count", KR(ret),
                K(rpc_count), K(return_code_array), "arg count",
                proxy.get_args().count());
    } else if (OB_FAIL(ret)) {
    } else {
      ObGetLSSyncScnRes res;
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        tmp_ret = return_code_array.at(i);
        if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("get checkpoints: send rpc failed", KR(ret), KR(tmp_ret), K(i));
          const obrpc::ObGetLSSyncScnArg &arg = proxy.get_args().at(i);
          if (OB_SUCCESS !=(tmp_ret = GCTX.location_service_->nonblock_renew(
                    GCONF.cluster_id, tenant_id, arg.get_ls_id()))) {
            LOG_WARN("failed to renew location", KR(tmp_ret), K(tenant_id), K(arg));
          }
        } else {
          const auto *result = proxy.get_results().at(i);
          const ObAddr &leader = proxy.get_dests().at(i);
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", KR(ret), K(i));
          } else {
            ObCheckpoint checkpoint(result->get_ls_id(), result->get_cur_sync_scn(), result->get_cur_restore_source_max_scn());
            if (OB_FAIL(checkpoints.push_back(checkpoint))) {
              LOG_WARN("failed to push back checkpoint", KR(ret), K(checkpoint));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantRoleTransitionService::get_max_checkpoint_scn_(
  const ObIArray<obrpc::ObCheckpoint> &checkpoints, SCN &max_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  max_checkpoint_scn.set_min();
  if (OB_UNLIKELY(0 >= checkpoints.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("checkpoint list is null", KR(ret), K(checkpoints));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < checkpoints.count(); ++i) {
      const obrpc::ObCheckpoint &checkpoint = checkpoints.at(i);
      if (max_checkpoint_scn < checkpoint.get_cur_sync_scn()) {
        max_checkpoint_scn = checkpoint.get_cur_sync_scn();
      }
    }
  }

  return ret;
}

int ObTenantRoleTransitionService::get_sys_ls_sync_scn_(
  const ObIArray<obrpc::ObCheckpoint> &checkpoints,
  SCN &sys_ls_sync_scn,
  bool &is_sync_to_latest)
{
  int ret = OB_SUCCESS;
  sys_ls_sync_scn.reset();
  is_sync_to_latest = false;
  if (0 >= checkpoints.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("checkpoints count is 0", KR(ret), K(checkpoints));
  } else {
    bool found = false;
    for (int64_t i = 0; i < checkpoints.count() && OB_SUCC(ret); i++) {
      const auto &checkpoint = checkpoints.at(i);
      if (checkpoint.get_ls_id().is_sys_ls()) {
        sys_ls_sync_scn = checkpoint.get_cur_sync_scn();
        is_sync_to_latest = checkpoint.is_sync_to_latest();
        found = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_ERROR("fail to get sys_ls_sync_scn", KR(ret), K(checkpoints));
    }
  }
  return ret;
}

void ObTenantRoleTransitionService::broadcast_tenant_info(const char* const log_mode)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = ObTenantRoleTransitionConstants::TENANT_INFO_LEASE_TIME_US; // tenant info lease is 200ms
  int64_t timeout_us = 0;
  const int64_t timeout_remain = THIS_WORKER.get_timeout_remain();
  ObUnitTableOperator unit_operator;
  common::ObArray<ObUnit> units;
  const int64_t begin_time = ObTimeUtility::current_time();
  ObArray<int> return_code_array;

  if (OB_FAIL(check_inner_stat())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_ISNULL(GCTX.server_tracer_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.server_tracer_), KP(GCTX.srv_rpc_proxy_));
  } else if (0 > timeout_remain) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid timeout_remain", KR(ret), K(timeout_remain));
  } else if (INT64_MAX == timeout_remain || timeout_remain > DEFAULT_TIMEOUT_US) {
    timeout_us = DEFAULT_TIMEOUT_US;
  } else {
    timeout_us = timeout_remain;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id_, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K_(tenant_id));
  } else {
    ObRefreshTenantInfoProxy proxy(
        *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::refresh_tenant_info);
    int64_t rpc_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
      obrpc::ObRefreshTenantInfoArg arg;
      const ObUnit &unit = units.at(i);
      bool alive = true;
      int64_t trace_time;
      if (OB_FAIL(GCTX.server_tracer_->is_alive(unit.server_, alive, trace_time))) {
        LOG_WARN("check server alive failed", KR(ret), K(unit));
      } else if (!alive) {
        //not send to alive
      } else if (OB_FAIL(arg.init(tenant_id_))) {
        LOG_WARN("failed to init arg", KR(ret), K_(tenant_id));
      // use meta rpc process thread
      } else if (OB_FAIL(proxy.call(unit.server_, timeout_us, gen_meta_tenant_id(tenant_id_), arg))) {
        LOG_WARN("failed to send rpc", KR(ret), K(unit), K(timeout_us), K_(tenant_id), K(arg));
      } else {
        rpc_count++;
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (rpc_count != return_code_array.count() ||
                rpc_count != proxy.get_args().count() ||
                rpc_count != proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc count not equal to result count", KR(ret),
                K(rpc_count), K(return_code_array), "arg count",
                proxy.get_args().count(), K(proxy.get_results().count()));
    } else if (OB_FAIL(ret)) {
    } else {
      ObRefreshTenantInfoRes res;
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        ret = return_code_array.at(i);
        const ObAddr &dest = proxy.get_dests().at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("send rpc is failed", KR(ret), K(i), K(dest));
        } else {
          LOG_INFO("refresh_tenant_info success", KR(ret), K(i), K(dest));
        }
      }
    }
  }
  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  LOG_INFO("broadcast_tenant_info finished", KR(ret), K(log_mode), K_(tenant_id), K(cost_time),
           K(units), K(return_code_array));
  return ;
}

}
}
