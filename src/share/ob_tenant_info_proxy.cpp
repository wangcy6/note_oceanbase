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

#define USING_LOG_PREFIX SHARE

#include "share/ob_tenant_info_proxy.h"
#include "share/ob_cluster_role.h"//ObClusterTYPE
#include "share/ob_share_util.h"//ObShareUtil
#include "share/config/ob_server_config.h"//GCONF
#include "share/inner_table/ob_inner_table_schema.h"//ALL_TENANT_INFO_TNAME
#include "share/ls/ob_ls_i_life_manager.h"//TODO SCN VALUE
#include "share/ls/ob_ls_recovery_stat_operator.h"//ObLSRecoveryStatOperator
#include "lib/string/ob_sql_string.h"//ObSqlString
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTrans
#include "common/ob_timeout_ctx.h"//ObTimeoutCtx
#include "rootserver/ob_root_utils.h"//ObRootUtils
#include "rootserver/ob_rs_event_history_table_operator.h" // ROOTSERVICE_EVENT_ADD
#include "share/restore/ob_log_restore_source_mgr.h"  // ObLogRestoreSourceMgr

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{

bool is_valid_tenant_scn(
  const SCN &sync_scn,
  const SCN &replayable_scn,
  const SCN &standby_scn,
  const SCN &recovery_until_scn)
{
  return standby_scn <= replayable_scn && replayable_scn <= sync_scn && sync_scn <= recovery_until_scn;
}

SCN gen_new_sync_scn(const SCN &cur_sync_scn, const SCN &desired_sync_scn, const SCN &cur_recovery_until_scn)
{
  return MIN(MAX(cur_sync_scn, desired_sync_scn), cur_recovery_until_scn);
}

SCN gen_new_replayable_scn(const SCN &cur_replayable_scn, const SCN &desired_replayable_scn, const SCN &new_sync_scn)
{
  return MIN(MAX(cur_replayable_scn, desired_replayable_scn), new_sync_scn);
}

SCN gen_new_standby_scn(const SCN &cur_standby_scn, const SCN &desired_standby_scn, const SCN &new_replayable_scn)
{
  return MIN(MAX(cur_standby_scn, desired_standby_scn), new_replayable_scn);
}

////////////ObAllTenantInfo
DEFINE_TO_YSON_KV(ObAllTenantInfo,
                  OB_ID(tenant_id), tenant_id_,
                  OB_ID(switchover_epoch), switchover_epoch_,
                  OB_ID(sync_scn), sync_scn_,
                  OB_ID(replayable_scn), replayable_scn_,
                  OB_ID(standby_scn), standby_scn_,
                  OB_ID(recovery_until_scn), recovery_until_scn_,
                  OB_ID(tenant_role), tenant_role_,
                  OB_ID(switchover_status), switchover_status_);

bool ObAllTenantInfo::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
<<<<<<< HEAD
         && sync_scn_.is_valid()
         && (sync_scn_ != SCN::min_scn())
         && replayable_scn_.is_valid()
         && (replayable_scn_ != SCN::min_scn())
         && standby_scn_.is_valid()
         && (standby_scn_ != SCN::min_scn())
         && recovery_until_scn_.is_valid()
         && (recovery_until_scn_ != SCN::min_scn())
         && tenant_role_.is_valid();
}

const SCN ObAllTenantInfo::get_ref_scn() const
{
  SCN ref_scn;
  if (!sync_scn_.is_valid()) {
    LOG_WARN("sync scn is invalid", K(sync_scn_));
  } else {
    ref_scn = sync_scn_;
  }
  return ref_scn;
}


int ObAllTenantInfo::init(const uint64_t tenant_id, const ObTenantRole tenant_role)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_role.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_role));
  } else {
    tenant_id_ = tenant_id;
    tenant_role_ = tenant_role;
    switchover_status_ = ObTenantSwitchoverStatus::NORMAL_STATUS;
    switchover_epoch_ = 0;
    sync_scn_.set_base();
    replayable_scn_.set_base();
    standby_scn_.set_base();
    recovery_until_scn_.set_max();
  }
  return ret;
}

int ObAllTenantInfo::init(
    const uint64_t tenant_id, const ObTenantRole &tenant_role, const ObTenantSwitchoverStatus &switchover_status,
    int64_t switchover_epoch, const SCN &sync_scn, const SCN &replayable_scn,
    const SCN &standby_scn, const SCN &recovery_until_scn)
=======
         && 0 <= switchover_epoch_
         && sync_scn_.is_valid_and_not_min()
         && replayable_scn_.is_valid_and_not_min()
         && standby_scn_.is_valid_and_not_min()
         && recovery_until_scn_.is_valid_and_not_min()
         && tenant_role_.is_valid()
         && switchover_status_.is_valid()
         && log_mode_.is_valid()
         && is_valid_tenant_scn(sync_scn_, replayable_scn_, standby_scn_, recovery_until_scn_);
}

int ObAllTenantInfo::init(
    const uint64_t tenant_id,
    const ObTenantRole &tenant_role,
    const ObTenantSwitchoverStatus &switchover_status,
    int64_t switchover_epoch,
    const SCN &sync_scn,
    const SCN &replayable_scn,
    const SCN &standby_scn,
    const SCN &recovery_until_scn,
    const ObArchiveMode &log_mode)
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_role.is_valid()
                  || !switchover_status.is_valid()
                  || 0 > switchover_epoch
                  || !sync_scn.is_valid_and_not_min()
                  || !replayable_scn.is_valid_and_not_min()
                  || !standby_scn.is_valid_and_not_min()
                  || !recovery_until_scn.is_valid_and_not_min()
                  || !log_mode.is_valid()
                  || !is_valid_tenant_scn(sync_scn, replayable_scn, standby_scn, recovery_until_scn))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_role), K(switchover_status),
             K(switchover_epoch), K(sync_scn), K(replayable_scn), K(standby_scn), K(recovery_until_scn),
             K(log_mode));
  } else {
    tenant_id_ = tenant_id;
    tenant_role_ = tenant_role;
    switchover_status_ = switchover_status;
    switchover_epoch_ = switchover_epoch;
    sync_scn_ = sync_scn;
    replayable_scn_ = replayable_scn;
    standby_scn_ = standby_scn;
    recovery_until_scn_ = recovery_until_scn;
    log_mode_ = log_mode;
  }
  return ret;
}

void ObAllTenantInfo::assign(const ObAllTenantInfo &other)
{
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    tenant_role_ = other.tenant_role_;
    switchover_status_ = other.switchover_status_;
    switchover_epoch_ = other.switchover_epoch_;
    sync_scn_ = other.sync_scn_;
    replayable_scn_ = other.replayable_scn_;
    standby_scn_ = other.standby_scn_;
    recovery_until_scn_ = other.recovery_until_scn_;
    log_mode_ = other.log_mode_;
  }
  return ;
}

void ObAllTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_role_ = ObTenantRole::INVALID_TENANT;
  switchover_status_ = ObTenantSwitchoverStatus::INVALID_STATUS;
  switchover_epoch_ = OB_INVALID_VERSION;
  sync_scn_.set_min();
  replayable_scn_.set_min();
  standby_scn_.set_min() ;
  recovery_until_scn_.set_min();
<<<<<<< HEAD
=======
  log_mode_.reset();
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
}
OB_SERIALIZE_MEMBER(ObAllTenantInfo, tenant_id_, tenant_role_,
                    switchover_status_, switchover_epoch_, sync_scn_,
                    replayable_scn_, standby_scn_, recovery_until_scn_, log_mode_);

ObAllTenantInfo& ObAllTenantInfo::operator= (const ObAllTenantInfo &other)
{
  if (this != &other) {
    (void)assign(other);
  }
  return *this;
}

////////////ObAllTenantInfoProxy
int ObAllTenantInfoProxy::init_tenant_info(
    const ObAllTenantInfo &tenant_info,
    ObISQLClient *proxy)
{
  int64_t begin_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_info.get_tenant_id());
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_info));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (!is_user_tenant(tenant_info.get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta tenant no need init tenant info", KR(ret), K(tenant_info));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
                 "insert into %s (tenant_id, tenant_role, "
                 "switchover_status, switchover_epoch, "
<<<<<<< HEAD
                 "sync_scn, replayable_scn, readable_scn) "
                 "values(%lu, '%s', '%s', %ld, %lu, %lu, %lu)",
=======
                 "sync_scn, replayable_scn, readable_scn, recovery_until_scn, log_mode) "
                 "values(%lu, '%s', '%s', %ld, %lu, %lu, %lu, %lu, '%s')",
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
                 OB_ALL_TENANT_INFO_TNAME, tenant_info.get_tenant_id(),
                 tenant_info.get_tenant_role().to_str(),
                 tenant_info.get_switchover_status().to_str(),
                 tenant_info.get_switchover_epoch(),
                 tenant_info.get_sync_scn().get_val_for_inner_table_field(),
                 tenant_info.get_replayable_scn().get_val_for_inner_table_field(),
<<<<<<< HEAD
                 tenant_info.get_standby_scn().get_val_for_inner_table_field()))) {
=======
                 tenant_info.get_standby_scn().get_val_for_inner_table_field(),
                 tenant_info.get_recovery_until_scn().get_val_for_inner_table_field(),
                 tenant_info.get_log_mode().to_str()))) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_info), K(sql));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  }

  int64_t cost = ObTimeUtility::current_time() - begin_time;
  ROOTSERVICE_EVENT_ADD("tenant_info", "init_tenant_info", K(ret),
                        K(tenant_info), K(affected_rows), K(cost));

  return ret;
}

int ObAllTenantInfoProxy::is_standby_tenant(
    ObISQLClient *proxy,
    const uint64_t tenant_id,
    bool &is_standby)
{
  int ret = OB_SUCCESS;
  is_standby = false;
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    is_standby = false;
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      ObTimeoutCtx ctx;
      common::sqlclient::ObMySQLResult *result = NULL;
      uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
      } else if (OB_FAIL(sql.assign_fmt("select tenant_role from %s where tenant_id = %lu",
                     OB_ALL_TENANT_INFO_TNAME, tenant_id))) {
        LOG_WARN("failed to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(proxy->read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", KR(ret), K(sql));
        } else {
          LOG_WARN("fail to get next", KR(ret), K(sql));
        }
      } else {
        ObString tenant_role_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tenant_role", tenant_role_str);
        if (OB_SUCC(ret)) {
          ObTenantRole tenant_role(tenant_role_str);
          is_standby = tenant_role.is_standby();
        }
      }
    } // end HEAP_VAR
  }
  return ret;
}

int ObAllTenantInfoProxy::get_primary_tenant_ids(
    ObISQLClient *proxy,
    ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObSqlString sql;
  ObTenantRole primary_role(ObTenantRole::PRIMARY_TENANT);
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(sql.append_fmt("select tenant_id from %s where tenant_role = '%s'",
           OB_ALL_VIRTUAL_TENANT_INFO_TNAME, primary_role.to_str()))) {
      LOG_WARN("gnenerate sql failed", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next sql result failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;;
            }
          } else {
            uint64_t tenant_id = OB_INVALID_TENANT_ID;
            EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
            if (OB_FAIL(ret)) {
              LOG_WARN("failed to get result", KR(ret));
            } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
              LOG_WARN("push back tenant id failed", K(ret), K(tenant_id), K(tenant_ids.count()));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAllTenantInfoProxy::load_tenant_info(const uint64_t tenant_id,
                                           ObISQLClient *proxy,
                                           const bool for_update,
                                           ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  int64_t ora_rowscn = 0;

  if (OB_FAIL(load_tenant_info(tenant_id, proxy, for_update, ora_rowscn, tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret), K(tenant_id), KP(proxy), K(for_update));
  }
  return ret;
}


int ObAllTenantInfoProxy::load_tenant_info(const uint64_t tenant_id,
                                           ObISQLClient *proxy,
                                           const bool for_update,
                                           int64_t &ora_rowscn,
                                           ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  ora_rowscn = 0;
  ObTimeoutCtx ctx;
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (!is_user_tenant(tenant_id)) {
    //sys and meta tenant is primary
    if (OB_FAIL(tenant_info.init(tenant_id, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id));
    }
  } else {
    ObSqlString sql;
    uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt("select ORA_ROWSCN, * from %s where tenant_id = %lu ",
                   OB_ALL_TENANT_INFO_TNAME, tenant_id))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else if(for_update && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(proxy->read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get sql result", KR(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to get tenant info", KR(ret), K(sql));
        } else if (OB_FAIL(fill_cell(result, tenant_info, ora_rowscn))) {
          LOG_WARN("failed to fill cell", KR(ret), K(sql));
        }
      }
    }//end else
  }
  return ret;
}

int ObAllTenantInfoProxy::update_tenant_recovery_status(
    const uint64_t tenant_id, ObMySQLProxy *proxy,
    ObTenantSwitchoverStatus status, const SCN &sync_scn,
    const SCN &replay_scn, const SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  common::ObMySQLTransaction trans;
  ObAllTenantInfo old_tenant_info;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id ||
                  !status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_id), K(status));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta tenant no need init tenant info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(proxy, exec_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(load_tenant_info(tenant_id, &trans, true, old_tenant_info))) {
    LOG_WARN("failed to load all tenant info", KR(ret), K(tenant_id));
  } else {
<<<<<<< HEAD
    const SCN new_sync_scn = old_tenant_info.get_sync_scn() > sync_scn ? old_tenant_info.get_sync_scn() : sync_scn;

    SCN new_replay_scn_tmp = old_tenant_info.get_replayable_scn() > replay_scn ? old_tenant_info.get_replayable_scn() : replay_scn;
    SCN new_replay_scn = new_replay_scn_tmp < new_sync_scn ? new_replay_scn_tmp : new_sync_scn;

    SCN new_sts_tmp = old_tenant_info.get_standby_scn() > readable_scn ? old_tenant_info.get_standby_scn() : readable_scn;
    SCN new_sts = new_sts_tmp < new_replay_scn ? new_sts_tmp : new_replay_scn;

    if (old_tenant_info.get_sync_scn() == new_sync_scn
        && old_tenant_info.get_replayable_scn() == new_replay_scn
        && old_tenant_info.get_standby_scn() == new_sts) {
      LOG_DEBUG("no need update", K(old_tenant_info), K(new_sync_scn), K(new_replay_scn), K(new_sts));
    } else if (OB_FAIL(sql.assign_fmt(
                 "update %s set sync_scn = %lu, replayable_scn = %lu, "
                 "readable_scn = %lu where tenant_id = %lu "
                 "and switchover_status = '%s'", OB_ALL_TENANT_INFO_TNAME,
                 new_sync_scn.get_val_for_inner_table_field(),
                 new_replay_scn.get_val_for_inner_table_field(),
                 new_sts.get_val_for_inner_table_field(),
=======
    SCN new_sync_scn = gen_new_sync_scn(old_tenant_info.get_sync_scn(), sync_scn, old_tenant_info.get_recovery_until_scn());
    SCN new_replay_scn = gen_new_replayable_scn(old_tenant_info.get_replayable_scn(), replay_scn, new_sync_scn);
    SCN new_scn = gen_new_standby_scn(old_tenant_info.get_standby_scn(), readable_scn, new_replay_scn);

    if (old_tenant_info.get_sync_scn() == new_sync_scn
        && old_tenant_info.get_replayable_scn() == new_replay_scn
        && old_tenant_info.get_standby_scn() == new_scn) {
      LOG_DEBUG("no need update", K(old_tenant_info), K(new_sync_scn), K(new_replay_scn), K(new_scn));
    } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt(
                 "update %s set sync_scn = %ld, replayable_scn = %ld, "
                 "readable_scn = %ld where tenant_id = %lu "
                 "and switchover_status = '%s' and readable_scn <= replayable_scn and "
                 "replayable_scn <= sync_scn and sync_scn <= recovery_until_scn", OB_ALL_TENANT_INFO_TNAME,
                 new_sync_scn.get_val_for_inner_table_field(),
                 new_replay_scn.get_val_for_inner_table_field(),
                 new_scn.get_val_for_inner_table_field(),
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
                 tenant_id, status.to_str()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(status),
               K(sql));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
    }

    LOG_TRACE("update_tenant_recovery_status", KR(ret), K(tenant_id), K(affected_rows), K(status),
              K(sql), K(old_tenant_info), K(new_sync_scn), K(new_replay_scn), K(new_scn),
              K(sync_scn), K(replay_scn), K(readable_scn));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret; 
}

int ObAllTenantInfoProxy::fill_cell(common::sqlclient::ObMySQLResult *result, ObAllTenantInfo &tenant_info, int64_t &ora_rowscn)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    ObString tenant_role_str;
    ObString status_str;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    int64_t switchover_epoch = OB_INVALID_TIMESTAMP;
<<<<<<< HEAD
    int64_t sync_ts = OB_INVALID_SCN_VAL;
    int64_t replay_ts = OB_INVALID_SCN_VAL;
    int64_t sts = OB_INVALID_SCN_VAL;
    SCN sync_scn;
    SCN replay_scn;
    SCN sts_scn;
    SCN recovery_scn;
    recovery_scn.set_base();
=======
    uint64_t sync_scn_val = OB_INVALID_SCN_VAL;
    uint64_t replay_scn_val = OB_INVALID_SCN_VAL;
    uint64_t sts_scn_val = OB_INVALID_SCN_VAL;
    uint64_t recovery_until_scn_val = OB_INVALID_SCN_VAL;
    ora_rowscn = 0;
    ObString log_mode_str;
    ObString log_mode_default_value("NOARCHIVELOG");
    SCN sync_scn;
    SCN replay_scn;
    SCN sts_scn;
    SCN recovery_until_scn;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tenant_role", tenant_role_str);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "switchover_status", status_str);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "switchover_epoch", switchover_epoch, int64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "sync_scn", sync_scn_val, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "replayable_scn", replay_scn_val, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "readable_scn", sts_scn_val, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "recovery_until_scn", recovery_until_scn_val, uint64_t, false /* skip_null_error */, true /* skip_column_error */, OB_MAX_SCN_TS_NS);
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "log_mode", log_mode_str,
                false /* skip_null_error */, true /* skip_column_error */, log_mode_default_value);
    EXTRACT_INT_FIELD_MYSQL(*result, "ORA_ROWSCN", ora_rowscn, int64_t);
    ObTenantRole tmp_tenant_role(tenant_role_str);
    ObTenantSwitchoverStatus tmp_tenant_sw_status(status_str);
    ObArchiveMode tmp_log_mode(log_mode_str);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get result", KR(ret));
      //tenant_id in inner table can be used directly
<<<<<<< HEAD
    } else if (OB_FAIL(sync_scn.convert_for_inner_table_field(sync_ts))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(sync_ts));
    } else if (OB_FAIL(replay_scn.convert_for_inner_table_field(replay_ts))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(replay_ts));
    } else if (OB_FAIL(sts_scn.convert_for_inner_table_field(sts))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(sts));
    } else if (OB_FAIL(tenant_info.init(
            tenant_id, tmp_tenant_role,
            tmp_tenant_sw_status, switchover_epoch,
            sync_scn, replay_scn, sts_scn, recovery_scn))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id), K(tmp_tenant_role), K(tenant_role_str),
          K(tmp_tenant_sw_status), K(status_str), K(switchover_epoch), K(sync_ts));
=======
    } else if (OB_FAIL(sync_scn.convert_for_inner_table_field(sync_scn_val))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(sync_scn_val));
    } else if (OB_FAIL(replay_scn.convert_for_inner_table_field(replay_scn_val))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(replay_scn_val));
    } else if (OB_FAIL(sts_scn.convert_for_inner_table_field(sts_scn_val))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(sts_scn_val));
    } else if (OB_FAIL(recovery_until_scn.convert_for_inner_table_field(recovery_until_scn_val))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(recovery_until_scn_val));
    } else if (OB_FAIL(tenant_info.init(
            tenant_id, tmp_tenant_role,
            tmp_tenant_sw_status, switchover_epoch,
            sync_scn, replay_scn, sts_scn, recovery_until_scn, tmp_log_mode))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id), K(tmp_tenant_role), K(tenant_role_str),
          K(tmp_tenant_sw_status), K(status_str), K(switchover_epoch), K(sync_scn), K(recovery_until_scn),
          K(log_mode_str), K(tmp_log_mode));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    }
  }
  return ret;
}

int ObAllTenantInfoProxy::update_tenant_role(
    const uint64_t tenant_id,
    ObISQLClient *proxy,
    int64_t old_switchover_epoch,
    const ObTenantRole &new_role,
    const ObTenantSwitchoverStatus &old_status,
    const ObTenantSwitchoverStatus &new_status,
    int64_t &new_switchover_ts)
{
  int64_t begin_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
    || OB_INVALID_VERSION == old_switchover_epoch
    || !new_role.is_valid()
    || !old_status.is_valid()
    || !new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_id), K(old_switchover_epoch), K(old_status),
                                       K(new_role), K(new_status));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(get_new_switchover_epoch_(old_switchover_epoch, old_status, new_status,
                                               new_switchover_ts))) {
    LOG_WARN("fail to get_new_switchover_epoch_", KR(ret), K(old_switchover_epoch), K(old_status),
                                                  K(new_status));
  } else if (OB_UNLIKELY(OB_INVALID_VERSION == new_switchover_ts)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_switchover_ts is invalid", KR(ret), K(new_switchover_ts),
             K(old_switchover_epoch), K(old_status), K(new_status));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
          "update %s set tenant_role = '%s', switchover_status = '%s', switchover_epoch = %ld "
          "where tenant_id = %lu and switchover_epoch = %ld and switchover_status = '%s'",
          OB_ALL_TENANT_INFO_TNAME,
          new_role.to_str(), new_status.to_str(),
          new_switchover_ts, tenant_id, old_switchover_epoch, old_status.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(old_switchover_epoch),
             K(new_role), K(old_status), K(sql));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover may concurrency, need retry", KR(ret), K(old_switchover_epoch), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  }
  int64_t cost = ObTimeUtility::current_time() - begin_time;
  ROOTSERVICE_EVENT_ADD("tenant_info", "update_tenant_role", K(ret), K(tenant_id),
                        K(new_status), K(new_switchover_ts), K(old_status), K(cost));

  return ret;
}

int ObAllTenantInfoProxy::update_tenant_switchover_status(
    const uint64_t tenant_id,
    ObISQLClient *proxy,
    int64_t switchover_epoch,
    const ObTenantSwitchoverStatus &old_status, const ObTenantSwitchoverStatus &status)
{
  int64_t begin_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;

  //update switchover epoch while role change
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
    || OB_INVALID_VERSION == switchover_epoch
    || old_status == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_id),
            K(switchover_epoch), K(old_status), K(status));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
          "update %s set switchover_status = '%s'"
          "where tenant_id = %lu and switchover_epoch = %ld and switchover_status = '%s'",
          OB_ALL_TENANT_INFO_TNAME,
          status.to_str(),
          tenant_id, switchover_epoch, old_status.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(switchover_epoch),
             K(status), K(old_status), K(sql));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover may concurrency, need retry", KR(ret), K(switchover_epoch), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  }

  int64_t cost = ObTimeUtility::current_time() - begin_time;
  ROOTSERVICE_EVENT_ADD("tenant_info", "update_tenant_switchover_status", K(ret), K(tenant_id),
                        K(status), K(switchover_epoch), K(old_status), K(cost));

  return ret;
}

int ObAllTenantInfoProxy::update_tenant_recovery_until_scn(
    const uint64_t tenant_id,
    common::ObMySQLTransaction &trans,
    const int64_t switchover_epoch,
    const SCN &recovery_until_scn)
{
  DEBUG_SYNC(BEFORE_UPDATE_RECOVERY_UNTIL_SCN);

  int64_t begin_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;
  ObLSRecoveryStatOperator ls_recovery_operator;
  ObLSRecoveryStat sys_ls_recovery;
  ObLogRestoreSourceMgr restore_source_mgr;
  uint64_t compat_version = 0;

  if (!is_user_tenant(tenant_id) || OB_INVALID_VERSION == switchover_epoch) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(switchover_epoch));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Tenant COMPATIBLE is below 4.1.0.0, update tenant recovery_until_scn is not suppported",
             KR(ret), K(compat_version));
  } else if (OB_UNLIKELY(!recovery_until_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("recovery_until_scn invalid", KR(ret), K(recovery_until_scn));
  } else if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, share::SYS_LS,
        true /*for_update*/, sys_ls_recovery, trans))) {
    LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id));
  } else if (recovery_until_scn < sys_ls_recovery.get_sync_scn()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("recover before SYS LS sync_scn is not allowed", KR(ret), K(tenant_id), K(recovery_until_scn), K(sys_ls_recovery));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recover before SYS LS sync_scn is");
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set recovery_until_scn = %lu where tenant_id = %lu and sync_scn <= %lu and switchover_epoch = %ld",
      OB_ALL_TENANT_INFO_TNAME, recovery_until_scn.get_val_for_inner_table_field(), tenant_id,
      recovery_until_scn.get_val_for_inner_table_field(), switchover_epoch))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(recovery_until_scn), K(sql));
  } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("state changed, check sync_scn and switchover status", KR(ret), K(tenant_id),
             K(switchover_epoch), K(recovery_until_scn), K(sql));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "state changed, check sync_scn and switchover status, recover is");
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows),
             K(switchover_epoch), K(recovery_until_scn), K(sql));
  // update __all_log_restore_source
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id, &trans))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), K(tenant_id), K(recovery_until_scn));
  } else if (OB_FAIL(restore_source_mgr.update_recovery_until_scn(recovery_until_scn))) {
    LOG_WARN("failed to update_recovery_until_scn", KR(ret), K(tenant_id), K(recovery_until_scn));
  }

  int64_t cost = ObTimeUtility::current_time() - begin_time;
  LOG_INFO("update_recovery_until_scn finish", KR(ret), K(tenant_id), K(sys_ls_recovery),
                      K(recovery_until_scn), K(affected_rows), K(switchover_epoch), K(sql), K(cost));
  ROOTSERVICE_EVENT_ADD("tenant_info", "update_recovery_until_scn", K(ret), K(tenant_id),
                        K(recovery_until_scn), K(affected_rows), K(switchover_epoch), K(sys_ls_recovery));
  return ret;
}

int ObAllTenantInfoProxy::update_tenant_status(
    const uint64_t tenant_id,
    common::ObMySQLTransaction &trans,
    const ObTenantRole new_role,
    const ObTenantSwitchoverStatus &old_status,
    const ObTenantSwitchoverStatus &new_status,
    const share::SCN &sync_scn,
    const share::SCN &replayable_scn,
    const share::SCN &readable_scn,
    const share::SCN &recovery_until_scn,
    const int64_t old_switchover_epoch)
{
  int64_t begin_time = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;
  int64_t new_switchover_epoch = OB_INVALID_VERSION;
  ObLogRestoreSourceMgr restore_source_mgr;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
    || !new_role.is_valid()
    || !old_status.is_valid()
    || !new_status.is_valid()
    || old_status == new_status
    || !sync_scn.is_valid_and_not_min()
    || !replayable_scn.is_valid_and_not_min()
    || !readable_scn.is_valid_and_not_min()
    || !recovery_until_scn.is_valid_and_not_min()
    || OB_INVALID_VERSION == old_switchover_epoch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_id), K(new_role), K(old_status),
                K(new_status), K(sync_scn), K(replayable_scn), K(readable_scn), K(recovery_until_scn),
                K(old_switchover_epoch));
  } else if (OB_FAIL(get_new_switchover_epoch_(old_switchover_epoch, old_status, new_status,
                                               new_switchover_epoch))) {
    LOG_WARN("fail to get_new_switchover_epoch_", KR(ret), K(old_switchover_epoch), K(old_status),
                                                  K(new_status));
  } else if (OB_UNLIKELY(OB_INVALID_VERSION == new_switchover_epoch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_switchover_ts is invalid", KR(ret), K(new_switchover_epoch),
             K(old_switchover_epoch), K(old_status), K(new_status));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
                "update %s set tenant_role = '%s', switchover_status = '%s', "
                "switchover_epoch = %ld, sync_scn = %lu, replayable_scn = %lu, "
                "readable_scn = %lu, recovery_until_scn = %lu where tenant_id = %lu "
                "and switchover_status = '%s' and switchover_epoch = %ld "
                "and readable_scn <= replayable_scn and replayable_scn <= sync_scn and sync_scn <= recovery_until_scn "
                "and sync_scn <= %lu and replayable_scn <= %lu and readable_scn <= %lu ",
                OB_ALL_TENANT_INFO_TNAME, new_role.to_str(), new_status.to_str(),
                new_switchover_epoch,
                sync_scn.get_val_for_inner_table_field(),
                replayable_scn.get_val_for_inner_table_field(),
                readable_scn.get_val_for_inner_table_field(),
                recovery_until_scn.get_val_for_inner_table_field(),
                tenant_id, old_status.to_str(), old_switchover_epoch,
                sync_scn.get_val_for_inner_table_field(),
                replayable_scn.get_val_for_inner_table_field(),
                readable_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover may concurrency, need retry", KR(ret), K(old_switchover_epoch), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  // update __all_log_restore_source
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id, &trans))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), K(tenant_id), K(recovery_until_scn));
  } else if (OB_FAIL(restore_source_mgr.update_recovery_until_scn(recovery_until_scn))) {
    LOG_WARN("failed to update_recovery_until_scn", KR(ret), K(tenant_id), K(recovery_until_scn));
  }

  ObAllTenantInfo tenant_info;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = tenant_info.init(tenant_id,
                                                new_role,
                                                new_status,
                                                new_switchover_epoch,
                                                sync_scn,
                                                replayable_scn,
                                                readable_scn,
                                                recovery_until_scn))) {
    LOG_WARN("failed to init tenant_info", KR(ret), KR(tmp_ret), K(tenant_id), K(new_role),
                                           K(new_status), K(new_switchover_epoch), K(sync_scn),
                                           K(replayable_scn), K(readable_scn), K(recovery_until_scn));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }

  int64_t cost = ObTimeUtility::current_time() - begin_time;
  ROOTSERVICE_EVENT_ADD("tenant_info", "update_tenant_role", K(ret), K(tenant_id),
                        K(tenant_info), K(old_status), K(old_switchover_epoch), K(cost));
  return ret;
}

int ObAllTenantInfoProxy::get_new_switchover_epoch_(
    const int64_t old_switchover_epoch,
    const ObTenantSwitchoverStatus &old_status,
    const ObTenantSwitchoverStatus &new_status,
    int64_t &new_switchover_epoch)
{
  int ret = OB_SUCCESS;
  new_switchover_epoch = OB_INVALID_VERSION;

  //update switchover epoch when entering and leaving normal switchover status
  if (OB_UNLIKELY(OB_INVALID_VERSION == old_switchover_epoch
    || !old_status.is_valid()
    || !new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(old_switchover_epoch), K(old_status), K(new_status));
  } else if ((share::NORMAL_SWITCHOVER_STATUS == new_status
             || share::NORMAL_SWITCHOVER_STATUS == old_status) && (old_status != new_status)) {
    new_switchover_epoch = max(old_switchover_epoch + 1, ObTimeUtility::current_time());
  } else {
    new_switchover_epoch = old_switchover_epoch;
  }

  return ret;
}

int ObAllTenantInfoProxy::update_tenant_log_mode(
    const uint64_t tenant_id,
    ObISQLClient *proxy,
    const ObArchiveMode &old_log_mode,
    const ObArchiveMode &new_log_mode)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
                  || !old_log_mode.is_valid()
                  || !new_log_mode.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(old_log_mode), K(new_log_mode));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
             "update %s set log_mode = '%s' where tenant_id = %lu and switchover_status = '%s' "
             "and log_mode = '%s' ",
             OB_ALL_TENANT_INFO_TNAME, new_log_mode.to_str(), tenant_id,
             NORMAL_SWITCHOVER_STATUS.to_str(), old_log_mode.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(old_log_mode), K(new_log_mode), K(sql));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_NEED_RETRY;
    LOG_WARN("may concurrency with switchover, need retry", KR(ret), K(tenant_id), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

}
}

