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

#ifndef OCEANBASE_SHARE_OB_LS_I_LIFE_MANAGER_H_
#define OCEANBASE_SHARE_OB_LS_I_LIFE_MANAGER_H_

#include "share/ob_share_util.h"
#include "share/ob_tenant_switchover_status.h"//ObTenantSwitchoverStatus
#include "common/ob_timeout_ctx.h"
#include "share/config/ob_server_config.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/scn.h"
namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObMySQLProxy;
class ObISQLClient;
class ObString;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class ObLSID;
struct ObLSStatusInfo;
<<<<<<< HEAD
=======
/**
 * @description:
 *    In order to let switchover switch the accessmode of all LS correctly,
 *    when creating, deleting, and updating LS status,
 *    it needs to be mutually exclusive with switchover status of __all_tenant_info
 */
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

enum ObLSStatus
{
  OB_LS_EMPTY = -1,
  OB_LS_CREATING = 0,
  OB_LS_CREATED,
  OB_LS_NORMAL,
  OB_LS_DROPPING,
  OB_LS_TENANT_DROPPING,
  OB_LS_WAIT_OFFLINE,
  OB_LS_CREATE_ABORT,
  OB_LS_PRE_TENANT_DROPPING,//only for sys ls
};

/*
 *log stream lifetime description:
 If an inner_table needs to be aware of the creation and deletion of the log stream; 
 or it needs to be an atomic transaction with the insertion and deletion of __all_ls_status.
 The operation of this internal table needs to inherit this class, implement the following two methods,
 and register it in the class of ObLSLifeAgentManager.
 The specific implementation can refer to ObLSRecoveryStatOperator
 * */
class ObLSLifeIAgent
{
public:
  ObLSLifeIAgent() {}
  virtual ~ObLSLifeIAgent () {} 
  //create new ls
  virtual int create_new_ls(const ObLSStatusInfo &ls_info,
                            const SCN &create_scn,
                            const common::ObString &zone_priority,
                            const share::ObTenantSwitchoverStatus &working_sw_status,
                            ObMySQLTransaction &trans) = 0;
  //drop ls
  virtual int drop_ls(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const ObTenantSwitchoverStatus &working_sw_status,
                      ObMySQLTransaction &trans) = 0;
  //set ls to offline
  virtual int set_ls_offline(const uint64_t &tenant_id,
                      const share::ObLSID &ls_id,
                      const share::ObLSStatus &ls_status,
                      const SCN &drop_scn,
<<<<<<< HEAD
=======
                      const ObTenantSwitchoverStatus &working_sw_status,
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
                      ObMySQLTransaction &trans) = 0;
  //update ls primary zone
  virtual int update_ls_primary_zone(
      const uint64_t &tenant_id,
      const share::ObLSID &ls_id,
      const common::ObZone &primary_zone,
      const common::ObString &zone_priority,
      ObMySQLTransaction &trans) = 0;

  /*
   *description: The table related to the log stream status needs to be reported to the meta tenant or system tenant.
   *param[in]: the ls's tenant_id
   *return : need to operator's tenant_id
   * */
  static uint64_t get_exec_tenant_id(const uint64_t tenant_id)
  {
    return get_private_table_exec_tenant_id(tenant_id);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSLifeIAgent);
};

/*
 *description:
 * Many inner_tables related to log streams require read and write operations. 
 * Most of them are written in units of log streams and scanned in units of log streams or full tables.
 * Therefore, a template class that provides read and write is perfected.
 * To use this template class, you need to implement the interface: get_exec_tenant_id and the fill_cell.
 * For specific implementation, please refer to ObLSStatusOperator*/
class ObLSTemplateOperator
{
public:
  ObLSTemplateOperator() {}
  virtual ~ObLSTemplateOperator() {}

  /*
   * description: write to the inner_table by ls, it will check affected_row must equal to one,
   *              the interface need operator has get_exec_tenant_id
   * param[in]: tenant_id of ls
   * param[in]: the exec_write sql
   * param[in]: the inner_table to operator
   * param[in]: client*/
  template <typename TableOperator>
  int exec_write(const uint64_t &tenant_id, const common::ObSqlString &sql,
                 TableOperator *table_operator, ObISQLClient &client,
                 const bool ignore_row = false);
  /*
   * description: read the inner_table.the interface need operator has get_exec_tenant_id and fill_cell
   * param[in]: tenant_id of ls
   * param[in]: the exec_read sql
   * param[in]: client
   * param[in]: the inner_table to operator
   * param[in]: the result array of inner_table*/

  template <typename TableOperator, typename LS_Result>
  int exec_read(const uint64_t &tenant_id,
                       const common::ObSqlString &sql, ObISQLClient &client,
                       TableOperator *table_operator,
                       common::ObIArray<LS_Result> &res);

 private:
  DISALLOW_COPY_AND_ASSIGN(ObLSTemplateOperator);
};

template <typename TableOperator, typename LS_Result>
int ObLSTemplateOperator::exec_read(const uint64_t &tenant_id,
                        const common::ObSqlString &sql, ObISQLClient &client,
                        TableOperator *table_operator, common::ObIArray<LS_Result> &ls_res)
{
  int ret = OB_SUCCESS;
  ls_res.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_ISNULL(table_operator)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), KP(table_operator));
  } else {
    ObTimeoutCtx ctx;
    const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
    uint64_t exec_tenant_id = table_operator->get_exec_tenant_id(tenant_id);
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to get exec tenant id", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
      SHARE_LOG(WARN, "failed to set default timeout ctx", KR(ret), K(default_timeout));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(client.read(res, exec_tenant_id, sql.ptr()))) {
          SHARE_LOG(WARN, "failed to read", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WARN, "failed to get sql result", KR(ret));
        } else {
           LS_Result single_res;
           while (OB_SUCC(ret) && OB_SUCC(result->next())) {
             single_res.reset();
             if (OB_FAIL(table_operator->fill_cell(result, single_res))) {
               SHARE_LOG(WARN, "failed to read cell from result", KR(ret), K(sql));
             } else if (OB_FAIL(ls_res.push_back(single_res))) {
               SHARE_LOG(WARN, "failed to get cell", KR(ret), K(single_res));
             }
           }  // end while
           if (OB_ITER_END == ret) {
             ret = OB_SUCCESS;
           } else if (OB_FAIL(ret)) {
             SHARE_LOG(WARN, "failed to get ls", KR(ret));
           } else {
             ret = OB_ERR_UNEXPECTED;
             SHARE_LOG(WARN, "ret can not be success", KR(ret));
           }
        }  // end heap var 
      }
    }//end else
  }
  return ret;
}

template <typename TableOperator>
int ObLSTemplateOperator::exec_write(const uint64_t &tenant_id,
                                      const common::ObSqlString &sql,
                                      TableOperator *table_operator, 
                                      ObISQLClient &client,
                                      const bool ignore_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql.empty() || OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(table_operator)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KR(ret), K(sql), K(tenant_id), KP(table_operator));
  } else {
    int64_t affected_rows = 0;
    ObTimeoutCtx ctx;
    const int64_t timestamp = ObTimeUtility::current_time();
    const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
    uint64_t exec_tenant_id = table_operator->get_exec_tenant_id(tenant_id);
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "failed to get exec tenant id", KR(ret), K(exec_tenant_id));
    } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx,
                                                            default_timeout))) {
      SHARE_LOG(WARN, "failed to set default timeout ctx", KR(ret),
               K(default_timeout));
    } else if (OB_FAIL(
                   client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      SHARE_LOG(WARN, "failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    } else if (!is_single_row(affected_rows) && !ignore_row) {
      ret = OB_NEED_RETRY;
      SHARE_LOG(WARN, "expected one row, may need retry", KR(ret), K(affected_rows),
               K(sql), K(ignore_row));
    }
  }
  return ret;
}


}
}

#endif /* !OCEANBASE_SHARE_OB_LS_I_LIFE_MANAGER_H_ */
