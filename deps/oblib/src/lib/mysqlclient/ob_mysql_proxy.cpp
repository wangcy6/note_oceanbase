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

#define USING_LOG_PREFIX COMMON_MYSQLP
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

OB_SERIALIZE_MEMBER(ObSessionDDLInfo, ddl_info_);

ObCommonSqlProxy::ObCommonSqlProxy() : pool_(NULL)
{
}

ObCommonSqlProxy::~ObCommonSqlProxy()
{
}

int ObCommonSqlProxy::init(ObISQLConnectionPool *pool)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice");
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument");
  } else {
    pool_ = pool;
  }
  return ret;
}

void ObCommonSqlProxy::operator=(const ObCommonSqlProxy &o)
{
  this->ObISQLClient::operator=(o);
  active_ = o.active_;
  pool_ = o.pool_;
}

int ObCommonSqlProxy::read(ReadResult &result, const uint64_t tenant_id, const char *sql)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *conn = NULL;
  if (OB_FAIL(acquire(tenant_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_FAIL(read(conn, result, tenant_id, sql))) {
    LOG_WARN("read failed", K(ret));
  }
  close(conn, ret);
  return ret;
}

int ObCommonSqlProxy::read(ReadResult &result, const uint64_t tenant_id, const char *sql, const common::ObAddr *exec_sql_addr)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *conn = NULL;
  if (OB_ISNULL(exec_sql_addr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read with typically exec addr failed", K(ret), K(exec_sql_addr));
  } else if (OB_FAIL(acquire(tenant_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_FAIL(read(conn, result, tenant_id, sql, exec_sql_addr))) {
    LOG_WARN("read failed", K(ret));
  }
  close(conn, ret);
  return ret;
}

int ObCommonSqlProxy::read(ReadResult &result, const uint64_t tenant_id, const char *sql, const ObSessionParam *session_param)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *conn = NULL;
  if (OB_FAIL(acquire(tenant_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (nullptr != session_param) {
    conn->set_ddl_info(&session_param->ddl_info_);
    conn->set_use_external_session(session_param->use_external_session_);
<<<<<<< HEAD
=======
    conn->set_group_id(session_param->consumer_group_id_);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    if (nullptr != session_param->sql_mode_) {
      if (OB_FAIL(conn->set_session_variable("sql_mode", *session_param->sql_mode_))) {
        LOG_WARN("set inner connection sql mode failed", K(ret));
      }
    }
    if (session_param->ddl_info_.is_ddl()) {
      conn->set_force_remote_exec(true);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read(conn, result, tenant_id, sql))) {
    LOG_WARN("read failed", K(ret));
  }
  close(conn, ret);
  return ret;
}

int ObCommonSqlProxy::read(ObISQLConnection *conn, ReadResult &result,
                           const uint64_t tenant_id, const char *sql, const common::ObAddr *exec_sql_addr)
{
  int ret = OB_SUCCESS;
  const int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  result.reset();
  if (OB_ISNULL(sql) || OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql or null conn", K(ret), KP(sql), KP(conn));
  } else if (!is_active()) { // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), KCSTRING(sql));
  } else {
    if (OB_FAIL(conn->execute_read(tenant_id, sql, result, exec_sql_addr))) {
      LOG_WARN("query failed", K(ret), K(conn), K(start), KCSTRING(sql));
    }
  }
  LOG_TRACE("execute sql", KCSTRING(sql), K(ret));
  return ret;
}

int ObCommonSqlProxy::write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  ObISQLConnection *conn = NULL;
  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql");
  } else if (OB_FAIL(acquire(tenant_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_ISNULL(conn)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) { // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), KCSTRING(sql));
  } else {
    if (OB_FAIL(conn->execute_write(tenant_id, sql, affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(conn), K(start), KCSTRING(sql));
    }
  }
  close(conn, ret);
  LOG_TRACE("execute sql", KCSTRING(sql), K(ret));
  return ret;
}

int ObCommonSqlProxy::write(const uint64_t tenant_id, const ObString sql,
                        int64_t &affected_rows, int64_t compatibility_mode,
                        const ObSessionParam *param /* = nullptr*/,
                        const common::ObAddr *sql_exec_addr)
{
  int ret = OB_SUCCESS;
  bool is_user_sql = false;
  int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  ObISQLConnection *conn = NULL;
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql");
  } else if (OB_FAIL(acquire(tenant_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_ISNULL(conn)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) { // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), K(sql));
  }
  int64_t old_compatibility_mode;
  int64_t old_sql_mode = 0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(conn->get_session_variable("ob_compatibility_mode", old_compatibility_mode))) {
      LOG_WARN("fail to get inner connection compatibility mode", K(ret));
    } else if (old_compatibility_mode != compatibility_mode
               && OB_FAIL(conn->set_session_variable("ob_compatibility_mode", compatibility_mode))) {
      LOG_WARN("fail to set inner connection compatibility mode", K(ret), K(compatibility_mode));
    } else {
      if (is_oracle_compatible(static_cast<ObCompatibilityMode>(compatibility_mode))) {
        conn->set_oracle_compat_mode();
      } else {
        conn->set_mysql_compat_mode();
      }
      LOG_TRACE("compatibility mode switch successfully!",
                K(old_compatibility_mode), K(compatibility_mode));
    }
  }
  if (OB_SUCC(ret) && nullptr != param) {
    conn->set_is_load_data_exec(param->is_load_data_exec_);
    conn->set_use_external_session(param->use_external_session_);
<<<<<<< HEAD
=======
    conn->set_group_id(param->consumer_group_id_);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    if (param->is_load_data_exec_) {
      is_user_sql = true;
    }
    if (OB_FAIL(conn->set_ddl_info(&param->ddl_info_))) {
      LOG_WARN("fail to set ddl info", K(ret));
    }
    if (param->ddl_info_.is_ddl()) {
      conn->set_force_remote_exec(true);
      conn->set_nls_formats(param->nls_formats_);
    }
  }
  if (OB_SUCC(ret) && nullptr != param && nullptr != param->sql_mode_) {
    // TODO(cangdi): fix get_session_variable not working
    /*if (OB_FAIL(conn->get_session_variable("sql_mode", old_sql_mode))) {
      LOG_WARN("get inner connection sql mode", K(ret));
    } else*/ if (OB_FAIL(conn->set_session_variable("sql_mode", *param->sql_mode_))) {
      LOG_WARN("set inner connection sql mode failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr != param && nullptr != param->tz_info_wrap_) {
    if (OB_FAIL(conn->set_tz_info_wrap(*param->tz_info_wrap_))) {
      LOG_WARN("fail to set time zone info wrap", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(conn->execute_write(tenant_id, sql, affected_rows, is_user_sql, sql_exec_addr))) {
      LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(conn), K(start), K(sql));
    } else if (old_compatibility_mode != compatibility_mode
               && OB_FAIL(conn->set_session_variable("ob_compatibility_mode", old_compatibility_mode))) {
      LOG_WARN("fail to recover inner connection sql mode", K(ret));
    /*} else if (nullptr != sql_mode && old_sql_mode != *sql_mode && OB_FAIL(conn->set_session_variable("sql_mode", old_sql_mode))) {
      LOG_WARN("set inner connection sql mode failed", K(ret));*/
    } else {
      if (is_oracle_compatible(static_cast<ObCompatibilityMode>(old_compatibility_mode))) {
        conn->set_oracle_compat_mode();
      } else {
        conn->set_mysql_compat_mode();
      }
      LOG_TRACE("compatibility mode switch successfully!",
                K(compatibility_mode), K(old_compatibility_mode));
    }
  }
  close(conn, ret);
  LOG_TRACE("execute sql with sql mode", K(sql), K(compatibility_mode), K(ret));
  return ret;
}

int ObCommonSqlProxy::execute(const uint64_t tenant_id, ObIExecutor &executor)
{
  int ret = OB_SUCCESS;
  int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  ObISQLConnection *conn = NULL;
  if (OB_FAIL(acquire(tenant_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_ISNULL(conn)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) { // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret));
  } else {
    if (OB_FAIL(conn->execute(tenant_id, executor))) {
      LOG_WARN("execute failed", K(ret), K(conn), K(start));
    }
  }
  close(conn, ret);
  return ret;
}

int ObCommonSqlProxy::close(ObISQLConnection *conn, const int succ)
{
  int ret = OB_SUCCESS;
  if (conn != NULL) {
    ret = pool_->release(conn, OB_SUCCESS == succ);
    if (OB_FAIL(ret)) {
      LOG_WARN("release connection failed", K(ret), K(conn));
    }
  }
  return ret;
}

int ObCommonSqlProxy::escape(const char *from, const int64_t from_size,
    char *to, const int64_t to_size, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited");
  } else if (OB_FAIL(pool_->escape(from, from_size, to, to_size, out_size))) {
    LOG_WARN("escape string failed",
        "from", ObString(from_size, from), K(from_size),
        "to", static_cast<void *>(to), K(to_size));
  }
  return ret;
}

int ObCommonSqlProxy::add_slashes(const char *from, const int64_t from_size,
                              char *to, const int64_t to_size, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  // In order to facilitate the use of ObString's ptr() and length() functions to pass in, from can be NULL when from_size is 0
  if (OB_UNLIKELY(OB_ISNULL(from) && 0 != from_size)
      || OB_ISNULL(to)
      || OB_UNLIKELY(to_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(from), KP(to), K(to_size), K(ret));
  } else if (OB_UNLIKELY(to_size < (2 * from_size + 1))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("to size must be 2 times longer than from length + 1",
             K(from_size), K(to_size), K(ret));
  } else {
    out_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < from_size; ++i) {
      switch (from[i]) {
      case '\0':
        to[out_size++] = '\\';
        to[out_size++] = '0';
        break;
      case '\'':
      case '\"':
      case '\\':
        to[out_size++] = '\\';
        to[out_size++] = from[i];
        break;
      default:
        to[out_size++] = from[i];
        break;
      }
    }
    to[out_size] = '\0';
  }
  return ret;
}

int ObCommonSqlProxy::acquire(const uint64_t tenant_id, sqlclient::ObISQLConnection *&conn)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited", K(ret));
  } else if (OB_FAIL(pool_->acquire(tenant_id, conn, this))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("connection must not be null", K(ret), K(conn));
  }
  return ret;
}

int ObCommonSqlProxy::read(
    ReadResult &result,
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const char *sql)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *conn = NULL;
  const int64_t start = ::oceanbase::common::ObTimeUtility::current_time();
  result.reset();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited", K(ret), K(cluster_id), K(tenant_id), KCSTRING(sql));
  } else if (NULL == sql) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty sql");
  } else if (OB_FAIL(acquire(tenant_id, conn))) {
    LOG_WARN("acquire connection failed", K(ret), K(conn));
  } else if (NULL == conn) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL");
  } else if (!is_active()) { // check client active after connection acquired
    ret = OB_INACTIVE_SQL_CLIENT;
    LOG_WARN("in active sql client", K(ret), KCSTRING(sql));
  } else {
    if (OB_FAIL(conn->execute_read(cluster_id, tenant_id, sql, result))) {
      LOG_WARN("query failed", K(ret), K(conn), K(start), KCSTRING(sql), K(cluster_id));
    }
  }
  close(conn, ret);
  LOG_TRACE("execute sql", KCSTRING(sql), K(ret));
  return ret;
}

int ObDbLinkProxy::init(ObDbLinkConnectionPool *pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null dblink connection pool", K(ret));
  } else if (OB_FAIL(ObCommonSqlProxy::init(&(pool->get_mysql_pool())))) {
    LOG_WARN("failed to init common proxy", K(ret));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    link_pool_ = pool;
  }
  return ret;
}

int ObDbLinkProxy::switch_dblink_conn_pool(DblinkDriverProto type, ObISQLConnectionPool *&dblink_conn_pool)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(link_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to switch dblink conn pool", K(ret));
  } else {
    switch (type)
    {
    case DBLINK_DRV_OB:
      dblink_conn_pool = static_cast<ObISQLConnectionPool *>(&(link_pool_->get_mysql_pool()));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown dblink type", K(ret), K(type));
      break;
    }
    if (OB_ISNULL(dblink_conn_pool)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObDbLinkProxy::create_dblink_pool(uint64_t tenant_id, uint64_t dblink_id, DblinkDriverProto dblink_type, const ObAddr &server,
                                      const ObString &db_tenant, const ObString &db_user,
                                      const ObString &db_pass, const ObString &db_name,
                                      const common::ObString &conn_str,
                                      const common::ObString &cluster_str,
                                      const dblink_param_ctx &param_ctx)
{
  int ret = OB_SUCCESS;
  ObISQLConnectionPool *dblink_pool = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy not inited");
  } else if (OB_FAIL(switch_dblink_conn_pool(dblink_type, dblink_pool))) {
    LOG_WARN("failed to get dblink interface", K(ret));
  } else if (OB_FAIL(dblink_pool->create_dblink_pool(tenant_id, dblink_id, server, db_tenant,
                                                     db_user, db_pass, db_name,
                                                     conn_str, cluster_str, param_ctx))) {
    LOG_WARN("create dblink pool failed", K(ret), K(dblink_id), K(server),
             K(db_tenant), K(db_user), K(db_pass), K(db_name));
  }
  return ret;
}

int ObDbLinkProxy::acquire_dblink(uint64_t dblink_id,
                                  DblinkDriverProto dblink_type,
                                  const dblink_param_ctx &param_ctx,
                                  ObISQLConnection *&dblink_conn,
                                  uint32_t sessid,
                                  int64_t sql_request_level,
                                  const char *set_sql_mode_cstr)
{
  int ret = OB_SUCCESS;
  ObISQLConnectionPool *dblink_pool = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dblink proxy not inited");
  } else if (OB_FAIL(switch_dblink_conn_pool(dblink_type, dblink_pool))) {
    LOG_WARN("failed to get dblink interface", K(ret), K(dblink_type));
  } else if (OB_FAIL(dblink_pool->acquire_dblink(dblink_id, param_ctx, dblink_conn, sessid, sql_request_level))) {
    LOG_WARN("acquire dblink failed", K(ret), K(dblink_id), K(dblink_type));
  } else if (OB_FAIL(prepare_enviroment(dblink_conn, dblink_type, set_sql_mode_cstr))) {
    LOG_WARN("failed to prepare dblink env", K(ret));
  } else {
    dblink_conn->set_dblink_id(dblink_id);
    dblink_conn->set_dblink_driver_proto(static_cast<int64_t>(dblink_type));
  }
  return ret;
}

int ObDbLinkProxy::prepare_enviroment(ObISQLConnection *dblink_conn, int link_type, const char *set_sql_mode_cstr)
{
  int ret = OB_SUCCESS;
  bool is_inited = false;
  if (OB_ISNULL(dblink_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dblink is null", K(ret));
  } else if (OB_FAIL(dblink_conn->is_session_inited(set_sql_mode_cstr, is_inited))) {
    LOG_WARN("failed to get init status", K(ret));
  } else if (is_inited) {
    // do nothing
  } else {
    if (OB_FAIL(execute_init_sql(dblink_conn, link_type, set_sql_mode_cstr))) {
      LOG_WARN("failed to execute init sql", K(ret));
    } else {
      LOG_DEBUG("set session variable nls_date_format");
      dblink_conn->set_init_remote_env(true);
    }
  }
  return ret;
}

int ObDbLinkProxy::execute_init_sql(ObISQLConnection *dblink_conn, int link_type, const char *set_sql_mode_cstr)
{
  int ret = OB_SUCCESS;
  typedef const char * sql_ptr_type;
  if (!lib::is_oracle_mode()) {
    const int64_t sql_ptr_count = 2;
    sql_ptr_type sql_ptr[sql_ptr_count] = {set_sql_mode_cstr,
                                           "set character_set_results = NULL"};
    if (OB_ISNULL(set_sql_mode_cstr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret), KP(set_sql_mode_cstr));
    } else {
      ObMySQLStatement stmt;
      ObMySQLConnection *mysql_conn = static_cast<ObMySQLConnection *>(dblink_conn);
      for (int i = 0; OB_SUCC(ret) && i < sql_ptr_count; ++i) {
        if (OB_FAIL(stmt.init(*mysql_conn, sql_ptr[i]))) {
          LOG_WARN("create statement failed", K(ret), K(link_type));
        } else if (OB_FAIL(stmt.execute_update())) {
          LOG_WARN("execute sql failed",  K(ret), K(link_type));
        } else {
          // do nothing
        }
      }
    }
  } else if (DBLINK_DRV_OB == link_type) {
    static sql_ptr_type sql_ptr[] = {
      "set character_set_results = NULL",
      "set nls_date_format='YYYY-MM-DD HH24:MI:SS'",
      "set nls_timestamp_format = 'YYYY-MM-DD HH24:MI:SS.FF'",
      "set nls_timestamp_tz_format = 'YYYY-MM-DD HH24:MI:SS.FF TZR TZD'"
    };
    int64_t sql_ptr_count = sizeof(sql_ptr) / sizeof(sql_ptr_type);
    // todo statement may different
    ObMySQLStatement stmt;
    ObMySQLConnection *mysql_conn = static_cast<ObMySQLConnection *>(dblink_conn);
    for (int i = 0; OB_SUCC(ret) && i < sql_ptr_count; ++i) {
      if (OB_FAIL(stmt.init(*mysql_conn, sql_ptr[i]))) {
        LOG_WARN("create statement failed", K(ret), K(link_type));
      } else if (OB_FAIL(stmt.execute_update())) {
        LOG_WARN("execute sql failed",  K(ret), K(link_type));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObDbLinkProxy::release_dblink(/*uint64_t dblink_id,*/ DblinkDriverProto dblink_type, ObISQLConnection *dblink_conn, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  ObISQLConnectionPool *dblink_pool = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("dblink proxy not inited");
  } else if (OB_FAIL(switch_dblink_conn_pool(dblink_type, dblink_pool))) {
    LOG_WARN("failed to get dblink interface", K(ret));
  } else if (OB_FAIL(dblink_pool->release_dblink(dblink_conn, sessid))) {
    LOG_WARN("release dblink failed", K(ret), K(sessid));
  }
  return ret;
}

int ObDbLinkProxy::dblink_read(ObISQLConnection *dblink_conn, ReadResult &result, const char *sql)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_ISNULL(dblink_conn) || OB_ISNULL(sql)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), KP(dblink_conn), KP(sql));
  } else if (OB_FAIL(dblink_conn->execute_read(OB_INVALID_TENANT_ID, sql, result))) {
    LOG_WARN("read from dblink failed", K(ret), K(dblink_conn), KCSTRING(sql));
  } else {
    LOG_DEBUG("succ to read from dblink", K(sql));
  }
  return ret;
}

int ObDbLinkProxy::dblink_write(ObISQLConnection *dblink_conn, int64_t &affected_rows, const char *sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dblink_conn) || OB_ISNULL(sql)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), KP(dblink_conn), KP(sql));
  } else if (OB_FAIL(dblink_conn->execute_write(OB_INVALID_TENANT_ID, sql, affected_rows))) {
    LOG_WARN("write to dblink failed", K(ret), K(dblink_conn), K(sql));
  } else {
    LOG_DEBUG("succ to write by dblink", K(sql));
  }
  return ret;
}

int ObDbLinkProxy::rollback(ObISQLConnection *dblink_conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dblink_conn)) {
    LOG_WARN("dblink conn is NULL", K(ret));
  } else if (OB_FAIL(dblink_conn->rollback())) {
    LOG_WARN("read from dblink failed", K(ret));
  }
  return ret;
}
