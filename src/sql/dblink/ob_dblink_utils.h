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

#ifndef OCEANBASE_SQL_OB_DBLINK_UTILS_H
#define OCEANBASE_SQL_OB_DBLINK_UTILS_H

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace sql
{

class ObDblinkService
{
public:
  static int check_lob_in_result(common::sqlclient::ObMySQLResult *result, bool &have_lob);
  static int get_length_from_type_text(ObString &type_text, int32_t &length);
  static int get_set_sql_mode_cstr(sql::ObSQLSessionInfo *session_info, const char *&set_sql_mode_cstr, ObIAllocator &allocator);
};

enum DblinkGetConnType {
  DBLINK_POOL = 0,
  TEMP_CONN,
  TM_CONN
};

class ObReverseLink
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObReverseLink();
  virtual ~ObReverseLink();
  inline void set_user(ObString name) { user_ = name; }
  inline void set_tenant(ObString name) { tenant_ = name; }
  inline void set_cluster(ObString name) { cluster_ = name; }
  inline void set_passwd(ObString name) { passwd_ = name; }
  inline void set_addr(common::ObAddr addr) { addr_ = addr; }
  inline void set_self_addr(common::ObAddr addr) { self_addr_ = addr; }
  inline void set_tx_id(int64_t tx_id) { tx_id_ = tx_id; }
  inline void set_tm_sessid(uint32_t tm_sessid) { tm_sessid_ = tm_sessid; }
  const ObString &get_user() { return user_; }
  const ObString &get_tenant() { return tenant_; }
  const ObString &get_cluster() { return cluster_; }
  const ObString &get_passwd() { return passwd_; }
  const common::ObAddr &get_addr() { return addr_; }
  const common::ObAddr &get_self_addr() { return self_addr_; }
  int64_t get_tx_id() { return tx_id_; }
  uint32_t get_tm_sessid() { return tm_sessid_; }

  int open(int64_t session_sql_req_level);
  int read(const char *sql, ObISQLClient::ReadResult &res);
  int close();
  TO_STRING_KV(K_(user),
              K_(tenant),
              K_(cluster),
              K_(passwd),
              K_(addr),
              K_(self_addr),
              K_(tx_id),
              K_(tm_sessid),
              K_(is_close));
public:
  static const char *SESSION_VARIABLE;
  static const int64_t VARI_LENGTH;
  static const ObString SESSION_VARIABLE_STRING;
  static const int64_t LONG_QUERY_TIMEOUT;
private:
  common::ObArenaAllocator allocator_;
  ObString user_;
  ObString tenant_;
  ObString cluster_;
  ObString passwd_;
  common::ObAddr addr_; // for rm connect to tm
  common::ObAddr self_addr_; // for proxy to route reverse link sql
  int64_t tx_id_;
  uint32_t tm_sessid_;
  bool is_close_;
  common::sqlclient::ObMySQLConnection reverse_conn_; // ailing.lcq to do, ObReverseLink can be used by serval connection, not just one
  char db_user_[OB_MAX_USER_NAME_LENGTH + OB_MAX_TENANT_NAME_LENGTH + OB_MAX_CLUSTER_NAME_LENGTH];
  char db_pass_[OB_MAX_PASSWORD_LENGTH];
};

class ObDblinkUtils
{
public:
  static int has_reverse_link_or_any_dblink(const ObDMLStmt *stmt, bool &has, bool has_any_dblink = false);
};

class ObSQLSessionInfo;

class ObDblinkCtxInSession
{
public:
  explicit ObDblinkCtxInSession(ObSQLSessionInfo *session_info)
    :
      session_info_(session_info),
      reverse_dblink_(NULL),
      reverse_dblink_buf_(NULL),
      sys_var_reverse_info_buf_(NULL),
      sys_var_reverse_info_buf_size_(0)
  {}
  ~ObDblinkCtxInSession()
  {
    reset();
  }
  inline void reset()
  {
    arena_alloc_.reset();
    const bool force_disconnect = true;
    clean_dblink_conn(force_disconnect);
    free_dblink_conn_pool();
    // session_info_ = NULL; // do not need reset session_info_
    reverse_dblink_ = NULL;
  }
  int register_dblink_conn_pool(common::sqlclient::ObCommonServerConnectionPool *dblink_conn_pool);
  int free_dblink_conn_pool();
  int get_dblink_conn(uint64_t dblink_id, common::sqlclient::ObISQLConnection *&dblink_conn);
  int set_dblink_conn(common::sqlclient::ObISQLConnection *dblink_conn);
  int clean_dblink_conn(const bool force_disconnect);
  inline bool is_dblink_xa_tras() { return !dblink_conn_holder_array_.empty(); }
  int get_reverse_link(ObReverseLink *&reverse_dblink);
private:
  ObSQLSessionInfo *session_info_;
  ObReverseLink *reverse_dblink_;
  void * reverse_dblink_buf_;
  void * sys_var_reverse_info_buf_;
  int64_t sys_var_reverse_info_buf_size_;
  common::ObArenaAllocator arena_alloc_;
  ObArray<common::sqlclient::ObCommonServerConnectionPool *> dblink_conn_pool_array_;  //for dblink read to free connection when session drop.
  ObArray<int64_t> dblink_conn_holder_array_; //for dblink write to hold connection during trasaction.
  ObString last_reverse_info_values_;
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_DBLINK_UTILS_H
