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

#ifndef OCEANBASE_STORAGE_OB_DDL_SERVER_CLIENT_H
#define OCEANBASE_STORAGE_OB_DDL_SERVER_CLIENT_H

#include "share/ob_rpc_struct.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace storage
{
class ObDDLServerClient final
{
public:
  static int create_hidden_table(const obrpc::ObCreateHiddenTableArg &arg, obrpc::ObCreateHiddenTableRes &res, int64_t &snapshot_version, sql::ObSQLSessionInfo &session);
  static int start_redef_table(const obrpc::ObStartRedefTableArg &arg, obrpc::ObStartRedefTableRes &res, sql::ObSQLSessionInfo &session);
  static int copy_table_dependents(const obrpc::ObCopyTableDependentsArg &arg);
  static int finish_redef_table(const obrpc::ObFinishRedefTableArg &finish_redef_arg,
                                const obrpc::ObDDLBuildSingleReplicaResponseArg &build_single_arg,
                                sql::ObSQLSessionInfo &session);
  static int abort_redef_table(const obrpc::ObAbortRedefTableArg &arg, sql::ObSQLSessionInfo &session);
  static int build_ddl_single_replica_response(const obrpc::ObDDLBuildSingleReplicaResponseArg &arg);
private:
  static int wait_task_reach_pending(const uint64_t tenant_id, const int64_t task_id, int64_t &snapshot_version, ObMySQLProxy &sql_proxy, sql::ObSQLSessionInfo &session);
  static int heart_beat_clear(const int64_t task_id);
};

}  // end of namespace observer
}  // end of namespace oceanbase

#endif /*_OCEANBASE_STORAGE_OB_DDL_SERVER_CLIENT_H_ */