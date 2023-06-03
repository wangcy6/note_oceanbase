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

#ifndef OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_

#include "lib/container/ob_array.h"
#include "share/ob_ddl_task_executor.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
}

namespace share
{
class ObReplicaFilterHolder;
class SCN;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
}
}

namespace obrpc
{
class ObSrvRpcProxy;
}

namespace rootserver
{
class ObZoneManager;
class ObDDLService;
class ObDDLTaskRecord;

class ObIndexBuilder
{
public:
  explicit ObIndexBuilder(ObDDLService &ddl_service);
  virtual ~ObIndexBuilder();

  int create_index(const obrpc::ObCreateIndexArg &arg,
<<<<<<< HEAD
                   const share::SCN &frozen_scn,
=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
                   obrpc::ObAlterTableRes &res);
  int drop_index(const obrpc::ObDropIndexArg &arg, obrpc::ObDropIndexRes &res);

  // Check and update local index status.
  // if not all index table updated return OB_EAGAIN.
  int do_create_index(
      const obrpc::ObCreateIndexArg &arg,
<<<<<<< HEAD
      const share::SCN &frozen_scn,
=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
      obrpc::ObAlterTableRes &res);
  int do_create_global_index(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &table_schema,
<<<<<<< HEAD
      const share::SCN &frozen_scn,
=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
      obrpc::ObAlterTableRes &res);
  int do_create_local_index(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &table_schema,
<<<<<<< HEAD
      const share::SCN &frozen_scn,
      obrpc::ObAlterTableRes &res);
  int generate_schema(const obrpc::ObCreateIndexArg &arg,
                      const share::SCN &frozen_scn,
=======
      obrpc::ObAlterTableRes &res);
  int generate_schema(const obrpc::ObCreateIndexArg &arg,
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
                      share::schema::ObTableSchema &data_schema,
                      const bool global_index_without_column_info,
                      share::schema::ObTableSchema &index_schema);
  int submit_drop_index_task(
      const share::schema::ObTableSchema &index_schema,
      const obrpc::ObDropIndexArg &arg,
      int64_t &task_id);
  int submit_build_index_task(common::ObMySQLTransaction &trans,
                              const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema *data_schema,
                              const share::schema::ObTableSchema *index_schema,
                              const int64_t parallelism,
                              common::ObIAllocator &allocator,
                              ObDDLTaskRecord &task_record,
                              const int64_t group_id);
private:
  typedef common::ObArray<std::pair<int64_t, common::ObString> > OrderFTColumns;
  class FulltextColumnOrder
  {
  public:
    FulltextColumnOrder() {}
    ~FulltextColumnOrder() {}

    bool operator()(const std::pair<int64_t, common::ObString> &left,
                    const std::pair<int64_t, common::ObString> &right) const
    {
      return left.first < right.first;
    }
  };

  int set_basic_infos(const obrpc::ObCreateIndexArg &arg,
<<<<<<< HEAD
                      const share::SCN &frozen_scn,
=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
                      const share::schema::ObTableSchema &data_schema,
                      share::schema::ObTableSchema &schema);
  int set_index_table_columns(const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema &data_schema,
                              share::schema::ObTableSchema &schema);
  int set_index_table_options(const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema &data_schema,
                              share::schema::ObTableSchema &schema);

  bool is_final_index_status(const share::schema::ObIndexStatus index_status) const;

private:
  ObDDLService &ddl_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexBuilder);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_
