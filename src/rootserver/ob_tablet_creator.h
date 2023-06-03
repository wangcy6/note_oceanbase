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

#ifndef OB_TABLET_CREATOR_H
#define OB_TABLET_CREATOR_H

#include "ob_rs_async_rpc_proxy.h" //async rpc
#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/ob_cached_allocator.h"
#include "common/ob_tablet_id.h"//ObTabletID
#include "share/ob_rpc_struct.h"//ObBatchCreateTabletArg
#include "share/ob_ls_id.h"//share::ObLSID
#include "share/ls/ob_ls_table_operator.h"

namespace oceanbase
{
namespace rpc
{
class ObBatchCreateTabletArg;
}
namespace share
{
class ObLSTableOperator;
}
namespace rootserver
{
struct ObTabletCreatorArg
{
public:
  ObTabletCreatorArg()
   : tablet_ids_(),
     data_tablet_id_(),
     ls_key_(),
     table_schemas_(),
     compat_mode_(lib::Worker::CompatMode::INVALID),
     is_create_bind_hidden_tablets_(false) {}
  virtual ~ObTabletCreatorArg() {}
  bool is_valid() const;
  void reset();
  int assign (const ObTabletCreatorArg &arg);
  int init(const ObIArray<common::ObTabletID> &tablet_ids,
           const share::ObLSID &ls_key,
           const common::ObTabletID data_tablet_id,
           const ObIArray<const share::schema::ObTableSchema*> &table_schemas,
           const lib::Worker::CompatMode &mode,
           const bool is_create_bind_hidden_tablets);

  DECLARE_TO_STRING;
  common::ObArray<common::ObTabletID> tablet_ids_;
  common::ObTabletID data_tablet_id_;
  share::ObLSID ls_key_;
  common::ObArray<const share::schema::ObTableSchema*> table_schemas_;
  lib::Worker::CompatMode compat_mode_;
  bool is_create_bind_hidden_tablets_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletCreatorArg);
};

struct ObBatchCreateTabletHelper
{
public:
  ObBatchCreateTabletHelper() :arg_(), table_schemas_map_(), result_(common::OB_NOT_MASTER), next_(NULL) {}
  int init(const share::ObLSID &ls_key,
           const int64_t tenant_id,
<<<<<<< HEAD
           const share::SCN &major_frozen_scn);
=======
           const share::SCN &major_frozen_scn,
           const bool need_check_tablet_cnt);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  int try_add_table_schema(const share::schema::ObTableSchema *table_schema, int64_t &index);
  int add_arg_to_batch_arg(const ObTabletCreatorArg &arg);
  void reset()
  {
    arg_.reset();
    table_schemas_map_.clear();
    result_ = common::OB_NOT_MASTER;
  }
  DECLARE_TO_STRING;
  obrpc::ObBatchCreateTabletArg arg_;
  //table_id : index of table_schems_ in arg
  common::hash::ObHashMap<int64_t, int64_t> table_schemas_map_;
  //the result of create tablet
  int result_;
  ObBatchCreateTabletHelper *next_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchCreateTabletHelper);
};

class ObTabletCreator
{
public:
  ObTabletCreator(
      const uint64_t tenant_id,
      const share::SCN &major_frozen_scn,
      share::ObLSTableOperator &lst_operator,
      ObMySQLTransaction &trans)
                : tenant_id_(tenant_id),
                  major_frozen_scn_(major_frozen_scn),
                  lst_operator_(&lst_operator),
                  allocator_("TbtCret"),
                  args_map_(),
                  trans_(trans),
                  need_check_tablet_cnt_(false),
                  inited_(false) {}
  virtual ~ObTabletCreator();
  int init(const bool need_check_tablet_cnt);
  int execute();
  bool need_retry(int ret);
  int add_create_tablet_arg(const ObTabletCreatorArg &arg);
  void reset();
private:
  int find_leader_of_ls(const share::ObLSID &id, ObAddr &addr);
private:
  const int64_t MAP_BUCKET_NUM = 1024;
private:
  const uint64_t tenant_id_;
  const share::SCN major_frozen_scn_;
  share::ObLSTableOperator *lst_operator_;
  ObArenaAllocator allocator_;
  common::hash::ObHashMap<share::ObLSID, ObBatchCreateTabletHelper*> args_map_;
  ObMySQLTransaction &trans_;
  bool need_check_tablet_cnt_;
  bool inited_;
};
}
}



#endif /* !OB_TABLET_CREATOR_H */
