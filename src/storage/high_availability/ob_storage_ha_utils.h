// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEABASE_STORAGE_HA_UTILS_H_
#define OCEABASE_STORAGE_HA_UTILS_H_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{

class ObStorageHAUtils
{
public:
  static int check_tablet_replica_validity(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObAddr &addr, const common::ObTabletID &tablet_id, common::ObISQLClient &sql_client);
  static int get_server_version(uint64_t &server_version);
  static int check_server_version(const uint64_t server_version);

private:
  static int check_merge_error_(const uint64_t tenant_id, common::ObISQLClient &sql_client);
  static int fetch_src_tablet_meta_info_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const common::ObAddr &src_addr, common::ObISQLClient &sql_client,
    share::SCN &compaction_scn);
  static int check_tablet_replica_checksum_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const share::SCN &compaction_scn, common::ObISQLClient &sql_client);
};

} // end namespace storage
} // end namespace oceanbase

#endif
