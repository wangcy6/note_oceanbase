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

#ifndef OCEANBASE_ROOTSERVER_OB_RECCOVERY_LS_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_RECCOVERY_LS_SERVICE_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "logservice/ob_log_base_type.h"//ObIRoleChangeSubHandler ObICheckpointSubHandler ObIReplaySubHandler
#include "logservice/palf/lsn.h"//palf::LSN
#include "logservice/palf/palf_iterator.h"          //PalfBufferIterator
#include "ob_primary_ls_service.h" //ObTenantThreadHelper
#include "lib/lock/ob_spin_lock.h" //ObSpinLock
#include "storage/tx/ob_multi_data_source.h" //ObTxBufferNode
#include "src/share/restore/ob_log_restore_source.h" //ObLogRestoreSourceItem
#include "src/share/backup/ob_backup_struct.h" //ObRestoreSourceServiceAttr
#include "share/restore/ob_log_restore_source_mgr.h" //ObLogRestoreSourceMgr

namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObMySQLTransaction;
}
namespace share
{
class ObLSTableOperator;
struct ObLSAttr;
struct ObLSRecoveryStat;
class SCN;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
}
}
namespace logservice
{
class ObLogHandler;
class ObGCLSLog;
}
namespace transaction
{
class ObTxLogBlock;
}
namespace rootserver 
{
/*description:
 *Restores the status of each log stream according to the logs to which the
 *system log stream is synchronized, and updates the recovery progress of the
 *system log stream. This thread should only exist in the standby database or
 *the recovery process, and needs to be registered in the RestoreHandler.
 *This thread is only active on the leader of the system log stream under the user tenant*/
class ObRecoveryLSService : public ObTenantThreadHelper
{
public:
  ObRecoveryLSService() : inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID), proxy_(NULL), primary_is_avaliable_(true) {}
  virtual ~ObRecoveryLSService() {}
  int init();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObRecoveryLSService)
private:
 //get log iterator by start_scn
 int seek_log_iterator_(const share::SCN &syn_scn,
                        palf::PalfBufferIterator &iterator);
<<<<<<< HEAD
 int process_ls_log_(const share::SCN &start_scn,palf::PalfBufferIterator &iterator);
=======
 int process_ls_log_(const ObAllTenantInfo &tenant_info,
                     share::SCN &start_scn,
                     palf::PalfBufferIterator &iterator);
 int process_upgrade_log_(const transaction::ObTxBufferNode &node);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
 int process_gc_log_(logservice::ObGCLSLog &gc_log,
                     const share::SCN &syn_scn);
 int process_ls_tx_log_(transaction::ObTxLogBlock &tx_log,
                        const share::SCN &syn_scn);
 int process_ls_operator_(const share::ObLSAttr &ls_attr,
                          const share::SCN &syn_scn);
 int create_new_ls_(const share::ObLSAttr &ls_attr,
                    const share::SCN &syn_scn,
                    common::ObMySQLTransaction &trans);
 int process_recovery_ls_manager();
 int construct_ls_recovery_stat(const share::SCN &syn_scn,
                                share::ObLSRecoveryStat &ls_stat);
 //wait other ls is larger than sycn ts
<<<<<<< HEAD
 int check_valid_to_operator_ls_(const share::SCN &syn_scn);
 int check_can_do_recovery_();
 //check restore finish and update sync scn to recovery_unitl_scn
 int update_sys_ls_restore_finish_();
 //readable scn need report
 int report_sys_ls_recovery_stat_(const share::SCN &sync_scn);
=======
 int check_valid_to_operator_ls_(const share::ObLSAttr &ls_attr,
                                 const share::SCN &syn_scn);
 int check_can_do_recovery_(const ObAllTenantInfo &tenant_info);
 //readable scn need report
 int report_sys_ls_recovery_stat_(const share::SCN &sync_scn);
 void try_tenant_upgrade_end_();
 int get_min_data_version_(uint64_t &compatible);
 int process_ls_operator_in_trans_(const share::ObLSAttr &ls_attr,
     const share::SCN &sync_scn, common::ObMySQLTransaction &trans);
 void try_update_primary_ip_list();
 bool check_need_update_ip_list(share::ObLogRestoreSourceItem &item);
 int get_restore_source_value(ObLogRestoreSourceItem &item, ObSqlString &standby_source_value);
 int do_update_restore_source(ObRestoreSourceServiceAttr &old_attr, ObLogRestoreSourceMgr &restore_source_mgr);
 int update_source_inner_table(char *buf, const int64_t buf_size, ObMySQLTransaction &trans, const ObLogRestoreSourceItem &item);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
private:
  bool inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *proxy_;
  bool primary_is_avaliable_;
};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_RECCOVERY_LS_SERVICE_H */
