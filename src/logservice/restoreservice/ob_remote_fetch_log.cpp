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

#include "ob_remote_fetch_log.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/restoreservice/ob_log_restore_define.h"
#include "ob_log_restore_archive_driver.h"    // ObLogRestoreArchiveDriver
#include "ob_log_restore_net_driver.h"        // ObLogRestoreNetDriver
#include "share/restore/ob_log_restore_source.h"

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::common;
using namespace oceanbase::share;
ObRemoteFetchLogImpl::ObRemoteFetchLogImpl() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  archive_driver_(NULL),
  net_driver_(NULL)
{}

ObRemoteFetchLogImpl::~ObRemoteFetchLogImpl()
{
  destroy();
}

int ObRemoteFetchLogImpl::init(const uint64_t tenant_id,
    ObLogRestoreArchiveDriver *archive_driver,
    ObLogRestoreNetDriver *net_driver)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObRemoteFetchLogImpl init twice", K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(archive_driver)
      || OB_ISNULL(net_driver)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(archive_driver), K(net_driver));
  } else {
    tenant_id_ = tenant_id;
    archive_driver_ = archive_driver;
    net_driver_ = net_driver;
    inited_ = true;
  }
  return ret;
}

void ObRemoteFetchLogImpl::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  archive_driver_ = NULL;
  net_driver_ = NULL;
}

int ObRemoteFetchLogImpl::do_schedule(const share::ObLogRestoreSourceItem &source)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObRemoteFetchLogImpl not init", K(inited_));
  } else if (is_service_log_source_type(source.type_)) {
    RestoreServiceAttr service_attr;
    ObSqlString value;
  if (OB_FAIL(value.assign(source.value_))) {
    CLOG_LOG(WARN, "string assign failed", K(source));
  } else if (OB_FAIL(service_attr.parse_service_attr_from_str(value))) {
    CLOG_LOG(WARN, "parse_service_attr failed", K(source));
  } else {
    ret = net_driver_->do_schedule(service_attr);
  }
<<<<<<< HEAD
  return ret;
}

int ObRemoteFetchLogImpl::do_fetch_log_(ObLS &ls)
{
  int ret = OB_SUCCESS;
  bool can_fetch_log = false;
  bool need_schedule = false;
  int64_t proposal_id = -1;
  LSN lsn;
  SCN pre_scn;   // scn to locate piece
  LSN max_fetch_lsn;
  int64_t last_fetch_ts = OB_INVALID_TIMESTAMP;
  int64_t size = 0;
  const ObLSID &id = ls.get_ls_id();
  ObLSRestoreStatus restore_status;
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(ls.get_restore_status(restore_status))) {
    LOG_WARN("failed to get ls restore status", K(ret), K(id));
  } else if (!restore_status.can_restore_log()) {
  } else if (OB_FAIL(check_replica_status_(ls, can_fetch_log))) {
    LOG_WARN("check replica status failed", K(ret), K(ls));
  } else if (! can_fetch_log) {
    // just skip
  } else if (OB_FAIL(check_need_schedule_(ls, need_schedule,
          proposal_id, max_fetch_lsn, last_fetch_ts))) {
    LOG_WARN("check need schedule failed", K(ret), K(id));
  } else if (! need_schedule) {
  } else if (OB_FAIL(get_fetch_log_base_lsn_(ls, max_fetch_lsn, last_fetch_ts, pre_scn, lsn, size))) {
    LOG_WARN("get fetch log base lsn failed", K(ret), K(id));
  } else if (OB_FAIL(submit_fetch_log_task_(ls, pre_scn, lsn, size, proposal_id))) {
    LOG_WARN("submit fetch log task failed", K(ret), K(id), K(lsn), K(size));
=======
  } else if (is_location_log_source_type(source.type_)) {
    ret = archive_driver_->do_schedule();
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  net_driver_->scan_ls(source.type_);
  return ret;
}

void ObRemoteFetchLogImpl::clean_resource()
{
  net_driver_->clean_resource();
}

void ObRemoteFetchLogImpl::update_restore_upper_limit()
{
  (void)net_driver_->set_restore_log_upper_limit();
}
<<<<<<< HEAD

// TODO 参考租户同步位点/可回放点/日志盘空间等, 计算可以拉取日志上限
int ObRemoteFetchLogImpl::get_fetch_log_max_lsn_(ObLS &ls, palf::LSN &max_lsn)
{
  UNUSED(ls);
  UNUSED(max_lsn);
  int ret = OB_SUCCESS;
  return ret;
}

//TODO fetch log strategy
int ObRemoteFetchLogImpl::get_fetch_log_base_lsn_(ObLS &ls,
    const LSN &max_fetch_lsn,
    const int64_t last_fetch_ts,
    SCN &heuristic_scn,
    LSN &lsn,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  LSN end_lsn;
  LSN restore_lsn;
  const int64_t default_fetch_size = LEADER_DEFAULT_GROUP_BUFFER_SIZE;
  const bool ignore_restore = OB_INVALID_TIMESTAMP == last_fetch_ts
    || last_fetch_ts < ObTimeUtility::current_time() - 5 * 1000 * 1000L;
  if (OB_FAIL(get_palf_base_lsn_scn_(ls, end_lsn, heuristic_scn))) {
    LOG_WARN("get palf base lsn failed", K(ret), K(ls));
  } else {
    lsn = ignore_restore ? end_lsn : max_fetch_lsn;
    size = default_fetch_size;
  }
  return ret;
}

// 如果日志流当前LSN为0, 以日志流create_scn作为基准时间戳
int ObRemoteFetchLogImpl::get_palf_base_lsn_scn_(ObLS &ls, LSN &lsn, SCN &scn)
{
  int ret = OB_SUCCESS;
  PalfHandleGuard palf_handle_guard;
  const ObLSID &id = ls.get_ls_id();
  if (OB_FAIL(log_service_->open_palf(id, palf_handle_guard))) {
    LOG_WARN("open palf failed", K(ret));
  } else if (OB_FAIL(palf_handle_guard.get_end_scn(scn))) {
    LOG_WARN("get end log scn failed", K(ret), K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_lsn(lsn))) {
    LOG_WARN("get end lsn failed", K(ret), K(id));
  } else {
    const SCN &checkpoint_scn = ls.get_clog_checkpoint_scn();
    scn = SCN::max(scn, checkpoint_scn);
  }
  return ret;
}

int ObRemoteFetchLogImpl::submit_fetch_log_task_(ObLS &ls,
    const SCN &scn,
    const LSN &lsn,
    const int64_t size,
    const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  ObFetchLogTask *task = NULL;
  ObMemAttr attr(MTL_ID(), "FetchLogTask");
  const ObLSID &id = ls.get_ls_id();
  bool scheduled = false;
  if (OB_ISNULL(data = mtl_malloc(sizeof(FetchLogTask), "RFLTask"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(id));
  } else {
    task = new (data) ObFetchLogTask(id, scn, lsn, size, proposal_id);
    ObLogRestoreHandler *restore_handler = NULL;
    if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get restore_handler failed", K(ret), K(ls));
    } else if (OB_FAIL(restore_handler->schedule(id.id(), proposal_id, lsn + size, scheduled))) {
      LOG_WARN("schedule failed", K(ret), K(ls));
    } else if (! scheduled) {
      // not scheduled
    } else if (OB_FAIL(worker_->submit_fetch_log_task(task))) {
      scheduled = false;
      LOG_ERROR("submit fetch log task failed", K(ret), K(id));
    } else {
      worker_->signal();
      LOG_INFO("submit fetch log task succ", KPC(task));
    }
    if (! scheduled && NULL != data) {
      mtl_free(data);
      data = NULL;
    }
  }
  return ret;
}

=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
} // namespace logservice
} // namespace oceanbase
