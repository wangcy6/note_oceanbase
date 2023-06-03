// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "lib/utility/ob_tracepoint.h"
#include "ob_dup_table_base.h"
#include "ob_dup_table_tablets.h"
#include "ob_dup_table_util.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{

using namespace common;
using namespace share;
namespace transaction
{

int64_t ObLSDupTabletsMgr::GC_DUP_TABLETS_TIME_INTERVAL = 5 * 60 * 1000 * 1000L; // 5 min
int64_t ObLSDupTabletsMgr::GC_DUP_TABLETS_FAILED_TIMEOUT =
    5 * GC_DUP_TABLETS_TIME_INTERVAL;                           // 25 min
const int64_t ObLSDupTabletsMgr::GC_TIMEOUT = 1 * 1000 * 1000L; // 1s

const int64_t ObLSDupTabletsMgr::RESERVED_FREE_SET_COUNT = 64;
const int64_t ObLSDupTabletsMgr::MAX_FREE_SET_COUNT = 1000;
const int64_t ObLSDupTabletsMgr::MAX_CONFIRMING_TABLET_COUNT = 20000;

OB_SERIALIZE_MEMBER(DupTabletCommonHeader, unique_id_, tablet_set_type_, sp_op_type_);
OB_SERIALIZE_MEMBER(DupTabletChangeLogTail, readable_version_, has_confirmed_);
OB_SERIALIZE_MEMBER(DupTabletSpecialOpArg, op_objects_);

OB_SERIALIZE_MEMBER(DupTabletCommonLogBody, tablet_id_map_);
OB_SERIALIZE_MEMBER_INHERIT(DupTabletChangeLogBody, DupTabletCommonLogBody, change_tail_);
OB_SERIALIZE_MEMBER_INHERIT(DupTabletSpecialOpLogBody, DupTabletChangeLogBody, sp_op_arg_);

//**********************************************************************
//******  Hash Callback
//**********************************************************************

int TabletsSerCallBack::operator()(
    const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair)
{
  return hash_pair.first.serialize(buf_, buf_len_, pos_);
}

int TabletsDeSerCallBack::operator()(DupTabletChangeMap &dup_tablet_map)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  DupTabletInfo tmp_info;
  tmp_info.update_dup_schema_ts_ = deser_time_;

  if (OB_FAIL(tablet_id.deserialize(buf_, buf_len_, pos_))) {
    DUP_TABLE_LOG(WARN, "deserialize tablet id failed", K(ret));
  } else if (OB_FAIL(dup_tablet_map.set_refactored(tablet_id, tmp_info, 1))) {
    DUP_TABLE_LOG(WARN, "insert tablet failed", K(ret), K(tablet_id), K(tmp_info));
  }

  return ret;
}

int64_t TabletsGetSizeCallBack::operator()(
    const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair)
{
  return hash_pair.first.get_serialize_size();
}

bool ObLSDupTabletsMgr::GcDiscardedDupTabletHandler::operator()(
    common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair)
{
  bool will_remove = false;
  int tmp_ret = OB_SUCCESS;

  if (0 > hash_pair.second.update_dup_schema_ts_ || 0 > gc_ts_) {
    tmp_ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG_RET(WARN, tmp_ret, "invalid timestamp", K(hash_pair.first),
                      K(hash_pair.second.update_dup_schema_ts_), K(gc_ts_));
  } else if (OB_SUCCESS == ret_) { // if ret_ is failed, not need continue
    if ((gc_ts_ - hash_pair.second.update_dup_schema_ts_) >= gc_time_interval_) {
      if (src_common_header_.is_old_set()) {
        // do nothing
      } else if (src_common_header_.is_new_set()) {
        will_remove = true;
        gc_tablet_cnt_++;
      } else if (src_common_header_.is_readable_set()) {
        DupTabletInfo tmp_info = hash_pair.second;
        if (!old_tablets_.get_change_status()->is_modifiable()) {
          tmp_ret = OB_EAGAIN;
        } else if (OB_TMP_FAIL(old_tablets_.set_refactored(hash_pair.first, tmp_info))) {
          DUP_TABLE_LOG_RET(WARN, tmp_ret, "insert into old_tablets_ failed", K(tmp_ret));
        } else {
          will_remove = true;
          gc_tablet_cnt_++;
        }
      } else {
        DUP_TABLE_LOG_RET(ERROR, tmp_ret, "unexpected src type", K(tmp_ret), K(src_common_header_));
      }
    }
    if (OB_TMP_FAIL(tmp_ret)) {
      ret_ = tmp_ret;
    }
  }
  DUP_TABLE_LOG(DEBUG, "gc handler", K(ret_), K(hash_pair.first), K(src_common_header_),
                K(gc_tablet_cnt_),
                K((gc_ts_ - hash_pair.second.update_dup_schema_ts_) >= gc_time_interval_));

  return will_remove;
}

int ObLSDupTabletsMgr::ConfirmedDupTabletHandler::operator()(
    common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(readable_.set_refactored(hash_pair.first, hash_pair.second, 1))) {
    DUP_TABLE_LOG(WARN, "insert into readable_tablets_ failed", K(ret));
  }
  return ret;
}

int ObLSDupTabletsMgr::DiagInfoGenerator::operator()(
    const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair)
{
  int ret = OB_SUCCESS;

  if ((iter_count_) % 2 == 0) {
    // no need \n after tablet set header
    ret = ::oceanbase::common::databuff_printf(
        info_buf_, info_buf_len_, info_buf_pos_, "\n%s%s[%sTablet Set Member - from %lu] ",
        DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_INDENT_SPACE,
        DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, tablet_set_id_);
  }
  if (OB_SUCC(ret)) {
    ret = ::oceanbase::common::databuff_printf(info_buf_, info_buf_len_, info_buf_pos_,
                                               "{ TabletID = %-10lu, RefreshDupSchemaTs = %-20lu} ",
                                               hash_pair.first.id(),
                                               hash_pair.second.update_dup_schema_ts_);
  }

  iter_count_++;
  return ret;
}

int ObLSDupTabletsMgr::CollectTabletsHandler::operator()(
    const common::hash::HashMapPair<common::ObTabletID, DupTabletInfo> &hash_pair)
{
  int ret = OB_SUCCESS;

  ObDupTableLSTabletsStat tmp_stat;
  tmp_stat.set_tenant_id(tenant_id_);
  tmp_stat.set_ls_id(ls_id_);
  // tmp_stat.set_addr(addr_);
  tmp_stat.set_is_master(is_master_);
  tmp_stat.set_unique_id(tablet_set_id_);
  tmp_stat.set_attr(attr_);
  tmp_stat.set_tablet_id(hash_pair.first);
  tmp_stat.set_refresh_schema_ts(hash_pair.second.update_dup_schema_ts_);
  // tmp_stat.set_need_gc(hash_pair.second.update_dup_schema_ts_ -
  //                      collect_ts_ > tablet_gc_window_);

  if (OB_FAIL(collect_iter_.push(tmp_stat))) {
    DUP_TABLE_LOG(WARN, "push into iter failed", K(tmp_stat));
  }

  return ret;
}

//**********************************************************************
//******  DupTabletSet & DupTabletLog
//**********************************************************************

int DupTabletChangeMap::create(int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (!common_header_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "invalid unique_id", K(ret), K(common_header_));
  } else if (OB_FAIL(DupTabletIdMap::create(bucket_num, "DupTabletHash"))) {
    DUP_TABLE_LOG(WARN, "create dup tablet id map failed", K(ret), K(common_header_),
                  K(bucket_num));
  }

  return ret;
}

int DupTabletChangeMap::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  TabletsSerCallBack ser_cb(buf, buf_len, tmp_pos);

  if (OB_FAIL(hash_for_each_serialize(*this, ser_cb))) {
    DUP_TABLE_LOG(WARN, "serialize dup tablet hash map faild", K(ret));
  } else {
    tmp_pos = ser_cb.get_pos();
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

int DupTabletChangeMap::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t deser_time = ObTimeUtility::fast_current_time();

  int64_t tmp_pos = pos;
  TabletsDeSerCallBack deser_cb(buf, data_len, tmp_pos, deser_time);
  if (OB_FAIL(this->clear())) {
    DUP_TABLE_LOG(WARN, "clear dup tablet hash map faild", K(ret));
  } else if (OB_FAIL(hash_for_each_deserialize(*this, deser_cb))) {
    DUP_TABLE_LOG(WARN, "deserialize dup tablet hash map faild", K(ret));
  } else {
    tmp_pos = deser_cb.get_pos();
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }

  return ret;
}

int64_t DupTabletChangeMap::get_serialize_size() const
{
  int64_t serialize_size = 0;

  TabletsGetSizeCallBack get_size_cb;
  serialize_size += hash_for_each_serialize_size(*this, get_size_cb);

  return serialize_size;
}

bool DupTabletChangeLogTail::is_valid() const { return readable_version_.is_valid(); }

int DupTabletLog::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_FAIL(common_header_.serialize(buf, buf_len, tmp_pos))) {
    DUP_TABLE_LOG(WARN, "serialize dup tablet set header faild", K(ret), K(buf_len), K(tmp_pos),
                  K(pos));

  } else if (!common_header_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "invalid common header", K(ret), K(common_header_), K(buf_len), K(tmp_pos),
                  K(pos));
  } else if (common_header_.is_readable_set()) {
    if (OB_FAIL(hash_map_->serialize(buf, buf_len, tmp_pos))) {
      DUP_TABLE_LOG(WARN, "serialize readable hash map failed", K(ret), KPC(this), K(buf_len),
                    K(tmp_pos), K(pos));
    }
  } else {
    if (!change_tail_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      DUP_TABLE_LOG(WARN, "invalid change header", K(ret), KPC(this));
    } else if (OB_FAIL(hash_map_->serialize(buf, buf_len, tmp_pos))) {
      DUP_TABLE_LOG(WARN, "serialize new/old hash map failed", K(ret), KPC(this), K(buf_len),
                    K(tmp_pos), K(pos));
    } else if (OB_FAIL(change_tail_.serialize(buf, buf_len, tmp_pos))) {
      DUP_TABLE_LOG(WARN, "serialize change header failed", K(ret), KPC(this), K(buf_len),
                    K(tmp_pos), K(pos));
    }
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

int DupTabletLog::deserialize_common_header(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = pos;

  if (OB_FAIL(common_header_.deserialize(buf, data_len, tmp_pos))) {
    DUP_TABLE_LOG(WARN, "deserialize dup tablet set header faild", K(ret), K(data_len), K(tmp_pos),
                  K(pos));

  } else if (!common_header_.is_valid()) {

    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "invalid common header", K(ret), K(common_header_), K(data_len), K(tmp_pos),
                  K(pos));
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

int DupTabletLog::deserialize_content(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = pos;

  if (!common_header_.is_valid() || OB_ISNULL(hash_map_)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), KP(hash_map_), K(common_header_));
  } else if (hash_map_->get_common_header().get_unique_id() != common_header_.get_unique_id()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid hash map", K(ret), K(hash_map_->get_common_header()),
                  K(common_header_));
  } else {

    if (common_header_.is_readable_set()) {
      if (OB_FAIL(hash_map_->deserialize(buf, data_len, tmp_pos))) {
        DUP_TABLE_LOG(WARN, "serialize readable hash map failed", K(ret), KPC(this), K(data_len),
                      K(tmp_pos), K(pos));
      }
    } else {
      if (OB_FAIL(hash_map_->deserialize(buf, data_len, tmp_pos))) {
        DUP_TABLE_LOG(WARN, "serialize new/old hash map failed", K(ret), KPC(this), K(data_len),
                      K(tmp_pos), K(pos));
      } else if (OB_FAIL(change_tail_.deserialize(buf, data_len, tmp_pos))) {
        DUP_TABLE_LOG(WARN, "serialize change header failed", K(ret), KPC(this), K(data_len),
                      K(tmp_pos), K(pos));
      } else if (!change_tail_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        DUP_TABLE_LOG(WARN, "invalid change header", K(ret), KPC(this));
      } else {
        DUP_TABLE_LOG(DEBUG, "deser tablet end", K(ret), K(change_tail_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

int64_t DupTabletLog::get_serialize_size()
{
  int64_t max_size = 0;

  if (OB_NOT_NULL(hash_map_)) {
    max_size += common_header_.get_serialize_size();
    max_size += hash_map_->get_serialize_size();
    if (!common_header_.is_readable_set()) {
      max_size += change_tail_.get_serialize_size();
    }
  } else {
    DUP_TABLE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected error");
  }

  return max_size;
}

int DupTabletLog::set_hash_map_ptr(DupTabletChangeMap *hash_map_ptr, DupTabletSpecialOpArg *arg_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_map_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid hash map", K(ret), KP(hash_map_ptr));
  } else if (hash_map_ptr->get_common_header().get_unique_id() != common_header_.get_unique_id()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "unexpected hash map", K(ret), K(hash_map_ptr->get_common_header()),
                  K(common_header_));
  } else {
    hash_map_ = hash_map_ptr;
  }

  return ret;
}

const DupTabletCommonHeader &DupTabletLog::get_common_header() { return common_header_; }

const DupTabletChangeLogTail &DupTabletLog::get_change_tail() { return change_tail_; }

//**********************************************************************
//******  ObLSDupTabletsMgr
//**********************************************************************
int ObLSDupTabletsMgr::init(ObDupTableLSHandler *dup_ls_handle)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(dup_tablets_lock_);

  if (!ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(init_free_tablet_pool_())) {
    DUP_TABLE_LOG(WARN, "init tablet change set failed", K(ret));
  } else if (OB_FAIL(op_arg_map_.create(8, "DupSpecOp"))) {
    DUP_TABLE_LOG(WARN, "create spec op failed", K(ret));
  } else {
    ATOMIC_STORE(&is_stopped_, false);
    ls_id_ = dup_ls_handle->get_ls_id();
    ATOMIC_STORE(&is_master_, false);
  }

  if (OB_FAIL(ret)) {
    reset();
  }

  return ret;
}

int ObLSDupTabletsMgr::init_free_tablet_pool_()
{
  int ret = OB_SUCCESS;

  destroy_free_tablet_pool_();

  for (int i = 0; i < RESERVED_FREE_SET_COUNT && OB_SUCC(ret); i++) {
    DupTabletChangeMap *tmp_map_ptr = nullptr;
    if (OB_ISNULL(tmp_map_ptr = static_cast<DupTabletChangeMap *>(
                      share::mtl_malloc(sizeof(DupTabletChangeMap), "DupTabletMap")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      // } else if (OB_FALSE_IT(extra_free_set_alloc_count_++)) {
    } else if (OB_FALSE_IT(new (tmp_map_ptr) DupTabletChangeMap(i + 1))) {
    } else if (OB_FAIL(tmp_map_ptr->create(1024))) {
      DUP_TABLE_LOG(WARN, "create dup_tablet hash map", K(ret));
    } else if (false == (free_set_pool_.add_last(tmp_map_ptr))) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "push back into free_set_pool failed", K(ret),
                    K(free_set_pool_.get_size()), KPC(tmp_map_ptr));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_free_tablet_set(removing_old_set_))) {
    DUP_TABLE_LOG(WARN, "get free tablet set failed", K(ret));
  } else {
    removing_old_set_->get_common_header().set_old();
  }

  DUP_TABLE_LOG(INFO, "finish init tablet map", K(ret), KPC(removing_old_set_),
                K(free_set_pool_.get_size()));
  return ret;
}

int ObLSDupTabletsMgr::destroy_free_tablet_pool_()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(removing_old_set_)) {
    return_tablet_set(removing_old_set_);
    if (free_set_pool_.add_last(removing_old_set_) == false) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(ERROR, "insert into free set failed", K(ret), KPC(removing_old_set_));
    }
    removing_old_set_ = nullptr;
  }

  if (OB_NOT_NULL(changing_new_set_)) {
    return_tablet_set(changing_new_set_);
  }

  while (!readable_tablets_list_.is_empty()) {
    return_tablet_set(readable_tablets_list_.remove_last());
  }

  while (!need_confirm_new_queue_.is_empty()) {
    return_tablet_set(need_confirm_new_queue_.remove_last());
  }

  while (!free_set_pool_.is_empty()) {
    DupTabletChangeMap *dup_map_ptr = free_set_pool_.remove_last();
    dup_map_ptr->destroy();
    share::mtl_free(dup_map_ptr);
  }

  return ret;
}

void ObLSDupTabletsMgr::destroy() { reset(); }

void ObLSDupTabletsMgr::reset()
{
  destroy_free_tablet_pool_();
  ls_id_.reset();
  ATOMIC_STORE(&is_stopped_, true);
  ATOMIC_STORE(&is_master_, false);
  last_gc_succ_time_ = 0;
  last_no_free_set_time_ = 0;
  extra_free_set_alloc_count_ = 0;

  if (OB_NOT_NULL(tablet_diag_info_log_buf_)) {
    ob_free(tablet_diag_info_log_buf_);
  }
  tablet_diag_info_log_buf_ = nullptr;
}

int ObLSDupTabletsMgr::check_readable(const common::ObTabletID &tablet_id,
                                      bool &readable,
                                      const share::SCN &snapshot,
                                      DupTableInterfaceStat interface_stat)
{
  int ret = OB_SUCCESS;
  readable = false;
  DupTabletInfo tmp_status;

  SpinRLockGuard guard(dup_tablets_lock_);

  DLIST_FOREACH(readable_node, readable_tablets_list_)
  {
    ret = readable_node->get_refactored(tablet_id, tmp_status);
    if (OB_SUCCESS == ret) {
      readable = true;
      break;
    } else if (OB_HASH_NOT_EXIST == ret) {
      readable = false;
      ret = OB_SUCCESS;
    } else {
      DUP_TABLE_LOG(WARN, "check readable tablet failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !readable) {

    DLIST_FOREACH_X(new_change_map_ptr, need_confirm_new_queue_, !readable && OB_SUCC(ret))
    {
      share::SCN readable_version;

      if (OB_ISNULL(new_change_map_ptr->get_change_status())) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(ERROR, "unexpected tablet set", K(ret), KPC(new_change_map_ptr));

      } else if (FALSE_IT(readable_version =
                              new_change_map_ptr->get_change_status()->readable_version_)) {
      } else if (!readable_version.is_valid()) {
        // can not read
        readable = false;
      } else if (readable_version < snapshot) {
        // can not read
        readable = false;
      } else if (OB_SUCC(new_change_map_ptr->get_refactored(tablet_id, tmp_status))) {
        readable = true;
      } else if (OB_HASH_NOT_EXIST != ret) {
        DUP_TABLE_LOG(WARN, "check dup_table new_tablets_ failed", K(ret));
      } else {
        interface_stat.dup_table_follower_read_tablet_not_exist_cnt_++;
        ret = OB_SUCCESS;
      }
    }
  }

  if (!readable && OB_SUCC(ret)) {
    interface_stat.dup_table_follower_read_tablet_not_ready_cnt_++;
  }

  return ret;
}

int ObLSDupTabletsMgr::find_dup_tablet_in_set(const common::ObTabletID &tablet_id,
                                              bool &is_dup_table,
                                              const share::SCN &from_scn,
                                              const share::SCN &to_scn)
{
  int ret = OB_SUCCESS;

  is_dup_table = false;
  DupTabletInfo tmp_status;

  SpinRLockGuard guard(dup_tablets_lock_);

  // for DEBUG
  // no need to check dup_table which has not submitted
  // if (OB_NOT_NULL(changing_new_set_)) {
  //   if (OB_SUCC(changing_new_set_->get_tablet_id_map().get_refactored(tablet_id, tmp_status))) {
  //     is_dup_table = true;
  //   } else if (OB_HASH_NOT_EXIST != ret) {
  //     DUP_TABLE_LOG(WARN, "check dup_table old_tablets_ failed", K(ret));
  //   } else {
  //     ret = OB_SUCCESS;
  //   }
  // }
  if (!need_confirm_new_queue_.is_empty()) {
    if (need_confirm_new_queue_.get_first()->get_common_header().need_clean_all_readable_set()) {
      is_dup_table = true;
      DUP_TABLE_LOG(INFO, "set all redo as dup_table during clean all followers' readable set",
                    K(ret), K(is_dup_table), K(tablet_id), K(from_scn), K(to_scn),
                    KPC(need_confirm_new_queue_.get_first()));
    }
  }

  if (OB_SUCC(ret) && !is_dup_table) {
    DupTabletChangeMap *new_change_map_ptr = need_confirm_new_queue_.get_first();
    DLIST_FOREACH_X(new_change_map_ptr, need_confirm_new_queue_, !is_dup_table && OB_SUCC(ret))
    {
      share::SCN tablet_change_scn;

      if (OB_ISNULL(new_change_map_ptr->get_change_status())) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(ERROR, "unexpected tablet set type", K(ret), KPC(new_change_map_ptr));

      } else if (FALSE_IT(tablet_change_scn =
                              new_change_map_ptr->get_change_status()->tablet_change_scn_)) {
        if (tablet_change_scn >= from_scn) {
          if (tablet_change_scn > to_scn) {
            break;
          } else if (OB_SUCC(new_change_map_ptr->get_refactored(tablet_id, tmp_status))) {
            is_dup_table = true;
          } else if (OB_HASH_NOT_EXIST != ret) {
            DUP_TABLE_LOG(WARN, "check dup_table new_tablets_ failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }

        new_change_map_ptr = new_change_map_ptr->get_next();
      }
    }
  }

  if (OB_SUCC(ret) && !is_dup_table) {
    DupTabletChangeMap *old_tablet_set = nullptr;
    DupTabletCommonHeader target_common_header;
    target_common_header.set_old();
    target_common_header.set_invalid_unique_id();

    if (OB_FAIL(get_target_tablet_set_(target_common_header, old_tablet_set))) {
      DUP_TABLE_LOG(WARN, "get old tablet set failed", K(ret), KPC(old_tablet_set));
    } else if (OB_SUCC(old_tablet_set->get_refactored(tablet_id, tmp_status))) {
      is_dup_table = true;
    } else if (OB_HASH_NOT_EXIST != ret) {
      DUP_TABLE_LOG(WARN, "check dup_table old_tablets_ failed", K(ret), KPC(old_tablet_set));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret) && !is_dup_table) {
    DLIST_FOREACH_X(readable_set_ptr, readable_tablets_list_, !is_dup_table && OB_SUCC(ret))
    {
      if (OB_SUCC(readable_set_ptr->get_refactored(tablet_id, tmp_status))) {
        is_dup_table = true;
      } else if (OB_HASH_NOT_EXIST != ret) {
        DUP_TABLE_LOG(WARN, "check dup_table old_tablets_ failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  if (is_dup_table) {
    DUP_TABLE_LOG(INFO, "modify a dup tablet by redo log", K(ret), K(tablet_id), K(is_dup_table),
                  K(*this));
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DUP_TABLE_GC_RIGHT_NOW);
// for gc those not refreshed tablets
int ObLSDupTabletsMgr::gc_dup_tablets(const int64_t gc_ts, const int64_t max_task_interval)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(dup_tablets_lock_);
  int gc_tablet_cnt = 0;
  ObTabletID tmp_id;

  // run gc now
  if (OB_FAIL(ERRSIM_DUP_TABLE_GC_RIGHT_NOW)) {
    ret = OB_SUCCESS;
    last_gc_succ_time_ = gc_ts -  GC_DUP_TABLETS_TIME_INTERVAL;
    DUP_TABLE_LOG(WARN, "use errsim to invoke gc", KR(ret), K(last_gc_succ_time_), K(gc_ts),
                  K(max_task_interval));
  }

  if (0 > (gc_ts - last_gc_succ_time_) || 0 > last_gc_succ_time_ || 0 > gc_ts) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "Invalid gc_ts or last_gc_time_", KR(ret), K(last_gc_succ_time_), K(gc_ts));
  } else if ((gc_ts - last_gc_succ_time_) < GC_DUP_TABLETS_TIME_INTERVAL) {
    DUP_TABLE_LOG(DEBUG, "not need gc now", K(last_gc_succ_time_));
  } else {
    tablet_gc_window_ = 2
                        * (max_task_interval > ObDupTabletScanTask::DUP_TABLET_SCAN_INTERVAL
                               ? max_task_interval
                               : ObDupTabletScanTask::DUP_TABLET_SCAN_INTERVAL);

    int64_t gc_timeout = 0;
    if ((gc_ts - last_gc_succ_time_) > GC_DUP_TABLETS_FAILED_TIMEOUT && last_gc_succ_time_ != 0) {
      gc_timeout = INT64_MAX;
      DUP_TABLE_LOG(WARN, "gc failed too much times, this time should not break", );
    } else {
      gc_timeout = GC_TIMEOUT;
    }

    int64_t gc_start_time = ObTimeUtility::fast_current_time();

    /**
     * Gc readable tablet set
     * */
    DupTabletChangeMap *old_tablet_set = nullptr;
    DupTabletCommonHeader old_tablet_common_header;
    old_tablet_common_header.set_old();
    old_tablet_common_header.set_invalid_unique_id();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_target_tablet_set_(old_tablet_common_header, old_tablet_set))) {
      DUP_TABLE_LOG(WARN, "get old tablet set failed, need skip gc readable tablets", K(ret),
                    KPC(old_tablet_set));
      ret = OB_SUCCESS;
    } else if (!old_tablet_set->get_change_status()->is_modifiable()) {
      ret = OB_EAGAIN; // should not update gc succ time to increase gc freq
      DUP_TABLE_LOG(INFO, "old tablet set can not be modified, skip gc readable tablets", K(ret),
                    KPC(old_tablet_set));
    } else {
      DLIST_FOREACH(readable_tablets_ptr, readable_tablets_list_)
      {
        GcDiscardedDupTabletHandler readable_gc_handler(
            gc_ts, tablet_gc_window_, readable_tablets_ptr->get_common_header(), *old_tablet_set);
        if (OB_FAIL(hash_for_each_remove_with_timeout(tmp_id, *readable_tablets_ptr,
                                                      readable_gc_handler, gc_timeout))) {
          DUP_TABLE_LOG(WARN, "remove readable tablets failed", KR(ret),
                        K(readable_gc_handler.get_gc_tablet_cnt()));
        } else if (OB_FAIL(readable_gc_handler.get_ret())) {
          // if fail, not update last gc succ time to increase gc freqency
          DUP_TABLE_LOG(WARN, "remove readable tablets failed, may need retry", KR(ret),
                        K(readable_gc_handler.get_gc_tablet_cnt()));
        }
        gc_tablet_cnt += readable_gc_handler.get_gc_tablet_cnt();
      }
    }

    /**
     * Gc new tablet set
     * */
    DupTabletChangeMap *changing_new_set = nullptr;
    DupTabletCommonHeader new_tablet_common_header;
    new_tablet_common_header.set_new();
    new_tablet_common_header.set_invalid_unique_id();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_target_tablet_set_(new_tablet_common_header, changing_new_set))) {
      DUP_TABLE_LOG(WARN, "get changing new set failed", K(ret), KPC(changing_new_set));
    } else if (OB_NOT_NULL(changing_new_set)) {
      if (changing_new_set->empty()) {
        // do nothing
        DUP_TABLE_LOG(DEBUG, "changing_new_set is empty, not need gc", K(ret));
      } else {
        GcDiscardedDupTabletHandler new_gc_handler(
            gc_ts, tablet_gc_window_, changing_new_set->get_common_header(), *old_tablet_set);

        if (OB_FAIL(hash_for_each_remove_with_timeout(tmp_id, *changing_new_set, new_gc_handler,
                                                      gc_timeout))) {
          DUP_TABLE_LOG(WARN, "remove new tablets failed", KR(ret));
        }
        // collect gc in new tablets count
        gc_tablet_cnt += new_gc_handler.get_gc_tablet_cnt();
      }
    }
    // collect gc readable tablet
    if (OB_SUCC(ret)) {
      last_gc_succ_time_ = gc_ts;
    } else if (OB_TIMEOUT == ret) {
      DUP_TABLE_LOG(WARN, "gc tablets failed, scan all tablets set cost too much time", K(ret),
                    K(gc_start_time), K(gc_timeout), K(gc_tablet_cnt));
    } else if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }

    if (0 != gc_tablet_cnt) {
      DUP_TABLE_LOG(INFO, "finish gc dup tablet on time", K(ret), KPC(changing_new_set_),
                    KPC(removing_old_set_), K(readable_tablets_list_.get_size()), K(gc_tablet_cnt));
    }
  }

  return ret;
}

int ObLSDupTabletsMgr::refresh_dup_tablet(const common::ObTabletID &tablet_id,
                                          bool is_dup_table,
                                          int64_t refresh_time)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(dup_tablets_lock_);

  if (!tablet_id.is_valid() || ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret));
  } else if (!is_master()) {
    ret = OB_NOT_MASTER;
    // DUP_TABLE_LOG(INFO, "dup_table ls is not master", K(ret),
    // K(dup_ls_handle_ptr_->get_ls_id()));
  } else if (is_dup_table) {
    // exist in readable_tablets_、new_tablets_ => do nothing
    // exist in old_tablets_ => remove from old_tablets_ and insert into readable_tablets_
    // not exist => insert into new_tablets_

    if (OB_FAIL(discover_dup_tablet_(tablet_id, refresh_time))) {
      DUP_TABLE_LOG(WARN, "discover a dup tablet failed", K(tablet_id), K(refresh_time));
    }

  } else {
    if (OB_FAIL(lose_dup_tablet_(tablet_id))) {
      DUP_TABLE_LOG(WARN, "a dup tablet lose dup attr failed", K(tablet_id));
    }
  }

  return ret;
}

int ObLSDupTabletsMgr::prepare_serialize(int64_t &max_ser_size,
                                         DupTabletSetIDArray &unique_id_array,
                                         const int64_t max_log_buf_len)
{
  int ret = OB_SUCCESS;

  SpinRLockGuard guard(dup_tablets_lock_);

  // max_ser_size = 0;
  unique_id_array.reuse();

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(changing_new_set_)) {
      // do nothing
    } else if (changing_new_set_->empty()) {
      // empty change map not need add to need confirm and ser
      DUP_TABLE_LOG(DEBUG, "changing_new_set_ is empty", K(ret), K(changing_new_set_->empty()));
    } else if (OB_FAIL(changing_new_set_->get_change_status()->prepare_serialize())) {
      DUP_TABLE_LOG(WARN, "changing new set prepare serialize failed", K(ret));
    } else if (false == need_confirm_new_queue_.add_last(changing_new_set_)) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "push back change_new_set_ failed", K(ret));
    } else if (OB_FALSE_IT(changing_new_set_ = nullptr)) {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    bool can_be_confirmed = true;
    DLIST_FOREACH(cur_map, need_confirm_new_queue_)
    {
      if (OB_ISNULL(cur_map->get_change_status())) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(ERROR, "unexpected tablet set type", K(ret), KPC(cur_map));
      } else if (!cur_map->get_change_status()->need_log()) {
        DUP_TABLE_LOG(INFO, "no need serialize need_confirm_set in log", K(ret), KPC(cur_map));
      } else if (OB_FAIL(cal_single_set_max_ser_size_(cur_map, max_ser_size, unique_id_array))) {
        DUP_TABLE_LOG(WARN, "cal new set max ser_size failed", K(ret));
      } else {
        int64_t tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(cur_map->get_change_status()->try_set_confirmed(can_be_confirmed))) {
          if (tmp_ret != OB_EAGAIN) {
            ret = tmp_ret;
            DUP_TABLE_LOG(WARN, "try to set confirmed error", K(ret), K(cur_map));
          } else {
            can_be_confirmed = false;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (max_ser_size > max_log_buf_len) {
      ret = OB_LOG_TOO_LARGE;
      DUP_TABLE_LOG(INFO, "Too large tablet log, we will not serialize old or readable tablets",
                    K(ret), K(ls_id_), K(max_ser_size), K(max_log_buf_len),
                    K(unique_id_array.count()), K(unique_id_array));
    }
  }

  if (OB_SUCC(ret)) {
    DupTabletChangeMap *old_tablet_set = nullptr;
    DupTabletCommonHeader old_tablets_header;
    old_tablets_header.set_old();
    old_tablets_header.set_invalid_unique_id();
    if (OB_FAIL(get_target_tablet_set_(old_tablets_header, old_tablet_set))) {
      DUP_TABLE_LOG(WARN, "get old tablets failed", K(ret));
    } else if (old_tablet_set->empty()) {
      // do nothing
    } else if (!old_tablet_set->get_change_status()->need_log()) {
      DUP_TABLE_LOG(INFO, "no need serialize old tablets in log", K(ret), KPC(old_tablet_set));
    } else if (OB_FAIL(
                   cal_single_set_max_ser_size_(old_tablet_set, max_ser_size, unique_id_array))) {
      DUP_TABLE_LOG(WARN, "cal old set max ser_size failed", K(ret));
    } else if (OB_FAIL(old_tablet_set->get_change_status()->prepare_serialize())) {
      DUP_TABLE_LOG(WARN, "old set prepare serialize failed", K(ret));
    } else {
      // try confirm old tablets
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(old_tablet_set->get_change_status()->try_set_confirmed(true))) {
        if (tmp_ret != OB_EAGAIN) {
          ret = tmp_ret;
          DUP_TABLE_LOG(WARN, "try to set confirmed error", K(ret), K(old_tablet_set));
        }
      }
    }
  }

  // TODO serialize readable tablets
  if (OB_SUCC(ret)) {
    DLIST_FOREACH(readable_ptr, readable_tablets_list_)
    {
      if (OB_FAIL(cal_single_set_max_ser_size_(readable_ptr, max_ser_size, unique_id_array))) {
        DUP_TABLE_LOG(WARN, "cal readable set max ser_size failed", K(ret));
      }
    }
  }

  if (OB_LOG_TOO_LARGE == ret) {
    ret = OB_SUCCESS;
  }
  DUP_TABLE_LOG(DEBUG, "finish prepare ser", K(ret), K(max_ser_size), K(unique_id_array));
  return ret;
}

int ObLSDupTabletsMgr::serialize_tablet_log(const DupTabletSetIDArray &unique_id_array,
                                            char *buf,
                                            const int64_t buf_len,
                                            int64_t &pos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  int64_t tmp_pos = pos;

  SpinRLockGuard guard(dup_tablets_lock_);

  if (OB_ISNULL(buf) || buf_len <= 0 || pos <= 0) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (unique_id_array.count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    DUP_TABLE_LOG(INFO, "no need to serialize tablet log", K(ret), K(unique_id_array.count()));
  } else {
    for (int i = 0; i < unique_id_array.count() && OB_SUCC(ret); i++) {
      DupTabletChangeMap *tablet_set_ptr = nullptr;
      const DupTabletCommonHeader &seralize_common_header = unique_id_array.at(i);
      if (OB_FAIL(get_target_tablet_set_(seralize_common_header, tablet_set_ptr))) {
        DUP_TABLE_LOG(WARN, "get target tablet set failed", K(ret), K(i), KPC(tablet_set_ptr));
      } else if (OB_FAIL(seralize_common_header.serialize(buf, buf_len, tmp_pos))) {
        DUP_TABLE_LOG(WARN, "serialize common header failed", K(ret), K(seralize_common_header));
      } else if (seralize_common_header.is_readable_set()) {
        // DupTabletLog readable_log(tablet_set_ptr);
        // if (OB_FAIL(readable_log.serialize(buf, buf_len, tmp_pos))) {
        //   DUP_TABLE_LOG(WARN, "serialize readable log failed", K(ret));
        // }
        DupTabletCommonLogBody readable_log(*tablet_set_ptr);
        if (OB_FAIL(readable_log.serialize(buf, buf_len, tmp_pos))) {
          DUP_TABLE_LOG(WARN, "serialize readable tablet log failed", K(ret), KPC(tablet_set_ptr));
        }
      } else if (seralize_common_header.is_new_set() || seralize_common_header.is_old_set()) {
        if (seralize_common_header.no_specail_op()) {
          DupTabletChangeLogBody change_log(*tablet_set_ptr);
          if (OB_FAIL(change_log.serialize(buf, buf_len, tmp_pos))) {
            DUP_TABLE_LOG(WARN, "serialize new/old tablet log failed", K(ret), KPC(tablet_set_ptr));
          }

        } else {
          DupTabletSpecialOpArg *sp_op_arg = nullptr;
          uint64_t uid = seralize_common_header.get_unique_id();
          if (OB_ISNULL(sp_op_arg = op_arg_map_.get(uid))) {
            ret = OB_ERR_UNEXPECTED;
            DUP_TABLE_LOG(WARN, "get special op arg failed", K(ret), KPC(sp_op_arg));
          } else {
            DupTabletSpecialOpLogBody sp_op_log(*tablet_set_ptr, *sp_op_arg);
            if (OB_FAIL(sp_op_log.serialize(buf, buf_len, tmp_pos))) {
              DUP_TABLE_LOG(WARN, "serialize special op log failed", K(ret), KPC(tablet_set_ptr),
                            KPC(sp_op_arg));
            }
          }
        }
        // DupTabletChangeLogTail log_tail(tablet_set_ptr->get_change_status()->readable_version_,
        //                                 tablet_set_ptr->get_change_status()->has_confirmed());
        // DupTabletSpecialOpArg sp_op_arg;
        // if (!tablet_set_ptr->get_common_header().no_specail_op()) {
        //   if (OB_FAIL(op_arg_map_.get_refactored(
        //           tablet_set_ptr->get_common_header().get_unique_id(), sp_op_arg))) {
        //     DUP_TABLE_LOG(WARN, "get special op failed", K(ret), KPC(tablet_set_ptr),
        //                   K(sp_op_arg));
        //   }
        // }
        //
        // DupTabletLog change_log(log_tail, tablet_set_ptr, &sp_op_arg);
        // if (OB_FAIL(ret)) {
        // } else if (OB_FAIL(change_log.serialize(buf, buf_len, tmp_pos))) {
        //   DUP_TABLE_LOG(WARN, "serialize tablet change log failed", K(ret));
        // }
      }
    }

    if (OB_SUCC(ret)) {
      pos = tmp_pos;
    }
  }

  DUP_TABLE_LOG(DEBUG, "after ser log all", K(ret), K(buf_len), K(pos), K(tmp_pos));
  return ret;
}

int ObLSDupTabletsMgr::deserialize_tablet_log(DupTabletSetIDArray &unique_id_array,
                                              const char *buf,
                                              const int64_t data_len,
                                              int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = pos;

  unique_id_array.reset();

  SpinWLockGuard guard(dup_tablets_lock_);
  if (OB_ISNULL(buf) || data_len <= 0 || pos <= 0) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    // DupTabletLog tablet_log;
    DupTabletCommonHeader deser_common_header;
    while (OB_SUCC(ret) && tmp_pos < data_len) {
      deser_common_header.reset();
      DupTabletChangeMap *tablet_set_ptr = nullptr;
      bool construct_from_free = false;
      share::SCN readable_version;
      bool deser_has_confirmed = false;

      /*
       * 1. deserialize tablet set common header
       * 2. find a target tablet set by common header
       * */
      if (OB_FAIL(deser_common_header.deserialize(buf, data_len, tmp_pos))) {
        DUP_TABLE_LOG(WARN, "deserialize common header failed", K(ret), K(tmp_pos), K(data_len));
      } else if (OB_FAIL(get_target_tablet_set_(deser_common_header, tablet_set_ptr,
                                                true /*construct_target_set*/))) {
        DUP_TABLE_LOG(WARN, "get target tablet set failed", K(ret), K(deser_common_header),
                      KPC(tablet_set_ptr));
      } else if (tablet_set_ptr->get_common_header().is_free()) {
        tablet_set_ptr->get_common_header().copy_tablet_set_type(deser_common_header);
        construct_from_free = true;
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(unique_id_array.push_back(deser_common_header))) {
        DUP_TABLE_LOG(WARN, "push back unique_id into logging array failed", K(ret),
                      K(deser_common_header));
      } else if (deser_common_header.is_free()) {
        /*
         * free a empty readable tablet set
         * */
        if (!tablet_set_ptr->get_common_header().is_readable_set()) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "can not free a tablet_set in changing", K(ret));
        } else if (OB_FAIL(return_tablet_set(tablet_set_ptr))) {
          DUP_TABLE_LOG(WARN, "free a readable set because of compact", K(ret),
                        KPC(tablet_set_ptr));
        }
        DUP_TABLE_LOG(INFO, "deserialize a free tablet set", K(ret), K(tablet_set_ptr));
      } else if (deser_common_header.is_readable_set()) {
        /*
         * deserialize readable tablet set
         * */
        if (construct_from_free) {
          if (false == readable_tablets_list_.add_last(tablet_set_ptr)) {
            if (OB_FAIL(return_tablet_set(tablet_set_ptr))) {
              DUP_TABLE_LOG(WARN, "return tablet set failed", K(ret), KPC(tablet_set_ptr));
            }
            // rewrite ret code
            ret = OB_ERR_UNEXPECTED;
            DUP_TABLE_LOG(WARN, "push back into readable_tablets_list_ failed", K(ret),
                          KPC(tablet_set_ptr));
          }
        }
        if (OB_SUCC(ret)) {
          DupTabletCommonLogBody readable_log_body(*tablet_set_ptr);
          if (OB_FAIL(readable_log_body.deserialize(buf, data_len, tmp_pos))) {
            DUP_TABLE_LOG(WARN, "deserialize dup tablet readable log failed", K(ret));
          }
        }
      } else {
        // DUP_TABLE_LOG(INFO, "deser a change set", K(ret), K(tablet_log.get_common_header()),
        //               KPC(tablet_set_ptr));
        DupTabletSpecialOpArg tmp_op_arg;
        if (construct_from_free && deser_common_header.is_new_set()
            && (false == need_confirm_new_queue_.add_last(tablet_set_ptr))) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "push back into need_confirm_new_queue_ failed", K(ret),
                        KPC(tablet_set_ptr));
          return_tablet_set(tablet_set_ptr);
        } else if (deser_common_header.is_old_set() && removing_old_set_ != tablet_set_ptr) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "unexpected old tablets set ptr", K(ret), KPC(tablet_set_ptr),
                        KPC(removing_old_set_));
        } else {
          if (deser_common_header.no_specail_op()) {
            /*
             * deserialize new/old tablet set without special op
             * */
            DupTabletChangeLogBody change_log_body(*tablet_set_ptr);
            if (OB_FAIL(change_log_body.deserialize(buf, data_len, tmp_pos))) {
              DUP_TABLE_LOG(WARN, "deserialize new/old tablet log failed", K(ret),
                            K(change_log_body), KPC(tablet_set_ptr));
            } else {
              readable_version = change_log_body.get_change_tail().readable_version_;
              deser_has_confirmed = change_log_body.get_change_tail().has_confirmed_;
            }
          } else {
            /*
             * deserialize special op arg
             * */
            DupTabletSpecialOpArg tmp_op_arg;
            DupTabletSpecialOpLogBody sp_op_log_body(*tablet_set_ptr, tmp_op_arg);
            if (OB_FAIL(sp_op_log_body.deserialize(buf, data_len, tmp_pos))) {
              DUP_TABLE_LOG(WARN, "deserialize new/old tablet log failed", K(ret),
                            K(sp_op_log_body), KPC(tablet_set_ptr));
            } else if (OB_FAIL(op_arg_map_.set_refactored(deser_common_header.get_unique_id(),
                                                          tmp_op_arg, 1))) {
              DUP_TABLE_LOG(WARN, "insert into op_arg_map_ failed", K(ret), K(deser_common_header),
                            K(tmp_op_arg));
            } else {
              readable_version = sp_op_log_body.get_change_tail().readable_version_;
              deser_has_confirmed = sp_op_log_body.get_change_tail().has_confirmed_;
            }
          }
        }

        /*
         * set tablet set state as tablet_log_submitted
         * */
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(tablet_set_ptr->get_change_status()->prepare_serialize())) {
          DUP_TABLE_LOG(WARN, "prepare serialize failed", K(ret), KPC(tablet_set_ptr));
          // } else if (tablet_set_ptr->get_change_status()->is_change_logging()
          //            && OB_FAIL(tablet_set_ptr->get_change_status()->tablet_change_log_submitted(
          //                share::SCN::min_scn(), true /*submit_result*/))) {
          //   DUP_TABLE_LOG(WARN, "set tablet change log submitted failed", K(ret),
          //                 KPC(tablet_set_ptr));
          // } else if (tablet_set_ptr->get_change_status()->is_change_logging()
          //            && OB_FAIL(tablet_set_ptr->get_change_status()->prepare_confirm(
          //                share::SCN::min_scn(), true /*sync _result*/))) {
          //   DUP_TABLE_LOG(WARN, "prepare confirm tablet_set failed", K(ret),
          //   KPC(tablet_set_ptr));
          //
          /* Step 2 : as try_to_confirm_tablets*/
        } else if (OB_FAIL(tablet_set_ptr->get_change_status()->push_need_confirm_scn(
                       readable_version))) {
          DUP_TABLE_LOG(WARN, "set need_confirm_scn_ failed", K(ret), K(readable_version),
                        KPC(tablet_set_ptr));
        } else if (OB_FAIL(
                       tablet_set_ptr->get_change_status()->push_readable_scn(readable_version))) {
          DUP_TABLE_LOG(WARN, "set readable version failed", K(ret), K(readable_version),
                        KPC(tablet_set_ptr));
        } else if (tablet_set_ptr->get_change_status()->is_confirming()
                   && OB_FAIL(tablet_set_ptr->get_change_status()->try_set_confirmed(
                       deser_has_confirmed))) {
          DUP_TABLE_LOG(WARN, "replay confirmed flag failed", K(ret), K(deser_has_confirmed),
                        KPC(tablet_set_ptr));
        }
      }
      // DUP_TABLE_LOG(INFO, "deser tablet log for one set", K(ret),
      // K(tablet_log.get_common_header()),
      //               KPC(tablet_set_ptr), K(need_confirm_new_queue_.get_size()));
    }
  }

  DUP_TABLE_LOG(DEBUG, "after deser tablet log", K(ret), K(tmp_pos), K(data_len), K(pos));

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }
  // TODO  rollback if replay failed
  return ret;
}

int ObLSDupTabletsMgr::tablet_log_submitted(const bool submit_result,
                                            const share::SCN &tablet_log_scn,
                                            const bool for_replay,
                                            const DupTabletSetIDArray &unique_id_array)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(dup_tablets_lock_);

  UNUSED(for_replay);

  for (int i = 0; OB_SUCC(ret) && i < unique_id_array.count(); i++) {
    const DupTabletCommonHeader logging_common_header = unique_id_array[i];
    DupTabletChangeMap *logging_tablet_set = nullptr;
    if (!logging_common_header.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      DUP_TABLE_LOG(WARN, "invalid common header", K(ret), K(logging_common_header));
    } else if (logging_common_header.is_readable_set()) {
      // do nothing
    } else if (OB_FAIL(get_target_tablet_set_(logging_common_header, logging_tablet_set))) {
      DUP_TABLE_LOG(WARN, "get logging tablet set failed", K(ret), KPC(logging_tablet_set),
                    K(logging_common_header));
    } else if (logging_tablet_set->get_change_status()->is_change_logging()
               && OB_FAIL(logging_tablet_set->get_change_status()->tablet_change_log_submitted(
                   tablet_log_scn, submit_result))) {
      DUP_TABLE_LOG(WARN, "modify tablet change status failed", K(ret), KPC(logging_tablet_set));
    }
  }

  return ret;
}

int ObLSDupTabletsMgr::tablet_log_synced(const bool sync_result,
                                         const share::SCN &scn,
                                         const bool for_replay,
                                         const DupTabletSetIDArray &unique_id_array,
                                         bool &modify_readable_set)
{
  int ret = OB_SUCCESS;

  bool clean_readable = false;

  modify_readable_set = false;
  SpinWLockGuard guard(dup_tablets_lock_);

  for (int i = 0; OB_SUCC(ret) && i < unique_id_array.count(); i++) {
    const DupTabletCommonHeader logging_common_header = unique_id_array[i];
    DupTabletChangeMap *logging_tablet_set = nullptr;

    if (!logging_common_header.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      DUP_TABLE_LOG(WARN, "invalid common header", K(ret), K(logging_common_header));
    } else if (OB_FAIL(get_target_tablet_set_(logging_common_header, logging_tablet_set))) {
      if (clean_readable && OB_ENTRY_NOT_EXIST == ret && logging_common_header.is_readable_set()) {
        DUP_TABLE_LOG(INFO, "the tablet set has been removed in this log", K(ret),
                      KPC(logging_tablet_set), K(logging_common_header), K(scn), K(sync_result));
        ret = OB_SUCCESS;
      } else {
        DUP_TABLE_LOG(WARN, "get target tablet set failed", K(ret), KPC(logging_tablet_set),
                      K(logging_common_header), K(scn), K(sync_result));
      }
    } else if (logging_common_header.is_readable_set()) {
      // try return empty readable set
      bool need_remove = false;
      if (OB_FAIL(check_and_recycle_empty_readable_set(logging_tablet_set, need_remove))) {
        DUP_TABLE_LOG(WARN, "try return empty readable tablet set", K(ret),
                      KPC(logging_tablet_set));
      }
      if (need_remove) {
        modify_readable_set = true;
      }
    } else if (logging_tablet_set->get_change_status()->is_change_logging()) {
      if (OB_SUCC(ret) && sync_result) {
        if (OB_FAIL(try_exec_special_op_(logging_tablet_set, scn, for_replay))) {
          DUP_TABLE_LOG(WARN, "try to execute special opertion for dup tablet set", K(ret),
                        KPC(logging_tablet_set));
        } else if(logging_common_header.need_clean_all_readable_set())
        {
         clean_readable = true;
        }

        if (logging_common_header.is_old_set() && !logging_tablet_set->empty()) {
          modify_readable_set = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(logging_tablet_set->get_change_status()->prepare_confirm(scn, sync_result))) {
          DUP_TABLE_LOG(WARN, "modify tablet change status failed", K(ret),
                        KPC(logging_tablet_set));
        }
      }
    } else if (logging_tablet_set->get_change_status()->has_confirmed()) {
      if (OB_SUCC(ret) && sync_result) {
        // if old is confirmed, clear it
        if (logging_common_header.is_old_set()) {
          return_tablet_set(logging_tablet_set);
          // move need_confirm_queue to readable
        } else if (OB_FAIL(merge_into_readable_tablets_(logging_tablet_set, for_replay))) {
          DUP_TABLE_LOG(WARN, "merge into readable tablet set failed", K(ret));
        } else {
          modify_readable_set = true;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !for_replay && (!is_master() || sync_result == false)) {
    if (OB_FAIL(clean_unlog_tablets_())) {
      DUP_TABLE_LOG(WARN, "clean unlog tablets failed", K(ret), K(ls_id_), K(for_replay),
                    K(sync_result), K(is_master()), K(unique_id_array));
    }
  }

  if (unique_id_array.count() > 0) {
    DUP_TABLE_LOG(INFO, "tablet log sync", K(ret), K(sync_result), K(for_replay), K(is_master()),
                  K(unique_id_array), K(scn), K(modify_readable_set));
  }

  return ret;
}

int ObLSDupTabletsMgr::cal_single_set_max_ser_size_(DupTabletChangeMap *hash_map,
                                                    int64_t &max_ser_size,
                                                    DupTabletSetIDArray &unique_id_array)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(hash_map)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid hash map", K(ret));
  } else {
    int64_t tmp_ser_size = hash_map->get_serialize_size();
    if (hash_map->get_common_header().is_readable_set()) {
      DupTabletCommonLogBody common_log_body(*hash_map);
      tmp_ser_size += common_log_body.get_serialize_size();
    } else if (hash_map->get_common_header().no_specail_op()) {
      DupTabletChangeLogBody change_log_body(*hash_map);
      tmp_ser_size += change_log_body.get_serialize_size();
    } else {
      uint64_t uid = hash_map->get_common_header().get_unique_id();
      DupTabletSpecialOpArg *op_arg = op_arg_map_.get(uid);
      DupTabletSpecialOpLogBody sp_log_body(*hash_map, *op_arg);

      if (OB_ISNULL(op_arg)) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(ERROR, "invalid special op arg", K(ret), KPC(op_arg), KPC(hash_map));
      } else {
        tmp_ser_size += sp_log_body.get_serialize_size();
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(unique_id_array.push_back(hash_map->get_common_header()))) {
      DUP_TABLE_LOG(WARN, "push back unique_id array failed", K(ret));
    } else {
      max_ser_size += tmp_ser_size;
    }
  }

  return ret;
}

int ObLSDupTabletsMgr::merge_into_readable_tablets_(DupTabletChangeMap *change_map_ptr,
                                                    const bool for_replay)
{
  int ret = OB_SUCCESS;

  // merge a need confirm set into readable list
  if (OB_ISNULL(change_map_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid hash map ptr", K(ret), KP(change_map_ptr));
  } else if (!for_replay && change_map_ptr != need_confirm_new_queue_.get_first()) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "we must merge the first set into readable tablets", K(ret),
                  KPC(change_map_ptr), KPC(need_confirm_new_queue_.get_first()));
  } else if (OB_ISNULL(need_confirm_new_queue_.remove(change_map_ptr))) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "remove from need_confirm_new_queue_ failed", K(ret), KPC(change_map_ptr));
  } else if (false == (readable_tablets_list_.add_last(change_map_ptr))) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "insert into readable_tablets_list_ failed", K(ret), KPC(change_map_ptr));
  } else if (OB_FALSE_IT(change_map_ptr->get_common_header().set_readable())) {
    // do nothing
  }

  // an empty set first merge into readable list, return it to free pool
  if (OB_SUCC(ret)) {
    bool need_remove = false;
    if (OB_FAIL(check_and_recycle_empty_readable_set(change_map_ptr, need_remove))) {
      DUP_TABLE_LOG(WARN, "return empty readable failed", K(ret), KPC(change_map_ptr));
    }
  }

  DUP_TABLE_LOG(DEBUG, "merge into readable", K(ret), KPC(change_map_ptr),
                K(need_confirm_new_queue_.get_size()));
  return ret;
}

int64_t ObLSDupTabletsMgr::get_dup_tablet_count()
{
  SpinRLockGuard guard(dup_tablets_lock_);
  int64_t total_size = 0;

  if (OB_NOT_NULL(changing_new_set_)) {
    total_size += changing_new_set_->size();
  }

  DLIST_FOREACH_X(need_confirm_new_set, need_confirm_new_queue_, true)
  {
    total_size += need_confirm_new_set->size();
  }

  if (OB_NOT_NULL(removing_old_set_)) {
    total_size += removing_old_set_->size();
  }

  DLIST_FOREACH_X(readable_set_ptr, readable_tablets_list_, true)
  {
    total_size += readable_set_ptr->size();
  }

  // total_size += readable_tablets_.size();
  DUP_TABLE_LOG(DEBUG, "has dup tablet", K(total_size));

  return total_size;
}

bool ObLSDupTabletsMgr::has_dup_tablet() { return 0 < get_dup_tablet_count(); }

int64_t ObLSDupTabletsMgr::get_readable_tablet_set_count()
{
  int64_t cnt = 0;

  SpinRLockGuard guard(dup_tablets_lock_);
  cnt = readable_tablets_list_.get_size();

  return cnt;
}

int64_t ObLSDupTabletsMgr::get_all_tablet_set_count()
{
  int64_t cnt = 0;
  SpinRLockGuard guard(dup_tablets_lock_);

  if (OB_NOT_NULL(changing_new_set_)) {
    cnt += 1;
  }

  cnt += need_confirm_new_queue_.get_size();
  cnt += readable_tablets_list_.get_size();

  if (OB_NOT_NULL(removing_old_set_)) {
    cnt += 1;
  }

  return cnt;
}

int ObLSDupTabletsMgr::leader_takeover(const bool is_resume,
                                       const bool recover_all_readable_from_ckpt)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(dup_tablets_lock_);

  if (!is_resume) {
    if (OB_FAIL(construct_clean_confirming_set_task_())) {
      DUP_TABLE_LOG(WARN, "clean new/old tablets set failed", K(ret),
                    K(need_confirm_new_queue_.get_size()), KPC(removing_old_set_),
                    KPC(changing_new_set_));
    } else if (!recover_all_readable_from_ckpt /*incomplete readable set*/) {
      if (OB_FAIL(construct_clean_all_readable_set_task_())) {
        DUP_TABLE_LOG(WARN, "construct clean all readable set task failed", K(ret),
                      K(need_confirm_new_queue_.get_size()));
      }
    }
  }

  // TODO make replay_active_tx_count as the trx_ref of first empty need_confirm_tablet_set
  // TODO check the completeness of readable_tablets_set

  ATOMIC_STORE(&is_master_, true);
  return ret;
}

int ObLSDupTabletsMgr::leader_revoke(const bool is_logging)
{

  int ret = OB_SUCCESS;

  SpinWLockGuard guard(dup_tablets_lock_);

  if (!is_logging) {
    // clean unreadable tablets to make replay from clean sets.
    if (OB_FAIL(clean_unlog_tablets_())) {
      DUP_TABLE_LOG(WARN, "clean unlog tablets failed", K(ret), K(ls_id_),
                    K(need_confirm_new_queue_.get_size()), KPC(removing_old_set_),
                    KPC(changing_new_set_));
    }
  }

  ATOMIC_STORE(&is_master_, false);
  return ret;
}

int ObLSDupTabletsMgr::try_to_confirm_tablets(
    const share::SCN &lease_valid_follower_max_replayed_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SpinRLockGuard guard(dup_tablets_lock_);
  if (!lease_valid_follower_max_replayed_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid confirm ts", KR(ret), K(lease_valid_follower_max_replayed_scn));
  } else {
    // comfirm need_confirm_new_queue
    if (need_confirm_new_queue_.is_empty()) {
      DUP_TABLE_LOG(DEBUG, "need_confirm_new_queue_ is empty", KR(ret),
                    K(need_confirm_new_queue_.get_size()), K(readable_tablets_list_.get_size()));
    } else {
      DLIST_FOREACH_X(node, need_confirm_new_queue_, OB_SUCC(ret))
      {
        if (node->get_change_status()->is_confirming()) {
          // update readable scn
          const share::SCN readable_scn = SCN::min(lease_valid_follower_max_replayed_scn,
                                                   node->get_change_status()->need_confirm_scn_);
          if (OB_FAIL(node->get_change_status()->push_readable_scn(readable_scn))) {
            DUP_TABLE_LOG(WARN, "fail to confirm succ in this tablets set", K(ret),
                          K(lease_valid_follower_max_replayed_scn), KPC(node), K(readable_scn));
            // @input param can_be_confirmed is true and for_replay is false
            // } else if (OB_FAIL(node->get_change_status()->set_confirmed(true))) {
            //   if (OB_EAGAIN != ret) {
            //     DUP_TABLE_LOG(WARN, "fail to set confimed, may need retry", K(ret),
            //                   K(lease_valid_follower_max_replayed_scn), KPC(node),
            //                   K(readable_scn));
            //   } else {
            //     ret = OB_SUCCESS;
            //   }
          }
        }
      }
    }
    // confirm old tablets
    if (OB_SUCC(ret)) {
      // update old tablets readable_version for confirm, though readable_version not used
      if (removing_old_set_->get_change_status()->is_confirming()) {
        const share::SCN readable_scn =
            SCN::min(lease_valid_follower_max_replayed_scn,
                     removing_old_set_->get_change_status()->need_confirm_scn_);
        if (OB_FAIL(removing_old_set_->get_change_status()->push_readable_scn(readable_scn))) {
          DUP_TABLE_LOG(WARN, "fail to confirm old_tablets succ", K(ret),
                        K(lease_valid_follower_max_replayed_scn), KPC(removing_old_set_),
                        K(readable_scn));
          // @input param can_be_confirmed is true and for_replay is false
          // } else if (OB_FAIL(removing_old_set_->get_change_status()->set_confirmed(true))) {
          //   if (OB_EAGAIN != ret) {
          //     DUP_TABLE_LOG(WARN, "fail to set old_tablets confimed, may need retry", K(ret),
          //                   K(lease_valid_follower_max_replayed_scn), KPC(removing_old_set_),
          //                   K(readable_scn));
          //   } else {
          //     ret = OB_SUCCESS;
          //   }
        }
      }
    }
  }
  return ret;
}

// bool ObLSDupTabletsMgr::need_log_tablets()
// {
//   bool need_log = false;
//   if (!need_confirm_new_queue_.is_empty()) {
//     DLIST_FOREACH_NORET(node, need_confirm_new_queue_)
//     {
//       if (node->get_change_status()->need_log()) {
//         need_log = true;
//         break;
//       }
//     }
//   }
//   if (false == need_log) {
//     if (!readable_tablets_list_.is_empty()) {
//       need_log = true;
//     } else if (removing_old_set_->get_change_status()->need_log()) {
//       need_log = true;
//     } else if (OB_NOT_NULL(changing_new_set_)) {
//       if (changing_new_set_->get_change_status()->need_log()) {
//         need_log = true;
//       }
//     }
//   }
//   DUP_TABLE_LOG(DEBUG, "need log tablet", K(need_log));
//   return need_log;
// }

void ObLSDupTabletsMgr::print_tablet_diag_info_log(bool is_master)
{
  SpinRLockGuard guard(dup_tablets_lock_);
  int ret = OB_SUCCESS;

  const uint64_t TABLET_PRINT_BUF_LEN =
      DupTableDiagStd::DUP_DIAG_INFO_LOG_BUF_LEN[DupTableDiagStd::TypeIndex::TABLET_INDEX];

  const int64_t tenant_id = MTL_ID();
  const ObLSID ls_id = ls_id_;

  if (OB_ISNULL(tablet_diag_info_log_buf_)) {
    if (OB_ISNULL(tablet_diag_info_log_buf_ =
                      static_cast<char *>(ob_malloc(TABLET_PRINT_BUF_LEN, "DupTableDiag")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _DUP_TABLE_LOG(WARN, "%salloc tablet diag info buf failed, ret=%d, ls_id=%lu",
                     DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
    }
  }

  if (OB_SUCC(ret)) {
    int64_t tablet_diag_pos = 0;
    // new tablet print
    if (OB_SUCC(ret)) {

      if (OB_SUCC(ret) && OB_NOT_NULL(changing_new_set_)) {
        if (OB_FAIL(::oceanbase::common::databuff_printf(
                tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN, tablet_diag_pos,
                "\n%s[%sNew Dup Tablet Set - Changing] unique_id = %lu, tablet_count = %lu",
                DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX,
                changing_new_set_->get_common_header().get_unique_id(),
                changing_new_set_->size()))) {
          _DUP_TABLE_LOG(WARN, "%sprint changing tablet list header failed, ret=%d, ls_id=%lu",
                         DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
        } else {
          DiagInfoGenerator diag_info_gen(tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN,
                                          tablet_diag_pos,
                                          changing_new_set_->get_common_header().get_unique_id());
          if (OB_FAIL(hash_for_each_update(*changing_new_set_, diag_info_gen))) {
            _DUP_TABLE_LOG(WARN, "%sprint changing tablet list failed, ret=%d, ls_id=%lu",
                           DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
          } else {
            tablet_diag_pos = diag_info_gen.get_buf_pos();
          }
        }
      }

      if (OB_SUCC(ret) && !need_confirm_new_queue_.is_empty()) {
        DLIST_FOREACH_X(need_confirm_set, need_confirm_new_queue_, OB_SUCC(ret))
        {
          if (OB_FAIL(::oceanbase::common::databuff_printf(
                  tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN, tablet_diag_pos,
                  "\n%s[%sNew Dup Tablet Set - NeedConfirm] unique_id = %lu, tablet_count = %lu, "
                  "flag "
                  "= %s, change_scn = %s, readable_version = %s, trx_ref_ = %lu",
                  DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX,
                  need_confirm_set->get_common_header().get_unique_id(), need_confirm_set->size(),
                  get_dup_tablet_flag_str(need_confirm_set->get_change_status()->flag_),
                  to_cstring(need_confirm_set->get_change_status()->tablet_change_scn_),
                  to_cstring(need_confirm_set->get_change_status()->readable_version_),
                  need_confirm_set->get_change_status()->trx_ref_))) {

            _DUP_TABLE_LOG(WARN,
                           "%sprint need confirm tablet list header failed, ret=%d, ls_id=%lu",
                           DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
          } else {
            DiagInfoGenerator diag_info_gen(tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN,
                                            tablet_diag_pos,
                                            need_confirm_set->get_common_header().get_unique_id());
            if (OB_FAIL(hash_for_each_update(*need_confirm_set, diag_info_gen))) {
              _DUP_TABLE_LOG(WARN, "%sprint need confirm tablet list failed, ret=%d, ls_id=%lu",
                             DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
            } else {
              tablet_diag_pos = diag_info_gen.get_buf_pos();
            }
          }
        }
      }
    }
    // } else {
    //   // for (int i = 0; i < MAX_NEW_DUP_TABLET_SET_CNT && OB_SUCC(ret); i++) {
    //   DLIST_FOREACH(need_confirm_set, need_confirm_new_queue_) {
    //     if (need_confirm_set->size() > 0) {
    //
    //       if (OB_FAIL(::oceanbase::common::databuff_printf(
    //               tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN, tablet_diag_pos,
    //               "\n%s[%sNew Dup Tablet Set - Replay] unique_id = %lu, tablet_count = %lu, "
    //               "flag "
    //               "= %s, change_scn = %s, readable_version = %s, trx_ref_ = %lu",
    //               DupTableDiagStd::DUP_DIAG_INDENT_SPACE,
    //               DupTableDiagStd::DUP_DIAG_COMMON_PREFIX,
    //               need_confirm_set->get_common_header()->unique_id_,
    //               new_tablets_array_[i].tablet_change_map_.size(),
    //               get_dup_tablet_flag_str(new_tablets_array_[i].change_status_.flag_),
    //               to_cstring(new_tablets_array_[i].change_status_.tablet_change_scn_),
    //               to_cstring(new_tablets_array_[i].change_status_.readable_version_),
    //               new_tablets_array_[i].change_status_.trx_ref_))) {
    //
    //         _DUP_TABLE_LOG(WARN,
    //                        "%sprint replay new tablet list header failed, ret=%d, ls_id=%lu",
    //                        DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
    //       } else {
    //         DiagInfoGenerator diag_info_gen(
    //             tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN, tablet_diag_pos,
    //             new_tablets_array_[i].tablet_change_map_.get_common_header().unique_id_);
    //         if (OB_FAIL(hash_for_each_update(new_tablets_array_[i].tablet_change_map_,
    //                                          diag_info_gen))) {
    //           _DUP_TABLE_LOG(WARN, "%sprint replay new  tablet list failed, ret=%d, ls_id=%lu",
    //                          DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
    //         } else {
    //           tablet_diag_pos = diag_info_gen.get_buf_pos();
    //         }
    //       }
    //     }
    //   }
    // }
    //
    // old tablet print
    if (OB_SUCC(ret) && OB_NOT_NULL(removing_old_set_) && removing_old_set_->size() > 0) {

      if (OB_FAIL(::oceanbase::common::databuff_printf(
              tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN, tablet_diag_pos,
              "\n%s[%sOld Dup Tablet Set] unique_id = %lu, tablet_count = %lu, "
              "flag "
              "= %s, change_scn = %s, readable_version = %s, trx_ref_ = %lu",
              DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX,
              removing_old_set_->get_common_header().get_unique_id(), removing_old_set_->size(),
              get_dup_tablet_flag_str(removing_old_set_->get_change_status()->flag_),
              to_cstring(removing_old_set_->get_change_status()->tablet_change_scn_),
              to_cstring(removing_old_set_->get_change_status()->readable_version_),
              removing_old_set_->get_change_status()->trx_ref_))) {

        _DUP_TABLE_LOG(WARN, "%sprint need confirm tablet list header failed, ret=%d, ls_id=%lu",
                       DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
      } else {
        DiagInfoGenerator diag_info_gen(tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN,
                                        tablet_diag_pos,
                                        removing_old_set_->get_common_header().get_unique_id());
        if (OB_FAIL(hash_for_each_update(*removing_old_set_, diag_info_gen))) {
          _DUP_TABLE_LOG(WARN, "%sprint need confirm tablet list failed, ret=%d, ls_id=%lu",
                         DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
        } else {
          tablet_diag_pos = diag_info_gen.get_buf_pos();
        }
      }
    }

    // readable tablet print
    if (OB_SUCC(ret) && !readable_tablets_list_.is_empty()) {
      DLIST_FOREACH(readable_set_ptr, readable_tablets_list_)
      {
        if (OB_FAIL(::oceanbase::common::databuff_printf(
                tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN, tablet_diag_pos,
                "\n%s[%sReadable Dup Tablet Set] unique_id = %lu, tablet_count = %lu",
                DupTableDiagStd::DUP_DIAG_INDENT_SPACE, DupTableDiagStd::DUP_DIAG_COMMON_PREFIX,
                readable_set_ptr->get_common_header().get_unique_id(), readable_set_ptr->size()))) {

          _DUP_TABLE_LOG(WARN, "%sprint readable tablet list header failed, ret=%d, ls_id=%lu",
                         DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
        } else {
          DiagInfoGenerator diag_info_gen(tablet_diag_info_log_buf_, TABLET_PRINT_BUF_LEN,
                                          tablet_diag_pos,
                                          readable_set_ptr->get_common_header().get_unique_id());
          if (OB_FAIL(hash_for_each_update(*readable_set_ptr, diag_info_gen))) {
            _DUP_TABLE_LOG(WARN, "%sprint readable tablet list failed, ret=%d, ls_id=%lu",
                           DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, ret, ls_id.id());
          } else {
            tablet_diag_pos = diag_info_gen.get_buf_pos();
          }
        }
      }
    }

    tablet_diag_info_log_buf_[MIN(tablet_diag_pos, TABLET_PRINT_BUF_LEN - 1)] = '\0';
    _DUP_TABLE_LOG(INFO,
                   "[%sDup Tablet Info] tenant: %lu, ls: %lu, is_master: %s, "
                   "need_confirm_new_set_count: %u, readable_set_count: %u %s",
                   DupTableDiagStd::DUP_DIAG_COMMON_PREFIX, tenant_id, ls_id.id(),
                   to_cstring(is_master), need_confirm_new_queue_.get_size(),
                   readable_tablets_list_.get_size(), tablet_diag_info_log_buf_);
  }
}

int ObLSDupTabletsMgr::lose_dup_tablet_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  /**
   * no need update schema ts
   *
   * HASH_NOT_EXIST: no need move
   * In old : no need move
   * In readable: move into old
   * In changing_new: remove from new set
   * */
  DupTabletInfo tmp_info;

  DupTabletCommonHeader changing_new_header;
  changing_new_header.set_invalid_unique_id();
  changing_new_header.set_new();
  DupTabletChangeMap *changing_new_map = nullptr;
  if (OB_FAIL(get_target_tablet_set_(changing_new_header, changing_new_map))) {
    DUP_TABLE_LOG(WARN, "get changing new set failed", K(ret));
    if (ret == OB_EAGAIN) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(changing_new_map)) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "unexpected changing new map", K(ret));
  } else if (OB_SUCC(changing_new_map->get_refactored(tablet_id, tmp_info))) {
    if (OB_FAIL(changing_new_map->erase_refactored(tablet_id))) {
      DUP_TABLE_LOG(WARN, "remove from changing_new_set_ failed", K(ret), K(tablet_id));
    }
  } else if (ret != OB_HASH_NOT_EXIST) {
    DUP_TABLE_LOG(WARN, "get dup table status from new_tablets_ failed", K(ret));
  } else if (ret == OB_HASH_NOT_EXIST) {
    ret = OB_SUCCESS;
  }

  DupTabletCommonHeader old_set_header;
  old_set_header.set_invalid_unique_id();
  old_set_header.set_old();
  DupTabletChangeMap *old_tablet_set = nullptr;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(get_target_tablet_set_(old_set_header, old_tablet_set))) {
    DUP_TABLE_LOG(WARN, "get old tablets set failed", K(ret));
  } else if (!old_tablet_set->get_change_status()->is_modifiable()) {
    ret = OB_SUCCESS;
    DUP_TABLE_LOG(DEBUG, "old tablet set can not be modified", K(ret), K(tablet_id),
                  KPC(old_tablet_set));
  } else {
    DLIST_FOREACH(readable_set_ptr, readable_tablets_list_)
    {
      if (OB_SUCC(readable_set_ptr->get_refactored(tablet_id, tmp_info))) {
        if (OB_FAIL(old_tablet_set->set_refactored(tablet_id, tmp_info))) {
          DUP_TABLE_LOG(WARN, "insert into old tablet set failed", K(ret));
        } else if (OB_FAIL(readable_set_ptr->erase_refactored(tablet_id))) {
          DUP_TABLE_LOG(WARN, "remove from readable tablet set failed", K(ret));
        }
      } else if (ret != OB_HASH_NOT_EXIST) {
        DUP_TABLE_LOG(WARN, "get dup table status from readable_tablets_ failed", K(ret));
      } else if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_FAIL(ret)) {
    DUP_TABLE_LOG(WARN, "lose dup tablet failed", K(ret), K(tablet_id), KPC(changing_new_set_),
                  KPC(removing_old_set_), K(readable_tablets_list_.get_size()));
  }
  return ret;
}

int ObLSDupTabletsMgr::discover_dup_tablet_(const common::ObTabletID &tablet_id,
                                            const int64_t update_ts)
{
  int ret = OB_SUCCESS;

  DupTabletInfo tmp_status;
  int64_t confirming_tablet_cnt = 0;
  bool contain_confirming_special_op = false;

  // search new dup tablet in new, readable, old
  ret = OB_HASH_NOT_EXIST;
  DLIST_FOREACH_X(need_confirm_new_set, need_confirm_new_queue_, OB_HASH_NOT_EXIST == ret)
  {
    confirming_tablet_cnt += need_confirm_new_set->size();
    if (!need_confirm_new_set->get_common_header().no_specail_op()) {
      contain_confirming_special_op = true;
    }
    if (OB_SUCC(need_confirm_new_set->get_refactored(tablet_id, tmp_status))) {
      tmp_status.update_dup_schema_ts_ = update_ts;
      if (OB_FAIL(need_confirm_new_set->set_refactored(tablet_id, tmp_status, 1))) {
        DUP_TABLE_LOG(WARN, "update new_tablet ts failed", K(ret));
      }
    } else if (OB_HASH_NOT_EXIST != ret) {
      DUP_TABLE_LOG(WARN, "get from need_confirm_new_set failed", K(ret),
                    KPC(need_confirm_new_set));
    }
  }

  if (OB_HASH_NOT_EXIST != ret) {
    if (OB_SUCC(ret)) {
      DUP_TABLE_LOG(DEBUG, "tablet has existed in new_tablets_", K(ret), K(tablet_id),
                    K(update_ts));
    }
  } else {
    DLIST_FOREACH_X(readable_set_ptr, readable_tablets_list_, OB_HASH_NOT_EXIST == ret)
    {
      if (OB_SUCC(readable_set_ptr->get_refactored(tablet_id, tmp_status))) {
        tmp_status.update_dup_schema_ts_ = update_ts;
        if (OB_FAIL(readable_set_ptr->set_refactored(tablet_id, tmp_status, 1))) {
          DUP_TABLE_LOG(WARN, "update readable_tablet ts failed", K(ret));
        }
        // DUP_TABLE_LOG(INFO, "tablet has existed in readable_tablets_", K(ret), K(tablet_id),
        //               K(update_ts));
      } else if (ret != OB_HASH_NOT_EXIST) {
        DUP_TABLE_LOG(WARN, "get dup table status from readable_tablets_ failed", K(ret),
                      KPC(readable_set_ptr));
      }
    }
  }

  DupTabletCommonHeader old_set_header;
  old_set_header.set_invalid_unique_id();
  old_set_header.set_old();
  DupTabletChangeMap *old_tablets_ptr = nullptr;
  if (OB_HASH_NOT_EXIST != ret) {
    // do nothing
    if (OB_SUCC(ret)) {
      DUP_TABLE_LOG(DEBUG, "tablet has existed in new or readable tablets", K(ret), K(tablet_id),
                    K(update_ts));
    }
  } else if (OB_FAIL(get_target_tablet_set_(old_set_header, old_tablets_ptr))) {
    DUP_TABLE_LOG(WARN, "get old tablet set failed", K(ret), KPC(old_tablets_ptr));
  } else if (OB_SUCC(old_tablets_ptr->get_refactored(tablet_id, tmp_status))) {
    tmp_status.update_dup_schema_ts_ = update_ts;
    if (OB_FAIL(old_tablets_ptr->set_refactored(tablet_id, tmp_status, 1))) {
      DUP_TABLE_LOG(WARN, "update old_tablets_ ts failed", K(ret));
    }
    // DUP_TABLE_LOG(INFO, "tablet has existed in old_tablet_set", K(ret), K(tablet_id),
    //               K(update_ts));
  } else if (ret != OB_HASH_NOT_EXIST) {
    DUP_TABLE_LOG(WARN, "get dup table status from old_tablets_set failed", K(ret));
  }

  // We can not move a tablet_id from a confirming or logging old_tablets set.
  // To make it simple, we will not move a tablet_id from a temporary old tablets set right now.
  if (OB_HASH_NOT_EXIST == ret) {
    // search a temporary new tablets set and insert it
    tmp_status.update_dup_schema_ts_ = update_ts;
    DupTabletCommonHeader changing_new_set_header;
    changing_new_set_header.set_invalid_unique_id();
    changing_new_set_header.set_new();
    DupTabletChangeMap *changing_new_map = nullptr;
    if (OB_FAIL(get_target_tablet_set_(changing_new_set_header, changing_new_map))) {
      DUP_TABLE_LOG(WARN, "get changing new set failed", K(ret), KPC(changing_new_map));
    } else if (confirming_tablet_cnt + changing_new_map->size() > MAX_CONFIRMING_TABLET_COUNT
               || contain_confirming_special_op) {
      DUP_TABLE_LOG(
          INFO,
          "Too large confirming tablet set. We will not insert new tablet into changing_new_set_.",
          K(ret), K(ls_id_), K(changing_new_set_->size()), K(confirming_tablet_cnt),
          K(MAX_CONFIRMING_TABLET_COUNT), K(contain_confirming_special_op));
    } else if (OB_FAIL(changing_new_map->set_refactored(tablet_id, tmp_status, 1))) {
      DUP_TABLE_LOG(WARN, "insert into changing new tablets failed", K(ret));
    }
  }

  DUP_TABLE_LOG(DEBUG, "finish discover dup tablet", K(ret), K(tablet_id), KPC(changing_new_set_),
                K(need_confirm_new_queue_.get_size()), KPC(removing_old_set_),
                K(readable_tablets_list_.get_size()));
  return ret;
}

int ObLSDupTabletsMgr::alloc_extra_free_tablet_set_()
{
  int ret = OB_SUCCESS;

  DupTabletChangeMap *tmp_map_ptr = nullptr;
  if (OB_ISNULL(tmp_map_ptr = static_cast<DupTabletChangeMap *>(
                    share::mtl_malloc(sizeof(DupTabletChangeMap), "DupTabletMap")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FALSE_IT(extra_free_set_alloc_count_++)) {
  } else if (OB_FALSE_IT(new (tmp_map_ptr) DupTabletChangeMap(RESERVED_FREE_SET_COUNT
                                                              + extra_free_set_alloc_count_))) {
  } else if (OB_FAIL(tmp_map_ptr->create(1024))) {
    DUP_TABLE_LOG(WARN, "create dup_tablet hash map", K(ret));
  } else if (false == (free_set_pool_.add_last(tmp_map_ptr))) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "push back into free_set_pool failed", K(ret), K(free_set_pool_.get_size()),
                  KPC(tmp_map_ptr));
  }
  DUP_TABLE_LOG(INFO, "alloc a extra free tablet set", K(ret), K(last_no_free_set_time_),
                K(extra_free_set_alloc_count_), KPC(changing_new_set_),
                K(free_set_pool_.get_size()), K(need_confirm_new_queue_.get_size()),
                K(readable_tablets_list_.get_size()));
  return ret;
}

int ObLSDupTabletsMgr::get_free_tablet_set(DupTabletChangeMap *&free_set, const uint64_t target_id)
{

  int ret = OB_SUCCESS;

  const int64_t changing_new_set_count = OB_ISNULL(changing_new_set_) ? 0 : 1;
  const int64_t removing_old_set_count = OB_ISNULL(removing_old_set_) ? 0 : 1;
  const int64_t all_used_free_set_count =
      free_set_pool_.get_size() + changing_new_set_count + removing_old_set_count
      + need_confirm_new_queue_.get_size() + readable_tablets_list_.get_size();
  if (RESERVED_FREE_SET_COUNT + extra_free_set_alloc_count_ != all_used_free_set_count) {
    DUP_TABLE_LOG(ERROR, "the free set may be leaked from the pool", K(ret),
                  K(RESERVED_FREE_SET_COUNT), K(extra_free_set_alloc_count_),
                  K(all_used_free_set_count), K(changing_new_set_count), K(removing_old_set_count),
                  K(need_confirm_new_queue_.get_size()), K(readable_tablets_list_.get_size()),
                  K(free_set_pool_.get_size()), KPC(removing_old_set_), KPC(changing_new_set_));
  }

  while (OB_SUCC(ret) && target_id > RESERVED_FREE_SET_COUNT + extra_free_set_alloc_count_) {
    if (OB_FAIL(alloc_extra_free_tablet_set_())) {
      DUP_TABLE_LOG(WARN, "alloc extra free tablet set failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && free_set_pool_.is_empty()) {
    if (last_no_free_set_time_ < 0) {
      last_no_free_set_time_ = ObTimeUtility::fast_current_time();
    }
    if (extra_free_set_alloc_count_ < MAX_FREE_SET_COUNT - RESERVED_FREE_SET_COUNT
        || ObTimeUtility::fast_current_time() - last_no_free_set_time_ >= 3 * 1000 * 1000) {
      if (OB_FAIL(alloc_extra_free_tablet_set_())) {
        DUP_TABLE_LOG(WARN, "alloc extra free tablet set failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (free_set_pool_.is_empty()) {
    ret = OB_EAGAIN;
  } else {
    if (target_id <= 0) {
      free_set = free_set_pool_.remove_first();
      last_no_free_set_time_ = -1;
    } else {
      DLIST_FOREACH(free_set_ptr, free_set_pool_)
      {
        if (free_set_ptr->get_common_header().get_unique_id() == target_id) {
          free_set = free_set_ptr;
          break;
        }
      }
      if (OB_ISNULL(free_set)) {
        ret = OB_ENTRY_NOT_EXIST;
        DUP_TABLE_LOG(WARN, "no free set in free_set_pool_", K(ret), KPC(free_set), K(target_id),
                      K(free_set_pool_.get_size()));
      } else if (OB_ISNULL(free_set_pool_.remove(free_set))) {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "remove free set from pool failed", K(ret), KPC(free_set));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(free_set->get_change_status())) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(ERROR, "find a readable set in free_set_pool_", K(ret), KPC(free_set));
    } else {
      free_set->get_change_status()->set_temporary();
    }
  }

  // DUP_TABLE_LOG(DEBUG, "get a free set from pool", K(ret), K(free_set_pool_.get_size()),
  //               KPC(free_set), K(lbt()));
  return ret;
}

int ObLSDupTabletsMgr::get_target_tablet_set_(const DupTabletCommonHeader &target_common_header,
                                              DupTabletChangeMap *&target_set,
                                              const bool construct_target_set,
                                              const bool need_changing_new_set)
{
  int ret = OB_SUCCESS;
  const uint64_t unique_id = target_common_header.get_unique_id();

  if (target_set != nullptr) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), K(target_common_header), KPC(target_set));
    // } else if (need_changing_new_set) {
    //   if (OB_ISNULL(changing_new_set_)) {
    //     if (OB_FAIL(get_free_tablet_set(changing_new_set_))) {
    //       DUP_TABLE_LOG(WARN, "get free tablet set failed", K(ret));
    //     }
    //   }
    //
    //   if (OB_SUCC(ret)) {
    //     target_set = changing_new_set_
    //   }
  } else if (unique_id == DupTabletCommonHeader::INVALID_UNIQUE_ID) {
    if (target_common_header.is_free()) {
      if (OB_FAIL(get_free_tablet_set(target_set, unique_id))) {
        DUP_TABLE_LOG(WARN, "get free tablet set failed", K(ret));
      }
    } else if (target_common_header.is_old_set()) {

      if (OB_NOT_NULL(removing_old_set_)) {
        target_set = removing_old_set_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "unexpected null old_tablets_set", K(ret), KPC(removing_old_set_));
      }
    } else if (target_common_header.is_new_set()) {

      if (OB_ISNULL(changing_new_set_)) {
        if (OB_FAIL(get_free_tablet_set(changing_new_set_))) {
          DUP_TABLE_LOG(WARN, "get free tablet set failed", K(ret));
        } else {

          changing_new_set_->get_common_header().set_new();
        }
      }

      if (OB_SUCC(ret)) {
        target_set = changing_new_set_;
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      DUP_TABLE_LOG(WARN, "invalid unique_id with readable set", K(ret), K(target_common_header));
    }
  } else {
    if (target_common_header.is_readable_set()) {
      DLIST_FOREACH(readable_set_ptr, readable_tablets_list_)
      {
        if (readable_set_ptr->get_common_header().get_unique_id()
            == target_common_header.get_unique_id()) {
          target_set = readable_set_ptr;
          break;
        }
      }

    } else if (target_common_header.is_new_set()) {
      if (OB_NOT_NULL(changing_new_set_)
          && changing_new_set_->get_common_header().get_unique_id()
                 == target_common_header.get_unique_id()) {
        target_set = changing_new_set_;
      } else {
        // DUP_TABLE_LOG(INFO, "111 get need confirm tablet set", K(target_common_header));
        DLIST_FOREACH(new_set_ptr, need_confirm_new_queue_)
        {
          // DUP_TABLE_LOG(INFO, "222 get need confirm tablet set",
          // K(target_common_header),KPC(new_set_ptr));
          if (new_set_ptr->get_common_header().get_unique_id()
              == target_common_header.get_unique_id()) {
            target_set = new_set_ptr;
            break;
          }
        }
      }
      // DUP_TABLE_LOG(INFO, "333 get need confirm tablet set",
      // K(target_common_header),KPC(target_set));
    } else if (target_common_header.is_old_set()) {

      if (OB_NOT_NULL(removing_old_set_)
          && removing_old_set_->get_common_header().get_unique_id()
                 == target_common_header.get_unique_id()) {
        target_set = removing_old_set_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "unexpected null old_tablets_set", K(ret), KPC(removing_old_set_));
      }
    }

    // DUP_TABLE_LOG(INFO, "444 get need confirm tablet set",
    // K(target_common_header),K(target_common_header.is_new_set()),K(target_common_header.is_old_set()),K(target_common_header.is_readable_set()));
    if (OB_SUCC(ret) && OB_ISNULL(target_set) && !target_common_header.is_old_set()) {
      if (construct_target_set) {
        if (OB_FAIL(get_free_tablet_set(target_set, target_common_header.get_unique_id()))) {
          DUP_TABLE_LOG(WARN, "get free tablet set failed", K(ret), KPC(target_set),
                        K(target_common_header), K(need_confirm_new_queue_.get_size()),
                        K(readable_tablets_list_.get_size()), KPC(removing_old_set_));
        }
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        DUP_TABLE_LOG(WARN, "no tartget tablet set", K(ret), K(target_common_header),
                      KPC(target_set), K(construct_target_set), KPC(changing_new_set_),
                      KPC(removing_old_set_), K(need_confirm_new_queue_.get_size()));
      }
    }
  }

  if (OB_NOT_NULL(target_set) && OB_SUCC(ret)) {
    if (target_set->get_common_header().is_readable_set()
        && OB_NOT_NULL(target_set->get_change_status())) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(ERROR, "unexpected readbale tablet set with valid change status", K(ret),
                    KPC(target_set));
      target_set = nullptr;
    } else if (!target_set->get_common_header().is_readable_set()
               && OB_ISNULL(target_set->get_change_status())) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(ERROR, "unexpected new/old tablet set with invalid change status", K(ret),
                    KPC(target_set));
    }
  }

  return ret;
}

int ObLSDupTabletsMgr::return_tablet_set(DupTabletChangeMap *need_free_set)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(need_free_set)) {

  } else if (need_free_set->get_common_header().is_old_set()) {
    need_free_set->reuse();
    need_free_set->get_common_header().set_old();
  } else {
    if (!need_free_set->get_common_header().no_specail_op()) {
      if (OB_FAIL(
              op_arg_map_.erase_refactored(need_free_set->get_common_header().get_unique_id()))) {
        DUP_TABLE_LOG(WARN, "remove from op_arg_map failed", K(ret), KPC(need_free_set));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      need_free_set->reuse();
      if (free_set_pool_.add_last(need_free_set) == false) {
        ret = OB_ERR_UNEXPECTED;
      }
      if (need_free_set == changing_new_set_) {
        changing_new_set_ = nullptr;
      }
    }
  }

  return ret;
}

// remove empty readable set
int ObLSDupTabletsMgr::check_and_recycle_empty_readable_set(DupTabletChangeMap *readable_set,
                                                            bool &need_remove)
{
  int ret = OB_SUCCESS;

  need_remove = false;
  if (OB_ISNULL(readable_set) || !readable_set->get_common_header().is_readable_set()) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "unexpected tablet set", K(ret), KPC(readable_set));
  } else if (readable_set->empty()) {
    need_remove = true;
    DUP_TABLE_LOG(INFO, "try to remove empty readable tablet set from list", K(ret), K(need_remove),
                  KPC(readable_set));
    if (OB_ISNULL(readable_tablets_list_.remove(readable_set))) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "remove empty readable set from list failed", K(ret), KPC(readable_set));
    } else if (OB_FAIL(return_tablet_set(readable_set))) {
      DUP_TABLE_LOG(WARN, "return empty readable set failed", K(ret), KPC(readable_set));
    }
  }

  return ret;
}

int ObLSDupTabletsMgr::clean_readable_tablets_(const share::SCN &min_reserve_tablet_scn)
{
  int ret = OB_SUCCESS;

  if (!min_reserve_tablet_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), K(ls_id_), K(min_reserve_tablet_scn));
  } else {

    DUP_TABLE_LOG(INFO, "try to clean all readable tablets", K(ret), K(ls_id_),
                  K(min_reserve_tablet_scn), K(readable_tablets_list_.get_size()));

    DLIST_FOREACH_REMOVESAFE(readable_set, readable_tablets_list_)
    {
      DUP_TABLE_LOG(INFO, "try to clean one durable tablet set", K(ret), K(min_reserve_tablet_scn),
                    KPC(readable_set));
      if (!readable_set->need_reserve(min_reserve_tablet_scn)) {
        if (OB_ISNULL(readable_tablets_list_.remove(readable_set))) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "remove need_confirm_set failed", K(ret), KPC(readable_set));
        } else if (OB_FAIL(return_tablet_set(readable_set))) {
          DUP_TABLE_LOG(WARN, "free need_confirm_set failed", K(ret), KPC(readable_set),
                        K(readable_tablets_list_.get_size()), K(free_set_pool_.get_size()));
        }
      }
    }
  }

  return ret;
}

int ObLSDupTabletsMgr::clean_durable_confirming_tablets_(const share::SCN &min_reserve_tablet_scn)
{
  int ret = OB_SUCCESS;

  if (!min_reserve_tablet_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), K(ls_id_), K(min_reserve_tablet_scn));
  } else {

    DUP_TABLE_LOG(INFO, "try to clean durable confirming tablets", K(ret), K(ls_id_),
                  K(min_reserve_tablet_scn), K(need_confirm_new_queue_.get_size()));

    DLIST_FOREACH_REMOVESAFE(need_confirm_set, need_confirm_new_queue_)
    {
      DUP_TABLE_LOG(INFO, "try to clean one confirming tablet set", K(ret), K(min_reserve_tablet_scn),
                    KPC(need_confirm_set));
      if (!need_confirm_set->need_reserve(min_reserve_tablet_scn)) {
        if (OB_ISNULL(need_confirm_new_queue_.remove(need_confirm_set))) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "remove need_confirm_set failed", K(ret), KPC(need_confirm_set));
        } else if (OB_FAIL(return_tablet_set(need_confirm_set))) {
          DUP_TABLE_LOG(WARN, "free need_confirm_set failed", K(ret), KPC(need_confirm_set),
                        K(need_confirm_new_queue_.get_size()), K(free_set_pool_.get_size()));
        }
      }
    }
  }
  return ret;
}

int ObLSDupTabletsMgr::clean_unlog_tablets_()
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && OB_NOT_NULL(changing_new_set_)) {
    DUP_TABLE_LOG(INFO, "try to clean one unlog tablet set", K(ret), KPC(changing_new_set_));
    if (OB_FAIL(return_tablet_set(changing_new_set_))) {
      DUP_TABLE_LOG(WARN, "free changing_new_set_ failed", K(ret), KPC(changing_new_set_),
                    K(free_set_pool_.get_size()));
    } else {
      changing_new_set_ = nullptr;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(removing_old_set_)
      && removing_old_set_->get_change_status()->is_unlog()) {
    DUP_TABLE_LOG(INFO, "try to clean one unlog tablet set", K(ret), KPC(removing_old_set_));
    if (OB_FAIL(return_tablet_set(removing_old_set_))) {
      DUP_TABLE_LOG(WARN, "free removing_old_set_ failed", K(ret), KPC(removing_old_set_),
                    K(free_set_pool_.get_size()));
    } else {
    }
  }

  if (OB_SUCC(ret)) {
    DLIST_FOREACH_REMOVESAFE(need_confirm_set, need_confirm_new_queue_)
    {
      if (need_confirm_set->get_change_status()->is_unlog()) {
        DUP_TABLE_LOG(INFO, "try to clean one unlog tablet set", K(ret), KPC(need_confirm_set));

        if (nullptr == need_confirm_new_queue_.remove(need_confirm_set)) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "remove need_confirm_set failed", K(ret), KPC(need_confirm_set));
        } else if (OB_FAIL(return_tablet_set(need_confirm_set))) {
          DUP_TABLE_LOG(WARN, "free need_confirm_set failed", K(ret), KPC(need_confirm_set),
                        K(need_confirm_new_queue_.get_size()), K(free_set_pool_.get_size()));
        }
      }
    }
  }
  return ret;
}

int ObLSDupTabletsMgr::construct_empty_block_confirm_task_(const int64_t trx_ref)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  DupTabletCommonHeader empty_new_common_header;
  empty_new_common_header.set_invalid_unique_id();
  empty_new_common_header.set_free();
  uint64_t block_confirm_uid = DupTabletCommonHeader::INVALID_UNIQUE_ID;

  DupTabletChangeMap *block_confirm_task = nullptr;
  DupTabletSpecialOpArg tmp_op;

  if (trx_ref < 0) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret));
  } else if (OB_FAIL(get_target_tablet_set_(empty_new_common_header, block_confirm_task))) {
    DUP_TABLE_LOG(WARN, "get free_set as empty_new_set", K(ret), KPC(block_confirm_task),
                  K(free_set_pool_.get_size()));
  } else {
    block_confirm_task->get_common_header().set_new();
    block_confirm_task->get_common_header().set_op_of_block_confirming();
    block_confirm_task->get_change_status()->trx_ref_ = trx_ref; // TODO
    block_confirm_uid = block_confirm_task->get_common_header().get_unique_id();
    // set empty tablet_set as a normal tablet_set which has submit log failed
    if (OB_FAIL(block_confirm_task->get_change_status()->prepare_serialize())) {
      DUP_TABLE_LOG(WARN, "prepare serialize for block_confirm_task failed", K(ret));
    } else if (false == need_confirm_new_queue_.add_last(block_confirm_task)) {
      DUP_TABLE_LOG(WARN, "insert into need_confirm_new_queue_ failed", K(ret),
                    KPC(block_confirm_task), K(need_confirm_new_queue_.get_size()));
    } else if (OB_FAIL(op_arg_map_.set_refactored(block_confirm_uid, tmp_op))) {
      DUP_TABLE_LOG(WARN, "insert into special op map failed", K(ret), KPC(block_confirm_task),
                    K(tmp_op));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(block_confirm_task)) {
      if (OB_ISNULL(need_confirm_new_queue_.remove(block_confirm_task))) {
        // may be error before insert into need_confirm_new_queue_
        DUP_TABLE_LOG(WARN, "remove block_confirm_task failed, it may not have been inserted",
                      K(ret), KPC(block_confirm_task));
      }
      if (OB_TMP_FAIL(return_tablet_set(block_confirm_task))) {
        DUP_TABLE_LOG(WARN, "return block_confirm_task failed", K(tmp_ret), KPC(block_confirm_task));
      }
    }
    // if task is null, it would not insert into op_arg_map, do nothing
  } else {
    DUP_TABLE_LOG(INFO, "construct empty block confirming set task successfully", K(ret),
                  KPC(block_confirm_task), K(tmp_op));
  }

  return ret;
}

int ObLSDupTabletsMgr::search_special_op_(uint64_t special_op_type)
{
  int ret = OB_SUCCESS;

  return ret;
}

int ObLSDupTabletsMgr::construct_clean_confirming_set_task_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  DupTabletCommonHeader clean_confirming_common_header;
  clean_confirming_common_header.set_invalid_unique_id();
  clean_confirming_common_header.set_free();
  uint64_t clean_confirming_uid = DupTabletCommonHeader::INVALID_UNIQUE_ID;

  DupTabletChangeMap *clean_confirming_task = nullptr;
  DupTabletSpecialOpArg tmp_op;

  if (OB_FAIL(get_target_tablet_set_(clean_confirming_common_header, clean_confirming_task))) {
    DUP_TABLE_LOG(WARN, "get free_set as empty_new_set", K(ret), KPC(clean_confirming_task),
                  K(free_set_pool_.get_size()));
  } else {
    clean_confirming_task->get_common_header().set_new();
    clean_confirming_task->get_common_header().set_op_of_clean_data_confirming_set();
    clean_confirming_uid = clean_confirming_task->get_common_header().get_unique_id();
    // set empty tablet_set as a normal tablet_set which has submit log failed
    if (OB_FAIL(clean_confirming_task->get_change_status()->prepare_serialize())) {
      DUP_TABLE_LOG(WARN, "prepare serialize for empty_new_set failed", K(ret));
    } else if (false == need_confirm_new_queue_.add_last(clean_confirming_task)) {
      DUP_TABLE_LOG(WARN, "insert into need_confirm_new_queue_ failed", K(ret),
                    KPC(clean_confirming_task), K(need_confirm_new_queue_.get_size()));
    } else {
      DLIST_FOREACH(need_confirm_ptr, need_confirm_new_queue_)
      {
        if (need_confirm_ptr == clean_confirming_task) {
          // do nothing
          // } else if
          // (OB_FAIL(tmp_op.op_objects_.push_back(need_confirm_ptr->get_common_header()))) {
          //   DUP_TABLE_LOG(WARN, "push back into special op arg failed", K(ret),
          //                 KPC(need_confirm_ptr));
        } else {
          need_confirm_ptr->get_change_status()->set_confirm_invalid();
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(op_arg_map_.set_refactored(clean_confirming_uid, tmp_op))) {
        DUP_TABLE_LOG(WARN, "insert into special op map failed", K(ret), KPC(clean_confirming_task),
                      K(tmp_op));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(clean_confirming_task)) {
      if (OB_ISNULL(need_confirm_new_queue_.remove(clean_confirming_task))) {
        // may be error before insert into need_confirm_new_queue_
        DUP_TABLE_LOG(WARN, "remove clean_confirming_task failed, it may not have been inserted",
                      K(ret), KPC(clean_confirming_task));
      }
      if (OB_TMP_FAIL(return_tablet_set(clean_confirming_task))) {
        DUP_TABLE_LOG(WARN, "return clean_confirming_task failed", K(tmp_ret),
                      KPC(clean_confirming_task));
      }
    }
    // if task is null, it would not insert into op_arg_map, do nothing
  } else {
    DUP_TABLE_LOG(INFO, "construct clean data confirming set task successfully", K(ret),
                  KPC(clean_confirming_task), K(tmp_op));
  }

  return ret;
}

int ObLSDupTabletsMgr::construct_clean_all_readable_set_task_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  DupTabletCommonHeader clean_readable_common_header;
  clean_readable_common_header.set_invalid_unique_id();
  clean_readable_common_header.set_free();
  uint64_t clean_readbale_uid = DupTabletCommonHeader::INVALID_UNIQUE_ID;

  DupTabletChangeMap *clean_readable_task = nullptr;
  DupTabletSpecialOpArg tmp_op;

  if (OB_FAIL(get_target_tablet_set_(clean_readable_common_header, clean_readable_task))) {
    DUP_TABLE_LOG(WARN, "get free_set as empty_new_set", K(ret), KPC(clean_readable_task),
                  K(free_set_pool_.get_size()));
  } else {
    clean_readable_task->get_common_header().set_new();
    clean_readable_task->get_common_header().set_op_of_clean_all_readable_set();
    clean_readbale_uid = clean_readable_task->get_common_header().get_unique_id();
    // set empty tablet_set as a normal tablet_set which has submit log failed
    if (OB_FAIL(clean_readable_task->get_change_status()->prepare_serialize())) {
      DUP_TABLE_LOG(WARN, "prepare serialize for empty_new_set failed", K(ret));
    } else if (false == need_confirm_new_queue_.add_last(clean_readable_task)) {
      DUP_TABLE_LOG(WARN, "insert into need_confirm_new_queue_ failed", K(ret),
                    KPC(clean_readable_task), K(need_confirm_new_queue_.get_size()));
    } else {
      // DLIST_FOREACH(readable_ptr, readable_tablets_list_)
      // {
      //   if (OB_FAIL(tmp_op.op_objects_.push_back(readable_ptr->get_common_header()))) {
      //     DUP_TABLE_LOG(WARN, "push back into special op arg failed", K(ret),
      //                   KPC(clean_readable_task));
      //   }
      // }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(op_arg_map_.set_refactored(clean_readbale_uid, tmp_op))) {
        DUP_TABLE_LOG(WARN, "insert into special op map failed", K(ret), KPC(clean_readable_task),
                      K(tmp_op));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(clean_readable_task)) {
      if (OB_ISNULL(need_confirm_new_queue_.remove(clean_readable_task))) {
        // may be error before insert into need_confirm_new_queue_
        DUP_TABLE_LOG(WARN, "remove clean_readable_task failed, it may not have been inserted",
                      K(ret), KPC(clean_readable_task));
      }

      if (OB_TMP_FAIL(return_tablet_set(clean_readable_task))) {
        DUP_TABLE_LOG(WARN, "return clean_readable_task failed", K(ret), KPC(clean_readable_task));
      }
    }
    // if task is null, it would not insert into op_arg_map, do nothing
  } else {
    DUP_TABLE_LOG(INFO, "construct clean all readable task successfully", K(ret),
                  KPC(clean_readable_task), K(tmp_op));
  }
  return ret;
}

int ObLSDupTabletsMgr::try_exec_special_op_(DupTabletChangeMap *op_tablet_set,
                                            const share::SCN &min_reserve_tablet_scn,
                                            const bool for_replay)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(op_tablet_set)) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid argument", K(ret), KPC(op_tablet_set));
  } else if (op_tablet_set->get_common_header().no_specail_op()) {
      // filter no sp op tablet set
  } else if (!op_tablet_set->get_change_status()->tablet_change_scn_.is_valid() ||
              (op_tablet_set->get_change_status()->tablet_change_scn_.is_valid() &&
              min_reserve_tablet_scn > op_tablet_set->get_change_status()->tablet_change_scn_)) {
    // filter those sp op set with invalid change scn or not equal to min reserve scn
    // do nothing
    DUP_TABLE_LOG(INFO, "not need exec sp op", K(ret), K(min_reserve_tablet_scn), KPC(op_tablet_set));
  } else if (op_tablet_set->get_common_header().need_clean_all_readable_set()) {
    if (OB_FAIL(clean_readable_tablets_(min_reserve_tablet_scn))) {
      DUP_TABLE_LOG(WARN, "clean readable tablets failed", K(ret), K(min_reserve_tablet_scn));
    }
    // if (OB_SUCC(ret) && !readable_tablets_list_.is_empty()) {
    // DLIST_FOREACH_REMOVESAFE(readable_set_ptr, readable_tablets_list_)
    // {
    //   if (nullptr == readable_tablets_list_.remove(readable_set_ptr)) {
    //     ret = OB_ERR_UNEXPECTED;
    //     DUP_TABLE_LOG(WARN, "remove readable_set failed", K(ret), KPC(readable_set_ptr));
    //   } else if (OB_FAIL(return_tablet_set(readable_set_ptr))) {
    //     DUP_TABLE_LOG(WARN, "free readable_set failed", K(ret), KPC(readable_set_ptr),
    //                   K(readable_tablets_list_.get_size()), K(free_set_pool_.get_size()));
    //   }
    // }
    // }
  } else if (op_tablet_set->get_common_header().need_clean_data_confirming_set()) {
    // DupTabletSpecialOpArg tmp_arg;
    // DUP_TABLE_LOG(WARN, "try clean unreadable tablets", K(ret),
    //               K(op_tablet_set->get_common_header()));
    // if (OB_FAIL(op_arg_map_.get_refactored(op_tablet_set->get_common_header().get_unique_id(),
    //                                        tmp_arg))) {
    //   DUP_TABLE_LOG(WARN, "get sp op arg failed", K(ret), K(op_tablet_set->get_common_header()));
    //   // @param1 type clean_all
    // } else
    if (OB_FAIL(clean_durable_confirming_tablets_(min_reserve_tablet_scn))) {
      DUP_TABLE_LOG(WARN, "clean unreadable tablets failed", K(ret), K(min_reserve_tablet_scn));
    }
  } else if (op_tablet_set->get_common_header().need_block_confirming()) {
    // do nothing
    // only block confirm new tablet set before all_trx_ref has been clear
  }

  return ret;
}
// tablet set virtual table interface
// all tablets virtual table interface
int ObLSDupTabletsMgr::get_tablets_stat(ObDupLSTabletsStatIterator &collect_iter,
                                        const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const ObAddr addr = GCTX.self_addr();
  const int64_t tenant_id = MTL_ID();
  const int64_t collect_ts = ObTimeUtility::current_time();
  SpinRLockGuard rlock(dup_tablets_lock_);

  // iter changing new
  if (OB_NOT_NULL(changing_new_set_)) {
    if (0 == changing_new_set_->size()) {
      // do nothing
    } else {
      CollectTabletsHandler changing_new_handler(
          collect_ts, ls_id, tenant_id, addr, is_master(),
          changing_new_set_->get_common_header().get_unique_id(), TabletSetAttr::DATA_SYNCING,
          // tablet_gc_window_,
          collect_iter);
      if (OB_FAIL(hash_for_each_update(*changing_new_set_, changing_new_handler))) {
        DUP_TABLE_LOG(WARN, "push into iter failed", KPC(this));
      }
    }
  }
  // iter need confirm
  if (OB_SUCC(ret)) {
    DLIST_FOREACH(need_confirm_set, need_confirm_new_queue_)
    {
      if (OB_NOT_NULL(need_confirm_set)) {
        if (0 == need_confirm_set->size()) {
          // do nothing
        } else {
          CollectTabletsHandler changing_new_handler(
              collect_ts, ls_id, tenant_id, addr, is_master(),
              need_confirm_set->get_common_header().get_unique_id(), TabletSetAttr::DATA_SYNCING,
              // tablet_gc_window_,
              collect_iter);
          if (OB_FAIL(hash_for_each_update(*need_confirm_set, changing_new_handler))) {
            DUP_TABLE_LOG(WARN, "push into iter failed", KPC(this));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "iter need confirm failed", K(ret), KPC(this), KP(need_confirm_set));
      }
    }
  }
  // iter readable
  if (OB_SUCC(ret)) {
    DLIST_FOREACH(readable_set, readable_tablets_list_)
    {
      if (OB_NOT_NULL(readable_set)) {
        if (0 == readable_set->size()) {
          // do nothing
        } else {
          CollectTabletsHandler changing_new_handler(
              collect_ts, ls_id, tenant_id, addr, is_master(),
              readable_set->get_common_header().get_unique_id(), TabletSetAttr::READABLE,
              // tablet_gc_window_,
              collect_iter);
          if (OB_FAIL(hash_for_each_update(*readable_set, changing_new_handler))) {
            DUP_TABLE_LOG(WARN, "push into iter failed", KPC(this));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "iter readable failed", K(ret), KPC(this), KP(readable_set));
      }
    }
  }
  // iter old
  if (OB_SUCC(ret) && OB_NOT_NULL(removing_old_set_)) {
    if (0 == removing_old_set_->size()) {
      // do nothing
    } else {
      CollectTabletsHandler changing_new_handler(
          collect_ts, ls_id, tenant_id, addr, is_master(),
          removing_old_set_->get_common_header().get_unique_id(), TabletSetAttr::DELETING,
          // tablet_gc_window_,
          collect_iter);
      if (OB_FAIL(hash_for_each_update(*removing_old_set_, changing_new_handler))) {
        DUP_TABLE_LOG(WARN, "push into iter failed", KPC(this));
      }
    }
  }
  // TODO siyu: for debug
  DUP_TABLE_LOG(WARN, "collect all", K(ret), KPC(this));
  return ret;
}

int ObLSDupTabletsMgr::get_tablet_set_stat(ObDupLSTabletSetStatIterator &collect_iter,
                                               const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  // iter changing new
  // const ObAddr addr = GCTX.self_addr();
  const int64_t tenant_id = MTL_ID();
  SpinRLockGuard rlock(dup_tablets_lock_);

  if (OB_NOT_NULL(changing_new_set_)) {
    DupTabletSetChangeStatus *tmp_status = changing_new_set_->get_change_status();
    if (OB_NOT_NULL(tmp_status)) {
      // share::SCN not_used = share::SCN::min_scn();
      ObDupTableLSTabletSetStat tmp_stat;
      tmp_stat.set_basic_info(tenant_id, ls_id, is_master());

      tmp_stat.set_unique_id(changing_new_set_->get_common_header().get_unique_id());
      tmp_stat.set_attr(TabletSetAttr::DATA_SYNCING);
      // set state, trx_ref, change_scn, need_confirm_scn and readable_scn
      tmp_stat.set_from_change_status(tmp_status);
      tmp_stat.set_count(changing_new_set_->size());

      if (OB_FAIL(collect_iter.push(tmp_stat))) {
        DUP_TABLE_LOG(WARN, "push into iter failed", K(tmp_stat));
      }
    } else {
      DUP_TABLE_LOG(WARN, "change status is null", KPC(this), KP(tmp_status));
    }
  }
  // iter need confirm
  if (OB_SUCC(ret)) {
    DLIST_FOREACH(need_confirm_set, need_confirm_new_queue_)
    {
      if (OB_NOT_NULL(need_confirm_set)) {
        DUP_TABLE_LOG(WARN, "need confirm  tablets ", KPC(need_confirm_set));
        DupTabletSetChangeStatus *tmp_status = need_confirm_set->get_change_status();
        if (OB_NOT_NULL(tmp_status)) {
          ObDupTableLSTabletSetStat tmp_stat;
          tmp_stat.set_basic_info(tenant_id, ls_id, is_master());

          tmp_stat.set_unique_id(need_confirm_set->get_common_header().get_unique_id());
          tmp_stat.set_attr(TabletSetAttr::READABLE);
          tmp_stat.set_from_change_status(tmp_status);
          tmp_stat.set_count(need_confirm_set->size());

          if (OB_FAIL(collect_iter.push(tmp_stat))) {
            DUP_TABLE_LOG(WARN, "push into iter failed", K(tmp_stat));
          }
        } else {
          DUP_TABLE_LOG(WARN, "change status is null", KPC(this), KP(tmp_status));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "iter need confirm failed", K(ret), KPC(this),
                      KP(need_confirm_set));
      }
    }
  }
  // iter readable
  if (OB_SUCC(ret)) {
    DLIST_FOREACH(readable_set, readable_tablets_list_)
    {
      if (OB_NOT_NULL(readable_set)) {
        share::SCN not_used = share::SCN::min_scn();
        ObDupTableLSTabletSetStat tmp_stat;
        tmp_stat.set_basic_info(tenant_id, ls_id, is_master());

        tmp_stat.set_unique_id(readable_set->get_common_header().get_unique_id());
        tmp_stat.set_attr(TabletSetAttr::READABLE);
        tmp_stat.set_state(TabletSetState::CONFIRMED);
        tmp_stat.set_from_change_status(nullptr);
        tmp_stat.set_count(readable_set->size());

        if (OB_FAIL(collect_iter.push(tmp_stat))) {
          DUP_TABLE_LOG(WARN, "push into iter failed", K(tmp_stat));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "iter readable failed", K(ret), KPC(this),
                      KP(readable_set));
      }
    }
  }
  // iter old
  if (OB_SUCC(ret) && OB_NOT_NULL(removing_old_set_)) {
    share::SCN not_used = share::SCN::min_scn();
    DupTabletSetChangeStatus *tmp_status = removing_old_set_->get_change_status();
    DUP_TABLE_LOG(WARN, "old tablets ", KPC(removing_old_set_), KPC(tmp_status));
    if (OB_NOT_NULL(tmp_status)) {
      ObDupTableLSTabletSetStat tmp_stat;
      tmp_stat.set_basic_info(tenant_id, ls_id, is_master());

      tmp_stat.set_unique_id(removing_old_set_->get_common_header().get_unique_id());
      tmp_stat.set_attr(TabletSetAttr::DELETING);
      tmp_stat.set_from_change_status(tmp_status);
      tmp_stat.set_count(removing_old_set_->size());

      if (OB_FAIL(collect_iter.push(tmp_stat))) {
        DUP_TABLE_LOG(WARN, "push into iter failed", K(tmp_stat));
      }
    } else {
      DUP_TABLE_LOG(WARN, "change status is null", KPC(this), KP(tmp_status));
    }
  }
  // TODO siyu: for debug
  DUP_TABLE_LOG(WARN, "collect all", K(ret), KPC(this));
  return ret;
}

int ObTenantDupTabletSchemaHelper::get_all_dup_tablet_set_(TabletIDSet &tablet_set)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSEArray<const ObSimpleTableSchemaV2 *, 16> table_schemas;
  if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    DUP_TABLE_LOG(WARN, "get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(MTL_ID(), table_schemas))) {
    DUP_TABLE_LOG(WARN, "get table schemas in tenant failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < table_schemas.count(); i++) {
      bool is_duplicated = false;
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      if (OB_FAIL(table_schema->check_is_duplicated(schema_guard, is_duplicated))) {
        DUP_TABLE_LOG(WARN, "check duplicate failed", K(ret));
      } else if (is_duplicated) {
        ObArray<ObTabletID> tablet_id_arr;
        if (OB_FAIL(table_schema->get_tablet_ids(tablet_id_arr))) {
          DUP_TABLE_LOG(WARN, "get tablet ids from tablet schema failed");
        } else {
          for (int j = 0; OB_SUCCESS == ret && j < tablet_id_arr.size(); j++) {
            if (OB_FAIL(tablet_set.set_refactored(tablet_id_arr[j]))) {
              DUP_TABLE_LOG(WARN, "insert into dup tablet set faild", K(ret));
            }
          }
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObTenantDupTabletSchemaHelper::refresh_and_get_tablet_set(TabletIDSet &tenant_dup_tablet_set)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_all_dup_tablet_set_(tenant_dup_tablet_set))) {
    DUP_TABLE_LOG(WARN, "get tenant dup tablet set faild", K(ret));
  }

  DUP_TABLE_LOG(DEBUG, "get all dup tablet ids", K(tenant_dup_tablet_set.size()));
  return ret;
}

} // namespace transaction

} // namespace oceanbase
