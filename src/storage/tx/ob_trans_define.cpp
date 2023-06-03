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

#define USING_LOG_PREFIX TRANS

#include "ob_trans_define.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "ob_trans_part_ctx.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "ob_trans_service.h"
#include "observer/ob_server.h"
#include "lib/profile/ob_trace_id.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "share/ob_define.h"
#include "ob_tx_log.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace sql;
using namespace storage;
using namespace memtable;
using namespace observer;

namespace transaction
{
int ObTransID::compare(const ObTransID& other) const
{
  int compare_ret = 0;
  if (this == &other) {
    compare_ret = 0;
  } else if (tx_id_ != other.tx_id_) {
    // iterate transaction ctx sequentially
    compare_ret = tx_id_ > other.tx_id_ ? 1 : -1;
  } else {
    compare_ret = 0;
  }
  return compare_ret;
}

OB_SERIALIZE_MEMBER(ObTransID, tx_id_);
OB_SERIALIZE_MEMBER(ObStartTransParam, access_mode_, type_, isolation_, consistency_type_,
                    cluster_version_, is_inner_trans_, read_snapshot_type_);
OB_SERIALIZE_MEMBER(ObElrTransInfo, trans_id_, commit_version_, result_);
OB_SERIALIZE_MEMBER(ObLSLogInfo, id_, offset_);
OB_SERIALIZE_MEMBER(ObStateInfo, ls_id_, state_, version_, snapshot_version_);
OB_SERIALIZE_MEMBER(ObTransDesc, a_);

// class ObStartTransParam
void ObStartTransParam::reset()
{
  access_mode_ = ObTransAccessMode::UNKNOWN;
  type_ = ObTransType::UNKNOWN;
  isolation_ = ObTransIsolation::UNKNOWN;
  magic_ = 0xF0F0F0F0F0F0F0F0;
  autocommit_ = false;
  consistency_type_ = ObTransConsistencyType::CURRENT_READ;
  read_snapshot_type_ = ObTransReadSnapshotType::STATEMENT_SNAPSHOT;
  cluster_version_ = ObStartTransParam::INVALID_CLUSTER_VERSION;
  is_inner_trans_ = false;
}

bool ObStartTransParam::is_valid() const
{
  return ObTransAccessMode::is_valid(access_mode_) && ObTransType::is_valid(type_)
      && ObTransIsolation::is_valid(isolation_)
      && ObTransConsistencyType::is_valid(consistency_type_)
      && ObTransReadSnapshotType::is_valid(read_snapshot_type_);
}

int ObStartTransParam::set_access_mode(const int32_t access_mode)
{
  int ret = OB_SUCCESS;

  if (!ObTransAccessMode::is_valid(access_mode)) {
    TRANS_LOG(WARN, "invalid argument", K(access_mode));
    ret = OB_INVALID_ARGUMENT;
  } else if (MAGIC_NUM != magic_) {
    TRANS_LOG(ERROR, "magic number error", K_(magic));
    ret = OB_ERR_UNEXPECTED;
  } else {
    access_mode_ = access_mode;
  }

  return ret;
}

int ObStartTransParam::set_type(const int32_t type)
{
  int ret = OB_SUCCESS;

  if (!ObTransType::is_valid(type)) {
    TRANS_LOG(WARN, "invalid argument", K(type));
    ret = OB_INVALID_ARGUMENT;
  } else if (MAGIC_NUM != magic_) {
    TRANS_LOG(ERROR, "magic number error", K_(magic));
    ret = OB_ERR_UNEXPECTED;
  } else {
    type_ = type;
  }

  return ret;
}

int ObStartTransParam::set_isolation(const int32_t isolation)
{
  int ret = OB_SUCCESS;

  if (!ObTransIsolation::is_valid(isolation)) {
    TRANS_LOG(WARN, "invalid argument", K(isolation));
    ret = OB_INVALID_ARGUMENT;
  } else if (MAGIC_NUM != magic_) {
    TRANS_LOG(ERROR, "magic number error", K_(magic));
    ret = OB_ERR_UNEXPECTED;
  } else {
    isolation_ = isolation;
  }

  return ret;
}

bool ObStartTransParam::is_serializable_isolation() const
{
  return ObTransIsolation::SERIALIZABLE == isolation_
    || ObTransIsolation::REPEATABLE_READ == isolation_;
}

int64_t ObStartTransParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos,
                  "[access_mode=%d, type=%d, isolation=%d, magic=%lu, autocommit=%d, "
                  "consistency_type=%d(%s), read_snapshot_type=%d(%s), cluster_version=%lu, "
                  "is_inner_trans=%d]",
                  access_mode_, type_, isolation_, magic_, autocommit_,
                  consistency_type_, ObTransConsistencyType::cstr(consistency_type_),
                  read_snapshot_type_, ObTransReadSnapshotType::cstr(read_snapshot_type_),
                  cluster_version_, is_inner_trans_);
  return pos;
}

int ObStartTransParam::reset_read_snapshot_type_for_isolation()
{
  int ret = OB_SUCCESS;
  if (is_serializable_isolation()) {
    read_snapshot_type_ = ObTransReadSnapshotType::TRANSACTION_SNAPSHOT;
  } else {
    read_snapshot_type_ = ObTransReadSnapshotType::STATEMENT_SNAPSHOT;
  }
  return ret;
}

void ObTraceInfo::reset()
{
  if (app_trace_info_.length() >= MAX_TRACE_INFO_BUFFER) {
    void *buf = app_trace_info_.ptr();
    if (NULL != buf &&
        buf != &app_trace_info_) {
      ob_free(buf);
      buf = NULL;
    }
    app_trace_info_.assign_buffer(app_trace_info_buffer_, sizeof(app_trace_info_buffer_));
  } else {
    app_trace_info_.set_length(0);
  }
  app_trace_id_.set_length(0);
}

int ObTraceInfo::set_app_trace_info(const ObString &app_trace_info)
{
  const int64_t len = app_trace_info.length();
  int ret = OB_SUCCESS;

  if (len < 0 || len > OB_MAX_TRACE_INFO_BUFFER_SIZE) {
    TRANS_LOG(WARN, "unexpected trace info str", K(app_trace_info));
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != app_trace_info_.length()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "different app trace info", K(ret), K(app_trace_info_), K(app_trace_info));
  } else if (len < MAX_TRACE_INFO_BUFFER) {
    (void)app_trace_info_.write(app_trace_info.ptr(), len);
    app_trace_info_buffer_[len] = '\0';
  } else {
    char *buf = NULL;
    if (NULL == (buf = (char *)ob_malloc(len+1, "AppTraceInfo"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "allocate memory for app trace info failed", K(ret), K(app_trace_info));
    } else {
      app_trace_info_.reset();
      (void)app_trace_info_.assign_buffer(buf, len+1);
      (void)app_trace_info_.write(app_trace_info.ptr(), len);
      buf[len] = '\0';
    }
  }

  return ret;
}

int ObTraceInfo::set_app_trace_id(const ObString &app_trace_id)
{
  int ret = OB_SUCCESS;
  const int64_t len = app_trace_id.length();

  if (len < 0 || len > OB_MAX_TRACE_ID_BUFFER_SIZE) {
    TRANS_LOG(WARN, "unexpected trace id str", K(app_trace_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != app_trace_id_.length()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "different app trace id", K(ret), K(app_trace_id_), K(app_trace_id));
  } else {
    (void)app_trace_id_.write(app_trace_id.ptr(), len);
    app_trace_id_buffer_[len] = '\0';
  }

  return ret;
}

const ObString ObTransIsolation::LEVEL_NAME[ObTransIsolation::MAX_LEVEL] =
{
  "READ-UNCOMMITTED",
  "READ-COMMITTED",
  "REPEATABLE-READ",
  "SERIALIZABLE"
};

int32_t ObTransIsolation::get_level(const ObString &level_name)
{
  int32_t level = UNKNOWN;
  for (int32_t i = 0; i < MAX_LEVEL; i++) {
    if (0 == LEVEL_NAME[i].case_compare(level_name)) {
      level = i;
    }
  }
  return level;
}

const ObString &ObTransIsolation::get_name(int32_t level)
{
  static const ObString EMPTY_NAME;
  const ObString *level_name = &EMPTY_NAME;
  if (ObTransIsolation::UNKNOWN < level && level < ObTransIsolation::MAX_LEVEL) {
    level_name = &LEVEL_NAME[level];
  }
  return *level_name;
}

int ObMemtableKeyInfo::init(const uint64_t hash_val)
{
  int ret = OB_SUCCESS;

  if (hash_val == 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable key info init fail", KR(ret), K(hash_val));
  } else {
    hash_val_ = hash_val;
  }

  return ret;
}

void ObMemtableKeyInfo::reset()
{
  hash_val_ = 0;
  row_lock_ = NULL;
  buf_[0] = '\0';
}

bool ObMemtableKeyInfo::operator==(const ObMemtableKeyInfo &other) const
{
  return hash_val_ == other.get_hash_val();
}

void ObElrTransInfo::reset()
{
  trans_id_.reset();
  commit_version_.reset();
  result_ = ObTransResultState::UNKNOWN;
  ctx_id_ = 0;
}

int ObElrTransInfo::init(const ObTransID &trans_id, uint32_t ctx_id, const SCN commit_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!trans_id.is_valid()) || ctx_id <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(trans_id), K(ctx_id), K(commit_version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    trans_id_ = trans_id;
    commit_version_ = commit_version;
    ctx_id_ = ctx_id;
  }

  return ret;
}

void ObTransTask::reset()
{
  retry_interval_us_ = 0;
  next_handle_ts_ = 0;
  task_type_ = ObTransRetryTaskType::UNKNOWN;
}

int ObTransTask::make(const int64_t task_type)
{
  int ret = OB_SUCCESS;

  if (!ObTransRetryTaskType::is_valid(task_type)) {
    TRANS_LOG(WARN, "invalid argument", K(task_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    task_type_ = task_type;
  }

  return ret;
}

int ObTransTask::set_retry_interval_us(const int64_t start_interval_us, const int64_t retry_interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(retry_interval_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(retry_interval_us));
  } else {
    retry_interval_us_ = retry_interval_us;
    next_handle_ts_ = ObTimeUtility::current_time() + start_interval_us;
  }

  return ret;
}

bool ObTransTask::ready_to_handle()
{
  bool boot_ret = false;;
  int64_t current_ts = ObTimeUtility::current_time();

  if (current_ts >= next_handle_ts_) {
    boot_ret = true;
    next_handle_ts_ = current_ts + retry_interval_us_;
  } else {
    int64_t left_time = next_handle_ts_ - current_ts;
    if (left_time > RETRY_SLEEP_TIME_US) {
      ob_usleep(RETRY_SLEEP_TIME_US);
      boot_ret = false;
    } else {
      ob_usleep(left_time);
      boot_ret = true;
      next_handle_ts_ += retry_interval_us_;
    }
  }

  return boot_ret;
}

ObPartitionAuditInfo& ObPartitionAuditInfo::operator=(const ObPartitionAuditInfo &other)
{
  if (this != &other) {
    this->base_row_count_ = other.base_row_count_;
    this->insert_row_count_ = other.insert_row_count_;
    this->delete_row_count_ = other.delete_row_count_;
    this->update_row_count_ = other.update_row_count_;
    this->query_row_count_ = other.query_row_count_;
    this->insert_sql_count_ = other.insert_sql_count_;
    this->delete_sql_count_ = other.delete_sql_count_;
    this->update_sql_count_ = other.update_sql_count_;
    this->query_sql_count_ = other.query_sql_count_;
    this->trans_count_ = other.trans_count_;
    this->sql_count_ = other.sql_count_;
    this->rollback_insert_row_count_ = other.rollback_insert_row_count_;
    this->rollback_delete_row_count_ = other.rollback_delete_row_count_;
    this->rollback_update_row_count_ = other.rollback_update_row_count_;
    this->rollback_insert_sql_count_ = other.rollback_insert_sql_count_;
    this->rollback_delete_sql_count_ = other.rollback_delete_sql_count_;
    this->rollback_update_sql_count_ = other.rollback_update_sql_count_;
    this->rollback_trans_count_ = other.rollback_trans_count_;
    this->rollback_sql_count_ = other.rollback_sql_count_;
  }
  return *this;
}

ObPartitionAuditInfo& ObPartitionAuditInfo::operator+=(const ObPartitionAuditInfo &other)
{
  this->base_row_count_ += other.base_row_count_;
  this->insert_row_count_ += other.insert_row_count_;
  this->delete_row_count_ += other.delete_row_count_;
  this->update_row_count_ += other.update_row_count_;
  this->query_row_count_ += other.query_row_count_;
  this->insert_sql_count_ += other.insert_sql_count_;
  this->delete_sql_count_ += other.delete_sql_count_;
  this->update_sql_count_ += other.update_sql_count_;
  this->query_sql_count_ += other.query_sql_count_;
  this->trans_count_ += other.trans_count_;
  this->sql_count_ += other.sql_count_;
  this->rollback_insert_row_count_ += other.rollback_insert_row_count_;
  this->rollback_delete_row_count_ += other.rollback_delete_row_count_;
  this->rollback_update_row_count_ += other.rollback_update_row_count_;
  this->rollback_insert_sql_count_ += other.rollback_insert_sql_count_;
  this->rollback_delete_sql_count_ += other.rollback_delete_sql_count_;
  this->rollback_update_sql_count_ += other.rollback_update_sql_count_;
  this->rollback_trans_count_ += other.rollback_trans_count_;
  this->rollback_sql_count_ += other.rollback_sql_count_;
  return *this;
}

int ObPartitionAuditInfo::update_audit_info(const ObPartitionAuditInfoCache &cache, const bool commit)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  query_row_count_ += cache.query_row_count_;
  insert_sql_count_ += cache.insert_sql_count_;
  delete_sql_count_ += cache.delete_sql_count_;
  update_sql_count_ += cache.update_sql_count_;
  query_sql_count_ += cache.query_sql_count_;
  sql_count_ += cache.sql_count_;
  if (commit) {
    insert_row_count_ += cache.insert_row_count_;
    delete_row_count_ += cache.delete_row_count_;
    update_row_count_ += cache.update_row_count_;
    rollback_insert_row_count_ += cache.rollback_insert_row_count_;
    rollback_delete_row_count_ += cache.rollback_delete_row_count_;
    rollback_update_row_count_ += cache.rollback_update_row_count_;
    trans_count_ += 1;
  } else {
    rollback_insert_row_count_ += (cache.insert_row_count_ + cache.rollback_insert_row_count_);
    rollback_delete_row_count_ += (cache.delete_row_count_ + cache.rollback_delete_row_count_);
    rollback_update_row_count_ += (cache.update_row_count_ + cache.rollback_update_row_count_);
    rollback_trans_count_ += 1;
  }
  rollback_insert_sql_count_ += cache.rollback_insert_sql_count_;
  rollback_delete_sql_count_ += cache.rollback_delete_sql_count_;
  rollback_update_sql_count_ += cache.rollback_update_sql_count_;
  rollback_sql_count_ += cache.rollback_sql_count_;
  return ret;
}

int ObPartitionAuditInfoCache::update_audit_info(const enum ObPartitionAuditOperator op, const int32_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op >= PART_AUDIT_OP_MAX || count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(op), K(count));
  } else {
    switch (op) {
      case PART_AUDIT_SET_BASE_ROW_COUNT: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_INSERT_ROW: {
        cur_insert_row_count_ += count;
        break;
      }
      case PART_AUDIT_DELETE_ROW: {
        cur_delete_row_count_ += count;
        break;
      }
      case PART_AUDIT_UPDATE_ROW: {
        cur_update_row_count_ += count;
        break;
      }
      case PART_AUDIT_QUERY_ROW: {
        query_row_count_ += count;
        break;
      }
      case PART_AUDIT_INSERT_SQL: {
        insert_sql_count_ += count;
        break;
      }
      case PART_AUDIT_DELETE_SQL: {
        delete_sql_count_ += count;
        break;
      }
      case PART_AUDIT_UPDATE_SQL: {
        update_sql_count_ += count;
        break;
      }
      case PART_AUDIT_QUERY_SQL: {
        query_sql_count_ += count;
        break;
      }
      case PART_AUDIT_TRANS: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_SQL: {
        sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_INSERT_ROW: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_DELETE_ROW: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_UPDATE_ROW: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_INSERT_SQL: {
        rollback_insert_sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_DELETE_SQL: {
        rollback_delete_sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_UPDATE_SQL: {
        rollback_update_sql_count_ += count;
        break;
      }
      case PART_AUDIT_ROLLBACK_TRANS: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
      case PART_AUDIT_ROLLBACK_SQL: {
        rollback_sql_count_ += count;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        TRANS_LOG(WARN, "operation not supported", KR(ret), K(op));
        break;
      }
    }
  }
  return ret;
}

int ObPartitionAuditInfoCache::stmt_end_update_audit_info(bool commit)
{
  int ret = OB_SUCCESS;

  if (commit) {
    insert_row_count_ += cur_insert_row_count_;
    delete_row_count_ += cur_delete_row_count_;
    update_row_count_ += cur_update_row_count_;
  } else {
    rollback_insert_row_count_ += cur_insert_row_count_;
    rollback_delete_row_count_ += cur_delete_row_count_;
    rollback_update_row_count_ += cur_update_row_count_;
  }
  cur_insert_row_count_ = 0;
  cur_delete_row_count_ = 0;
  cur_update_row_count_ = 0;

  return ret;
}

void ObCoreLocalPartitionAuditInfo::reset()
{
  if (NULL != val_array_) {
    for (int i = 0; i < array_len_; i++) {
      ObPartitionAuditInfoFactory::release(VAL_ARRAY_AT(ObPartitionAuditInfo*, i));
    }
    ob_free(val_array_);
    val_array_ = NULL;
  }
  core_num_ = 0;
  array_len_ = 0;
  is_inited_ = false;
}

int ObCoreLocalPartitionAuditInfo::init(int64_t array_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObCoreLocalStorage<ObPartitionAuditInfo*>::init(array_len))) {
    TRANS_LOG(WARN, "ObCoreLocalStorage init error", KR(ret), K(array_len));
  } else {
    int alloc_succ_pos = 0;
    ObPartitionAuditInfo *info = NULL;
    for (int i = 0; OB_SUCC(ret) && i < array_len; i++) {
      if (OB_ISNULL(info = ObPartitionAuditInfoFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc ObPartitionAuditInfo error", KR(ret));
      } else {
        VAL_ARRAY_AT(ObPartitionAuditInfo*, i) = info;
        alloc_succ_pos = i + 1;
      }
    }
    if (OB_FAIL(ret)) {
      for (int i = 0; i < alloc_succ_pos; i++) {
        ObPartitionAuditInfoFactory::release(VAL_ARRAY_AT(ObPartitionAuditInfo*, i));
      }
    }
  }

  return ret;
}

bool ObStateInfo::need_update(const ObStateInfo &state_info)
{
  bool need_update = true;
  if (ObTxState::PRE_COMMIT <= state_ && state_ <= ObTxState::CLEAR) {
    need_update = false;
  } else if (snapshot_version_ > state_info.snapshot_version_) {
    need_update = false;
  } else if (state_info.state_ < state_) {
    need_update = false;
  }
  return need_update;
}

void ObAddrLogId::reset()
{
  addr_.reset();
  log_id_ = 0;
}

bool ObAddrLogId::operator==(const ObAddrLogId &other) const
{
  return (addr_ == other.addr_) && (log_id_ == other.log_id_);
}

int64_t ObTransNeedWaitWrap::get_remaining_wait_interval_us() const
{
  int64_t ret_val = 0;

  if (receive_gts_ts_ <= MonotonicTs(0)) {
    ret_val = 0;
  } else if (need_wait_interval_us_ <= 0) {
    ret_val = 0;
  } else {
    MonotonicTs tmp_ts = MonotonicTs(need_wait_interval_us_) - (MonotonicTs::current_time() - receive_gts_ts_);
    ret_val = tmp_ts.mts_;
    ret_val = ret_val > 0 ? ret_val : 0;
  }

  return ret_val;
}

void ObTransNeedWaitWrap::set_trans_need_wait_wrap(const MonotonicTs receive_gts_ts,
                                                   const int64_t need_wait_interval_us)
{
  if (need_wait_interval_us > 0) {
    receive_gts_ts_ = receive_gts_ts;
    need_wait_interval_us_ = need_wait_interval_us;
  }
}

OB_SERIALIZE_MEMBER(ObUndoAction, undo_from_, undo_to_);

int ObUndoAction::merge(const ObUndoAction &other)
{
  int ret = OB_SUCCESS;

  if (!is_contain(other)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(*this), K(other));
  } else {
    undo_from_ = MAX(undo_from_, other.undo_from_);
    undo_to_ = MIN(undo_to_, other.undo_to_);
  }

  return ret;
}

int ObEndParticipantsRes::add_blocked_trans_id(const ObTransID &trans_id)
{
  return blocked_trans_ids_.push_back(trans_id);
}

int ObEndParticipantsRes::assign(const ObEndParticipantsRes &other)
{
  return blocked_trans_ids_.assign(other.blocked_trans_ids_);
}

ObBlockedTransArray &ObEndParticipantsRes::get_blocked_trans_ids()
{
  return blocked_trans_ids_;
}

DEF_TO_STRING(ObLockForReadArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(mvcc_acc_ctx_), K(data_trans_id_), K(data_sql_sequence_), K(read_latest_));
  J_OBJ_END();
  return pos;
}

DEFINE_TO_STRING_AND_YSON(ObTransKey, OB_ID(hash), hash_val_,
                                      OB_ID(trans_id), trans_id_);

void ObTxMDSRange::reset()
{
  list_ptr_ = nullptr;
  start_iter_ = ObTxBufferNodeList::iterator();
  count_ = 0;
}

void ObTxMDSRange::clear()
{
  list_ptr_ = nullptr;
  start_iter_ = ObTxBufferNodeList::iterator();
}

int ObTxMDSRange::init(ObTxBufferNodeList *list_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(list_ptr_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(list_ptr)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    list_ptr_ = list_ptr;
    start_iter_ = list_ptr_->end();
    count_ = 0;
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "init MDS range failed", K(ret));
  }

  return ret;
}

int ObTxMDSRange::update_range(ObTxBufferNodeList::iterator iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(list_ptr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MDS range is not init", K(ret));
  } else if (iter == list_ptr_->end()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid iter", K(ret));
  } else if (start_iter_ == list_ptr_->end() || 0 == count_) {
    start_iter_ = iter;
    count_ = 1;
  } else {
    count_++;
  }

  return ret;
}

int ObTxMDSRange::move_to(ObTxBufferNodeArray &tx_buffer_node_arr)
{
  int ret = OB_SUCCESS;

  ObTxBufferNodeList::iterator del_iterator, next_iterator;

  if (OB_ISNULL(list_ptr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MDS range is not init", K(ret));
  } else if (start_iter_ == list_ptr_->end() || 0 == count_) {
    // empty MDS range
    TRANS_LOG(WARN, "use empty mds range when move", K(this), K(lbt()));
  } else {
    int64_t i = 0;
    del_iterator = list_ptr_->end();
    next_iterator = start_iter_;

    for (i = 0; i < count_ && OB_SUCC(ret) && next_iterator != list_ptr_->end(); i++) {
      del_iterator = next_iterator;
      next_iterator++;

      if (!del_iterator->is_submitted()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "try to move unsubmitted MDS node", K(ret));
      } else if (OB_FALSE_IT(del_iterator->set_synced())) {
        TRANS_LOG(WARN, "set synced MDS node failed", K(*del_iterator));
      } else if (OB_FAIL(tx_buffer_node_arr.push_back(*del_iterator))) {
        TRANS_LOG(WARN, "push back MDS node failed", K(ret));
      } else if (OB_FAIL(list_ptr_->erase(del_iterator))) {
        TRANS_LOG(WARN, "earse from MDS list failed", K(ret));
      }
    }
  }

  return ret;
}

int ObTxMDSRange::copy_to(ObTxBufferNodeArray &tx_buffer_node_arr) const
{
  int ret = OB_SUCCESS;

  ObTxBufferNodeList::iterator next_iterator;

  if (OB_ISNULL(list_ptr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MDS range is not init", K(ret));
  } else if (start_iter_ == list_ptr_->end() || 0 == count_) {
    // empty MDS range
    TRANS_LOG(WARN, "use empty mds range when copy", K(this), K(lbt()));
  } else {
    int64_t i = 0;
    next_iterator = start_iter_;

    for (i = 0; i < count_ && OB_SUCC(ret) && next_iterator != list_ptr_->end();
         i++, next_iterator++) {
      if (OB_FAIL(tx_buffer_node_arr.push_back(*next_iterator))) {
        TRANS_LOG(WARN, "push back MDS node failed", K(ret));
      }
    }
  }

  return ret;
}

int ObTxMDSRange::range_submitted(ObTxMDSCache &cache)
{
  int ret = OB_SUCCESS;
  ObTxBufferNodeList::iterator next_iterator;
  int64_t i = 0;

  if (OB_ISNULL(list_ptr_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MDS range is not init", K(ret));
  } else if (start_iter_ == list_ptr_->end() || 0 == count_) {
    // empty MDS range
    TRANS_LOG(WARN, "use empty mds range when submit range", K(cache), K(this), K(lbt()));
  } else {
    next_iterator = start_iter_;

    for (i = 0; i < count_ && OB_SUCC(ret) && next_iterator != list_ptr_->end();
         i++, next_iterator++) {
      next_iterator->set_submitted();
      cache.update_submitted_iterator(next_iterator);
    }
  }

  return ret;
}

void ObTxMDSRange::range_sync_failed()
{
  ObTxBufferNodeList::iterator next_iterator;
  int64_t i = 0;

  next_iterator = start_iter_;

  for (i = 0; i < count_ && next_iterator != list_ptr_->end(); i++, next_iterator++) {
    next_iterator->log_sync_fail();
  }
}

void ObTxMDSCache::reset()
{
  // allocator_.reset();
  unsubmitted_size_ = 0;
  mds_list_.reset();
  submitted_iterator_ = mds_list_.end();//ObTxBufferNodeList::iterator();
}

void ObTxMDSCache::destroy()
{
  unsubmitted_size_ = 0;
  ObTxBufferNode tmp_node;
  while(!mds_list_.empty())
  {
    mds_list_.pop_front(tmp_node);
    if (nullptr != tmp_node.data_.ptr()) {
        share::mtl_free(tmp_node.data_.ptr());
    }
  }
}

int ObTxMDSCache::insert_mds_node(const ObTxBufferNode &buf_node)
{
  int ret = OB_SUCCESS;

  if (!buf_node.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "insert MDS buf node", K(ret));
  } else if (mds_list_.push_back(buf_node)) {
    TRANS_LOG(WARN, "push back MDS buf node", K(ret));
  } else {
    unsubmitted_size_ += buf_node.get_serialize_size();
  }

  return ret;
}

int ObTxMDSCache::rollback_last_mds_node()
{
  int ret = OB_SUCCESS;

  ObTxBufferNode buf_node = mds_list_.get_last();
  if (OB_FAIL(mds_list_.pop_back())) {
    TRANS_LOG(WARN, "pop back last node failed", K(ret));
  } else {
    share::mtl_free(buf_node.get_ptr());
  }

  clear_submitted_iterator();

  return ret;
}

int ObTxMDSCache::fill_mds_log(ObTxMultiDataSourceLog &mds_log,
                               ObTxMDSRange &mds_range,
                               bool &need_pre_replay_barrier)
{
  int ret = OB_SUCCESS;

  mds_range.reset();

  if (OB_FAIL(mds_range.init(&mds_list_))) {
    TRANS_LOG(WARN, "init mds range failed", K(ret));
  } else {
    if (submitted_iterator_ == mds_list_.end()) {
      submitted_iterator_ = mds_list_.begin();
    }
    ObTxBufferNodeList::iterator iter = submitted_iterator_;
    for (; iter != mds_list_.end() && OB_SUCC(ret); iter++) {
      if (iter->is_submitted()) {
        // do nothing
      } else if (OB_FAIL(mds_log.fill_MDS_data(*iter))) {
        if (ret != OB_SIZE_OVERFLOW) {
          TRANS_LOG(WARN, "fill mds data in log failed", K(ret));
        }
      } else if (OB_FAIL(mds_range.update_range(iter))) {
        TRANS_LOG(WARN, "update mds range failed", K(ret), K(iter));
      } else if (need_pre_replay_barrier) {
        //has been pre replay barrier
      } else if (OB_FALSE_IT(
                     need_pre_replay_barrier = ObTxLogTypeChecker::need_pre_replay_barrier(
                         ObTxLogType::TX_MULTI_DATA_SOURCE_LOG, iter->get_data_source_type()))) {
        // set need_barrier flag
      }
    }
  }

  if (OB_SIZE_OVERFLOW == ret) {
    ret = OB_EAGAIN;
  }

  if (mds_log.count() != mds_range.count()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unmatched mds_range and mds_log", K(mds_log), K(mds_range));
  } else if (0 == mds_range.count() && 0 == mds_log.count()) {
    if (OB_EAGAIN == ret) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected large buf node", K(ret), K(mds_range), K(mds_log), K(this));

    } else {
      ret = OB_EMPTY_RANGE;
    }
  }

  return ret;
}

int ObTxMDSCache::copy_to(ObTxBufferNodeArray &tmp_array) const
{
  int ret = OB_SUCCESS;

  ObTxBufferNodeList::const_iterator iter = mds_list_.begin();
  for (; iter != mds_list_.end() && OB_SUCC(ret); iter++) {
    if (OB_FAIL(tmp_array.push_back(*iter))) {
      TRANS_LOG(WARN, "push back failed", K(ret), K(*iter));
    }
  }

  return ret;
}

bool ObTxMDSCache::is_contain(const ObTxDataSourceType target_type) const
{
  bool contain = false;
  ObTxBufferNodeList::const_iterator iter = mds_list_.begin();
  for (; iter != mds_list_.end(); iter++) {
    if (iter->get_data_source_type() == target_type) {
      contain = true;
      break;
    }
  }
  return contain;
}

void ObTxExecInfo::reset()
{
  state_ = ObTxState::INIT;
  upstream_.reset();
  participants_.reset();
  incremental_participants_.reset();
  prev_record_lsn_.reset();
  redo_lsns_.reset();
  scheduler_.reset();
  prepare_version_.reset();
  trans_type_ = TransType::SP_TRANS;
  next_log_entry_no_ = 0;
  max_applied_log_ts_.reset();
  max_applying_log_ts_.reset();
  max_applying_part_log_no_ = INT64_MAX;
  max_submitted_seq_no_ = 0;
  checksum_ = 0;
  checksum_scn_.set_min();
  max_durable_lsn_.reset();
  data_complete_ = false;
  is_dup_tx_ = false;
  //touched_pkeys_.reset();
  multi_data_source_.reset();
  prepare_log_info_arr_.reset();
  xid_.reset();
  need_checksum_ = true;
  tablet_modify_record_.reset();
  is_sub2pc_ = false;
}

void ObTxExecInfo::destroy()
{
  for (int64_t i = 0; i < multi_data_source_.count(); ++i) {
    ObTxBufferNode &node = multi_data_source_.at(i);
    if (nullptr != node.data_.ptr()) {
      share::mtl_free(node.data_.ptr());
    }
  }
  reset();
}

OB_SERIALIZE_MEMBER(ObTxExecInfo,
                    state_,
                    upstream_,
                    participants_,
                    incremental_participants_,
                    prev_record_lsn_,
                    redo_lsns_,
                    multi_data_source_,
                    scheduler_,
                    prepare_version_,
                    trans_type_,
                    next_log_entry_no_,
                    max_applying_log_ts_,
                    max_applied_log_ts_,
                    max_applying_part_log_no_,
                    max_submitted_seq_no_,
                    checksum_,
                    checksum_scn_,
                    max_durable_lsn_,
                    data_complete_,
                    is_dup_tx_,
//                    touched_pkeys_,
                    prepare_log_info_arr_,
                    xid_,
                    need_checksum_,
                    tablet_modify_record_,
                    is_sub2pc_);

bool ObMulSourceDataNotifyArg::is_redo_submitted() const { return redo_submitted_; }

bool ObMulSourceDataNotifyArg::is_redo_confirmed() const
{
  bool redo_confirmed = false;

  if (redo_submitted_
      && (NotifyType::ON_PREPARE == notify_type_ || NotifyType::ON_ABORT == notify_type_
          || NotifyType::ON_COMMIT == notify_type_)) {
    redo_confirmed = true;
  }

  return redo_confirmed;
}

bool ObMulSourceDataNotifyArg::is_redo_synced() const { return redo_synced_; }

const char *trans_type_to_cstr(const TransType &trans_type)
{
  const char *str;
  switch (trans_type) {
    case TransType::UNKNOWN_TRANS:
      str = "UNKNOWN";
      break;
    case TransType::SP_TRANS:
      str = "SP";
      break;
    case TransType::DIST_TRANS:
      str = "DIST";
      break;
    default:
      str = "TX_TYPE_UNKNOWN";
      break;
  }
  return str;
}
} // transaction
} // oceanbase
