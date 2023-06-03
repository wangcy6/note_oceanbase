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

#include "storage/tx/ob_tx_log.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "common/cell/ob_cell_reader.h"

namespace oceanbase
{

using namespace common;
using namespace share;
namespace transaction
{

bool ObTxLogTypeChecker::need_pre_replay_barrier(const ObTxLogType log_type,
                                                 const ObTxDataSourceType data_source_type)
{
  bool need_barrier = false;

  //multi data source trans's redo log
  if (ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type) {
    if (data_source_type == ObTxDataSourceType::CREATE_TABLET
        || data_source_type == ObTxDataSourceType::REMOVE_TABLET
        || data_source_type == ObTxDataSourceType::MODIFY_TABLET_BINDING) {
      need_barrier = true;
    }
  }

  return need_barrier;
}

// ============================== Tx Log Header =============================

DEFINE_SERIALIZE(ObTxLogHeader)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, tmp_pos, static_cast<int64_t>(tx_log_type_)))) {
  } else {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTxLogHeader)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  int64_t log_type = 0;
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &log_type))) {
  } else {
    tx_log_type_ = static_cast<ObTxLogType>(log_type);
    pos = tmp_pos;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTxLogHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(static_cast<int64_t>(tx_log_type_));
  return size;
}

// ============================== Tx Log serialization =============================

int ObCtxRedoInfo::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(1))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  return ret;
}

OB_TX_SERIALIZE_MEMBER(ObCtxRedoInfo, compat_bytes_, cluster_version_);

// RedoLogBody serialize mutator_buf in log block
OB_DEF_SERIALIZE(ObTxRedoLog)
{
  int ret = OB_SUCCESS;
  uint32_t tmp_size = 0;
  int64_t tmp_pos = pos;
  if (mutator_size_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "INVALID mutator_buf_");
  } else if (OB_FAIL(ctx_redo_info_.serialize(buf, buf_len, tmp_pos))) {
    TRANS_LOG(WARN, "ctx_redo_info_ serialize failed", K(ret));
    // } else if (OB_FAIL(clog_encrypt_info_.serialize(buf, buf_len, tmp_pos))) {
    //   TRANS_LOG(WARN, "clog_encrypt_info_ serialize error", K(ret));
    // } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, tmp_pos,
    // static_cast<int64_t>(cluster_version_)))) {
    //   TRANS_LOG(WARN, "cluster_version_ serialize error", K(ret));
  } else if ((tmp_size = static_cast<uint32_t>(mutator_size_))
             && OB_FAIL(serialization::encode_i32(buf, buf_len, tmp_pos, tmp_size))) {
    TRANS_LOG(WARN, "encode mutator_size_ error", K(ret));
  } else {
    pos = tmp_pos + mutator_size_;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTxRedoLog)
{
  int ret = OB_SUCCESS;
  int64_t org_pos = pos;
  int32_t tmp_size = 0;

  if(OB_FAIL(ctx_redo_info_.deserialize(buf,data_len,pos)))
  {
    TRANS_LOG(WARN, "ctx_redo_info_ deserialize failed",K(ret));
  // if (OB_FAIL(clog_encrypt_info_.deserialize(buf, data_len, pos))) {
  //   TRANS_LOG(WARN, "deserialize clog_encrypt_info_ error", K(ret));
  // } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tmp_cluster_version))) {
  //   TRANS_LOG(WARN, "decode cluster_version_ error", K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &tmp_size))) {
    TRANS_LOG(WARN, "decode mutator_size_ error", K(ret));
  } else {
    // cluster_version_ = static_cast<uint64_t>(tmp_cluster_version);
    mutator_size_ = static_cast<int64_t>(tmp_size);
    replay_mutator_buf_ = buf + pos;
    pos = pos + mutator_size_;
  }
  if (OB_FAIL(ret)) {
    pos = org_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTxRedoLog)
{
  int64_t len = 0;
  if (mutator_size_ < 0) {
    len = mutator_size_;
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "mutator_buf_ has not set");
  } else {
    len = len + ctx_redo_info_.get_serialize_size();
    len = len + MUTATOR_SIZE_NEED_BYTES;
    len = len + mutator_size_;
  }
  return len;
}

// Other LogBody
OB_TX_SERIALIZE_MEMBER(ObTxActiveInfoLog,
                       compat_bytes_,
                       /* 1 */ scheduler_,
                       /* 2 */ trans_type_,
                       /* 3 */ session_id_,
                       /* 4 */ app_trace_id_str_,
                       /* 5 */ schema_version_,
                       /* 6 */ can_elr_,
                       /* 7 */ proposal_leader_,
                       /* 8 */ cur_query_start_time_,
                       /* 9 */ is_sub2pc_,
                       /* 10 */ is_dup_tx_,
                       /* 11 */ tx_expired_time_,
                       /* 12 */ epoch_,
                       /* 13 */ last_op_sn_,
                       /* 14 */ first_sn,
                       /* 15 */ last_sn,
                       /* 16 */ cluster_version_,
                       /* 17 */ max_submitted_seq_no_,
                       /* 18 */ xid_);

OB_TX_SERIALIZE_MEMBER(ObTxCommitInfoLog,
                       compat_bytes_,
                       /* 1 */ scheduler_,
                       /* 2 */ participants_,
                       /* 3 */ upstream_,
                       /* 4 */ is_sub2pc_,
                       /* 5 */ is_dup_tx_,
                       /* 6 */ can_elr_,
                       /* 7 */ incremental_participants_,
                       /* 8 */ cluster_version_,
                       /* 9 */ app_trace_id_str_,
                       /* 10 */ app_trace_info_,
                       /* 11 */ prev_record_lsn_,
                       /* 12 */ redo_lsns_,
                       /* 13 */ xid_);

OB_TX_SERIALIZE_MEMBER(ObTxPrepareLog,
                       compat_bytes_,
                       /* 1 */ incremental_participants_,
                       /* 2 */ prev_lsn_);

OB_TX_SERIALIZE_MEMBER(ObTxCommitLog,
                       compat_bytes_,
                       /* 1 */ commit_version_,
                       /* 2 */ checksum_,
                       /* 3 */ incremental_participants_,
                       /* 4 */ multi_source_data_,
                       /* 5 */ trans_type_,
                       /* 6 */ tx_data_backup_,
                       /* 7 */ prev_lsn_,
                       /* 8 */ ls_log_info_arr_);

OB_TX_SERIALIZE_MEMBER(ObTxClearLog, compat_bytes_, /* 1 */ incremental_participants_);

OB_TX_SERIALIZE_MEMBER(ObTxAbortLog,
                    compat_bytes_,
                    /* 1 */ multi_source_data_,
                    /* 2 */ tx_data_backup_);

OB_TX_SERIALIZE_MEMBER(ObTxRecordLog, compat_bytes_, /* 1 */ prev_record_lsn_, /* 2 */ redo_lsns_);

OB_TX_SERIALIZE_MEMBER(ObTxStartWorkingLog, compat_bytes_, /* 1 */ leader_epoch_);

OB_TX_SERIALIZE_MEMBER(ObTxRollbackToLog, compat_bytes_, /* 1 */ from_, /* 2 */ to_);

OB_TX_SERIALIZE_MEMBER(ObTxMultiDataSourceLog, compat_bytes_, /* 1 */ data_);

int ObTxActiveInfoLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(18))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(scheduler_.is_valid() == false, 1, compat_bytes_);
    TX_NO_NEED_SER(trans_type_ == TransType::UNKNOWN_TRANS, 2, compat_bytes_); /*trans_type_*/
    TX_NO_NEED_SER(session_id_ == 0, 3, compat_bytes_);
    TX_NO_NEED_SER(app_trace_id_str_.empty(), 4, compat_bytes_);
    TX_NO_NEED_SER(schema_version_ == 0, 5, compat_bytes_);
    TX_NO_NEED_SER(can_elr_ == false, 6, compat_bytes_);
    TX_NO_NEED_SER(proposal_leader_.is_valid() == false, 7, compat_bytes_);
    TX_NO_NEED_SER(cur_query_start_time_ == 0, 8, compat_bytes_);
    TX_NO_NEED_SER(is_sub2pc_ == false, 9, compat_bytes_);
    TX_NO_NEED_SER(is_dup_tx_ == false, 10, compat_bytes_);
    TX_NO_NEED_SER(tx_expired_time_ == 0, 11, compat_bytes_);
    TX_NO_NEED_SER(epoch_ == 0, 12, compat_bytes_);
    TX_NO_NEED_SER(last_op_sn_ == 0, 13, compat_bytes_);
    TX_NO_NEED_SER(first_sn == 0, 14, compat_bytes_);
    TX_NO_NEED_SER(last_sn == 0, 15, compat_bytes_);
    TX_NO_NEED_SER(cluster_version_ == 0, 16, compat_bytes_);
    TX_NO_NEED_SER(max_submitted_seq_no_ == 0, 17, compat_bytes_);
    TX_NO_NEED_SER(xid_.empty(), 18, compat_bytes_);
  }

  return ret;
}

int ObTxCommitInfoLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(13))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(scheduler_.is_valid() == false, 1, compat_bytes_);
    TX_NO_NEED_SER(participants_.empty(), 2, compat_bytes_);
    TX_NO_NEED_SER(upstream_.is_valid() == false, 3, compat_bytes_);
    TX_NO_NEED_SER(is_sub2pc_ == false, 4, compat_bytes_);
    TX_NO_NEED_SER(is_dup_tx_ == false, 5, compat_bytes_);
    TX_NO_NEED_SER(can_elr_ == false, 6, compat_bytes_);
    TX_NO_NEED_SER(incremental_participants_.empty(), 7, compat_bytes_);
    TX_NO_NEED_SER(cluster_version_ == 0, 8, compat_bytes_);
    TX_NO_NEED_SER(app_trace_id_str_.empty(), 9, compat_bytes_);
    TX_NO_NEED_SER(app_trace_info_.empty(), 10, compat_bytes_);
    TX_NO_NEED_SER(prev_record_lsn_.is_valid() == false, 11, compat_bytes_);
    TX_NO_NEED_SER(redo_lsns_.empty(), 12, compat_bytes_);
    TX_NO_NEED_SER(xid_.empty(), 13, compat_bytes_);
  }

  return ret;
}

int ObTxPrepareLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(incremental_participants_.empty(), 1, compat_bytes_);
    TX_NO_NEED_SER(prev_lsn_.is_valid() == false, 2, compat_bytes_);
  }
  return ret;
}

int ObTxCommitLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
<<<<<<< HEAD
=======
    if (OB_FAIL(compat_bytes_.init(8))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    TX_NO_NEED_SER(!commit_version_.is_valid(), 1, compat_bytes_);
    TX_NO_NEED_SER(checksum_ == 0, 2, compat_bytes_);
    TX_NO_NEED_SER(incremental_participants_.empty(), 3, compat_bytes_);
    TX_NO_NEED_SER(multi_source_data_.empty(), 4, compat_bytes_);
    TX_NO_NEED_SER(trans_type_ == TransType::UNKNOWN_TRANS, 5, compat_bytes_); /*trans_type_*/
    TX_NO_NEED_SER(false, 6, compat_bytes_);                                   // tx_data_backup_
    TX_NO_NEED_SER(prev_lsn_.is_valid() == false, 7, compat_bytes_);
    TX_NO_NEED_SER(ls_log_info_arr_.empty(), 8, compat_bytes_);
  }
  return ret;
}

int ObTxClearLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(1))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(incremental_participants_.empty(), 1, compat_bytes_);
  }

  return ret;
}

int ObTxAbortLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(multi_source_data_.empty(), 1, compat_bytes_);
    TX_NO_NEED_SER(false, 2, compat_bytes_); // tx_data_backup_
  }

  return ret;
}

int ObTxRecordLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(prev_record_lsn_.is_valid() == false, 1, compat_bytes_);
    TX_NO_NEED_SER(redo_lsns_.empty(), 2, compat_bytes_);
  }
  return ret;
}

int ObTxStartWorkingLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(leader_epoch_ == 0, 1, compat_bytes_);
  }
  return ret;
}

int ObTxRollbackToLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(false, 1, compat_bytes_);
    TX_NO_NEED_SER(false, 2, compat_bytes_);
  }

  return ret;
}

int ObTxMultiDataSourceLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(1))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(false, 1, compat_bytes_);
  }

  return ret;
}

// ============================== Tx Log Body ===========================

const ObTxLogType ObTxRedoLog::LOG_TYPE = ObTxLogType::TX_REDO_LOG;
const ObTxLogType ObTxActiveInfoLog::LOG_TYPE = ObTxLogType::TX_ACTIVE_INFO_LOG;
const ObTxLogType ObTxCommitInfoLog::LOG_TYPE = ObTxLogType::TX_COMMIT_INFO_LOG;
const ObTxLogType ObTxPrepareLog::LOG_TYPE = ObTxLogType::TX_PREPARE_LOG;
const ObTxLogType ObTxCommitLog::LOG_TYPE = ObTxLogType::TX_COMMIT_LOG;
const ObTxLogType ObTxClearLog::LOG_TYPE = ObTxLogType::TX_CLEAR_LOG;
const ObTxLogType ObTxAbortLog::LOG_TYPE = ObTxLogType::TX_ABORT_LOG;
const ObTxLogType ObTxRecordLog::LOG_TYPE = ObTxLogType::TX_RECORD_LOG;
// const ObTxLogType ObTxKeepAliveLog::LOG_TYPE = ObTxLogType::TX_KEEP_ALIVE_LOG;
const ObTxLogType ObTxStartWorkingLog::LOG_TYPE = ObTxLogType::TX_START_WORKING_LOG;
const ObTxLogType ObTxRollbackToLog::LOG_TYPE = ObTxLogType::TX_ROLLBACK_TO_LOG;
const ObTxLogType ObTxMultiDataSourceLog::LOG_TYPE = ObTxLogType::TX_MULTI_DATA_SOURCE_LOG;

int ObTxRedoLog::set_mutator_buf(char *buf)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || mutator_size_ >= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid mutator buf", K(buf), K(mutator_size_));
  } else {
    mutator_buf_ = buf;
  }
  return ret;
}

int ObTxRedoLog::set_mutator_size(const int64_t size, const bool after_fill)
{
  int ret = OB_SUCCESS;
  if (size < 0 || OB_ISNULL(mutator_buf_) || (!after_fill && mutator_size_ >= 0)
      || (after_fill && mutator_size_ < size)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument when set mutator size", K(after_fill), K(size),
               K(mutator_size_), K(mutator_buf_));
  } else if (!after_fill) {
    int len = 0;
    SERIALIZE_SIZE_HEADER(UNIS_VERSION);
    len = len + MUTATOR_SIZE_NEED_BYTES + ctx_redo_info_.get_serialize_size();
    if (size <= len) {
      ret = OB_SIZE_OVERFLOW;
      TRANS_LOG(WARN, "mutator buf is not enough", K(len), K(size));
    } else {
      mutator_size_ = size - len;
      mutator_buf_ = mutator_buf_ + len;
    }
  } else {
    mutator_size_ = size;
  }
  return ret;
}

void ObTxRedoLog::reset_mutator_buf()
{
  mutator_buf_ = nullptr;
  mutator_size_ = -1;
}

//TODO: if ob_admin_dump is called by others and clog is encrypted,
//      unused_encrypt_info can work. This may be perfected in the future.
int ObTxRedoLog::ob_admin_dump(memtable::ObMemtableMutatorIterator *iter_ptr,
                               ObAdminMutatorStringArg &arg,
                               palf::block_id_t block_id,
                               palf::LSN lsn,
                               int64_t tx_id,
                               SCN scn,
                               bool &has_dumped_tx_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  transaction::ObCLogEncryptInfo unused_encrypt_info;
  unused_encrypt_info.init();

    arg.log_stat_->tx_redo_log_size_ += get_serialize_size();
  if (OB_ISNULL(iter_ptr) || OB_ISNULL(arg.writer_ptr_) || OB_ISNULL(arg.buf_)
      || OB_NOT_NULL(mutator_buf_) || OB_ISNULL(replay_mutator_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KP(iter_ptr), KP(arg.writer_ptr_), KP(arg.buf_),
              KP(mutator_buf_), KP(replay_mutator_buf_));
  } else if (OB_FAIL(iter_ptr->deserialize(replay_mutator_buf_, mutator_size_, pos,
                                           unused_encrypt_info))) {
    TRANS_LOG(WARN, "deserialize replay_mutator_buf_ failed", K(ret));
  } else {
    bool has_output = false;
    arg.log_stat_->mutator_size_ += get_mutator_size();
    if (!arg.filter_.is_tablet_id_valid()) {
      arg.writer_ptr_->dump_key("###<TxRedoLog>");
      arg.writer_ptr_->start_object();
      arg.writer_ptr_->dump_key("txctxinfo");
      arg.writer_ptr_->dump_string(to_cstring(*this));
      arg.writer_ptr_->dump_key("MutatorMeta");
      arg.writer_ptr_->dump_string(to_cstring(iter_ptr->get_meta()));

      arg.writer_ptr_->dump_key("MutatorRows");
      arg.writer_ptr_->start_object();
      has_output = true;
    } else {
      if (!has_dumped_tx_id) {
        databuff_printf(arg.buf_, arg.buf_len_, arg.pos_, "{BlockID: %ld; LSN:%ld, TxID:%ld; SCN:%s",
                        block_id, lsn.val_, tx_id, to_cstring(scn));
      }
      databuff_printf(arg.buf_, arg.buf_len_, arg.pos_,
                      "<TxRedoLog>: {TxCtxInfo: {%s}; MutatorMeta: {%s}; MutatorRows: {",
                      to_cstring(*this), to_cstring(iter_ptr->get_meta()));
      //fill info in buf
    }
    bool has_dumped_meta_info = false;
    memtable::ObEncryptRowBuf unused_row_buf;
    while (OB_SUCC(iter_ptr->iterate_next_row(unused_row_buf, unused_encrypt_info))) {
      // arg.writer_ptr_->start_object();
      if (arg.filter_.is_tablet_id_valid()) {
        if (arg.filter_.get_tablet_id() != iter_ptr->get_row_head().tablet_id_) {
          TRANS_LOG(INFO, "just skip according to tablet_id", K(arg), K(iter_ptr->get_row_head()));
          continue;
        } else if (!has_dumped_meta_info) {
          arg.writer_ptr_->dump_string(arg.buf_);
          has_dumped_meta_info = true;
          has_dumped_tx_id = true;
          //print tx_id and RedoLog related info in arg
        }
      }
      has_output = true;
      arg.writer_ptr_->dump_key("RowHeader");
      arg.writer_ptr_->dump_string(to_cstring(iter_ptr->get_row_head()));

      switch (iter_ptr->get_row_head().mutator_type_) {
        case memtable::MutatorType::MUTATOR_ROW: {
          arg.writer_ptr_->dump_key("NORMAL_ROW");
          arg.writer_ptr_->start_object();
          arg.log_stat_->normal_row_count_++;
          if (OB_FAIL(format_mutator_row_(iter_ptr->get_mutator_row(), arg))) {
            TRANS_LOG(WARN, "format json mutator row failed", K(ret));
          }
          arg.writer_ptr_->end_object();
          break;
        }
        case memtable::MutatorType::MUTATOR_TABLE_LOCK: {
          arg.log_stat_->table_lock_count_++;
          arg.writer_ptr_->dump_key("TableLock");
          arg.writer_ptr_->dump_string(to_cstring(iter_ptr->get_table_lock_row()));
          break;
        }
        default: {
          arg.writer_ptr_->dump_key("ERROR:unknown mutator type");
          const int64_t mutator_type = static_cast<int64_t>(iter_ptr->get_row_head().mutator_type_);
          arg.writer_ptr_->dump_int64(mutator_type);
          ret = OB_NOT_SUPPORTED;
          TRANS_LOG(WARN, "ERROR:unknown mutator type", K(ret));
          break;
        }
      }
    }
    if (has_output) {
      //mutator row
      arg.writer_ptr_->end_object();
      //TxRedoLog
      arg.writer_ptr_->end_object();
    }
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "iterate_next_row failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObTxRedoLog::format_mutator_row_(const memtable::ObMemtableMutatorRow &row,
                                          ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;

  uint64_t table_id = OB_INVALID_ID;
  int64_t table_version = 0;
  uint32_t modify_count = 0;
  uint32_t acc_checksum = 0;
  int64_t version = 0;
  int32_t flag = 0;
  int64_t seq_no = 0;
  ObStoreRowkey rowkey;
  memtable::ObRowData new_row;
  memtable::ObRowData old_row;
  blocksstable::ObDmlFlag dml_flag = blocksstable::ObDmlFlag::DF_NOT_EXIST;

  if (OB_FAIL(row.copy(table_id, rowkey, table_version, new_row, old_row, dml_flag,
                       modify_count, acc_checksum, version, flag, seq_no))) {
    TRANS_LOG(WARN, "row_.copy fail", K(ret), K(table_id), K(rowkey), K(table_version), K(new_row),
              K(old_row), K(dml_flag), K(modify_count), K(acc_checksum), K(version));
  } else {
    arg.log_stat_->new_row_size_ += new_row.size_;
    arg.log_stat_->old_row_size_ += old_row.size_;
    arg.writer_ptr_->dump_key("RowKey");
    arg.writer_ptr_->dump_string(to_cstring(rowkey));
    arg.writer_ptr_->dump_key("TableVersion");
    arg.writer_ptr_->dump_int64(table_version);

    // new row
    arg.writer_ptr_->dump_key("NewRow Cols");
    arg.writer_ptr_->start_object();
    if (OB_FAIL(format_row_data_(new_row, arg))) {
      TRANS_LOG(WARN, "format new_row failed", K(ret));
    }
    arg.writer_ptr_->end_object();

    // old row
    arg.writer_ptr_->dump_key("OldRow Cols");
    arg.writer_ptr_->start_object();
    if (OB_FAIL(format_row_data_(old_row, arg))) {
      TRANS_LOG(WARN, "format old_row failed", K(ret));
    }
    arg.writer_ptr_->end_object();

    arg.writer_ptr_->dump_key("DmlFlag");
    arg.writer_ptr_->dump_string(get_dml_str(dml_flag));
    arg.writer_ptr_->dump_key("ModifyCount");
    arg.writer_ptr_->dump_uint64(modify_count);
    arg.writer_ptr_->dump_key("AccChecksum");
    arg.writer_ptr_->dump_uint64(acc_checksum);
    arg.writer_ptr_->dump_key("Version");
    arg.writer_ptr_->dump_int64(version);
    arg.writer_ptr_->dump_key("Flag");
    arg.writer_ptr_->dump_int64(flag);
    arg.writer_ptr_->dump_key("SeqNo");
    arg.writer_ptr_->dump_int64(seq_no);
    arg.writer_ptr_->dump_key("NewRowSize");
    arg.writer_ptr_->dump_int64(new_row.size_);
    arg.writer_ptr_->dump_key("OldRowSize");
    arg.writer_ptr_->dump_int64(old_row.size_);
  }
  return ret;
}

int ObTxRedoLog::format_row_data_(const memtable::ObRowData &row_data, ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;

  blocksstable::ObDatumRow datum_row;
  blocksstable::ObRowReader row_reader;
  const blocksstable::ObRowHeader *row_header = nullptr;
  if (row_data.size_ > 0) {
    if (OB_FAIL(row_reader.read_row(row_data.data_, row_data.size_, nullptr, datum_row))) {
      CLOG_LOG(WARN, "Failed to read datum row", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.get_column_count(); i++) {
      int64_t pos = 0;
      if (nullptr != arg.writer_ptr_) {
        sprintf(arg.buf_ + arg.pos_, "%lu", i);
        arg.writer_ptr_->dump_key(arg.buf_ + arg.pos_);
        pos = datum_row.storage_datums_[i].storage_to_string(arg.buf_ + arg.pos_, arg.buf_len_ - arg.pos_);
        arg.writer_ptr_->dump_string(arg.buf_ + arg.pos_);
      }
    }
  } else if (NULL == row_data.data_ && 0 == row_data.size_) {
  }
  return ret;
}

void ObTxMultiDataSourceLog::reset()
{
  compat_bytes_.reset();
  data_.reset();
  before_serialize();
}

int ObTxMultiDataSourceLog::fill_MDS_data(const ObTxBufferNode &node)
{
  int ret = OB_SUCCESS;

  if (node.get_serialize_size() + data_.get_serialize_size() >= MAX_MDS_LOG_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    TRANS_LOG(WARN, "MDS log is overflow", K(*this), K(node));
  } else {
    data_.push_back(node);
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObTxDataBackup, start_log_ts_);

int ObTxActiveInfoLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxActiveInfoLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxCommitInfoLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxCommitInfoLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Size");
    arg.writer_ptr_->dump_int64(get_serialize_size());
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxPrepareLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxPrepareLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxCommitLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxCommitLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Size");
    arg.writer_ptr_->dump_int64(get_serialize_size());
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxClearLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxClearLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxAbortLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxAbortLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxRecordLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxRecordLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxStartWorkingLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxStartWorkingLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxRollbackToLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("<TxRollbackToLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxMultiDataSourceLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("<TxMultiDataSourceLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("mds_count");
    arg.writer_ptr_->dump_string(to_cstring(data_.count()));

    arg.writer_ptr_->dump_key("mds_array");
    arg.writer_ptr_->start_object();
    for (int64_t i = 0; i < data_.count(); i++) {
      arg.writer_ptr_->dump_key("type");
        arg.writer_ptr_->dump_string(to_cstring(static_cast<int64_t>(data_[i].get_data_source_type())));
        arg.writer_ptr_->dump_key("buf_len");
        arg.writer_ptr_->dump_string(to_cstring(data_[i].get_data_size()));
        arg.writer_ptr_->dump_key("content");
        arg.writer_ptr_->dump_string(ObMulSourceTxDataDump::dump_buf(data_[i].get_data_source_type(),static_cast<char *>(data_[i].get_ptr()),data_[i].get_data_size()));
    }
    arg.writer_ptr_->end_object();

    arg.writer_ptr_->end_object();
  }
  return ret;
}

ObTxDataBackup::ObTxDataBackup() { reset(); }

int ObTxDataBackup::init(const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
<<<<<<< HEAD
  if (OB_ISNULL(tx_data)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KP(tx_data));
  } else {
    start_log_ts_ = tx_data->start_scn_;
  }
  return ret;
}

void ObTxDataBackup::reset()
{
  start_log_ts_.reset();
}
=======
  start_log_ts_ = start_scn;
  return ret;
}

void ObTxDataBackup::reset() { start_log_ts_.reset(); }
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

int ObTxCommitLog::init_tx_data_backup(const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_data_backup_.init(start_scn))) {
    TRANS_LOG(WARN, "init tx_data_backup_ failed", K(ret));
  }

  // TRANS_LOG(INFO, "init tx_data_backup_", K(ret), K(tx_data_backup_));
  return ret;
}

int ObTxAbortLog::init_tx_data_backup(const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_data_backup_.init(start_scn))) {
    TRANS_LOG(WARN, "init tx_data_backup_ failed", K(ret));
  }

  // TRANS_LOG(INFO, "init tx_data_backup_", K(ret), K(tx_data_backup_));
  return ret;
}

// ============================== Tx Log Blcok =============================

OB_TX_SERIALIZE_MEMBER(ObTxLogBlockHeader,
                       compat_bytes_,
                       org_cluster_id_,
                       log_entry_no_,
                       tx_id_,
                       scheduler_);

int ObTxLogBlockHeader::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(4))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }
  return ret;
}

const logservice::ObLogBaseType ObTxLogBlock::DEFAULT_LOG_BLOCK_TYPE =
    logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE; // TRANS_LOG
const int32_t ObTxLogBlock::DEFAULT_BIG_ROW_BLOCK_SIZE =
    62 * 1024 * 1024; // 62M redo log buf for big row

void ObTxLogBlock::reset()
{
  fill_buf_.reset();
  replay_buf_ = nullptr;
  len_ = pos_ = 0;
  cur_log_type_ = ObTxLogType::UNKNOWN;
  cb_arg_array_.reset();
  big_segment_buf_ = nullptr;
}

int ObTxLogBlock::reuse(const int64_t replay_hint, const ObTxLogBlockHeader &block_header)
{
  int ret = OB_SUCCESS;
  cur_log_type_ = ObTxLogType::UNKNOWN;
  cb_arg_array_.reset();
  big_segment_buf_ = nullptr;
  pos_ = 0;
  if (OB_FAIL(serialize_log_block_header_(replay_hint, block_header))) {
    TRANS_LOG(ERROR, "serialize log block header error when reuse", K(ret), KPC(this));
  }
  return ret;
}

ObTxLogBlock::ObTxLogBlock()
    : replay_buf_(nullptr), len_(0), pos_(0), cur_log_type_(ObTxLogType::UNKNOWN), cb_arg_array_(),
      big_segment_buf_(nullptr)
{
  // do nothing
}

int ObTxLogBlock::init(const int64_t replay_hint,
                       const ObTxLogBlockHeader &block_header,
                       const bool use_local_buf)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(replay_buf_) || !block_header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(replay_hint), K(*this), K(block_header));
  } else if (OB_FAIL(fill_buf_.init(use_local_buf))) {
    TRANS_LOG(WARN, "fill log buffer init error", K(ret), K(replay_hint), K(block_header), K(use_local_buf));
  } else {
    len_ = fill_buf_.get_length();
    pos_ = 0;
    if (OB_FAIL(serialize_log_block_header_(replay_hint, block_header))) {
      ret = OB_SERIALIZE_ERROR;
      TRANS_LOG(WARN, "serialize log block header error", K(ret), K(*this));
    }
  }
  return ret;
}

int ObTxLogBlock::init_with_header(const char *buf,
                                   const int64_t &size,
                                   int64_t &replay_hint,
                                   ObTxLogBlockHeader &block_header)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(replay_buf_)
      || OB_ISNULL(buf)
      || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(buf), K(size), K(*this));
  } else {
    replay_buf_ = buf;
    len_ = size;
    pos_ = 0;
    if (OB_FAIL(deserialize_log_block_header_(replay_hint, block_header))) {
      ret = OB_DESERIALIZE_ERROR;
      TRANS_LOG(WARN, "deserialize log block header error", K(ret), K(*this));
    }
  }
  return ret;
}

int ObTxLogBlock::init(const char *buf, const int64_t &size, int skip_pos, ObTxLogBlockHeader &block_header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(buf), K(size), K(*this));
  } else {
    replay_buf_ = buf;
    len_ = size;
    pos_ = skip_pos;

    if (OB_FAIL(block_header.deserialize(replay_buf_, len_, pos_))) {
      TRANS_LOG(WARN, "deserialize block header", K(ret));
    }
  }
  return ret;
}

int ObTxLogBlock::rewrite_barrier_log_block(int64_t replay_hint,
                                            const enum logservice::ObReplayBarrierType barrier_type)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = 0;
  char *serialize_buf = nullptr;
  logservice::ObLogBaseHeader header(logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE,
                                     barrier_type, replay_hint);
  if (OB_ISNULL(fill_buf_.get_buf())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    serialize_buf = fill_buf_.get_buf();
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(serialize_buf)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected empty serialize_buf", K(*this));
  } else if (OB_FAIL(header.serialize(serialize_buf, len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize log base header error", K(ret));
  }


  return ret;
}

int ObTxLogBlock::set_prev_big_segment_scn(const share::SCN prev_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(big_segment_buf_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "invalid big segment buf", K(ret), KPC(this));
  } else if (OB_FAIL(
                 big_segment_buf_->set_prev_part_id(prev_scn.get_val_for_inner_table_field()))) {
    TRANS_LOG(WARN, "set prev part scn", K(ret), KPC(this));
  }
  return ret;
}

int ObTxLogBlock::acquire_segment_log_buf(const char *&submit_buf,
                                          int64_t &submit_buf_len,
                                          const ObTxLogBlockHeader &block_header,
                                          const ObTxLogType big_segment_log_type,
                                          ObTxBigSegmentBuf *big_segment_buf)
{
  int ret = OB_SUCCESS;
  bool need_fill_part_scn = false;
  ObTxBigSegmentBuf *tmp_segment_buf = nullptr;
  ObTxLogHeader log_type_header(ObTxLogType::TX_BIG_SEGMENT_LOG);
  if (OB_ISNULL(big_segment_buf_) && OB_NOT_NULL(big_segment_buf) && big_segment_buf->is_active()) {
    big_segment_buf_ = big_segment_buf;
  }

  if (OB_ISNULL(big_segment_buf_) || OB_ISNULL(fill_buf_.get_buf())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KPC(this));
  } else if (OB_ISNULL(big_segment_buf_) || fill_buf_.is_use_local_buf()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, " big segment_buf", K(ret), KPC(this));
  } else if (OB_FALSE_IT(tmp_segment_buf = big_segment_buf_)) {
  } else if (OB_FAIL(reuse(block_header.get_tx_id(), block_header))) {
    TRANS_LOG(WARN, "serialize log block header failed", K(ret), K(block_header), KPC(this));
  } else if (OB_FAIL(log_type_header.serialize(fill_buf_.get_buf(), len_, pos_))) {
    TRANS_LOG(WARN, "serialize log type header failed", K(ret), KPC(this));
  } else if (OB_FAIL(cb_arg_array_.push_back(ObTxCbArg(ObTxLogType::TX_BIG_SEGMENT_LOG, NULL)))) {
    TRANS_LOG(WARN, "push the first log type arg failed", K(ret), K(*this));
  } else if (OB_FAIL(cb_arg_array_.push_back(ObTxCbArg(big_segment_log_type, NULL)))) {
    TRANS_LOG(WARN, "push the second log type arg failed", K(ret), K(*this));
  } else if (OB_FAIL(tmp_segment_buf->split_one_part(fill_buf_.get_buf(), len_, pos_,
                                                     need_fill_part_scn))) {
    TRANS_LOG(WARN, "acquire a part of big segment failed", K(ret), K(block_header), KPC(this));
  } else if (OB_FALSE_IT(submit_buf = fill_buf_.get_buf())) {
  } else if (OB_FALSE_IT(submit_buf_len = pos_)) {
  } else if (tmp_segment_buf->is_completed()) {
    // tmp_segment_buf->reset();
    // reset big_segment buf after set prev scn
    ret = OB_ITER_END;
  } else {
    big_segment_buf_ = tmp_segment_buf;
    cb_arg_array_.pop_back();
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObTxLogBlock::serialize_log_block_header_(const int64_t replay_hint,
                                              const ObTxLogBlockHeader &block_header,
                                              const logservice::ObReplayBarrierType barrier_type)
{
  int ret = OB_SUCCESS;
  char *serialize_buf = nullptr;
  logservice::ObLogBaseHeader header(logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE,
                                     barrier_type, replay_hint);
  if (OB_ISNULL(fill_buf_.get_buf()) || pos_ != 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    serialize_buf = fill_buf_.get_buf();
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(serialize_buf)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected empty serialize_buf", K(*this));
  } else if (OB_FAIL(header.serialize(serialize_buf, len_, pos_))) {
    TRANS_LOG(WARN, "serialize log base header error", K(ret));
  } else if (OB_FAIL(block_header.serialize(serialize_buf, len_, pos_))) {
    TRANS_LOG(WARN, "serialize block header error", K(ret));
  }

  return ret;
}

int ObTxLogBlock::deserialize_log_block_header_(int64_t &replay_hint, ObTxLogBlockHeader &block_header)
{
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader header;
  if (OB_ISNULL(replay_buf_) || pos_ != 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(header.deserialize(replay_buf_, len_, pos_))) {
    TRANS_LOG(WARN, "deserialize log base header error", K(ret),K(len_),K(pos_));
  } else if (OB_FAIL(block_header.deserialize(replay_buf_, len_, pos_))) {
    TRANS_LOG(WARN, "deserialize block header", K(ret), K(len_), K(pos_));
  } else {
    replay_hint = header.get_replay_hint();
  }
  return ret;
}

int ObTxLogBlock::get_next_log(ObTxLogHeader &header, ObTxBigSegmentBuf *big_segment_buf)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  if (OB_ISNULL(replay_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (OB_SUCC(update_next_log_pos_())) {
    if (OB_FAIL(header.deserialize(replay_buf_, len_, pos_))) {
      TRANS_LOG(WARN, "deserialize log header error", K(*this));
    } else {
      cur_log_type_ = header.get_tx_log_type();

      if (ObTxLogType::TX_BIG_SEGMENT_LOG == cur_log_type_) {
        if (OB_ISNULL(big_segment_buf)) {
          ret = OB_INVALID_ARGUMENT;
          TRANS_LOG(WARN, "invalid big_segment_buf", KPC(big_segment_buf));
        } else if (OB_NOT_NULL(big_segment_buf_)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "A completed big segment need be serialized", K(ret), KPC(this),
                    K(big_segment_buf));
        } else if (big_segment_buf->is_completed()) {
          ret = OB_NO_NEED_UPDATE;
          TRANS_LOG(WARN, "collect all part of big segment", K(ret));
        } else if (OB_FAIL(big_segment_buf->collect_one_part(replay_buf_, len_, pos_))) {
          TRANS_LOG(WARN, "merge one part of big segment failed", K(ret), KPC(this));
          // rollback to the start position
          pos_ = tmp_pos;
        } else {
          if (big_segment_buf->is_completed()) {
            big_segment_buf_ = big_segment_buf;
            // deserialize log_header
            if (OB_FAIL(big_segment_buf_->deserialize_object(header))) {
              TRANS_LOG(WARN, "deserialize log header from  big segment buf", K(ret), K(header),
                        KPC(this));
            } else {
              cur_log_type_ = header.get_tx_log_type();
            }
          } else {
            TRANS_LOG(INFO, "collect one part of big segment buf, need continue", K(ret),
                      KPC(big_segment_buf), KPC(this));
            ret = OB_LOG_TOO_LARGE;
          }
        }
      }
    }
    // TRANS_LOG(INFO, "[TxLogBlock] get_next_log in replay",K(cur_log_type_), K(len_), K(pos_));
  }
  return ret;
}

int ObTxLogBlock::prepare_mutator_buf(ObTxRedoLog &redo)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = get_buf();
  if (OB_ISNULL(tmp_buf)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (ObTxLogType::UNKNOWN != cur_log_type_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "MutatorBuf is using", K(ret), KPC(this));
  } else if (OB_FAIL(redo.set_mutator_buf(tmp_buf + pos_ + ObTxLogHeader::TX_LOG_HEADER_SIZE))) {
    TRANS_LOG(WARN, "set mutator buf error", K(ret));
  } else if (OB_FAIL(
                 redo.set_mutator_size(len_ - pos_ - ObTxLogHeader::TX_LOG_HEADER_SIZE, false))) {
    TRANS_LOG(WARN, "set mutator buf size error", K(ret));
  } else {
    cur_log_type_ = ObTxLogType::TX_REDO_LOG;
  }
  return ret;
}

int ObTxLogBlock::finish_mutator_buf(ObTxRedoLog &redo, const int64_t &mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  char * tmp_buf = get_buf();
  ObTxLogHeader header(ObTxLogType::TX_REDO_LOG);
  if (OB_ISNULL(tmp_buf)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (ObTxLogType::TX_REDO_LOG != cur_log_type_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "MutatorBuf not prepare");
  } else if (0 == mutator_size) {
    cur_log_type_ = ObTxLogType::UNKNOWN;
    redo.reset_mutator_buf();
  } else if (OB_FAIL(redo.set_mutator_size(mutator_size, true))) {
    TRANS_LOG(WARN, "set mutator buf size error after fill", K(ret));
  } else if (OB_FAIL(header.serialize(tmp_buf, len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize log header error", K(ret), K(header), K(*this));
  } else if (OB_FAIL(redo.before_serialize())) {
    TRANS_LOG(WARN, "before serialize for redo failed", K(ret), K(redo));
  } else if (OB_FAIL(redo.serialize(tmp_buf, len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize redo log body error", K(ret));
  } else {
    pos_ = tmp_pos;
    cur_log_type_ = ObTxLogType::UNKNOWN;
  }
  return ret;
}

int ObTxLogBlock::update_next_log_pos_()
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t body_size = 0;
  // use in DESERIALIZE_HEADER
  int64_t tmp_pos = pos_;

  if (ObTxLogType::UNKNOWN != cur_log_type_) {
    if (OB_FAIL(serialization::decode(replay_buf_, len_, tmp_pos, version))) {
      TRANS_LOG(WARN, "deserialize UNIS_VERSION error", K(ret), K(*this), K(tmp_pos), K(version));
    } else if (OB_FAIL(serialization::decode(replay_buf_, len_, tmp_pos, body_size))) {
      TRANS_LOG(WARN, "deserialize body_size error", K(ret), K(*this), K(tmp_pos), K(version));
    } else if (tmp_pos + body_size > len_) {
      ret = OB_SIZE_OVERFLOW;
      TRANS_LOG(WARN, "has not enough space for deserializing tx_log_body", K(body_size),
                K(tmp_pos), K(*this));
    } else {
      // skip log_body if cur_log_type_ isn't UNKNOWN
      // if deserialize_log_body success, cur_log_type_ will be UNKNOWN
      pos_ = tmp_pos + body_size;
    }
  }

  if (pos_ >= len_) {
    cur_log_type_ = ObTxLogType::UNKNOWN;
    ret = OB_ITER_END;
  }
  return ret;
}

} // namespace transaction
} // namespace oceanbase
