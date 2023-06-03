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

#include "storage/tx_table/ob_tx_table_iterator.h"

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/serialization.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_table/ob_tx_ctx_memtable.h"
#include "storage/tx_table/ob_tx_table.h"
#include <cmath>
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;

namespace storage
{

#define SSTABLE_HIDDEN_COLUMN_CNT ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()

#define GENERATE_ACCESS_CONTEXT                                                                  \
  ObTableAccessContext access_context;                                                           \
  ObStoreCtx store_ctx;                                                                          \
  ObQueryFlag query_flag(ObQueryFlag::Forward, false, /*is daily merge scan*/                    \
                         false,                       /*is read multiple macro block*/           \
                         false, /*sys task scan, read one macro block in single io*/             \
                         false, /*is full row scan?*/                                            \
                         false, false);                                                          \
  common::ObVersionRange trans_version_range;                                                    \
  trans_version_range.base_version_ = 0;                                                         \
  trans_version_range.multi_version_start_ = 0;                                                  \
  trans_version_range.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;               \
  if (OB_SUCC(ret)                                                                               \
      && OB_FAIL(                                                                                \
           access_context.init(query_flag, store_ctx, arena_allocator_, trans_version_range))) { \
    STORAGE_LOG(WARN, "init table access context fail.", KR(ret));                               \
  }

#define GENERATE_ROW_KEY                                         \
  blocksstable::ObDatumRowkey row_key;                           \
  key_datums_[0].set_int(int_tx_id);                             \
  key_datums_[1].set_int(idx);                                   \
  if (OB_SUCC(ret) && OB_FAIL(row_key.assign(key_datums_, 2))) { \
    STORAGE_LOG(WARN, "assign store row key failed.", KR(ret));  \
  }

/**************** ObTxDataMemtableScanIterator::TxData2DatumRowConverter ************************/

int ObTxDataMemtableScanIterator::TxData2DatumRowConverter::init(ObTxData *tx_data)
{
  int ret = OB_SUCCESS;
  int64_t need_ = 0;
  int64_t pos = 0;
  reset();
  if (OB_ISNULL(tx_data_ = tx_data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "tx data is null", KR(ret));
  } else if (INT64_MAX != tx_data->tx_id_.get_id()) {// normal tx data need local buffer to serialize
    buffer_len_ = tx_data->get_serialize_size();
    if (nullptr == (serialize_buffer_ = (char *)DEFAULT_TX_DATA_ALLOCATOR.alloc(buffer_len_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to serialize tx data, cause buffer allocated failed",
                        KR(ret), K(*this));
    } else if (OB_FAIL(tx_data->serialize(serialize_buffer_, buffer_len_, pos))) {
      STORAGE_LOG(WARN, "can not serialize tx data to buffer", KR(ret), K(*this));
    }
  }
  return ret;
}

void ObTxDataMemtableScanIterator::TxData2DatumRowConverter::reset()
{
  buffer_len_ = 0;
  if (OB_NOT_NULL(serialize_buffer_)) {
    ob_free(serialize_buffer_);
    serialize_buffer_ = nullptr;
  }
  tx_data_ = nullptr;
  generate_size_ = 0;
  datum_row_.reset();
}

int ObTxDataMemtableScanIterator
    ::TxData2DatumRowConverter::generate_next_now(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tx_data_)) {
    ret = OB_ITER_END;// no tx data remained
  /*****************[NEED REMOVE IN FUTURE]*****************/
  } else if (INT64_MAX == tx_data_->tx_id_.get_id() &&
             generate_size_ == 1) {
    ret = OB_ITER_END;// fake tx data datum row has been generated
  /*********************************************************/
  } else if (INT64_MAX != tx_data_->tx_id_.get_id() &&
             generate_size_ == std::ceil(buffer_len_ * 1.0 / common::OB_MAX_VARCHAR_LENGTH)) {
    ret = OB_ITER_END;// all tx data datum row has been generated
  } else {
    if (generate_size_ >= 1) {
      STORAGE_LOG(INFO, "[TX DATA MERGE]meet big tx data", KR(ret), K(*this));
    }
    datum_row_.reset();
    new (&datum_row_) ObDatumRow();// CAUTIONS: this is needed, or will core dump
    if (OB_FAIL(datum_row_.init(DEFAULT_TX_DATA_ALLOCATOR,
                                TX_DATA_MAX_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT))) {
      STORAGE_LOG(ERROR, "fail to init datum row", KR(ret), K(*this));
    } else {
      datum_row_.row_flag_.set_flag(blocksstable::ObDmlFlag::DF_INSERT);
      datum_row_.storage_datums_[TX_DATA_ID_COLUMN].set_int(tx_data_->tx_id_.get_id());
      datum_row_.storage_datums_[TX_DATA_IDX_COLUMN].set_int(generate_size_);
      datum_row_.storage_datums_[TX_DATA_IDX_COLUMN + 1].set_int(-4096);// storage layer needed
      datum_row_.storage_datums_[TX_DATA_IDX_COLUMN + 2].set_int(0);// storage layer needed
      int64_t total_row_cnt_column = TX_DATA_TOTAL_ROW_CNT_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
      int64_t end_ts_column = TX_DATA_END_TS_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
      int64_t value_column = TX_DATA_VAL_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
      char *p_value_begin = serialize_buffer_ + common::OB_MAX_VARCHAR_LENGTH * generate_size_;
      generate_size_++;
      ObString value;
      /*****************[NEED REMOVE IN FUTURE]*****************/
      // TODO : remove this after the sstables do not need upper trans version
      if (INT64_MAX == tx_data_->tx_id_.get_id()) {
        // NOTE : this fake tx data is generated in
        // ObTxDataMemtable::pre_process_commit_version_row_
        datum_row_.storage_datums_[total_row_cnt_column].set_int(1);
        datum_row_.storage_datums_[end_ts_column].set_int(INT64_MAX);
        value.assign((char *)(tx_data_->start_scn_.get_val_for_tx()), tx_data_->commit_version_.get_val_for_tx());
        /*********************************************************/
      } else {
        datum_row_.storage_datums_[total_row_cnt_column].set_int(std::ceil(buffer_len_ * 1.0 / common::OB_MAX_VARCHAR_LENGTH));
        datum_row_.storage_datums_[end_ts_column].set_int(tx_data_->end_scn_.get_val_for_tx());
        value.assign(p_value_begin,
                    std::min(common::OB_MAX_VARCHAR_LENGTH,
                              buffer_len_ - (p_value_begin - serialize_buffer_)));
      }
      datum_row_.storage_datums_[value_column].set_string(value);
      datum_row_.set_first_multi_version_row();// storage layer needed for compatibility
      datum_row_.set_last_multi_version_row();// storage layer needed for compatibility
      datum_row_.set_compacted_multi_version_row();// storage layer needed for compatibility
      row = &datum_row_;
    }
  }
  return ret;
}

/******************** ObTxDataMemtableScanIterator::TxData2DatumRowConverter **********************/

/***************************** ObTxDataMemtableScanIterator **********************************/

int ObTxDataMemtableScanIterator::init(ObTxDataMemtable *tx_data_memtable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    reset();
  }
  if (OB_ISNULL(tx_data_memtable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "init ObTxDataMemtableScanIterator with a null tx_data_memtable.", KR(ret));
  } else if (ObTxDataMemtable::State::FROZEN != tx_data_memtable->get_state()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "the state of this tx data memtable is not frozen",
                K(tx_data_memtable->get_state()));
  } else if (tx_data_memtable->get_tx_data_count() != tx_data_memtable->get_inserted_count()) {
    ret = OB_ERR_UNEXPECTED;
    int64_t tx_data_count = tx_data_memtable->get_tx_data_count();
    int64_t inserted_count = tx_data_memtable->get_inserted_count();
    int64_t deleted_count = tx_data_memtable->get_deleted_count();
    STORAGE_LOG(ERROR,
        "Inserted count is not equal to tx data count.",
        KR(ret),
        K(tx_data_count),
        K(inserted_count),
        K(deleted_count),
        KPC(tx_data_memtable));
  } else if (OB_FAIL(init_iterate_range_(tx_data_memtable))) {
    STORAGE_LOG(WARN, "init iterate range failed.", KR(ret));
  } else {
    tx_data_memtable_ = tx_data_memtable;
<<<<<<< HEAD
    cur_max_commit_version_.set_min();
    pre_start_scn_.set_min();
    tx_data_row_cnt_ = 0;
=======
    iterate_row_cnt_ = 0;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    pre_tx_data_ = nullptr;
    drop_tx_data_cnt_ = 0;

    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "[TX DATA MERGE]init tx data dump iter finish", KR(ret), KPC(this), KPC(tx_data_memtable_));
  } else {
    STORAGE_LOG(INFO, "[TX DATA MERGE]init tx data dump iter finish", KR(ret), KPC(this), KPC(tx_data_memtable_));
  }

  return ret;
}

int ObTxDataMemtableScanIterator::init_iterate_range_(ObTxDataMemtable *tx_data_memtable)
{
  int ret = OB_SUCCESS;
  // get start tx id
  if (range_.get_start_key().is_min_rowkey()) {
    ret = init_serial_range_(tx_data_memtable);
  } else {
    ret = init_parallel_range_(tx_data_memtable);
  }
  return ret;
}

int ObTxDataMemtableScanIterator::init_serial_range_(ObTxDataMemtable *tx_data_memtable)
{
  int ret = OB_SUCCESS;

  // end_key must be max_rowkey when start_key is min_rowkey
  if (!(range_.get_end_key().is_max_rowkey())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid iterate range when flush tx data", KR(ret), K(range_));
  } else if (OB_ISNULL(cur_node_ = tx_data_memtable->get_sorted_list_head())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected nullptr of sort list head", KR(ret), KPC(tx_data_memtable));
  } else if (FALSE_IT(row_cnt_to_dump_ = tx_data_memtable->get_inserted_count() - tx_data_memtable->get_deleted_count())) {
  } else if (row_cnt_to_dump_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected row count to dump", KR(ret), KPC(tx_data_memtable));
  } else {
    start_tx_id_ = 0;
    end_tx_id_ = INT64_MAX;
    is_parallel_merge_ = false;
    STORAGE_LOG(DEBUG, "init serial range finish", KR(ret), K(start_tx_id_), K(end_tx_id_), K(row_cnt_to_dump_), KPC(cur_node_->next_));
  }
  return ret;
}

int ObTxDataMemtableScanIterator::init_parallel_range_(ObTxDataMemtable *tx_data_memtable)
{
  int ret = OB_SUCCESS;
  const ObObj *start_obj = nullptr;
  const ObObj *end_obj = nullptr;

  // get start tx id of parallel merge
  if (OB_ISNULL(start_obj = range_.get_start_key().get_store_rowkey().get_rowkey().get_obj_ptr())) {
    STORAGE_LOG(WARN, "get start obj from range failed.", KR(ret), K(range_));
  } else if (OB_FAIL(start_obj[0].get_int(start_tx_id_))) {
    STORAGE_LOG(WARN, "get start tx id from start obj failed", KR(ret), KPC(start_obj));
  } else if (start_tx_id_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get an invalid start tx id from start obj ", KR(ret), KPC(start_obj));
  }

  // get end tx id of parallel merge
  if (OB_FAIL(ret)) {
  } else if (range_.get_end_key().is_max_rowkey()) {
    end_tx_id_ = INT64_MAX;
  } else if (OB_ISNULL(end_obj = range_.get_end_key().get_store_rowkey().get_rowkey().get_obj_ptr())) {
    STORAGE_LOG(WARN, "get end obj from range failed.", KR(ret), K(range_));
  } else if (OB_FAIL(end_obj[0].get_int(end_tx_id_))) {
    STORAGE_LOG(WARN, "get end tx id from end obj failed", KR(ret), KPC(end_obj));
  }

  // get iterate start node and iterate count
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tx_data_memtable->get_iter_start_and_count(start_tx_id_, cur_node_, row_cnt_to_dump_))) {
    STORAGE_LOG(WARN, "get iterate start node and iterate count failed", KR(ret), K(start_tx_id_), KPC(cur_node_->next_), K(iterate_row_cnt_));
  } else {
    STORAGE_LOG(DEBUG, "init parallel range finish", KR(ret), K(start_tx_id_), K(end_tx_id_), K(row_cnt_to_dump_), KPC(cur_node_->next_));
  }

  return ret;
}


void ObTxDataMemtableScanIterator::reset()
{
<<<<<<< HEAD
  if (OB_NOT_NULL(tx_data_memtable_)) {
    tx_data_memtable_->reset_is_iterating();
  }
  dump_tx_data_done_ = false;
  cur_max_commit_version_.set_min();
  pre_start_scn_.set_min();
  tx_data_row_cnt_ = 0;
=======
  iterate_row_cnt_ = 0;
  start_tx_id_ = 0;
  end_tx_id_ = INT64_MAX;
  row_cnt_to_dump_ = 0;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  pre_tx_data_ = nullptr;
  cur_node_ = nullptr;
  tx_data_memtable_ = nullptr;
  is_inited_ = false;
  drop_tx_data_cnt_ = 0;
}

void ObTxDataMemtableScanIterator::reuse() { reset(); }

int ObTxDataMemtableScanIterator::get_next_tx_data_(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cur_node_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "current node is unexpected nullptr.", KR(ret), KPC(tx_data_memtable_));
  } else if (OB_ISNULL(cur_node_->next_)) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(cur_node_ = &(cur_node_->next_->sort_list_node_))) {
  } else if (FALSE_IT(tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node_))) {
  } else if (!is_parallel_merge_ && OB_FAIL(drop_and_get_tx_data_(tx_data))) {
    STORAGE_LOG(WARN, "drop and get tx data failed", KR(ret));
  } else if (OB_NOT_NULL(pre_tx_data_) && tx_data->tx_id_ <= pre_tx_data_->tx_id_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "iterate an invalid rowkey in a single tx data memtable", KR(ret),
                KPC(pre_tx_data_), KPC(tx_data), KPC(tx_data_memtable_));
  } else if (FALSE_IT(pre_tx_data_ = tx_data)) {
  } else if (tx_data->tx_id_ <= start_tx_id_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "iterate an out of range row key", KR(ret), KPC(tx_data));
  } else if (tx_data->tx_id_.get_id() > end_tx_id_) {
    ret = OB_ITER_END;
  } else {
    // find a tx data which need to be flushed
  }

  return ret;
}

int ObTxDataMemtableScanIterator::drop_and_get_tx_data_(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  while (OB_NOT_NULL(cur_node_->next_)) {
    ObTxData *next_tx_data = cur_node_->next_;

    // the tx datas having the same rowkey must be rollback tx data excpet one commit tx data
    if (OB_UNLIKELY(next_tx_data->tx_id_ == tx_data->tx_id_)) {
      cur_node_ = &(cur_node_->next_->sort_list_node_);
      row_cnt_to_dump_--;
      drop_tx_data_cnt_++;
      if (OB_UNLIKELY(next_tx_data->end_scn_ > tx_data->end_scn_)) {
        // pointer to next_tx_data cause its end_log_ts is larger
        STORAGE_LOG(DEBUG, "drop one rollback tx data", "droped : ", to_cstring(tx_data), "keeped", to_cstring(next_tx_data));
        tx_data = next_tx_data;
      } else {
        STORAGE_LOG(DEBUG, "drop one rollback tx data", "droped : ", to_cstring(next_tx_data), "keeped", to_cstring(tx_data));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObTxDataMemtableScanIterator::inner_get_next_row(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObTxData *tx_data = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data memtable scan iterator is not inited");
<<<<<<< HEAD
  } else if (OB_NOT_NULL(cur_node_)) {
    ret = get_next_tx_data_row_(row);
  } else if (!dump_tx_data_done_) {
    // cur_node == nullptr && dump_tx_data_done == false
    // which means this row should be commit version row
    dump_tx_data_done_ = true;
    if (tx_data_row_cnt_ != tx_data_memtable_->get_inserted_count() - tx_data_memtable_->get_deleted_count()) {
      ret = OB_ERR_UNEXPECTED;
      int64_t tx_data_count_in_memtable = tx_data_memtable_->get_inserted_count();
      STORAGE_LOG(ERROR, "iterate tx data row count is not equal to tx data in memtable", KR(ret),
                  K(tx_data_row_cnt_), K(tx_data_count_in_memtable), KPC(tx_data_memtable_));
    } else if (OB_FAIL(prepare_commit_scn_list_())) {
      STORAGE_LOG(WARN, "prepare commit version array for calculating upper_trans_version failed.",
                  KR(ret), KPC(tx_data_memtable_));
    } else {
      cur_node_ = tx_data_memtable_->get_sorted_list_head();
      if (OB_ISNULL(cur_node_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "current node unexpected null.", KR(ret), KPC(cur_node_),
                    KPC(tx_data_memtable_));
      } else if (OB_ISNULL(cur_node_ = cur_node_->next_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "current node unexpected null.", KR(ret), KPC(cur_node_),
                    KPC(tx_data_memtable_));
      } else if (OB_FAIL(get_next_commit_scn_row_(row))) {
        STORAGE_LOG(WARN, "get pre-process commit versions row failed.", KR(ret), KPC(row));
      } else {
        STORAGE_LOG(INFO, "successfully get next commit versions row!", KPC(row));
=======
  } else if (OB_SUCC(tx_data_2_datum_converter_.generate_next_now(row))) {
    // do nothing, next row is assigned out
  } else if (OB_ITER_END != ret) {
    STORAGE_LOG(WARN, "fail to generate datum row", KR(ret), K_(tx_data_2_datum_converter));
  } else {// no lefeted row in tx_data_2_datum_converter
    if (OB_FAIL(get_next_tx_data_(tx_data))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "get next tx data failed.", KR(ret), KPC(tx_data_memtable_));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
      }
    } else if (OB_ISNULL(tx_data)) {
      ret = OB_BAD_NULL_ERROR;
      STORAGE_LOG(ERROR, "tx data is nullptr", KR(ret), KPC(tx_data_memtable_));
    } else if (OB_FAIL(tx_data_2_datum_converter_.init(tx_data))) {
      STORAGE_LOG(WARN, "fail to convert tx data to datum", KR(ret), KPC(tx_data_memtable_));
    } else if (OB_FAIL(tx_data_2_datum_converter_.generate_next_now(row))) {
      STORAGE_LOG(WARN, "fail to get row from tx_data_2_datum_converter",
                        KR(ret), KPC(tx_data_memtable_), K_(tx_data_2_datum_converter));
    } else if (++iterate_row_cnt_ > row_cnt_to_dump_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid iterate row count",
                         KR(ret), K(iterate_row_cnt_), K(row_cnt_to_dump_));
    }
  }

  if (OB_NOT_NULL(row)
      && (!row->is_first_multi_version_row() || !row->is_last_multi_version_row())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Invalid tx data sstable row", KPC(row));
  }
<<<<<<< HEAD
  return ret;
}

int ObTxDataMemtableScanIterator::prepare_commit_scn_list_()
{
  int ret = tx_data_memtable_->prepare_commit_scn_list();
  return ret;
}

int ObTxDataMemtableScanIterator::get_next_tx_data_row_(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node_);
  int64_t pos = 0;

  // TODO : @gengli
  // if there are too many undo actions, the serialize_size can be very large
  int64_t serialize_size = tx_data->get_serialize_size();
  if (OB_NOT_NULL(pre_tx_data_) && tx_data->tx_id_ <= pre_tx_data_->tx_id_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "iterate the same rowkey in a single tx data memtable", KR(ret),
                KPC(pre_tx_data_), KPC(tx_data), KPC(tx_data_memtable_));
  } else if (serialize_size > common::OB_MAX_VARCHAR_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "Too Much Undo Actions", KR(ret), KPC(tx_data));
  } else if (OB_FAIL(buf_.reserve(serialize_size))) {
    STORAGE_LOG(WARN, "Failed to reserve local buffer", KR(ret), KPC(tx_data));
  } else if (OB_FAIL(tx_data->serialize(buf_.get_ptr(), serialize_size, pos))) {
    STORAGE_LOG(WARN, "failed to serialize tx state info", KR(ret), KPC(tx_data), K(pos));
  } else {
    row_.storage_datums_[TX_DATA_ID_COLUMN].set_int(tx_data->tx_id_.get_id());
    row_.storage_datums_[TX_DATA_IDX_COLUMN].set_int(0);
    row_.storage_datums_[TX_DATA_IDX_COLUMN + 1].set_int(-4096);
    row_.storage_datums_[TX_DATA_IDX_COLUMN + 2].set_int(0);

    int64_t total_row_cnt_column = TX_DATA_TOTAL_ROW_CNT_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
    int64_t end_ts_column = TX_DATA_END_TS_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
    int64_t value_column = TX_DATA_VAL_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
    row_.storage_datums_[total_row_cnt_column].set_int(1);
    row_.storage_datums_[end_ts_column].set_int(tx_data->end_scn_.get_val_for_tx());
    row_.storage_datums_[value_column].set_string(ObString(serialize_size, buf_.get_ptr()));
    row_.set_first_multi_version_row();
    row_.set_last_multi_version_row();
    row_.set_compacted_multi_version_row();
    row = &row_;

    pre_tx_data_ = tx_data;
    ATOMIC_INC(&tx_data_row_cnt_);

    // fill in a new row successfully
    // point to the next tx data
    cur_node_ = cur_node_->next_;
  }
  return ret;
}

// This function is called after sorting tx_data by start_scn and the following steps is
// executed:
// 1. Select (start_scn, commit_version) point per second and push them into an array.
// 2. Read (start_scn, commit_version) array from the latest tx data sstable.
// 3. Get the recycle_scn to filtrate the point which is not needed any more.
// 4. Merge the arrays above. This procedure should filtrate the points are not needed and keep the
// commit versions monotonically increasing.
// 5. Serialize the merged array into one sstable row.
int ObTxDataMemtableScanIterator::get_next_commit_scn_row_(const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  SCN recycle_scn = SCN::min_scn();
  int64_t serialize_size = 0;
  ObCommitSCNsArray cur_commit_scns;
  ObCommitSCNsArray past_commit_scns;
  ObCommitSCNsArray merged_commit_scns;

  if (OB_FAIL(fill_in_cur_commit_scns_(cur_commit_scns) /*step 1*/)) {
    STORAGE_LOG(WARN, "periodical select commit version failed.", KR(ret));
  } else if (tx_data_row_cnt_ != DEBUG_iter_commit_ts_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected iter count when pre process commit versions array.", K(tx_data_row_cnt_), K(DEBUG_iter_commit_ts_cnt_));
    DEBUG_print_start_scn_list_();
    ob_abort();
  } else if (OB_FAIL(get_past_commit_scns_(past_commit_scns) /*step 2*/)) {
    STORAGE_LOG(WARN, "get past commit versions failed.", KR(ret));
  } else if (OB_FAIL(
               tx_data_memtable_->get_tx_data_memtable_mgr()->get_tx_data_table()->get_recycle_scn(
                 recycle_scn) /*step 3*/)) {
    STORAGE_LOG(WARN, "get recycle ts failed.", KR(ret));
  } else if (OB_FAIL(merge_cur_and_past_commit_verisons_(recycle_scn, cur_commit_scns,
                                                         past_commit_scns,
                                                         merged_commit_scns) /*step 4*/)) {
    STORAGE_LOG(WARN, "merge current and past commit versions failed.", KR(ret));
  } else if (!merged_commit_scns.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid commit versions", KR(ret));
  } else if (OB_FAIL(DEBUG_try_calc_upper_and_check_(merged_commit_scns))) {
  } else if (OB_FAIL(
               set_row_with_merged_commit_scns_(merged_commit_scns, row) /*step 5*/)) {
    STORAGE_LOG(WARN, "set row with merged commit versions failed.", KR(ret));
  } else {
    // get commit version row succeed.
  }

  return ret;
}

int ObTxDataMemtableScanIterator::DEBUG_try_calc_upper_and_check_(ObCommitSCNsArray &merged_commit_versions)
{
  int ret = OB_SUCCESS;

  ObTxDataSortListNode *cur_node = tx_data_memtable_->get_sorted_list_head()->next_;
  int64_t DEBUG_iter_cnt = 0;
  while (OB_SUCC(ret) && OB_NOT_NULL(cur_node)) {
    DEBUG_iter_cnt++;
    ObTxData *tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node);
    cur_node = cur_node->next_;

    if (ObTxData::COMMIT != tx_data->state_) {
      continue;
    }

    SCN upper_trans_version = SCN::min_scn();
    if (OB_FAIL(DEBUG_fake_calc_upper_trans_version(tx_data->start_scn_, upper_trans_version, merged_commit_versions))) {
      STORAGE_LOG(ERROR, "invalid upper trans version", KR(ret));
    } else if (upper_trans_version < tx_data->commit_version_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid upper trans version", KR(ret), K(upper_trans_version), KPC(tx_data));
    }

    if (OB_FAIL(ret)) {
      DEBUG_print_start_scn_list_();
      DEBUG_print_merged_commit_versions_(merged_commit_versions);
    }
  }
  if (OB_SUCC(ret) && DEBUG_iter_cnt != tx_data_row_cnt_) {
    ret = OB_SUCCESS;
    STORAGE_LOG(ERROR, "invalid iter cnt", KR(ret), K(DEBUG_iter_cnt), K(tx_data_row_cnt_));
  }

  return ret;
}

int ObTxDataMemtableScanIterator::DEBUG_fake_calc_upper_trans_version(const SCN sstable_end_scn,
                                                                      SCN &upper_trans_version,
                                                                      ObCommitSCNsArray &merged_commit_versions)
{
  int ret = OB_SUCCESS;

  ObIArray<ObCommitSCNsArray::Node> &array = merged_commit_versions.array_;
  int l = 0;
  int r = array.count() - 1;

  // Binary find the first start_scn that is greater than or equal to sstable_end_scn
  while (l < r) {
    int mid = (l + r) >> 1;
    if (array.at(mid).start_scn_ < sstable_end_scn) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }

  // Check if the start_scn is greater than or equal to the sstable_end_scn. If not, delay the
  // upper_trans_version calculation to the next time.
  if (0 == array.count() || !array.at(l).commit_version_.is_valid()) {
    upper_trans_version.set_max();
    ret = OB_ERR_UNDEFINED;
    STORAGE_LOG(WARN, "unexpected array count or commit version", K(array.count()), K(array.at(l)));
  } else {
    upper_trans_version = array.at(l).commit_version_;
  }

  return ret;
}

void ObTxDataMemtableScanIterator::DEBUG_print_start_scn_list_()
{
  int ret = OB_SUCCESS;
  const char *real_fname = "tx_data_start_scn_list";
  FILE *fd = NULL;

  if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(real_fname));
  } else {
    auto tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld \n", tenant_id);
    ObTxDataSortListNode *cur_node = tx_data_memtable_->get_sorted_list_head()->next_;
    while (OB_NOT_NULL(cur_node)) {
      ObTxData *tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node);
      cur_node = cur_node->next_;

      fprintf(fd,
              "ObTxData : tx_id=%-19ld is_in_memtable=%-3d state=%-8s start_scn=%-19s "
              "end_scn=%-19s "
              "commit_version=%-19s\n",
              tx_data->tx_id_.get_id(),
              tx_data->is_in_tx_data_table_,
              ObTxData::get_state_string(tx_data->state_),
              to_cstring(tx_data->start_scn_),
              to_cstring(tx_data->end_scn_),
              to_cstring(tx_data->commit_version_));
    }
  }

  if (NULL != fd) {
    fprintf(fd, "end of start scn list\n");
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "dump start scn list fail", K(real_fname), K(ret));
  }
}

void ObTxDataMemtableScanIterator::DEBUG_print_merged_commit_versions_(ObCommitSCNsArray &merged_commit_versions)
{
  int ret = OB_SUCCESS;
  const auto &array = merged_commit_versions.array_;
  const char *real_fname = "merge_commit_versions";
  FILE *fd = NULL;

  if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(real_fname));
  } else {
    auto tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld \n", tenant_id);
    for (int i = 0; i < array.count(); i++) {
      fprintf(fd,
              "start_scn=%-19s "
              "commit_version=%-19s\n",
              to_cstring(array.at(i).start_scn_),
              to_cstring(array.at(i).commit_version_));
    }
  }

  if (NULL != fd) {
    fprintf(fd, "end of commit versions array\n");
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "dump commit versions fail", K(real_fname), K(ret));
  }
}


int ObTxDataMemtableScanIterator::DEBUG_check_past_and_cur_arr(
  ObCommitSCNsArray &cur_commit_scns, ObCommitSCNsArray &past_commit_scns)
{
  int ret = OB_SUCCESS;
  auto &cur_arr = cur_commit_scns.array_;
  auto &past_arr = past_commit_scns.array_;
  STORAGE_LOG(INFO, "start debug check past and cur array", K(cur_arr.count()),
              K(past_arr.count()));

  for (int i = 0; OB_SUCC(ret) && i < cur_arr.count() - 1; i++) {
    if (cur_arr.at(i).start_scn_ > cur_arr.at(i + 1).start_scn_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected error in cur_arr", K(cur_arr.at(i)), K(cur_arr.at(i + 1)));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < past_arr.count() - 1; i++) {
    if (past_arr.at(i).start_scn_ > past_arr.at(i + 1).start_scn_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected error in cur_arr", K(past_arr.at(i)), K(past_arr.at(i + 1)));
    }
  }

  STORAGE_LOG(INFO, "finish debug check past and cur array", KR(ret), K(cur_arr.count()),
              K(past_arr.count()));
  return ret;
}

int ObTxDataMemtableScanIterator::fill_in_cur_commit_scns_(ObCommitSCNsArray &cur_commit_scns)
{
  int ret = OB_SUCCESS;
  ObCommitSCNsArray::Node node;
  DEBUG_iter_commit_ts_cnt_ = 0;
  DEBUG_last_start_scn_.set_min();
  while (OB_SUCC(periodical_get_next_commit_scn_(node))) {
    cur_commit_scns.array_.push_back(node);
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error occurs when periodical select commit version", KR(ret),
                KPC(tx_data_memtable_));
  }

  return ret;
}

int ObTxDataMemtableScanIterator::periodical_get_next_commit_scn_(ObCommitSCNsArray::Node &node)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = nullptr;

  while (OB_SUCC(ret) && nullptr != cur_node_) {
    ObTxData *tmp_tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node_);
    cur_node_ = cur_node_->next_;
    DEBUG_iter_commit_ts_cnt_++;

    // avoid rollback or abort transaction influencing commit versions array
    if (ObTxData::COMMIT != tmp_tx_data->state_) {
      continue;
    } else {
      tx_data = tmp_tx_data;
    }

    if (DEBUG_last_start_scn_ > tx_data->start_scn_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected start scn order", K(DEBUG_last_start_scn_), KPC(tx_data));
      break;
    } else {
      DEBUG_last_start_scn_ = tx_data->start_scn_;
    }

    // update pre_commit_version
    if (tx_data->commit_version_ > cur_max_commit_version_) {
      cur_max_commit_version_ = tx_data->commit_version_;
    }

    // If this tx data is the first tx data in sorted list or its start_scn is 1_s larger than
    // the pre_start_scn, we use this start_scn to calculate upper_trans_version
    if (pre_start_scn_.is_min() ||
        tx_data->start_scn_ >= SCN::plus(pre_start_scn_, PERIODICAL_SELECT_INTERVAL_NS)/*1s*/) {
      pre_start_scn_ = tx_data->start_scn_;
      break;
    }
  }
  
  if (nullptr != tx_data) {
    node.start_scn_ = tx_data->start_scn_;
    // use cur_max_commit_version_ to keep the commit versions monotonically increasing
    node.commit_version_ = cur_max_commit_version_;
    tx_data = nullptr;
  } else if (nullptr == cur_node_) {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObTxDataMemtableScanIterator::get_past_commit_scns_(
  ObCommitSCNsArray &past_commit_scns)
{
  int ret = OB_SUCCESS;
  ObLSTabletService *tablet_svr
    = tx_data_memtable_->get_tx_data_memtable_mgr()->get_ls_tablet_svr();
  ObTableIterParam iter_param = iter_param_;
  ObTabletHandle &tablet_handle = iter_param.tablet_handle_;

  if (tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet handle should be empty", KR(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet svr is nullptr", KR(ret), KPC(tx_data_memtable_));
  } else if (OB_FAIL(tablet_svr->get_tablet(LS_TX_DATA_TABLET, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service failed.", KR(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid tablet handle", KR(ret), K(tablet_handle));
  } else {
    // get the lastest sstable
    ObITable *table
      = tablet_handle.get_obj()->get_table_store().get_minor_sstables().get_boundary_table(
        true /*is_last*/);

    if (OB_NOT_NULL(table)) {
      ObCommitVersionsGetter getter(iter_param, table);
      if (OB_FAIL(getter.get_next_row(past_commit_scns))) {
        STORAGE_LOG(WARN, "get commit versions from tx data sstable failed.", KR(ret));
      }
    } else {
      STORAGE_LOG(DEBUG, "There is no tx data sstable yet", KR(ret), KPC(table));
    }
  }

  return ret;
}

int ObTxDataMemtableScanIterator::merge_cur_and_past_commit_verisons_(const SCN recycle_scn,
                                                                      ObCommitSCNsArray &cur_commit_scns,
                                                                      ObCommitSCNsArray &past_commit_scns,
                                                                      ObCommitSCNsArray &merged_commit_scns)
{
  int ret = OB_SUCCESS;
  ObIArray<ObCommitSCNsArray::Node> &cur_arr = cur_commit_scns.array_;
  ObIArray<ObCommitSCNsArray::Node> &past_arr = past_commit_scns.array_;
  ObIArray<ObCommitSCNsArray::Node> &merged_arr = merged_commit_scns.array_;


  int64_t cur_size = cur_commit_scns.get_serialize_size();
  int64_t past_size = past_commit_scns.get_serialize_size();
  int64_t step_len = 1;
  if (cur_size + past_size > common::OB_MAX_VARCHAR_LENGTH) {
    STORAGE_LOG(INFO,
                "Too Much Pre-Process Data to Desirialize",
                K(recycle_scn),
                K(past_size),
                K(cur_size),
                "past_array_count", past_commit_scns.array_.count(),
                "cur_array_count", cur_commit_scns.array_.count());
    step_len = step_len + ((cur_size + past_size) / OB_MAX_VARCHAR_LENGTH);
  }

  // here we merge the past commit versions and current commit versions. To keep merged array correct, the node in past
  // array whose start_scn is larger than the minimum start_scn in current array will be dropped. The reason is in this
  // issue: https://work.aone.alibaba-inc.com/issue/43389863
  SCN cur_min_start_scn = cur_arr.count() > 0 ? cur_arr.at(0).start_scn_ : SCN::max_scn();
  SCN max_commit_version = SCN::min_scn();
  if (OB_FAIL(
          merge_pre_process_node_(step_len, cur_min_start_scn, recycle_scn, past_arr, max_commit_version, merged_arr))) {
    STORAGE_LOG(WARN, "merge past commit versions failed.", KR(ret), K(past_arr), KPC(tx_data_memtable_));
  } else if (OB_FAIL(
                 merge_pre_process_node_(step_len, SCN::max_scn(), recycle_scn, cur_arr, max_commit_version, merged_arr))) {
    STORAGE_LOG(WARN, "merge current commit versions failed.", KR(ret), K(cur_arr), KPC(tx_data_memtable_));
  } else if (0 == merged_arr.count()) {
    if (OB_FAIL(merged_arr.push_back(ObCommitSCNsArray::Node(SCN::max_scn(), SCN::max_scn())))) {
      STORAGE_LOG(WARN, "push back commit version node failed.", KR(ret), KPC(tx_data_memtable_));
    } else {
      STORAGE_LOG(INFO, "push back an INT64_MAX node for upper trans version calculation", K(merged_arr));
    }
  }

  STORAGE_LOG(INFO,
              "genenrate commit versions array finish.",
              K(recycle_scn),
              K(step_len),
              "past_array_count", past_commit_scns.array_.count(),
              "cur_array_count", cur_commit_scns.array_.count(),
              "merged_array_count", merged_commit_scns.array_.count());

  return ret;
}

int ObTxDataMemtableScanIterator::merge_pre_process_node_(const int64_t step_len,
                                                          const SCN start_scn_limit,
                                                          const SCN recycle_scn,
                                                          const ObIArray<ObCommitSCNsArray::Node> &data_arr,
                                                          SCN &max_commit_version,
                                                          ObIArray<ObCommitSCNsArray::Node> &merged_arr)
{
  int ret = OB_SUCCESS;
  int64_t arr_len = data_arr.count();
  if (arr_len <= 0) {
    // skip push back
  } else {
    // push back pre-process node except the last one
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < arr_len - 1; i += step_len) {
      if (data_arr.at(i).start_scn_ >= start_scn_limit) {
        break;
      }
      max_commit_version = std::max(max_commit_version, data_arr.at(i).commit_version_);
      ObCommitSCNsArray::Node new_node(data_arr.at(i).start_scn_, max_commit_version);
      if (new_node.commit_version_ <= recycle_scn) {
        // this tx data should be recycled
        // do nothing
      } else if (OB_FAIL(merged_arr.push_back(new_node))) {
        STORAGE_LOG(WARN, "push back commit version node failed.", KR(ret), KPC(tx_data_memtable_));
      }
    }

    // push back the last pre-process node
    max_commit_version = std::max(max_commit_version, data_arr.at(arr_len - 1).commit_version_);
    if (OB_SUCC(ret) && data_arr.at(arr_len - 1).start_scn_ < start_scn_limit) {
      ObCommitSCNsArray::Node new_node(data_arr.at(arr_len - 1).start_scn_, max_commit_version);
      if (OB_FAIL(merged_arr.push_back(new_node))) {
        STORAGE_LOG(WARN, "push back commit version node failed.", KR(ret), KPC(tx_data_memtable_));
      }
    }

  }
  return ret;
}

/**
 * 1. This function set an special row for calculating upper trans version
 *
 */
int ObTxDataMemtableScanIterator::set_row_with_merged_commit_scns_(
  ObCommitSCNsArray &merged_commit_scns, const blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t serialize_size = merged_commit_scns.get_serialize_size();

  if (OB_FAIL(buf_.reserve(serialize_size))) {
    STORAGE_LOG(WARN, "Failed to reserve local buffer", K(ret));
  } else if (OB_FAIL(merged_commit_scns.serialize(buf_.get_ptr(), serialize_size, pos))) {
    STORAGE_LOG(WARN, "failed to serialize commit versions", KR(ret), K(serialize_size), K(pos),
                K(merged_commit_scns.array_));
  } else {
    row_.storage_datums_[TX_DATA_ID_COLUMN].set_int(INT64_MAX);
    row_.storage_datums_[TX_DATA_IDX_COLUMN].set_int(0);
    row_.storage_datums_[TX_DATA_IDX_COLUMN + 1].set_int(-4096);
    row_.storage_datums_[TX_DATA_IDX_COLUMN + 2].set_int(0);

    int64_t total_row_cnt_column = TX_DATA_TOTAL_ROW_CNT_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
    int64_t end_ts_column = TX_DATA_END_TS_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
    int64_t value_column = TX_DATA_VAL_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
    row_.storage_datums_[total_row_cnt_column].set_int(1);
    row_.storage_datums_[end_ts_column].set_int(INT64_MAX);
    row_.storage_datums_[value_column].set_string(ObString(serialize_size, buf_.get_ptr()));

    row_.set_first_multi_version_row();
    row_.set_last_multi_version_row();
    row_.set_compacted_multi_version_row();
    row = &row_;
  }

=======

  if (OB_ITER_END == ret) {
    if (is_parallel_merge_ && drop_tx_data_cnt_ > 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "parallel merge should not drop tx data", KPC(this), KPC(tx_data_memtable_));
    } else if (iterate_row_cnt_ != row_cnt_to_dump_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid iterate row count", K(iterate_row_cnt_), K(row_cnt_to_dump_), KPC(tx_data_memtable_));
    } else {
      STORAGE_LOG(INFO, "[TX DATA MERGE]iterate tx data memtable done.", KPC(this), KPC(tx_data_memtable_));
    }
  }
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  return ret;
}

/***************************** ObTxDataSingleRowGetter **********************************/

int ObTxDataSingleRowGetter::init(const transaction::ObTransID &tx_id)
{
  tx_id_ = tx_id;
  return OB_SUCCESS;
}

int ObTxDataSingleRowGetter::get_next_row(ObTxData &tx_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!iter_param_.tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid tablet handle", K(ret), K(iter_param_.tablet_handle_));
  } else {
    ObTabletTableStore &table_store = iter_param_.tablet_handle_.get_obj()->get_table_store();
    ObSSTableArray &sstables = table_store.get_minor_sstables();

    if (sstables.empty()) {
      ret = OB_ITER_END;
      STORAGE_LOG(WARN, "This tablet does not have sstables.", KR(ret), K(table_store));
    } else {
      tx_data_buffers_.reset();
      ret = get_next_row_(sstables, tx_data);
      if (OB_TIMEOUT == ret || OB_DISK_HUNG == ret) {
        ret = OB_EAGAIN;
        STORAGE_LOG(WARN,
                    "modify ret code from OB_TIMEOUT or OB_DISK_HUNG to OB_EAGAIN",
                    KR(ret));
      }
    }
  }
  return ret;
}

int ObTxDataSingleRowGetter::get_next_row_(ObSSTableArray &sstables, ObTxData &tx_data)
{
  int ret = OB_SUCCESS;

  GENERATE_ACCESS_CONTEXT
  int64_t int_tx_id = tx_id_.get_id();
  int64_t idx = 0;
  GENERATE_ROW_KEY

  if (OB_SUCC(ret)) {
    ObStringHolder temp_buffer;
    int64_t total_need_buffer_cnt = 0;
    if (OB_FAIL(get_row_from_sstables_(row_key,
                                       sstables,
                                       iter_param_,
                                       access_context,
                                       temp_buffer,
                                       total_need_buffer_cnt))) {
      if (OB_ITER_END == ret) {
        STORAGE_LOG(WARN, "tx data not found in sstables", KR(ret), K(tx_id_), K(sstables));
      } else {
        STORAGE_LOG(WARN, "get row from sstables fail.", KR(ret));
      }
    } else if (OB_FAIL(tx_data_buffers_.reserve(total_need_buffer_cnt))) {
      STORAGE_LOG(WARN, "array reserve spaces failed", KR(ret));
    } else if (OB_FAIL(tx_data_buffers_.push_back(std::move(temp_buffer)))) {
      STORAGE_LOG(WARN, "push element to reserved array should not fail", KR(ret));
    } else {
      STORAGE_LOG(INFO, "GENGLI total need buffer cnt", K(total_need_buffer_cnt));
      int64_t total_need_buffer_cnt2 = 0;
      for (int64_t idx = 1; idx < total_need_buffer_cnt && OB_SUCC(ret); ++idx) {
        key_datums_[1].set_int(idx);
        if (OB_FAIL(row_key.assign(key_datums_, 2))) {
          STORAGE_LOG(WARN, "assign row key failed", KR(ret));
        } else if (OB_FAIL(get_row_from_sstables_(row_key,
                                                  sstables,
                                                  iter_param_,
                                                  access_context,
                                                  temp_buffer,
                                                  total_need_buffer_cnt2))) {
          STORAGE_LOG(WARN, "get row from sstable failed",
                            KR(ret), K(idx), K_(tx_id), K(total_need_buffer_cnt));
        } else if (OB_FAIL(tx_data_buffers_.push_back(std::move(temp_buffer)))) {
          STORAGE_LOG(WARN, "push element to reserved array should not fail", KR(ret));
        } else {
          if (total_need_buffer_cnt != total_need_buffer_cnt2) {
            STORAGE_LOG(ERROR, "multi row's total column count not equal",
                               KR(ret), K(total_need_buffer_cnt), K(total_need_buffer_cnt2));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(deserialize_tx_data_from_store_buffers_(tx_data))) {
          STORAGE_LOG(WARN, "deserialize from buffers failed", KR(ret), K_(tx_id));
        }
      }
    }
  }

  return ret;
}

int ObTxDataSingleRowGetter::get_row_from_sstables_(blocksstable::ObDatumRowkey &row_key,
                                                    ObSSTableArray &sstables,
                                                    const ObTableIterParam &iter_param,
                                                    ObTableAccessContext &access_context,
                                                    ObStringHolder &temp_buffer,
                                                    int64_t &total_need_buffer_cnt)
{
  int ret = OB_SUCCESS;

  ObStoreRowIterator *row_iter = nullptr;
  ObITable *table = nullptr;
  int tmp_ret = OB_SUCCESS;
  bool find = false;
  const blocksstable::ObDatumRow *row = nullptr;
  for (int i = sstables.count() - 1; OB_SUCC(ret) && !find && i >= 0; i--) {
    if (OB_ISNULL(table = sstables[i])) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "Unexpected null table", KR(ret), K(i), K(sstables));
    } else if (OB_FAIL(table->get(iter_param, access_context, row_key, row_iter))) {
      STORAGE_LOG(WARN, "Failed to get param", KR(ret), KPC(table));
    } else if (OB_FAIL(row_iter->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "Failed to get next row", KR(ret), KPC(table));
      }
    } else if (row->row_flag_.is_not_exist()) {
      // this tx data not exsit in this sstable, try next one
    } else if (row->storage_datums_[TX_DATA_ID_COLUMN].get_int() != tx_id_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "read wrong tx data from sstable",
                         KR(ret), KPC(table),
                         K(row->storage_datums_[TX_DATA_ID_COLUMN].get_int()), K(tx_id_));
    } else {
      find = true;
      total_need_buffer_cnt = row->storage_datums_[TX_DATA_TOTAL_ROW_CNT_COLUMN].get_int();
      if (OB_FAIL(temp_buffer.assign(row->storage_datums_[TX_DATA_VAL_COLUMN].get_string()))) {
        STORAGE_LOG(WARN, "Failed to copy buffer", KR(ret), KPC(table));
      }
    }

    if (OB_NOT_NULL(row_iter)) {
      row_iter->~ObStoreRowIterator();
      row_iter = nullptr;
    }
  }

  if (OB_SUCC(ret) && !find) {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObTxDataSingleRowGetter::deserialize_tx_data_from_store_buffers_(ObTxData &tx_data)
{
  int ret = OB_SUCCESS;
  int64_t total_buffer_size = 0;
  int64_t pos = 0;
  char *merge_buffer = nullptr;
  for (int64_t idx = 0; idx < tx_data_buffers_.count(); ++idx) {
    total_buffer_size += tx_data_buffers_[idx].get_ob_string().length();
  }
  if (total_buffer_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected buffer size", KR(ret), K(total_buffer_size));
  } else if (nullptr == (merge_buffer = (char*)DEFAULT_TX_DATA_ALLOCATOR.
                                               alloc(total_buffer_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc merge buffer", KR(ret), K(total_buffer_size));
  } else {
    char *p_dest = merge_buffer;
    for (int64_t idx = 0; idx < tx_data_buffers_.count(); ++idx) {
      OB_ASSERT(p_dest + tx_data_buffers_[idx].get_ob_string().length() <=
                merge_buffer + total_buffer_size);// abort or even worse
      memcpy(p_dest, tx_data_buffers_[idx].get_ob_string().ptr(),
             tx_data_buffers_[idx].get_ob_string().length());
      p_dest += tx_data_buffers_[idx].get_ob_string().length();
    }
    tx_data.tx_id_ = tx_id_;
    if (OB_FAIL(tx_data.deserialize(merge_buffer, total_buffer_size, pos, slice_allocator_))) {
      STORAGE_LOG(WARN, "deserialize tx data failed",
                        KR(ret), KPHEX(merge_buffer, total_buffer_size));
      hex_dump(merge_buffer, total_buffer_size, true, OB_LOG_LEVEL_WARN);
    } else if (!tx_data.is_valid_in_tx_data_table()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "the deserialized tx data is invalid.", KR(ret), K(tx_data));
    }
  }
  if (OB_NOT_NULL(merge_buffer)) {
    DEFAULT_TX_DATA_ALLOCATOR.free(merge_buffer);
  }
  return ret;
}

/***************************** ObTxDataSingleRowGetter **********************************/

/***************************** ObCommitVersionsGetter **********************************/

int ObCommitVersionsGetter::get_next_row(ObCommitSCNsArray &commit_scns)
{
  int ret = OB_SUCCESS;
  GENERATE_ACCESS_CONTEXT
  int64_t int_tx_id = INT64_MAX;
  // TODO : @gengli The serializd data of commit versions may be divided into multiple rows which
  // means the idx can be greater than 0
  int64_t idx = 0;
  GENERATE_ROW_KEY

  if (OB_SUCC(ret)) {
    ObStoreRowIterator *row_iter = nullptr;
    const ObDatumRow *row = nullptr;
    if (!iter_param_.tablet_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "tablet handle in iter param is invalid", KR(ret), K(iter_param_));
    } else if (OB_FAIL(table_->get(iter_param_, access_context, row_key, row_iter))) {
      STORAGE_LOG(WARN, "Failed to get param", K(ret), KPC(table_));
    } else if (OB_FAIL(row_iter->get_next_row(row))) {
      STORAGE_LOG(ERROR, "Failed to get pre-process data for upper trans version calculation",
                  KR(ret), KPC(table_));
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected nullptr of row", KR(ret));
    } else if (row->row_flag_.is_not_exist()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "Failed to get pre-process data for upper trans version calculation",
                  KR(ret), KPC(table_));
    } else {
      int64_t pos = 0;

      const ObString str = row->storage_datums_[TX_DATA_VAL_COLUMN].get_string();
<<<<<<< HEAD
      if (OB_FAIL(commit_scns.deserialize(str.ptr(), str.length(), pos))) {
        STORAGE_LOG(WARN, "deserialize commit versions array failed.", KR(ret));
      } else if (0 == commit_scns.array_.count()) {
=======

      if (OB_FAIL(commit_versions.deserialize(str.ptr(), str.length(), pos))) {
        STORAGE_LOG(WARN, "deserialize commit versions array failed.", KR(ret), KPC(row));
      } else if (0 == commit_versions.array_.count()) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "Unexpected empty commit versions array.", KR(ret), KPC(row));
      } else if (!commit_scns.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "invalid cache", KR(ret));
      } else {
        // get commit versions from tx data sstable done.
      }
    }

    if (OB_NOT_NULL(row_iter)) {
      row_iter->~ObStoreRowIterator();
      row_iter = nullptr;
    }
  }
  return ret;
}

/***************************** ObCommitVersionsGetter **********************************/

int ObTxCtxMemtableScanIterator::init(ObTxCtxMemtable *tx_ctx_memtable)
{
  int ret = OB_SUCCESS;
  transaction::ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
  // TODO(handora.qc): Optimize the iterator

  if (OB_ISNULL(ls_tx_ctx_mgr = tx_ctx_memtable->get_ls_tx_ctx_mgr())) {
    ret = OB_BAD_NULL_ERROR;
    STORAGE_LOG(ERROR, "get ls tx ctx mgr failed", KR(ret));
  } else if (OB_FAIL(row_.init(allocator_, TX_CTX_TABLE_MAX_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT))) {
    STORAGE_LOG(WARN, "Failed to init datum row", KR(ret));
  } else if (OB_FAIL(buf_.reserve(TX_CTX_BUF_LENGTH))) {
    STORAGE_LOG(WARN, "Failed to reserve tx ctx buffer", KR(ret));
  } else if (OB_FAIL(meta_buf_.reserve(TX_CTX_META_BUF_LENGTH))) {
    STORAGE_LOG(WARN, "Failed to reserve tx ctx meta buffer", K(ret));
    // NB: We must first prepare the rec_scn for ObLSTxCtxMgr and then
    // prepare the rec_scn for tx ctx
  } else if (OB_FAIL(ls_tx_ctx_mgr->refresh_aggre_rec_scn())) {
    STORAGE_LOG(WARN, "Failed to prepare for dump tx ctx", K(ret));
  } else if (OB_FAIL(ls_tx_ctx_iter_.set_ready(ls_tx_ctx_mgr))) {
    STORAGE_LOG(WARN, "ls_tx_ctx_iter set_ready failed", KR(ret));
  } else {
    row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    idx_ = 0;
    is_inited_ = true;
    STORAGE_LOG(INFO, "ObTxCtxMemtableScanIterator init succ", KPC(this));
  }

  return ret;
}

int ObTxCtxMemtableScanIterator::get_next_tx_ctx_table_info_(transaction::ObPartTransCtx *&tx_ctx,
                                                             ObTxCtxTableInfo &ctx_info)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;

  while (OB_SUCC(ret) && need_retry) {
    if (OB_FAIL(ls_tx_ctx_iter_.get_next_tx_ctx(tx_ctx))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "ls_tx_ctx_iter_.get_next_tx_ctx failed", K(ret));
      }
    } else if (OB_FAIL(tx_ctx->get_tx_ctx_table_info(ctx_info))) {
      if (OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "tx_ctx->get_tx_ctx_table_info failed", K(ret));
      }
      ls_tx_ctx_iter_.revert_tx_ctx(tx_ctx);
    } else {
      need_retry = false;
    }
  }

  return ret;
}

int ObTxCtxMemtableScanIterator::inner_get_next_row(const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;

  ObTxCtxTableMeta curr_meta;
  ObTxCtxTableInfo ctx_info;
  transaction::ObPartTransCtx *tx_ctx = NULL;
  char *row_buf = NULL;
  int64_t need_merge_length = 0;
  int64_t cur_merge_length = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx ctx memtable scan iterator is not inited");
  }

  if (OB_SUCC(ret)) {
    if (has_unmerged_buf_) {
      row_buf = buf_.get_ptr() + unmerged_buf_start_pos_;
      need_merge_length = prev_meta_.get_tx_ctx_serialize_size() - unmerged_buf_start_pos_;
      if (OB_FAIL(prev_meta_.get_multi_row_next_extent(curr_meta))) {
        STORAGE_LOG(WARN, "prev_meta_.get_multi_row_next_extent failed", KR(ret), K_(prev_meta));
      }
      STORAGE_LOG(DEBUG, "write prev tx ctx unmerged buffer", K(prev_meta_));
    } else {
      if (OB_FAIL(get_next_tx_ctx_table_info_(tx_ctx, ctx_info))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "get_next_tx_ctx_table_info_ failed", K(ret));
        }
      } else {
        int64_t serialize_size = ctx_info.get_serialize_size();
        curr_meta.init(tx_ctx->get_trans_id(), tx_ctx->get_ls_id(), serialize_size,
                       // ceil((double)serialize_size / MAX_VALUE_LENGTH_)
                       (serialize_size + MAX_VALUE_LENGTH_ - 1) / MAX_VALUE_LENGTH_, 0);
        if (OB_FAIL(buf_.reserve(serialize_size))) {
          STORAGE_LOG(WARN, "Failed to reserve local buffer", KR(ret));
        } else {
          int64_t pos = 0;
          if (OB_FAIL(ctx_info.serialize(buf_.get_ptr(), serialize_size, pos))) {
            STORAGE_LOG(WARN, "failed to serialize ctx_info", KR(ret), K(ctx_info), K(pos));
          } else {
            row_buf = buf_.get_ptr();
            need_merge_length = serialize_size;
          }
        }
        STORAGE_LOG(DEBUG, "write tx ctx info", K(ctx_info), K(serialize_size));
        ls_tx_ctx_iter_.revert_tx_ctx(tx_ctx);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (need_merge_length > MAX_VALUE_LENGTH_) {
      has_unmerged_buf_ = true;
      unmerged_buf_start_pos_ += MAX_VALUE_LENGTH_;
      cur_merge_length = MAX_VALUE_LENGTH_;
    } else {
      has_unmerged_buf_ = false;
      unmerged_buf_start_pos_ = 0;
      cur_merge_length = need_merge_length;
    }
  }

  int64_t meta_serialize_size = curr_meta.get_serialize_size();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(meta_buf_.reserve(meta_serialize_size))) {
      STORAGE_LOG(WARN, "Failed to reserve tx ctx meta buffer", KR(ret));
    } else {
      int64_t pos = 0;
      if (OB_FAIL(curr_meta.serialize(meta_buf_.get_ptr(), meta_serialize_size, pos))) {
        STORAGE_LOG(WARN, "Failed to serialize curr_meta", KR(ret), K(curr_meta), K(pos));
      } else {
        // do nothing
        STORAGE_LOG(DEBUG, "Serialize curr_meta success", K(curr_meta));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // key column
    row_.storage_datums_[TX_CTX_TABLE_KEY_COLUMN].set_int((int)(idx_));
    row_.storage_datums_[TX_CTX_TABLE_KEY_COLUMN + 1].set_int(-4096);
    row_.storage_datums_[TX_CTX_TABLE_KEY_COLUMN + 2].set_int(0);
    // meta colomn
    int64_t meta_col
      = TX_CTX_TABLE_META_COLUMN + SSTABLE_HIDDEN_COLUMN_CNT;
    row_.storage_datums_[meta_col].set_string(ObString(meta_serialize_size, meta_buf_.get_ptr()));
    // value column
    int64_t value_col = meta_col + 1;
    row_.storage_datums_[value_col].set_string(ObString(cur_merge_length, row_buf));

    row_.set_first_multi_version_row();
    row_.set_last_multi_version_row();
    row_.set_compacted_multi_version_row();
    row = &row_;
    STORAGE_LOG(DEBUG, "write tx ctx info", K(ctx_info), K(idx_), K(curr_meta));
    idx_++;
  }

  if (OB_SUCC(ret)) {
    prev_meta_ = curr_meta;
  }
  STORAGE_LOG(DEBUG, "ObTxCtxMemtableScanIterator::inner_get_next_row finished", K_(prev_meta));
  return ret;
}

void ObTxCtxMemtableScanIterator::reset()
{
  idx_ = -1;
  ls_tx_ctx_iter_.reset();
  buf_.reset();
  row_.reset();
  allocator_.reset();
  is_inited_ = false;
}

void ObTxCtxMemtableScanIterator::reuse() { reset(); }

}  // namespace storage
}  // namespace oceanbase
