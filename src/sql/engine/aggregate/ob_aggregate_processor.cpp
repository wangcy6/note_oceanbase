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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/expr/ob_expr_less_than.h"
#include "sql/engine/expr/ob_expr_div.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_estimate_ndv.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"
#include "sql/parser/ob_item_type_str.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/basic/ob_material_op_impl.h"
#include "share/stat/ob_hybrid_hist_estimator.h"

namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{

OB_DEF_SERIALIZE(ObAggrInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              expr_,
              real_aggr_type_,
              has_distinct_,
              is_implicit_first_aggr_,
              has_order_by_,
              param_exprs_,
              distinct_collations_,
              distinct_cmp_funcs_,
              group_concat_param_count_,
              sort_collations_,
              sort_cmp_funcs_,
              separator_expr_,
              window_size_param_expr_,
              item_size_param_expr_,
              is_need_deserialize_row_,
              pl_agg_udf_type_id_,
              pl_agg_udf_params_type_,
              pl_result_type_,
              bucket_num_param_expr_,
              rollup_idx_,
              grouping_idxs_,
              group_idxs_,
              format_json_,
              strict_json_,
              absent_on_null_,
              returning_type_,
              with_unique_keys_
  );
  if (T_FUN_AGG_UDF == get_expr_type()) {
    OB_UNIS_ENCODE(*dll_udf_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObAggrInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              expr_,
              real_aggr_type_,
              has_distinct_,
              is_implicit_first_aggr_,
              has_order_by_,
              param_exprs_,
              distinct_collations_,
              distinct_cmp_funcs_,
              group_concat_param_count_,
              sort_collations_,
              sort_cmp_funcs_,
              separator_expr_,
              window_size_param_expr_,
              item_size_param_expr_,
              is_need_deserialize_row_,
              pl_agg_udf_type_id_,
              pl_agg_udf_params_type_,
              pl_result_type_,
              bucket_num_param_expr_,
              rollup_idx_,
              grouping_idxs_,
              group_idxs_,
              format_json_,
              strict_json_,
              absent_on_null_,
              returning_type_,
              with_unique_keys_
  );
  if (T_FUN_AGG_UDF == get_expr_type()) {
    CK(NULL != alloc_);
    if (OB_SUCC(ret)) {
      dll_udf_ = OB_NEWx(ObAggDllUdfInfo, alloc_, (*alloc_), real_aggr_type_);
      if (OB_ISNULL(dll_udf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate failed", K(ret));
      } else {
        OB_UNIS_DECODE(*dll_udf_);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObAggrInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              expr_,
              real_aggr_type_,
              has_distinct_,
              is_implicit_first_aggr_,
              has_order_by_,
              param_exprs_,
              distinct_collations_,
              distinct_cmp_funcs_,
              group_concat_param_count_,
              sort_collations_,
              sort_cmp_funcs_,
              separator_expr_,
              window_size_param_expr_,
              item_size_param_expr_,
              is_need_deserialize_row_,
              pl_agg_udf_type_id_,
              pl_agg_udf_params_type_,
              pl_result_type_,
              bucket_num_param_expr_,
              rollup_idx_,
              grouping_idxs_,
              group_idxs_,
              format_json_,
              strict_json_,
              absent_on_null_,
              returning_type_,
              with_unique_keys_
  );
  if (T_FUN_AGG_UDF == get_expr_type()) {
    OB_UNIS_ADD_LEN(*dll_udf_);
  }
  return len;
}

ObAggrInfo::~ObAggrInfo()
{
}

int64_t ObAggrInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_AGGR_FUNC, get_type_name(get_expr_type()),
       K_(real_aggr_type),
       KPC_(expr),
       N_DISTINCT, has_distinct_,
       K_(has_order_by),
       K_(is_implicit_first_aggr),
       K_(param_exprs),
       K_(distinct_collations),
       K_(distinct_cmp_funcs),
       K_(group_concat_param_count),
       K_(sort_collations),
       K_(sort_cmp_funcs),
       KPC_(separator_expr),
       KPC_(window_size_param_expr),
       KPC_(item_size_param_expr),
       K_(is_need_deserialize_row),
       K_(pl_agg_udf_type_id),
       K_(pl_result_type)
       );
  J_OBJ_END();
  return pos;
}

ObAggregateProcessor::AggrCell::~AggrCell()
{
  destroy();
}

// arena allocator alloc memory, it don't free
void ObAggregateProcessor::AggrCell::destroy()
{
  if (NULL != extra_) {
    auto *&extra = extra_;
    extra->~ExtraResult();
    extra_ = NULL;
  }
}

int ObAggregateProcessor::AggrCell::deep_copy_advance_collect_result(const ObDatum &datum, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint32_t len = datum.len_ > 0 ? datum.len_: 1;
  if (datum.is_null()) {
    advance_collect_result_ = datum;
  } else {
    if (NULL == collect_buf_ || collect_buf_len_ < len) {
      collect_buf_len_ = next_pow2(len);
      if (OB_ISNULL(collect_buf_ = static_cast<char *>(alloc.alloc(collect_buf_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(collect_buf_len_), K(datum), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(advance_collect_result_.deep_copy(datum, collect_buf_, collect_buf_len_, pos))) {
      LOG_WARN("failed to deep copy datum", K(ret));
    }
  }
  return ret;
}

int ObAggregateProcessor::AggrCell::collect_result(
  const ObObjTypeClass tc, ObEvalCtx &eval_ctx, const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  ObDatum &result = aggr_info.expr_->locate_datum_for_write(eval_ctx);
  aggr_info.expr_->set_evaluated_projected(eval_ctx);
  // sum(count(*)) should return same type as count(*), which is bigint in mysql mode
  if (!lib::is_oracle_mode() && T_FUN_COUNT_SUM == aggr_info.get_expr_type()) {
    result.set_int(tiny_num_int_);
    if (ObIntTC != tc) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count sum should be int", K(ret), K(tc), K(is_tiny_num_used_),
        K(aggr_info.is_implicit_first_aggr_), K(tiny_num_int_));
    }
  } else if (is_tiny_num_used_ && (ObIntTC == tc || ObUIntTC == tc)) {
    ObNumStackAllocator<2> tmp_alloc;
    ObNumber result_nmb;
    const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
    ObNumber right_nmb;
    if (ObIntTC == tc) {
      if (OB_FAIL(right_nmb.from(tiny_num_int_, tmp_alloc))) {
        LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(tc));
      }
    } else {
      if (OB_FAIL(right_nmb.from(tiny_num_uint_, tmp_alloc))) {
        LOG_WARN("create number from int failed", K(ret), K(right_nmb), K(tc));
      }
    }

    if (OB_SUCC(ret)) {
      if (iter_result_.is_null()) {
        result.set_number(right_nmb);
      } else {
        ObNumber left_nmb(iter_result_.get_number());
        if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, tmp_alloc, strict_mode))) {
          LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
        } else {
          result.set_number(result_nmb);
        }
      }
    }
  } else {
    ret = aggr_info.expr_->deep_copy_datum(eval_ctx, iter_result_);
  }
  return ret;
}

int64_t ObAggregateProcessor::AggrCell::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(row_count),
       K_(tiny_num_int),
       K_(tiny_num_uint),
       K_(is_tiny_num_used),
       K_(iter_result),
       KPC_(extra));
  J_OBJ_END();
  return pos;
}

void ObAggregateProcessor::ExtraResult::reuse()
{
  if (NULL != unique_sort_op_) {
    unique_sort_op_->reuse();
  }
}

ObAggregateProcessor::ExtraResult::~ExtraResult()
{
  if (NULL != unique_sort_op_) {
    unique_sort_op_->~ObUniqueSortImpl();
    alloc_.free(unique_sort_op_);
    unique_sort_op_ = NULL;
  }
}

int ObAggregateProcessor::ExtraResult::init_distinct_set(const uint64_t tenant_id,
    const ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, const bool need_rewind,
    ObIOEventObserver *io_event_observer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_ISNULL(unique_sort_op_ = static_cast<ObUniqueSortImpl *>(
      alloc_.alloc(sizeof(ObUniqueSortImpl))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fall to alloc buff", "size", sizeof(ObUniqueSortImpl), K(ret));
  } else {
    new (unique_sort_op_) ObUniqueSortImpl(op_monitor_info_);
    if (OB_FAIL(unique_sort_op_->init(tenant_id,
                                      &aggr_info.distinct_collations_,
                                      &aggr_info.distinct_cmp_funcs_,
                                      &eval_ctx,
                                      &eval_ctx.exec_ctx_,
                                      need_rewind,
                                      (4L << 10) /* 4kb */))) {
      LOG_WARN("init distinct set failed", K(ret));
    } else {
      unique_sort_op_->set_io_event_observer(io_event_observer);
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != unique_sort_op_) {
      unique_sort_op_->~ObUniqueSortImpl();
      alloc_.free(unique_sort_op_);
      unique_sort_op_ = NULL;
    }
  }
  return ret;
}

int ObAggregateProcessor::GroupConcatExtraResult::init(const uint64_t tenant_id,
    const ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, const bool need_rewind, int64_t dir_id,
    ObIOEventObserver *io_event_observer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    row_count_ = 0;
    iter_idx_ = 0;

    if (aggr_info.has_order_by_) {
      if (OB_ISNULL(sort_op_ = static_cast<ObSortOpImpl *>(alloc_.alloc(sizeof(ObSortOpImpl))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fall to alloc buff", "size", sizeof(ObSortOpImpl), K(ret));
      } else {
        new (sort_op_) ObSortOpImpl(op_monitor_info_);
        if (OB_FAIL(sort_op_->init(tenant_id,
                                   &aggr_info.sort_collations_,
                                   &aggr_info.sort_cmp_funcs_,
                                   &eval_ctx,
                                   &eval_ctx.exec_ctx_,
                                   false,
                                   false,
                                   need_rewind))) {
          LOG_WARN("init sort_op_ failed", K(ret));
        } else {
          sort_op_->set_io_event_observer(io_event_observer);
        }
      }
    } else {
      int64_t sort_area_size = 0;
      if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(
                  SORT_WORK_AREA, tenant_id, &eval_ctx.exec_ctx_, sort_area_size))) {
        LOG_WARN("failed to get workarea size", K(ret), K(tenant_id));
      } else if (OB_FAIL(row_store_.init(sort_area_size,
                                         tenant_id,
                                         ObCtxIds::WORK_AREA,
                                         ObModIds::OB_SQL_AGGR_FUN_GROUP_CONCAT,
                                         true /* enable dump */))) {
        LOG_WARN("row store failed", K(ret));
      } else {
        row_store_.set_dir_id(dir_id);
        row_store_.set_io_event_observer(io_event_observer);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != sort_op_) {
      sort_op_->~ObSortOpImpl();
      alloc_.free(sort_op_);
      sort_op_ = NULL;
    }
  }
  return ret;
}

ObAggregateProcessor::GroupConcatExtraResult::~GroupConcatExtraResult()
{
  if (sort_op_ != NULL) {
    sort_op_->~ObSortOpImpl();
    alloc_.free(sort_op_);
    sort_op_ = NULL;
  } else {
    row_store_.reset();
  }
}

void ObAggregateProcessor::GroupConcatExtraResult::reuse_self()
{
  if (sort_op_ != NULL) {
    sort_op_->reuse();
  } else {
    row_store_iter_.reset();
    row_store_.reset();
   }
  bool_mark_.reset();
  row_count_ = 0;
  iter_idx_ = 0;
};

void ObAggregateProcessor::GroupConcatExtraResult::reuse()
{
  reuse_self();
  ExtraResult::reuse();
}

int ObAggregateProcessor::GroupConcatExtraResult::finish_add_row()
{
  iter_idx_ = 0;
  int ret = OB_SUCCESS;
  if (sort_op_ != NULL) {
    ret = sort_op_->sort();
  } else {
    row_store_iter_.reset();
    ret = row_store_iter_.init(&row_store_);
  }
  return ret;
}

int ObAggregateProcessor::GroupConcatExtraResult::rewind()
{
  int ret = OB_SUCCESS;
  if (sort_op_ != NULL) {
    if (OB_FAIL(sort_op_->rewind())) {
      LOG_WARN("rewind failed", K(ret));
    }
  } else {
    row_store_iter_.reset();
    if (OB_FAIL(row_store_iter_.init(&row_store_))) {
      LOG_WARN("row store iterator init failed", K(ret));
    }
  }
  iter_idx_ = 0;
  return ret;
}

int64_t ObAggregateProcessor::GroupConcatExtraResult::to_string(char *buf,
    const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(row_count),
       K_(iter_idx),
       K_(row_store),
       KP_(sort_op),
       KP_(unique_sort_op)
       );
  J_OBJ_END();
  return pos;
}

int ObAggregateProcessor::GroupConcatExtraResult::get_bool_mark(int64_t col_index, bool &is_bool)
{
  INIT_SUCC(ret);
  if (col_index + 1 > bool_mark_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index is overflow", K(ret), K(col_index), K(bool_mark_.count()));
  } else {
    is_bool = bool_mark_[col_index];
  }
  return ret;
}

int ObAggregateProcessor::GroupConcatExtraResult::set_bool_mark(int64_t col_index, bool is_bool)
{
  INIT_SUCC(ret);
  if (col_index > bool_mark_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index is overflow", K(ret), K(col_index), K(bool_mark_.count()));
  } else if (col_index == bool_mark_.count()) {
    bool_mark_.push_back(is_bool);
  } else {
    bool_mark_[col_index] = is_bool;
  }
  return ret;
}

ObAggregateProcessor::HybridHistExtraResult::~HybridHistExtraResult()
{
  if (sort_op_ != NULL) {
    sort_op_->~ObSortOpImpl();
    alloc_.free(sort_op_);
    sort_op_ = NULL;
  }
  if (mat_op_ != NULL) {
    mat_op_->~ObMaterialOpImpl();
    alloc_.free(mat_op_);
    mat_op_ = NULL;
  }
}

void ObAggregateProcessor::HybridHistExtraResult::reuse_self()
{
  if (sort_op_ != NULL) {
    sort_op_->reuse();
  }
  if (mat_op_ != NULL) {
    mat_op_->reuse();
  }
  sort_row_count_ = 0;
  material_row_count_ = 0;
};

void ObAggregateProcessor::HybridHistExtraResult::reuse()
{
  reuse_self();
  ExtraResult::reuse();
}

int ObAggregateProcessor::HybridHistExtraResult::init(const uint64_t tenant_id,
    const ObAggrInfo &aggr_info, ObEvalCtx &eval_ctx, const bool need_rewind,
    ObIOEventObserver *io_event_observer, ObSqlWorkAreaProfile &profile,
    ObMonitorNode &op_monitor_info)
{
  int ret = OB_SUCCESS;
  sort_row_count_ = 0;
  material_row_count_ = 0;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id));
  } else {
    if (OB_ISNULL(sort_op_ = static_cast<ObSortOpImpl *>(alloc_.alloc(sizeof(ObSortOpImpl))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fall to alloc buff", "size", sizeof(ObSortOpImpl));
    } else {
      new (sort_op_) ObSortOpImpl(op_monitor_info_);
      if (OB_FAIL(sort_op_->init(tenant_id,
                                 &aggr_info.sort_collations_,
                                 &aggr_info.sort_cmp_funcs_,
                                 &eval_ctx,
                                 &eval_ctx.exec_ctx_,
                                 false,
                                 false,
                                 need_rewind))) {
        LOG_WARN("init sort_op_ failed");
      } else {
        sort_op_->set_io_event_observer(io_event_observer);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(mat_op_ = static_cast<ObMaterialOpImpl *>(alloc_.alloc(sizeof(ObMaterialOpImpl))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fall to alloc buff", "size", sizeof(ObMaterialOpImpl));
    } else {
      new (mat_op_) ObMaterialOpImpl(op_monitor_info, profile);
      if (OB_FAIL(mat_op_->init(tenant_id,
                                &eval_ctx,
                                &eval_ctx.exec_ctx_,
                                io_event_observer))) {
        LOG_WARN("init mat_op_ failed");
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != sort_op_) {
      sort_op_->~ObSortOpImpl();
      alloc_.free(sort_op_);
      sort_op_ = NULL;
    }
    if (NULL != mat_op_) {
      mat_op_->~ObMaterialOpImpl();
      alloc_.free(mat_op_);
      mat_op_ = NULL;
    }
  }
  return ret;
}

int ObAggregateProcessor::HybridHistExtraResult::add_sort_row(
    const ObIArray<ObExpr *> &expr, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sort_op_)) {
    if (OB_FAIL(sort_op_->add_row(expr))) {
      LOG_WARN("failed to add row to sort op", K(expr));
    } else {
      ++sort_row_count_;
    }
  }
  return ret;
}

int ObAggregateProcessor::HybridHistExtraResult::add_sort_row(
    const ObChunkDatumStore::StoredRow &sr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sort_op_)) {
    if (OB_FAIL(sort_op_->add_stored_row(sr))) {
      LOG_WARN("failed to add row to sort op", K(sr));
    } else {
      ++sort_row_count_;
    }
  }
  return ret;
}

int ObAggregateProcessor::HybridHistExtraResult::get_next_row_from_sort(
    const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  sr = NULL;
  if (OB_NOT_NULL(sort_op_)) {
    ret = sort_op_->get_next_row(sr);
  }
  return ret;
}

int ObAggregateProcessor::HybridHistExtraResult::finish_add_sort_row()
{

  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sort_op_)) {
    if (OB_FAIL(sort_op_->sort())) {
      LOG_WARN("failed to sort rows");
    }
  }
  return ret;
}

int ObAggregateProcessor::HybridHistExtraResult::add_material_row(
    const ObDatum *src_datums,
    const int64_t datum_cnt,
    const int64_t extra_size,
    const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(mat_op_)) {
    if (OB_FAIL(mat_op_->add_row(src_datums, datum_cnt, extra_size, store_row))) {
      LOG_WARN("failed to add row to sort op", K(src_datums), K(datum_cnt));
    } else {
      ++material_row_count_;
    }
  }
  return ret;
}
int ObAggregateProcessor::HybridHistExtraResult::get_next_row_from_material(
    const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  sr = NULL;
  if (OB_NOT_NULL(mat_op_)) {
    ret = mat_op_->get_next_row(sr);
  }
  return ret;
}

int ObAggregateProcessor::HybridHistExtraResult::finish_add_material_row()
{

  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(mat_op_)) {
    if (OB_FAIL(mat_op_->finish_add_row())) {
      LOG_WARN("failed to sort rows");
    }
  }
  return ret;
}

int64_t ObAggregateProcessor::HybridHistExtraResult::to_string(
    char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(sort_row_count),
       K_(material_row_count),
       KP_(sort_op),
       KP_(mat_op)
      );
  J_OBJ_END();
  return pos;
}

int64_t ObAggregateProcessor::ExtraResult::to_string(char *buf,
    const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(unique_sort_op));
  J_OBJ_END();
  return pos;
}

int ObAggrInfo::eval_aggr(ObChunkDatumStore::ShadowStoredRow &curr_row_results,
    ObEvalCtx &ctx) const
{
  int ret = OB_SUCCESS;
  if (param_exprs_.empty() && T_FUN_COUNT == get_expr_type()) {
    //do nothing
  } else {
    ObChunkDatumStore::StoredRow *store_row = curr_row_results.get_store_row();
    if (OB_ISNULL(curr_row_results.get_store_row())) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "NULL datums or count mismatch", K(ret),
                  KPC(store_row), K(param_exprs_.count()));
    } else {
      ObDatum *datum = nullptr;
      ObDatum *cells = store_row->cells();
      store_row->cnt_ = param_exprs_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs_.count(); i++) {
        if (OB_FAIL(param_exprs_.at(i)->eval(ctx, datum))) {
          SQL_ENG_LOG(WARN, "failed to evaluate expr datum", K(ret), K(i));
        } else {
          cells[i] = *datum;
        }
      }
    }
  }
  return ret;
}

int ObAggrInfo::eval_param_batch(const ObBatchRows &brs, ObEvalCtx &ctx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs_.count(); i++) {
    if (OB_FAIL(param_exprs_.at(i)->eval_batch(ctx, *(brs.skip_), brs.size_))) {
      SQL_ENG_LOG(WARN, "failed to evaluate expr datum", K(ret), K(i));
    }
  }
  return ret;
}

ObAggregateProcessor::DllUdfExtra::~DllUdfExtra()
{
  if (NULL != udf_fun_) {
    udf_fun_->process_deinit_func(udf_ctx_);
  }
}

ObAggregateProcessor::ObAggregateProcessor(ObEvalCtx &eval_ctx,
                                           ObIArray<ObAggrInfo> &aggr_infos,
                                           const lib::ObLabel &label,
                                           ObMonitorNode &op_monitor_info,
                                           const int64_t tenant_id)
    : has_distinct_(false),
      has_order_by_(false),
      has_group_concat_(false),
      in_window_func_(false),
      has_extra_(false),
      eval_ctx_(eval_ctx),
      aggr_alloc_(label,
                  common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
                  tenant_id,
                  ObCtxIds::WORK_AREA),
      cur_batch_group_idx_(0),
      cur_batch_group_buf_(nullptr),
      aggr_infos_(aggr_infos),
      aggr_func_ctxs_(eval_ctx.exec_ctx_.get_allocator()),
      group_rows_(),
      concat_str_max_len_(OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE),
      cur_concat_buf_len_(0),
      concat_str_buf_(NULL),
      skip_(nullptr),
      aggr_code_idx_(OB_INVALID_INDEX_INT64),
      distinct_aggr_count_(0),
      aggr_code_expr_(nullptr),
      aggr_stage_(ObThreeStageAggrStage::NONE_STAGE),
      rollup_status_(ObRollupStatus::NONE_ROLLUP),
      rollup_id_expr_(nullptr),
      start_partial_rollup_idx_(0),
      end_partial_rollup_idx_(0),
      dir_id_(-1),
      tmp_store_row_(nullptr),
      io_event_observer_(nullptr),
      removal_info_(),
      support_fast_single_row_agg_(false),
      op_eval_infos_(nullptr),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      op_monitor_info_(op_monitor_info),
      need_advance_collect_(false)
{
}

int ObAggregateProcessor::init()
{
  int ret = OB_SUCCESS;
  // add aggr columns
  has_distinct_ = false;
  has_order_by_ = false;
  has_group_concat_ = false;
  start_partial_rollup_idx_ = 0;
  end_partial_rollup_idx_ = 0;
  removal_info_.reset();
  set_tenant_id(eval_ctx_.exec_ctx_.get_my_session()->get_effective_tenant_id());

  if (OB_ISNULL(eval_ctx_.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(group_rows_.reserve(1))) {
    LOG_WARN("failed to reserve", K(ret));
  } else if (OB_FAIL(aggr_func_ctxs_.prepare_allocate(aggr_infos_.count()))) {
    LOG_WARN("init array failed", K(ret));
  }
  int64_t max_param_expr_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(i);
    max_param_expr_cnt = std::max(max_param_expr_cnt, aggr_info.get_child_output_count());
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(aggr_info), K(i), K(ret));
    } else {
      has_distinct_ |= aggr_info.has_distinct_;
      has_group_concat_ |= (T_FUN_GROUP_CONCAT == aggr_info.get_expr_type() ||
                            T_FUN_KEEP_WM_CONCAT == aggr_info.get_expr_type() ||
                            T_FUN_WM_CONCAT == aggr_info.get_expr_type() ||
                            T_FUN_JSON_ARRAYAGG == aggr_info.get_expr_type() ||
                            T_FUN_ORA_JSON_ARRAYAGG == aggr_info.get_expr_type() ||
                            T_FUN_JSON_OBJECTAGG == aggr_info.get_expr_type() ||
                            T_FUN_ORA_JSON_OBJECTAGG == aggr_info.get_expr_type() ||
                            T_FUN_ORA_XMLAGG == aggr_info.get_expr_type());
      has_order_by_ |= aggr_info.has_order_by_;
      if (!has_extra_) {
        has_extra_ |= aggr_info.has_distinct_;
        has_extra_ |= need_extra_info(aggr_info.get_expr_type());
      }

      if (T_FUN_MEDIAN == aggr_info.get_expr_type()
          || T_FUN_GROUP_PERCENTILE_CONT == aggr_info.get_expr_type()) {
        // ObAggregateProcessor::init would be invoked many times under groupby rescan
        // Only create LinearInterAggrFuncCtx once to prevent memory leak.
        //
        // Details:
        // Normally ObAggregateProcessor::init would ONLY be invoked once under open
        // stage and NEVER be triggered any more. Typically window function follows
        // this rule.
        // However, ObAggregateProcessor::init would be invoked many times under groupby
        // rescan cases, see ObGroupByOp::inner_rescan. And LinearInterAggrFuncCtx would
        // be created repeatedly. This break init semantic(invoked once) and leading
        // memory leak.
        //
        // Solution:
        // Only create LinearInterAggrFuncCtx once when ObAggregateProcessor::init is called
        // repeatedly. So both window function and groupby cases would be well handled.
        //
        // TODO qubin.qb:
        // Refactor group by rescan API to stop calling ObAggregateProcessor::init so that
        // init semantic (only invoke one time) would be strictly followed
        if (aggr_func_ctxs_.at(i) == nullptr) {
          LinearInterAggrFuncCtx *ctx = OB_NEWx(LinearInterAggrFuncCtx,
                                                (&eval_ctx_.exec_ctx_.get_allocator()));
          if (NULL == ctx) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            aggr_func_ctxs_.at(i) = ctx;
          }
        }
      }
    }
  } // end for

  // vector in case
  if (eval_ctx_.is_vectorized()) {
    void * data = nullptr;
    if (OB_ISNULL(data = aggr_alloc_.alloc(ObBitVector::memory_size(eval_ctx_.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for last_skip", K(ret));
    } else {
      skip_ = to_bit_vector(data);
      skip_->reset(eval_ctx_.max_batch_size_);
    }
  }
  if (OB_FAIL(ret) || OB_NOT_NULL(tmp_store_row_)) {
  } else if (OB_ISNULL(tmp_store_row_
                = static_cast<ObChunkDatumStore::ShadowStoredRow *>
                  (eval_ctx_.exec_ctx_.get_allocator()
                                      .alloc(sizeof(ObChunkDatumStore::ShadowStoredRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to init curr row results", K(ret));
  } else {
    new (tmp_store_row_) ObChunkDatumStore::ShadowStoredRow();
    if (OB_FAIL(tmp_store_row_->init(eval_ctx_.exec_ctx_.get_allocator(), max_param_expr_cnt))) {
      LOG_WARN("init failed", K(ret));
    }
  }
  LOG_DEBUG("succ to init ObAggregateProcessor", K(ret));
  return ret;
}

void ObAggregateProcessor::destroy()
{
  for (int64_t i = 0; i < group_rows_.count(); ++i) {
    group_rows_.at(i)->destroy();
  }
  if (concat_str_buf_ != NULL) {
    aggr_alloc_.free(concat_str_buf_);
    concat_str_buf_ = NULL;
  }
  if (nullptr != skip_) {
    aggr_alloc_.free(skip_);
    skip_ = NULL;
  }
  if (nullptr != tmp_store_row_) {
    tmp_store_row_->reset();
    tmp_store_row_ = nullptr;
  }
  group_rows_.reset();
  cur_batch_group_idx_ = 0;
  cur_batch_group_buf_ = nullptr;
  aggr_alloc_.reset();

  FOREACH_CNT(it, aggr_func_ctxs_) {
    if (NULL != *it) {
      (*it)->~IAggrFuncCtx();
      *it = NULL;
    }
  }
  aggr_func_ctxs_.reset();
  removal_info_.reset();
}

void ObAggregateProcessor::reuse()
{
  if (has_extra_) {
    // 只有extra信息才需要清理，否则aggr_alloc_直接reset掉内存就释放掉
    for (int64_t i = 0; i < group_rows_.count(); ++i) {
      group_rows_.at(i)->destroy();
    }
  }
  if (concat_str_buf_ != NULL) {
    aggr_alloc_.free(concat_str_buf_);
    concat_str_buf_ = NULL;
  }
  if (nullptr != skip_) {
    aggr_alloc_.free(skip_);
    skip_ = NULL;
  }
  cur_concat_buf_len_ = 0;
  group_rows_.reuse();
  cur_batch_group_idx_ = 0;
  cur_batch_group_buf_ = nullptr;
  need_advance_collect_ = false;
  aggr_alloc_.reset_remain_one_page();
  removal_info_.reset();
}

OB_INLINE int ObAggregateProcessor::clone_number_cell(const ObNumber &src_number, AggrCell &aggr_cell)
{
  int ret = OB_SUCCESS;
  //length + magic num + data
  int64_t need_size = sizeof(int64_t) * 2 + ObNumber::MAX_BYTE_LEN;
  if (OB_FAIL(clone_cell(aggr_cell, need_size, nullptr))) {
    SQL_LOG(WARN, "failed to clone cell", K(ret));
  } else {
    aggr_cell.get_iter_result().set_number(src_number);
    LOG_DEBUG("succ to clone cell", K(src_number), K(aggr_cell.get_iter_result()));
  }
  return ret;
}

int ObAggregateProcessor::prepare(GroupRow &group_row)
{
  int ret = OB_SUCCESS;
  // init aggr_info
  for (int64_t i = 0; OB_SUCC(ret) && i < group_row.n_cells_; ++i) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(i);
    AggrCell &aggr_cell = group_row.aggr_cells_[i];
    if (aggr_info.has_distinct_) {
      ExtraResult *ad_result = static_cast<ExtraResult *>(aggr_cell.get_extra());
      //only last one add distinct
      if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("distinct set is NULL", K(ret));
      } else if (T_FUN_TOP_FRE_HIST == aggr_info.get_expr_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("topk fre hist not support distinct", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "distinct on topk fre hist");
      }
    }
  }
  // for sort-based group by operator, for performance reason,
  // after producing a group, we will invoke reuse_group() function to clear the group
  // thus, we do not need to allocate the group space again here, simply reuse the space
  // process aggregate columns
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process(group_row, true))) {
      LOG_WARN("failed to process aggregate", K(ret));
    }
  }
  return ret;
}

int ObAggregateProcessor::inner_process(
  GroupRow &group_row, int64_t start_idx, int64_t end_idx, bool is_prepare)
{
  int ret = OB_SUCCESS;
  // for sort-based group by operator, for performance reason,
  // after producing a group, we will invoke reuse_group() function to clear the group
  // thus, we do not need to allocate the group space again here, simply reuse the space
  // process aggregate columns
  for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(i);
    AggrCell &aggr_cell = group_row.aggr_cells_[i];
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr info is null", K(aggr_cell), K(ret));
    } else if (aggr_info.is_implicit_first_aggr()) {
      if (!aggr_cell.get_is_evaluated()) {
        ObDatum *result = NULL;
        if (aggr_info.expr_->eval(eval_ctx_, result)) {
          LOG_WARN("eval failed", K(ret));
        } else if (OB_FAIL(clone_aggr_cell(aggr_cell, *result))) {
          LOG_WARN("failed to clone non_aggr cell", K(ret));
        } else {
          aggr_cell.set_is_evaluated(true);
        }
        LOG_DEBUG("debug prepare implicit aggr", K(ret), K(i),
          K(EXPR2STR(eval_ctx_, *aggr_info.expr_)));
      } else {
        LOG_DEBUG("debug prepare implicit aggr", K(ret), K(i),
          K(EXPR2STR(eval_ctx_, *aggr_info.expr_)));
      }
    } else if (OB_FAIL(aggr_info.eval_aggr(*tmp_store_row_, eval_ctx_))) {
      LOG_WARN("fail to eval", K(ret));
    } else {
      if (aggr_info.has_distinct_) {
        ExtraResult *ad_result = static_cast<ExtraResult *>(aggr_cell.get_extra());
        //only last one add distinct
        if (OB_FAIL(ad_result->unique_sort_op_->add_row(aggr_info.param_exprs_))) {
          LOG_WARN("add row to distinct set failed", K(ret));
        }
      } else {
        if (is_prepare && OB_FAIL(prepare_aggr_result(*tmp_store_row_->get_store_row(),
                                        &(aggr_info.param_exprs_),
                                        aggr_cell, aggr_info))) {
          LOG_WARN("failed to prepare_aggr_result", K(ret));
        } else if (!is_prepare && OB_FAIL(process_aggr_result(*tmp_store_row_->get_store_row(),
                                        &(aggr_info.param_exprs_),
                                        aggr_cell, aggr_info))) {
          LOG_WARN("failed to prepare_aggr_result", K(ret));
        }
      }
    }
    OX(LOG_DEBUG("finish prepare", K(aggr_cell)));
  } // end for
  return ret;
}

int ObAggregateProcessor::process(GroupRow &group_row, bool is_prepare)
{
  int ret = OB_SUCCESS;
  int64_t start_idx = 0;
  int64_t end_idx = group_row.n_cells_;
  if (ObThreeStageAggrStage::NONE_STAGE != aggr_stage_) {
    int64_t aggr_code = -1;
    // get aggr_code and then decide to go which path
    if (ObThreeStageAggrStage::THIRD_STAGE == aggr_stage_) {
      ObDatum *datum = nullptr;
      if (OB_FAIL(aggr_code_expr_->eval(eval_ctx_, datum))) {
        LOG_WARN("failed to eval aggr_code_expr", K(ret));
      } else {
        aggr_code = datum->get_int();
      }
    } else if (ObThreeStageAggrStage::SECOND_STAGE == aggr_stage_) {
      // only process one group and only process one aggregate function
      if (OB_INVALID_INDEX_INT64 == aggr_code_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: aggr_code_idx is invalid", K(ret));
      } else {
        aggr_code = group_row.groupby_store_row_->cells()[aggr_code_idx_].get_int();
      }
    }
    if (OB_SUCC(ret) && -1 != aggr_code) {
      if (aggr_code >= distinct_aggr_count_) {
        start_idx = distinct_aggr_count_;
        if (aggr_code != distinct_aggr_count_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: aggr_code is invalid", K(ret),
            K(aggr_code), K(distinct_aggr_count_));
        } else if (OB_ISNULL(dist_aggr_group_idxes_) || 0 == start_idx) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: distinct aggr group index is null", K(ret), K(start_idx));
        } else {
          start_idx = dist_aggr_group_idxes_->at(distinct_aggr_count_ - 1);
        }
      } else {
        start_idx = aggr_code;
        end_idx = aggr_code + 1;
        if (OB_ISNULL(dist_aggr_group_idxes_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: distinct aggr group index is null", K(ret));
        } else {
          start_idx = 0 == start_idx ? 0 : dist_aggr_group_idxes_->at(aggr_code - 1);
          end_idx = dist_aggr_group_idxes_->at(aggr_code);
        }
      }
    }
    LOG_DEBUG("debug process aggregate", K(ret), K(start_idx), K(end_idx), K(aggr_stage_),
      K(aggr_code));
  }
  if (OB_SUCC(ret) && OB_FAIL(inner_process(group_row, start_idx, end_idx, is_prepare))) {
    LOG_WARN("failed to process aggregate", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::collect(const int64_t group_id/*= 0*/,
    const ObExpr *diff_expr /*= NULL*/,
    const int64_t max_group_cnt /*= INT64_MIN*/)
{
  int ret = OB_SUCCESS;
  GroupRow *group_row = NULL;
  if (OB_FAIL(group_rows_.at(group_id, group_row))) {
    LOG_WARN("failed to get group_row", K(group_id), K(group_rows_.count()), K(ret));
  } else if (OB_FAIL(collect_group_row(group_row, group_id, diff_expr, max_group_cnt))) {
    LOG_WARN("failed to collect group row", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::collect_group_row(GroupRow *group_row,
                                            const int64_t group_id /*= 0*/,
                                            const ObExpr *diff_expr /*= NULL*/,
                                            const int64_t max_group_cnt /*= INT64_MIN*/)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(group_row));
  for (int64_t i = 0; OB_SUCC(ret) && i < group_row->n_cells_; ++i) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(i);
    AggrCell &aggr_cell = group_row->aggr_cells_[i];
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(aggr_info), K(ret));
    } else if (aggr_info.is_implicit_first_aggr()) {
      ObDatum &result = aggr_info.expr_->locate_expr_datum(eval_ctx_);
      aggr_info.expr_->set_evaluated_projected(eval_ctx_);
      result.set_datum(aggr_cell.get_iter_result());
      LOG_DEBUG("debug implicit first aggr", K(ret), K(i),
        K(EXPR2STR(eval_ctx_, *aggr_info.expr_)));
    } else {
      if (aggr_info.has_distinct_) {
        ExtraResult *ad_result = static_cast<ExtraResult *>(aggr_cell.get_extra());
        if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("distinct set is NULL", K(ret));
        } else {
          if (group_id > 0 && group_id > start_partial_rollup_idx_ &&
              group_id <= end_partial_rollup_idx_) {
            // Group id greater than zero in sort based group by must be rollup,
            // distinct set is sorted and iterated in rollup_process(), rewind here.
            // if partial rollup, then group_id > 0 may not sort
            //    the first partial_rolup_idx_ group need sort
            if (OB_FAIL(ad_result->unique_sort_op_->rewind())) {
              LOG_WARN("rewind iterator failed", K(ret));
            }
          } else {
            if (OB_FAIL(ad_result->unique_sort_op_->sort())) {
              LOG_WARN("sort failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(process_aggr_result_from_distinct(aggr_cell, aggr_info))) {
            LOG_WARN("aggregate distinct cell failed", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(collect_aggr_result(aggr_cell, diff_expr, aggr_info, group_id, max_group_cnt))) {
          LOG_WARN("collect_aggr_result failed", K(ret));
        }
      }
    }
    OX(LOG_DEBUG("finish collect", K(group_id), K(aggr_cell)));
  } // end for
  return ret;
}

int ObAggregateProcessor::eval_aggr_param_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
    ObAggrInfo &aggr_info = aggr_infos_.at(i);
    if (OB_FAIL(aggr_info.eval_param_batch(brs, eval_ctx_))) {
      LOG_WARN("fail to eval", K(ret));
    }
  }

  return ret;
}

int ObAggregateProcessor::prefetch_group_rows(GroupRow &group_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < group_rows.n_cells_; ++i) {
    ObAggrInfo &aggr_info = aggr_infos_.at(i);
    AggrCell &aggr_cell = group_rows.aggr_cells_[i];
    ObDatum &datum = aggr_cell.get_iter_result();
    if (!datum.is_null()) {
      __builtin_prefetch(datum.ptr_, 0/* read */, 2 /*high temp locality*/);
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::inner_process_batch(
  GroupRow &group_rows, T &selector, int64_t start_idx, int64_t end_idx)
{
  int ret = OB_SUCCESS;
  // process aggregate columns
  LOG_DEBUG("begin inner_process_batch batch size", K(group_rows));
  for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
    ObAggrInfo &aggr_info = aggr_infos_.at(i);
    AggrCell &aggr_cell = group_rows.aggr_cells_[i];
    if (!aggr_info.is_implicit_first_aggr() && !aggr_info.has_distinct_) {
      if (OB_FAIL(process_aggr_batch_result(
              &(aggr_info.param_exprs_), aggr_cell, aggr_info, selector))) {
        LOG_WARN("failed to calculate aggr cell", K(ret));
      }
    } else if (aggr_info.is_implicit_first_aggr()) {
      ObDatumVector results = aggr_info.expr_->locate_expr_datumvector(eval_ctx_);
      ObDatum *result = NULL;
      for (auto it = selector.begin();
           OB_SUCC(ret) && !aggr_cell.get_is_evaluated() && it < selector.end();
           selector.next(it)) {
        result = results.at(selector.get_batch_index(it));
        if (OB_FAIL(clone_aggr_cell(aggr_cell, *result))) {
          LOG_WARN("failed to clone non_aggr cell", K(ret));
        } else {
          aggr_cell.set_is_evaluated(true);
        }
      }
    } else if (aggr_info.has_distinct_) {
      ExtraResult *ad_result = static_cast<ExtraResult *>(aggr_cell.get_extra());
      //only last one add distinct
      if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("distinct set is NULL", K(ret));
      } else if (T_FUN_TOP_FRE_HIST == aggr_info.get_expr_type()) {
        // parse should report error instead of runtime
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("topk fre hist not support distinct", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "distinct on topk fre hist");
      } else if (OB_FAIL(selector.add_batch(
          &aggr_info.param_exprs_, ad_result->unique_sort_op_, nullptr, eval_ctx_))) {
        LOG_WARN("add row to distinct set failed", K(ret));
      } else {
        LOG_DEBUG("batch process disticnt", K(ret), K(ad_result->unique_sort_op_));
      }
    }
  } // end for
  return ret;
}

template <typename T>
int ObAggregateProcessor::inner_process_three_stage_batch(GroupRow &group_row, T &selector)
{
  int ret = OB_SUCCESS;
  // process aggregate columns
  LOG_DEBUG("begin inner_process_three_stage_batch batch size", K(group_row));
  int64_t aggr_code = distinct_aggr_count_;
  int64_t start_idx = 0;
  int64_t end_idx = group_row.n_cells_;
  // get aggr_code and then decide to go which path
  if (ObThreeStageAggrStage::THIRD_STAGE == aggr_stage_) {
  } else if (OB_INVALID_INDEX_INT64 == aggr_code_idx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: aggr_code_idx is invalid", K(ret));
  } else if (ObThreeStageAggrStage::SECOND_STAGE == aggr_stage_) {
    // only process one group and only process one aggregate function
    aggr_code = group_row.groupby_store_row_->cells()[aggr_code_idx_].get_int();
  }
  if (OB_FAIL(ret)) {
  } else if (aggr_code < distinct_aggr_count_) {
    if (ObThreeStageAggrStage::FIRST_STAGE == aggr_stage_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: first stage need to calc", K(ret));
    } else {
      // distinct aggregate function on first stage and second stage
      start_idx = 0 == aggr_code ? 0 : dist_aggr_group_idxes_->at(aggr_code - 1);
      end_idx = dist_aggr_group_idxes_->at(aggr_code);
    }
  } else if (aggr_code != distinct_aggr_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: aggr_code is invalid", K(ret),
      K(aggr_code), K(distinct_aggr_count_), K(aggr_stage_));
  } else {
    // calc the non-distinct aggregate function in first stage and second stage
    // or calc all aggregate function and implicit aggregate functions on third stage
    start_idx = ObThreeStageAggrStage::SECOND_STAGE != aggr_stage_ ? 0 : aggr_code;
    start_idx = 0 == start_idx ? 0 : dist_aggr_group_idxes_->at(start_idx - 1);
  }
  if (OB_SUCC(ret) && OB_FAIL(inner_process_batch(group_row, selector, start_idx, end_idx))) {
    LOG_WARN("failed to batch process", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::process_batch(
    const ObBatchRows *brs, GroupRow &group_rows, const uint16_t* selector_array, uint16_t count)
{
  int ret = OB_SUCCESS;
  if (ObThreeStageAggrStage::NONE_STAGE == aggr_stage_) {
    ObSelector selector(brs, selector_array, count);
    if (OB_FAIL(inner_process_batch(group_rows, selector, 0, group_rows.n_cells_))) {
      LOG_WARN("failed to inner process batch", K(ret));
    }
  } else {
    if (OB_UNLIKELY(ObThreeStageAggrStage::THIRD_STAGE == aggr_stage_)) {
      ObDatumVector aggr_code_datums = aggr_code_expr_->locate_expr_datumvector(eval_ctx_);
      int64_t i = 0;
      uint16_t tmp_selector_array;
      ObSelector selector(brs, &tmp_selector_array, 1);
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < count; ++row_idx) {
        int64_t i = selector_array[row_idx];
        (&tmp_selector_array)[0] = i;
        int64_t aggr_code = aggr_code_datums.at(i)->get_int();
        if (aggr_code < distinct_aggr_count_) {
          int64_t start_idx = 0 == aggr_code ? 0 : dist_aggr_group_idxes_->at(aggr_code - 1);
          int64_t end_idx = dist_aggr_group_idxes_->at(aggr_code);
          if (OB_FAIL(inner_process_batch(group_rows, selector, start_idx, end_idx))) {
            LOG_WARN("failed to inner process batch", K(ret));
          }
        } else {
          int64_t start_idx = dist_aggr_group_idxes_->at(distinct_aggr_count_ - 1);
          if (OB_FAIL(inner_process_batch(group_rows, selector, start_idx, group_rows.n_cells_))) {
            LOG_WARN("failed to inner process batch", K(ret));
          }
        }
      }
    } else {
      // only for first and second aggregate stage
      ObSelector selector(brs, selector_array, count);
      if (OB_FAIL(inner_process_three_stage_batch(group_rows, selector))) {
        LOG_WARN("failed to inner process batch", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::process_batch(
    GroupRow &group_rows, const ObBatchRows &brs, uint16_t begin, uint16_t end)
{
  int ret = OB_SUCCESS;
  if (ObThreeStageAggrStage::NONE_STAGE == aggr_stage_) {
    ObBatchRowsSlice brs_slice(&brs, begin, end);
    if (OB_FAIL(inner_process_batch(group_rows, brs_slice, 0, group_rows.n_cells_))) {
      LOG_WARN("failed to inner process batch", K(ret));
    }
  } else {
    if (ObThreeStageAggrStage::THIRD_STAGE == aggr_stage_) {
      ObDatumVector aggr_code_datums = aggr_code_expr_->locate_expr_datumvector(eval_ctx_);
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        if (brs.skip_->at(i)) {
          continue;
        }
        int64_t aggr_code = aggr_code_datums.at(i)->get_int();
        if (aggr_code < distinct_aggr_count_) {
          ObBatchRowsSlice brs_slice(&brs, i, i + 1);
          int64_t start_idx = 0 == aggr_code ? 0 : dist_aggr_group_idxes_->at(aggr_code - 1);
          int64_t end_idx = dist_aggr_group_idxes_->at(aggr_code);
          if (OB_FAIL(inner_process_batch(group_rows, brs_slice, start_idx, end_idx))) {
            LOG_WARN("failed to inner process batch", K(ret));
          }
        } else {
          ObBatchRowsSlice brs_slice(&brs, i, i + 1);
          int64_t start_idx = dist_aggr_group_idxes_->at(distinct_aggr_count_ - 1);
          if (OB_FAIL(inner_process_batch(group_rows, brs_slice, start_idx, group_rows.n_cells_))) {
            LOG_WARN("failed to inner process batch", K(ret));
          }
        }
      }
    } else {
      ObBatchRowsSlice brs_slice(&brs, begin, end);
      if (OB_FAIL(inner_process_three_stage_batch(group_rows, brs_slice))) {
        LOG_WARN("failed to inner process batch", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::advance_collect_result(int64_t group_id)
{
  int ret = OB_SUCCESS;
  int64_t aggr_cnt = aggr_infos_.count();
  GroupRow *group_row = NULL;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(1);
  for (int64_t aggr_idx = 0; OB_SUCC(ret) && aggr_idx < aggr_cnt; ++aggr_idx) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(aggr_idx);
    group_row = group_rows_.at(group_id);
    AggrCell &aggr_cell = group_row->aggr_cells_[aggr_idx];
    if (aggr_cell.get_need_advance_collect()) {
      if (aggr_info.has_distinct_) {
        if (OB_FAIL(process_distinct_batch(0, aggr_cell, aggr_info, eval_ctx_.max_batch_size_))) {
          LOG_WARN("aggregate distinct cell failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        guard.set_batch_idx(0);
        if (OB_FAIL(collect_aggr_result(aggr_cell, NULL, aggr_info))) {
          LOG_WARN("collect_aggr_result failed", K(ret), K(group_id), K(aggr_idx));
        } else if (OB_FAIL(aggr_cell.deep_copy_advance_collect_result(
            aggr_info.expr_->locate_expr_datum(eval_ctx_), aggr_alloc_))) {
          LOG_WARN("failed to deep copy datum", K(ret));
        } else {
          LOG_TRACE("finish collect", K(group_id), K(aggr_cell), K(aggr_cell.get_advance_collect_result()));
        }
      }
      aggr_cell.set_is_advance_evaluated();
      aggr_cell.reuse_extra();
    }
  } // end for
  return ret;
}

int ObAggregateProcessor::collect_result_batch(const ObIArray<ObExpr *> &group_exprs,
                                               const int64_t output_batch_size,
                                               ObBatchRows &output_brs,
                                               int64_t &cur_group_id)
{
  int ret = OB_SUCCESS;
  int64_t start_group_idx = cur_group_id;
  int64_t loop_cnt = min(output_batch_size,
                         group_rows_.count() - cur_group_id);
  int64_t aggr_cnt = aggr_infos_.count();
  int64_t group_col_cnt = group_exprs.count();
  GroupRow *group_row = NULL;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(loop_cnt);
  for (int64_t aggr_idx = 0; OB_SUCC(ret) && aggr_idx < aggr_cnt; ++aggr_idx) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(aggr_idx);
    for (int64_t loop_idx = 0; OB_SUCC(ret) && loop_idx < loop_cnt; loop_idx++) {
      int64_t group_cur_idx = start_group_idx + loop_idx;
      int64_t output_batch_idx = output_brs.size_ + loop_idx;
      group_row = group_rows_.at(group_cur_idx);
      AggrCell &aggr_cell = group_row->aggr_cells_[aggr_idx];
      guard.set_batch_idx(output_batch_idx);
      if (aggr_cell.get_need_advance_collect() && aggr_cell.get_is_advance_evaluated()) {
        aggr_info.expr_->locate_datum_for_write(eval_ctx_).set_datum(aggr_cell.get_advance_collect_result());
        LOG_TRACE("fill aggr result ", K(aggr_cell.get_advance_collect_result()),
          K(aggr_cell.get_is_evaluated()));
      } else {
        if (aggr_info.has_distinct_) {
          if (OB_FAIL(process_distinct_batch(0, aggr_cell, aggr_info, loop_cnt))) {
            LOG_WARN("aggregate distinct cell failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (aggr_info.is_implicit_first_aggr()) {
          // aggr_info.expr_ skip check null, check in cg
          // judge aggr_cell.get_is_evaluated() whether implicit expr is evaluated
          aggr_info.expr_->locate_expr_datumvector(eval_ctx_)
                        .at(output_batch_idx)
                        ->set_datum(aggr_cell.get_iter_result());
          LOG_DEBUG("first aggr result ", K(aggr_cell.get_iter_result()),
            K(aggr_cell.get_is_evaluated()));
        } else {
          if (OB_FAIL(collect_aggr_result(aggr_cell, NULL, aggr_info))) {
            LOG_WARN("collect_aggr_result failed", K(ret));
          }
          LOG_DEBUG("finish collect", K(cur_group_id), K(aggr_cell), K(group_cur_idx),
            K(output_batch_idx));
        }
      }
    } // end for
    aggr_info.expr_->get_eval_info(eval_ctx_).projected_ = true;
  }
  // clear operator evaluated flags to recalc expr after rollup set_null
  clear_op_evaluated_flag();
  // project group expr result
  for (int64_t loop_idx = 0; OB_SUCC(ret) && loop_idx < loop_cnt; loop_idx++) {
    guard.set_batch_idx(output_brs.size_ + loop_idx);
    group_row = group_rows_.at(start_group_idx + loop_idx);
    // need to skip const value
    if (OB_NOT_NULL(group_row->groupby_store_row_) &&
        OB_FAIL(group_row->groupby_store_row_->to_expr(group_exprs, eval_ctx_))) {
      LOG_WARN("failed to convert store row to expr", K(ret));
    } else if (ROLLUP_DISTRIBUTOR == rollup_status_) {
      if (OB_ISNULL(rollup_id_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: rollup_id_expr_ is null", K(ret));
      } else {
        ObDatum &datum = rollup_id_expr_->locate_datum_for_write(eval_ctx_);
        datum.set_int(*reinterpret_cast<int64_t *>(group_row->groupby_store_row_->get_extra_payload()));
        rollup_id_expr_->set_evaluated_projected(eval_ctx_);
        LOG_DEBUG("debug grouping_id expr", K(ret));
      }
    } else {
      LOG_DEBUG("debug group by exprs", K(ret), K(ROWEXPR2STR(eval_ctx_, group_exprs)),
        K(start_group_idx + loop_idx), K(output_brs.size_ + loop_idx));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < group_col_cnt; i++) {
    ObExpr *group_expr = group_exprs.at(i);
    group_expr->set_evaluated_projected(eval_ctx_);
  }
  cur_group_id += loop_cnt;
  output_brs.size_ += loop_cnt;

  LOG_DEBUG("debug group by exprs2", K(ret), K(ROWEXPR2STR(eval_ctx_, group_exprs)));
  return ret;
}

int ObAggregateProcessor::process_distinct_batch(
  const int64_t group_id,
  AggrCell &aggr_cell,
  const ObAggrInfo &aggr_info,
  const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  ExtraResult *extra_info = aggr_cell.get_extra();
  if (OB_ISNULL(extra_info) || OB_ISNULL(extra_info->unique_sort_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct set is NULL", K(ret));
  } else {
    // In non-rollup group_id must be 0
    if (group_id > 0) {
      // Group id greater than zero in sort based group by must be rollup,
      // distinct set is sorted and iterated in rollup_process(), rewind here.
      if (OB_FAIL(extra_info->unique_sort_op_->rewind())) {
        LOG_WARN("rewind iterator failed", K(ret));
      } else {
        LOG_DEBUG("debug process distinct batch", K(group_id),
          K(start_partial_rollup_idx_), K(end_partial_rollup_idx_));
      }
    } else {
      if (OB_FAIL(extra_info->unique_sort_op_->sort())) {
        LOG_WARN("sort failed", K(ret));
      } else {
        LOG_DEBUG("debug process distinct batch", K(group_id),
          K(start_partial_rollup_idx_), K(end_partial_rollup_idx_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(precompute_distinct_aggr_result(aggr_cell, aggr_info, max_cnt))) {
      LOG_WARN("aggregate distinct cell failed", K(ret));
    }
  }
  return ret;
}

int ObAggregateProcessor::precompute_distinct_aggr_result(
  AggrCell &aggr_cell,
  const ObAggrInfo &aggr_info,
  const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  ExtraResult *ad_result = static_cast<ExtraResult *>(aggr_cell.get_extra());
  if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct set is NULL", K(ret));
  } else {
    bool is_first = true;
    while (OB_SUCC(ret)) {
      const ObChunkDatumStore::StoredRow *stored_row = NULL;
      int64_t read_rows = 0;
      if (OB_FAIL(ad_result->unique_sort_op_->get_next_batch(
          aggr_info.param_exprs_, max_cnt, read_rows))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get row from distinct set failed", K(ret));
        }
        break;
      } else if (read_rows <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected status: the count of read rows is 0", K(ret));
      } else {
        LOG_DEBUG("get row from distinct set", K(read_rows), K(max_cnt));
        ObBatchRows brs;
        brs.skip_ = skip_;
        brs.size_ = read_rows;
        ObBatchRowsSlice selector(&brs, 0, read_rows);
        if (OB_FAIL(process_aggr_batch_result(
                &(aggr_info.param_exprs_), aggr_cell, aggr_info, selector))) {
          LOG_WARN("failed to calculate aggr cell", K(ret));
        }
      }
    }
    LOG_DEBUG("debug precompute distinct");
  }
  return ret;
}

int ObAggregateProcessor::collect_scalar_batch(const ObBatchRows &brs, const int64_t group_id,
    const ObExpr *diff_expr, const int64_t max_cnt)
{
  UNUSED(brs);
  int ret = OB_SUCCESS;
  GroupRow *group_row = NULL;
  if (OB_FAIL(group_rows_.at(group_id, group_row))) {
    LOG_WARN("failed to get group_row", K(group_id), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group_row->n_cells_; ++i) {
      const ObAggrInfo &aggr_info = aggr_infos_.at(i);
      AggrCell &aggr_cell = group_row->aggr_cells_[i];
      if (OB_ISNULL(aggr_info.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr node is null", K(aggr_info), K(ret));
      } else if (aggr_info.is_implicit_first_aggr()) {
        ObDatumVector results = aggr_info.expr_->locate_expr_datumvector(eval_ctx_);
        ObDatum &result = *(results.at(0));
        aggr_info.expr_->set_evaluated_projected(eval_ctx_);
        result.set_datum(aggr_cell.get_iter_result());
      } else {
        if (aggr_info.has_distinct_) {
          if (OB_FAIL(process_distinct_batch(group_id, aggr_cell, aggr_info, max_cnt))) {
            LOG_WARN("failed to process distinct batch", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(collect_aggr_result(aggr_cell, diff_expr, aggr_info))) {
            LOG_WARN("collect_aggr_result failed", K(ret));
          }
        }
      }
      OX(LOG_DEBUG("finish collect", K(group_id), K(aggr_cell)));
    } // end for
  }
  return ret;
}


int ObAggregateProcessor::collect_for_empty_set()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
    ObAggrInfo &aggr_info = aggr_infos_.at(i);
    if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(ret));
    } else if (aggr_info.is_implicit_first_aggr() && aggr_stage_ != ObThreeStageAggrStage::THIRD_STAGE) {
      set_expr_datum_null(aggr_info.expr_);
    } else {
      switch (aggr_info.get_expr_type()) {
        case T_FUN_COUNT:
        case T_FUN_COUNT_SUM:
        case T_FUN_APPROX_COUNT_DISTINCT:
        case T_FUN_KEEP_COUNT:
        case T_FUN_GROUP_PERCENT_RANK: {
          ObDatum &result = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
          aggr_info.expr_->set_evaluated_projected(eval_ctx_);
          if (lib::is_mysql_mode()) {
            result.set_int(0);
          } else {
            ObNumber result_num;
            result_num.set_zero();
            result.set_number(result_num);
          }
          break;
        }
        case T_FUN_GROUP_RANK:
        case T_FUN_GROUP_DENSE_RANK:
        case T_FUN_GROUP_CUME_DIST: {
          ObDatum &result = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
          aggr_info.expr_->set_evaluated_projected(eval_ctx_);
          ObNumber result_num;
          int64_t num = 1;
          if (OB_FAIL(result_num.from(num, aggr_alloc_))) {
            LOG_WARN("failed to create number", K(ret));
          } else {
            result.set_number(result_num);
          }
          break;
        }
        case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
        case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
          ret = llc_init_empty(*aggr_info.expr_, eval_ctx_);
          break;
        }
        case T_FUN_SYS_BIT_AND:
        case T_FUN_SYS_BIT_OR:
        case T_FUN_SYS_BIT_XOR: {
          ObDatum &result = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
          aggr_info.expr_->set_evaluated_projected(eval_ctx_);
          uint64_t init_val =
            aggr_info.get_expr_type() == T_FUN_SYS_BIT_AND ? UINT_MAX_VAL[ObUInt64Type] : 0;
          result.set_uint(init_val);
          break;
        }
        default: {
          set_expr_datum_null(aggr_info.expr_);
          //do nothing
          break;
        }
      }
    }
  }
  return ret;
}

void ObAggregateProcessor::set_expr_datum_null(ObExpr *expr)
{
  // not check whether expr is null, it needs to be checked before call this function.
  ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
  expr->set_evaluated_flag(eval_ctx_);
  datum.set_null();
  if (expr->is_batch_result()) {
    expr->get_eval_info(eval_ctx_).notnull_ = false;
  }
}

int ObAggregateProcessor::process_aggr_result_from_distinct(
  AggrCell &aggr_cell,
  const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  ExtraResult *ad_result = static_cast<ExtraResult *>(aggr_cell.get_extra());
  if (OB_ISNULL(ad_result) || OB_ISNULL(ad_result->unique_sort_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct set is NULL", K(ret));
  } else {
    bool is_first = true;
    while (OB_SUCC(ret)) {
      const ObChunkDatumStore::StoredRow *stored_row = NULL;
      if (OB_FAIL(ad_result->unique_sort_op_->get_next_stored_row(stored_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get row from distinct set failed", K(ret));
        }
        break;
      } else if (OB_ISNULL(stored_row) || OB_ISNULL(stored_row->cells())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stored_row is NULL", KP(stored_row), K(ret));
      } else {
        if (is_first) {
          is_first = false;
          if (OB_FAIL(prepare_aggr_result(*stored_row, NULL, aggr_cell,
                                          aggr_info))) {
            LOG_WARN("prepare_aggr_result failed", K(ret));
          }
        } else {
          if (OB_FAIL(process_aggr_result(*stored_row, NULL, aggr_cell,
                                          aggr_info))) {
            LOG_WARN("process_aggr_result failed", K(ret));
          }
        }
        OX(LOG_DEBUG("succ iter prepare/process aggr result", K(stored_row), K(aggr_cell)));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::init_group_rows(const int64_t col_count, bool is_empty/*false*/)
{
  int ret = OB_SUCCESS;
  if (is_empty) {
    if (OB_FAIL(group_rows_.reserve(col_count))) {
      LOG_WARN("failed to reserve", "cnt", col_count, K(ret));
    }
    for(int64_t i = group_rows_.count(); OB_SUCC(ret) && i < col_count; ++i) {
      if (OB_FAIL(group_rows_.push_back(nullptr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  } else if (col_count > 1) {
    if (OB_FAIL(group_rows_.reserve(col_count))) {
      LOG_WARN("failed to reserve", "cnt", col_count, K(ret));
    }
    for(int64_t i = group_rows_.count(); OB_SUCC(ret) && i < col_count; ++i) {
      if (OB_FAIL(init_one_group(i))) {
        LOG_WARN("failed to init one group", K(i), K(col_count), K(ret));
      }
    }
  } else {
    if (OB_FAIL(init_one_group())) {
      LOG_WARN("failed to init one group", K(col_count), K(ret));
    }
  }
  return ret;
}

int ObAggregateProcessor::generate_group_row(GroupRow *&new_group_row,
                                             const int64_t group_id)
{
  int ret = OB_SUCCESS;
  new_group_row = NULL;
  const int64_t alloc_size = GROUP_ROW_SIZE + GROUP_CELL_SIZE * aggr_infos_.count();
  if (0 == cur_batch_group_idx_ % BATCH_GROUP_SIZE) {
    if (OB_ISNULL(cur_batch_group_buf_ = (char *)aggr_alloc_.alloc(
                alloc_size * BATCH_GROUP_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc stored row failed", K(alloc_size), K(group_id), K(ret));
    } else {
      // The memset is not needed here because the object will be constructed by NEW.
      // But we memset first then NEW got a better performance because of better CPU cache locality.
      MEMSET(cur_batch_group_buf_, 0, alloc_size * BATCH_GROUP_SIZE);
    }
  }

  if (OB_SUCC(ret)) {
    new_group_row = new(cur_batch_group_buf_) GroupRow();
    AggrCell *aggr_cells = new (cur_batch_group_buf_ + GROUP_ROW_SIZE)AggrCell[aggr_infos_.count()];
    new_group_row->n_cells_ = aggr_infos_.count();
    new_group_row->aggr_cells_ = aggr_cells;

    cur_batch_group_buf_ += alloc_size;
    ++cur_batch_group_idx_;
    cur_batch_group_idx_ %= BATCH_GROUP_SIZE;
  }

  if (OB_SUCC(ret) && has_extra_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
      const ObAggrInfo &aggr_info = aggr_infos_.at(i);
      AggrCell &aggr_cell = new_group_row->aggr_cells_[i];
      switch (aggr_info.get_expr_type()) {
        case T_FUN_GROUP_CONCAT:
        case T_FUN_GROUP_RANK:
        case T_FUN_GROUP_DENSE_RANK:
        case T_FUN_GROUP_PERCENT_RANK:
        case T_FUN_GROUP_CUME_DIST:
        case T_FUN_MEDIAN:
        case T_FUN_GROUP_PERCENTILE_CONT:
        case T_FUN_GROUP_PERCENTILE_DISC:
        case T_FUN_KEEP_MAX:
        case T_FUN_KEEP_MIN:
        case T_FUN_KEEP_SUM:
        case T_FUN_KEEP_COUNT:
        case T_FUN_KEEP_WM_CONCAT:
        case T_FUN_WM_CONCAT:
        case T_FUN_PL_AGG_UDF:
        case T_FUN_JSON_ARRAYAGG:
        case T_FUN_ORA_JSON_ARRAYAGG:
        case T_FUN_JSON_OBJECTAGG:
        case T_FUN_ORA_JSON_OBJECTAGG:
        case T_FUN_ORA_XMLAGG:
        {
          void *tmp_buf = NULL;
          set_need_advance_collect();
          aggr_cell.set_need_advance_collect();
          if (OB_ISNULL(tmp_buf = aggr_alloc_.alloc(sizeof(GroupConcatExtraResult)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            GroupConcatExtraResult *result = new (tmp_buf) GroupConcatExtraResult(aggr_alloc_);
            aggr_cell.set_extra(result);
            const bool need_rewind = (in_window_func_ || group_id > 0);
            if (OB_FAIL(result->init(eval_ctx_.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                                     aggr_info,
                                     eval_ctx_,
                                     need_rewind, dir_id_,
                                     io_event_observer_))) {
              LOG_WARN("init GroupConcatExtraResult failed", K(ret));
            } else if (aggr_info.separator_expr_ != NULL) {
              ObDatum *separator_result = NULL;
              if (OB_UNLIKELY(!aggr_info.separator_expr_->obj_meta_.is_string_type())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expr node is null", K(ret), KPC(aggr_info.separator_expr_));
              } else if (OB_FAIL(aggr_info.separator_expr_->eval(eval_ctx_, separator_result))) {
                LOG_WARN("eval failed", K(ret));
              } else {
                int64_t pos = sizeof(ObDatum);
                int64_t len = pos + (separator_result->null_ ? 0 : separator_result->len_);
                char *buf = (char*)aggr_alloc_.alloc(len);
                if (OB_ISNULL(buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fall to alloc buff", K(len), K(ret));
                } else {
                  ObDatum **separator_datum = const_cast<ObDatum**>(&result->get_separator_datum());
                  *separator_datum = new (buf) ObDatum;
                  if (OB_FAIL((*separator_datum)->deep_copy(*separator_result, buf, len, pos))) {
                    LOG_WARN("failed to deep copy datum", K(ret), K(pos), K(len));
                  } else {
                    LOG_DEBUG("succ to calc separator", K(ret), KP(*separator_datum));
                  }
                }
              }
            }
          }
          break;
        }
        case T_FUN_HYBRID_HIST: {
          void *tmp_buf = NULL;
          if (OB_ISNULL(tmp_buf = aggr_alloc_.alloc(sizeof(HybridHistExtraResult)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", "size", sizeof(HybridHistExtraResult));
          } else {
            HybridHistExtraResult *result = new (tmp_buf) HybridHistExtraResult(aggr_alloc_);
            aggr_cell.set_extra(result);
            const bool need_rewind = (in_window_func_ || group_id > 0);
            if (OB_FAIL(result->init(eval_ctx_.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                                     aggr_info,
                                     eval_ctx_,
                                     need_rewind,
                                     io_event_observer_,
                                     profile_,
                                     op_monitor_info_))) {
              LOG_WARN("init hybrid hist extra result failed");
            }
          }
          break;
        }
        case T_FUN_TOP_FRE_HIST: {
          void *tmp_buf = NULL;
          set_need_advance_collect();
          aggr_cell.set_need_advance_collect();
          if (OB_ISNULL(tmp_buf = aggr_alloc_.alloc(sizeof(TopKFreHistExtraResult)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            TopKFreHistExtraResult *result = new (tmp_buf) TopKFreHistExtraResult(aggr_alloc_);
            aggr_cell.set_extra(result);
          }
          break;
        }

        case T_FUN_AGG_UDF: {
          CK(NULL != aggr_info.dll_udf_);
          DllUdfExtra *extra = NULL;
          if (OB_SUCC(ret)) {
            extra = OB_NEWx(DllUdfExtra, (&aggr_alloc_),
                    aggr_alloc_);
            if (NULL == extra) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            }
            aggr_cell.set_extra(extra);
          }
          if (OB_SUCC(ret)) {
            OZ(ObUdfUtil::init_udf_args(aggr_alloc_,
                                        aggr_info.dll_udf_->udf_attributes_,
                                        aggr_info.dll_udf_->udf_attributes_types_,
                                        extra->udf_ctx_.udf_args_));
            OZ(aggr_info.dll_udf_->udf_func_.process_init_func(extra->udf_ctx_));
            if (OB_SUCC(ret)) { // set func after udf ctx inited
              extra->udf_fun_ = &aggr_info.dll_udf_->udf_func_;
            }
            OZ(extra->udf_fun_->process_clear_func(extra->udf_ctx_));
          }
          break;
        }
        default:
          break;
      }

      if (OB_SUCC(ret) && aggr_info.has_distinct_) {
        set_need_advance_collect();
        aggr_cell.set_need_advance_collect();
        if (NULL == aggr_cell.get_extra()) {
          void *tmp_buf = NULL;
          if (OB_ISNULL(tmp_buf = aggr_alloc_.alloc(sizeof(ExtraResult)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            ExtraResult *result = new (tmp_buf) ExtraResult(aggr_alloc_);
            aggr_cell.set_extra(result);
          }
        }

        if (OB_SUCC(ret)) {
          // In window function, get result will be called more than once, rewind is needed.
          //
          // The distinct set will iterate twice with rollup, for rollup processing and aggregation,
          // rewind is needed for the second iteration.
          //
          // Rollup is supported and only supported in sort based group by with multi-groups,
          // only groups with group id greater than zero need to rewind.
          // The groupid of hash groupby also is greater then 0, then need rewind ???
          const bool need_rewind = (in_window_func_ || group_id > 0);
          if (OB_FAIL(aggr_cell.get_extra()->init_distinct_set(
              eval_ctx_.exec_ctx_.get_my_session()->get_effective_tenant_id(),
              aggr_info,
              eval_ctx_,
              need_rewind,
              io_event_observer_))) {
            LOG_WARN("init_distinct_set failed", K(ret));
          }
        }
      }
    }//end of for
  }
  return ret;
}

int ObAggregateProcessor::init_one_group(const int64_t group_id,
                                         bool fill_pos /* false */)
{
  int ret = OB_SUCCESS;
  GroupRow *group_row = nullptr;
  if (OB_FAIL(generate_group_row(group_row, group_id))) {
    LOG_WARN("failed to generate group row", K(ret));
  } else if (fill_pos) {
    if (group_rows_.count() <= group_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to fill group id", K(ret), K(group_id), K(group_rows_.count()));
    } else {
      group_rows_.at(group_id) = group_row;
    }
  } else if (OB_FAIL(group_rows_.push_back(group_row))) {
    LOG_WARN("push_back failed", K(group_id), K(group_row), K(ret));
  } else {
    LOG_DEBUG("succ init group_row", K(group_id), KPC(group_row), K(ret));
  }
  return ret;
}

int ObAggregateProcessor::rollup_process(
  const int64_t group_id,
  const int64_t rollup_group_id,
  const int64_t max_group_cnt,
  const ObExpr *diff_expr /*= NULL*/)
{
  int ret = OB_SUCCESS;
  GroupRow *group_row = NULL;
  GroupRow *rollup_row = NULL;
  if (OB_FAIL(group_rows_.at(group_id, group_row))) {
    LOG_WARN("get group_row failed", K(group_id), K(ret));
  } else if (rollup_group_id < max_group_cnt) {
    // partial rollup id is less than the count of groupby_exprs
    // it should not calc partial rollup
    // on before logic it redundently calculate aggregate function for last rollup row
  } else if (OB_FAIL(group_rows_.at(rollup_group_id, rollup_row))) {
    LOG_WARN("get group_row failed", "group_id", group_id - 1, K(ret));
  } else if (OB_ISNULL(group_row) || OB_ISNULL(rollup_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row is null", KP(group_row), KP(rollup_row), K(ret));
  } else if (OB_FAIL(rollup_base_process(group_row, rollup_row, diff_expr, group_id))) {
    LOG_WARN("failed to rollup process", K(ret));
  }
  LOG_DEBUG("debug rollup process", K(group_id), K(rollup_group_id), K(max_group_cnt));
  return ret;
}

int ObAggregateProcessor::rollup_batch_process(
  const int64_t group_row_id,
  const int64_t rollup_group_row_id,
  int64_t diff_group_idx,
  const int64_t max_group_cnt/*=INT64_MIN*/)
{
  int ret = OB_SUCCESS;
  GroupRow *group_row = NULL;
  GroupRow *rollup_row = NULL;
  if (OB_FAIL(group_rows_.at(group_row_id, group_row))) {
    LOG_WARN("get group_row failed", K(group_row_id), K(ret));
  } else if (OB_FAIL(group_rows_.at(rollup_group_row_id, rollup_row))) {
    LOG_WARN("get group_row failed", "group_id", rollup_group_row_id, K(ret));
  } else if (OB_ISNULL(group_row) || OB_ISNULL(rollup_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row is null", KP(group_row), KP(rollup_row), K(ret));
  } else if (OB_FAIL(rollup_base_process(group_row, rollup_row, nullptr, diff_group_idx, max_group_cnt))) {
    LOG_WARN("failed to rollup process", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::rollup_base_process(
  GroupRow *group_row,
  GroupRow *rollup_row,
  const ObExpr *diff_expr /*= NULL*/,
  int64_t diff_group_idx,
  const int64_t max_group_cnt/*INT64_MIN*/)
{
  int ret = OB_SUCCESS;
  // copy aggregation column
  for (int64_t i = 0; OB_SUCC(ret) && i < group_row->n_cells_; ++i) {
    const ObAggrInfo &aggr_info = aggr_infos_.at(i);
    AggrCell &aggr_cell = group_row->aggr_cells_[i];
    AggrCell &rollup_cell = rollup_row->aggr_cells_[i];
    if (T_FUN_TOP_FRE_HIST == aggr_info.get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("topk fre hist not support in group by rollup", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "topk fre hist in group by rollup");
    } else if (aggr_info.is_implicit_first_aggr()) {
      if (OB_FAIL(clone_aggr_cell(rollup_cell, aggr_cell.get_iter_result()))) {
        LOG_WARN("failed to clone cell", K(ret));
      }
    } else {
      if (aggr_info.has_distinct_) {
        if (OB_FAIL(rollup_distinct(aggr_cell, rollup_cell))) {
          LOG_WARN("failed to rollup aggregation results", K(ret));
        }
      } else {
        if (OB_FAIL(rollup_aggregation(aggr_cell, rollup_cell, diff_expr, aggr_info, diff_group_idx, max_group_cnt))) {
          LOG_WARN("failed to rollup aggregation results", K(ret));
        }
      }
    }
    OX(LOG_DEBUG("finish rollup_process iter", K(i), K(aggr_cell), K(rollup_cell), K(ret)));
  }
  OX(LOG_DEBUG("finish rollup_process", K(diff_group_idx), KPC(group_row),
            KPC(rollup_row), KPC(diff_expr),K(ret)));
  return ret;
}

int ObAggregateProcessor::rollup_aggregation(AggrCell &aggr_cell, AggrCell &rollup_cell,
    const ObExpr *diff_expr, const ObAggrInfo &aggr_info, int64_t cur_rollup_group_idx,
    const int64_t max_group_cnt/*=INT64_MIN*/)
{
  int ret = OB_SUCCESS;
  ObDatum &src_result = aggr_cell.get_iter_result();
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  ObDatum &target_result = rollup_cell.get_iter_result();
  switch (aggr_fun) {
    case T_FUN_COUNT: {
      rollup_cell.add_row_count(aggr_cell.get_row_count());
      break;
    }
    case T_FUN_MAX: {
      ret = max_calc(rollup_cell, target_result,
                     src_result,
                     aggr_info.expr_->basic_funcs_->null_first_cmp_,
                     aggr_info.is_number());
      break;
    }
    case T_FUN_MIN: {
      ret = min_calc(rollup_cell, target_result,
                     src_result,
                     aggr_info.expr_->basic_funcs_->null_first_cmp_,
                     aggr_info.is_number());
      break;
    }
    case T_FUN_AVG:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected func avg", K(ret));
    case T_FUN_SUM:
    case T_FUN_COUNT_SUM: {
      ret = rollup_add_calc(aggr_cell, rollup_cell, aggr_info);
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT: {
      if (rollup_cell.get_iter_result().len_ <= 0) {
        ret = clone_aggr_cell(rollup_cell, aggr_cell.get_iter_result());
      } else {
        ret = llc_add(target_result, aggr_cell.get_iter_result());
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      if (rollup_cell.get_iter_result().len_ <= 0) {
        ret = clone_aggr_cell(rollup_cell, src_result);
      } else {
        ret = llc_add(target_result, src_result);
      }
      break;
    }
    case T_FUN_GROUPING: {
      if (OB_UNLIKELY(aggr_info.param_exprs_.count() != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("param_exprs_ count is not 1", K(aggr_info));
      } else {
        if (diff_expr != NULL && diff_expr == aggr_info.param_exprs_.at(0)) {
          aggr_cell.set_tiny_num_int(1);
          LOG_DEBUG("rollup grouping", K(aggr_info.rollup_idx_), K(aggr_cell.get_tiny_num_int()));
        }
        if (-1 == cur_rollup_group_idx) {
          rollup_cell.set_tiny_num_int(aggr_cell.get_tiny_num_int());
          LOG_DEBUG("rollup grouping", K(aggr_info.rollup_idx_), K(aggr_cell.get_tiny_num_int()));
        } else if (INT64_MAX == aggr_info.rollup_idx_) {
          rollup_cell.set_tiny_num_int(aggr_cell.get_tiny_num_int());
          LOG_DEBUG("rollup grouping", K(aggr_info.rollup_idx_), K(aggr_cell.get_tiny_num_int()));
        } else if (aggr_info.rollup_idx_ >= cur_rollup_group_idx) {
          rollup_cell.set_tiny_num_int(1);
          LOG_DEBUG("rollup grouping", K(aggr_info.rollup_idx_), K(cur_rollup_group_idx));
        } else {
          rollup_cell.set_tiny_num_int(aggr_cell.get_tiny_num_int());
        }
      }
      break;
    }
    case T_FUN_GROUPING_ID: {
      if (OB_UNLIKELY(aggr_info.grouping_idxs_.count() == 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("param_exprs_ count is not 1", K(aggr_info));
      } else if (-1 == cur_rollup_group_idx) {
        rollup_cell.set_tiny_num_uint(aggr_cell.get_tiny_num_uint());
      } else {
        uint64_t new_value = 0;
        /*in vector engine: previous value should be used, if not match, use the previous value.
        * like the last branch in case T_FUN_GROUPING:
        * "
        * else {
        *   rollup_cell.set_tiny_num_int(aggr_cell.get_tiny_num_int());
        * }
        * "
        * we set the grouping_id as new_value | old_value
        */
        uint64_t old_value = aggr_cell.get_tiny_num_uint();
        for (int64_t i = 0; i < aggr_info.grouping_idxs_.count(); i++) {
          new_value = new_value << 1;
          int64_t grouping_idx = aggr_info.grouping_idxs_.at(i);
          if (grouping_idx >= cur_rollup_group_idx) {
            new_value++;
          }
        } 
        if (diff_expr == NULL) {
          rollup_cell.set_tiny_num_uint(new_value | old_value);
        } else {
          aggr_cell.set_tiny_num_uint(new_value);
          rollup_cell.set_tiny_num_uint(new_value);
        }
      }
      break;
    }
    case T_FUN_GROUP_ID: {
      bool match = false;
      // in batch process, cur_rollup_group_idx == max_group_cnt means that the diff expr is not distinct.
      // e.g., group by c1, rollup(c1);
      // in normal process, max_group_cnt is INT64_MIN.
      if (max_group_cnt == INT64_MIN) {
        // from normal process;
        for (int64_t i = 0; !match && i < aggr_info.group_idxs_.count(); i++) {
          if (cur_rollup_group_idx == aggr_info.group_idxs_.at(i)) {
            match = true;
          }
        }
        if (match) {
          aggr_cell.set_tiny_num_uint(aggr_cell.get_tiny_num_uint() + 1);
          rollup_cell.set_tiny_num_uint(aggr_cell.get_tiny_num_uint());
        } else {
          aggr_cell.set_tiny_num_uint(0);
          rollup_cell.set_tiny_num_uint(0);
        }
        LOG_WARN("1", K(match));
      } else {
        // from batch rollup
        match = cur_rollup_group_idx == max_group_cnt;
        if (match) {
          rollup_cell.set_tiny_num_uint(aggr_cell.get_tiny_num_uint() + 1);
        } else {
          rollup_cell.set_tiny_num_uint(0);
        }
      }
      break;
    }
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_MEDIAN:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT:
    case T_FUN_PL_AGG_UDF:
    case T_FUN_JSON_ARRAYAGG: 
    case T_FUN_ORA_JSON_ARRAYAGG:
    case T_FUN_JSON_OBJECTAGG:
    case T_FUN_ORA_JSON_OBJECTAGG:
    case T_FUN_ORA_XMLAGG:
    {
      GroupConcatExtraResult *aggr_extra = NULL;
      GroupConcatExtraResult *rollup_extra = NULL;
      if (OB_ISNULL(aggr_extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))
          || OB_ISNULL(rollup_extra = static_cast<GroupConcatExtraResult *>(rollup_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is NULL", K(ret), K(aggr_cell), K(rollup_cell));
      } else if (OB_FAIL(aggr_extra->finish_add_row())) {
        LOG_WARN("finish add row failed", K(ret));
      } else {
        const ObChunkDatumStore::StoredRow *stored_row = NULL;
        if (aggr_fun == T_FUN_JSON_ARRAYAGG || aggr_fun == T_FUN_JSON_OBJECTAGG) {
          int64_t len = aggr_extra->get_bool_mark_size();
          if (OB_FAIL(rollup_extra->reserve_bool_mark_count(len))) {
            LOG_WARN("reserve_bool_mark_count failed", K(ret), K(len));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < len; i++) {
            bool is_bool = false;
            if (OB_FAIL(aggr_extra->get_bool_mark(i, is_bool))) {
              LOG_WARN("get_bool_mark failed", K(ret));
            } else if (OB_FAIL(rollup_extra->set_bool_mark(i, is_bool))) {
              LOG_WARN("set_bool_mark failed", K(ret));
            }
          }
        }
        while (OB_SUCC(ret) && OB_SUCC(aggr_extra->get_next_row(stored_row))) {
          if (OB_ISNULL(stored_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("stored_row is NULL", K(ret), K(aggr_cell), K(rollup_cell));
          } else if (OB_FAIL(rollup_extra->add_row(*stored_row))) {
            LOG_WARN("add row failed", K(ret));
          }
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
      }
      break;
    }
    case T_FUN_HYBRID_HIST: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rollup contain agg hybrid hist still not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "rollup contain hybrid hist");
      break;
    }
    case T_FUN_AGG_UDF: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("rollup contain agg udfs still not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "rollup contain agg udfs");
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun));
      break;
  }
  return ret;
}

int ObAggregateProcessor::rollup_distinct(AggrCell &aggr_cell, AggrCell &rollup_cell)
{
  int ret = OB_SUCCESS;
  ExtraResult *ad_result = static_cast<ExtraResult *>(aggr_cell.get_extra());
  ExtraResult *rollup_result = static_cast<ExtraResult *>(rollup_cell.get_extra());
  if (OB_ISNULL(ad_result)
      || OB_ISNULL(ad_result->unique_sort_op_)
      || OB_ISNULL(rollup_result)
      || OB_ISNULL(rollup_result->unique_sort_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct set is NULL", K(ret));
  } else if (OB_FAIL(ad_result->unique_sort_op_->sort())) {
    LOG_WARN("sort failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      const ObChunkDatumStore::StoredRow *stored_row = NULL;
      if (OB_FAIL(ad_result->unique_sort_op_->get_next_stored_row(stored_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get row from distinct set failed", K(ret));
        }
        break;
      } else if (OB_ISNULL(stored_row) || OB_ISNULL(stored_row->cells())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stored_row is NULL", KP(stored_row), K(ret));
      } else if (OB_FAIL(rollup_result->unique_sort_op_->add_stored_row(*stored_row))) {
        LOG_WARN("add_row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::prepare_aggr_result(const ObChunkDatumStore::StoredRow &stored_row,
    const ObIArray<ObExpr *> *param_exprs, AggrCell &aggr_cell, const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  const ObItemType aggr_fun = aggr_info.get_expr_type();
   if (NULL != param_exprs && param_exprs->empty()) {
    if (T_FUN_COUNT == aggr_fun) {
      aggr_cell.inc_row_count();
    } else if (T_FUN_GROUP_ID == aggr_fun) {
     aggr_cell.set_tiny_num_uint(0);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum is null", K(aggr_fun), K(ret));
    }
  } else {
    switch (aggr_fun) {
      case T_FUN_COUNT: {
        bool has_null = false;
        for (int64_t i = 0; !has_null && i < stored_row.cnt_; ++i) {
          has_null = stored_row.cells()[i].is_null();
        }
        if (!has_null) {
          aggr_cell.inc_row_count();
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          ret = clone_aggr_cell(aggr_cell,
                           stored_row.cells()[0],
                           aggr_info.is_number());
        }
        break;
      }
      case T_FUN_AVG: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected func avg", K(ret));
        break;
      }
      case T_FUN_COUNT_SUM:
      case T_FUN_SUM: {
        if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("curr_row_results count is not 1", K(stored_row));
        } else if (!stored_row.cells()[0].is_null()) {
          ret = prepare_add_calc(stored_row.cells()[0], aggr_cell, aggr_info);
        } else {
          removal_info_.null_cnt_++;
        }
        break;
      }
      case T_FUN_GROUPING: {
        aggr_cell.set_tiny_num_int(0);
        break;
      }
      case T_FUN_GROUPING_ID: {
        aggr_cell.set_tiny_num_uint(0);
        break;
      }
    case T_FUN_APPROX_COUNT_DISTINCT:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
      ObDatum &llc_bitmap = aggr_cell.get_iter_result();
      if (OB_FAIL(llc_init(aggr_cell))) {
        LOG_WARN("llc_init failed");
      } else {
        bool has_null_cell = false;
        uint64_t hash_value = 0;
        if (OB_FAIL(llc_calc_hash_value(stored_row,
                                        aggr_info.param_exprs_,
                                        has_null_cell,
                                        hash_value))) {
          LOG_WARN("fail to do hash", K(ret));
        } else if (has_null_cell) {
          /*do nothing*/
        } else {
          ret = llc_add_value(hash_value, llc_bitmap.get_string());
        }
      }
      break;
    }
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_MEDIAN:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT:
    case T_FUN_PL_AGG_UDF:
    case T_FUN_JSON_ARRAYAGG:
    case T_FUN_ORA_JSON_ARRAYAGG:
    case T_FUN_JSON_OBJECTAGG:
    case T_FUN_ORA_JSON_OBJECTAGG:
    case T_FUN_ORA_XMLAGG:
    {
      GroupConcatExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else {
        extra->reuse_self();
        if (param_exprs == NULL && OB_FAIL(extra->add_row(stored_row))) {
          LOG_WARN("fail to add row", K(ret));
        } else if (param_exprs != NULL && OB_FAIL(extra->add_row(*param_exprs, eval_ctx_))) {
          LOG_WARN("fail to add row", K(ret));
        } else {
          if (aggr_fun == T_FUN_JSON_ARRAYAGG || aggr_fun == T_FUN_JSON_OBJECTAGG) {
            if (param_exprs == NULL) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param sexprs not be null", K(ret));
            } else {
              int64_t len = param_exprs->count();
              if (OB_FAIL(extra->reserve_bool_mark_count(len))) {
                LOG_WARN("reserve_bool_mark_count failed", K(ret), K(len));
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < len; i++) {
                ObExpr *tmp = NULL;
                if (OB_FAIL(param_exprs->at(i, tmp))){
                  LOG_WARN("fail to get param_exprs[i]", K(ret));
                } else {
                  bool is_bool = (tmp->is_boolean_ == 1);
                  if (OB_FAIL(extra->set_bool_mark(i, is_bool))){
                    LOG_WARN("fail to set_bool_mark", K(ret));
                  }
                }
              }
            }
          }
          LOG_DEBUG("succ to add row", K(stored_row), KPC(extra));
        }
      }
      break;
    }
    case T_FUN_HYBRID_HIST: {
      HybridHistExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<HybridHistExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is null", K(aggr_cell));
      } else {
        extra->reuse_self();
        if (param_exprs == NULL && OB_FAIL(extra->add_sort_row(stored_row))) {
          LOG_WARN("fail to add row");
        } else if (param_exprs != NULL && OB_FAIL(extra->add_sort_row(*param_exprs, eval_ctx_))) {
          LOG_WARN("fail to add row");
        } else {
          LOG_DEBUG("succ to add row", K(stored_row), KPC(extra));
        }
      }
      break;
    }
    case T_FUN_TOP_FRE_HIST: {
      TopKFreHistExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<TopKFreHistExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(stored_row.cnt_ != 1 ||
                      aggr_info.param_exprs_.count() != 1 ||
                      OB_ISNULL(aggr_info.param_exprs_.at(0)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(stored_row.cnt_), K(aggr_info));
      } else if (OB_FAIL(init_topk_fre_histogram_item(aggr_info, &(extra->topk_fre_hist_)))) {
        LOG_WARN("failed to init topk fre histogram", K(ret));
      } else {
        ObObj obj;
        if (OB_FAIL(stored_row.cells()[0].to_obj(obj, aggr_info.param_exprs_.at(0)->obj_meta_))) {
          LOG_WARN("failed to obj", K(ret));
        } else if (OB_FAIL(extra->topk_fre_hist_.add_top_k_frequency_item(obj))) {
          LOG_WARN("failed to process row", K(ret));
        } else {/*do nothing*/}
      }
      break;
    }
    case T_FUN_AGG_UDF: {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
      DllUdfExtra *extra = static_cast<DllUdfExtra *>(aggr_cell.get_extra());
      CK(NULL != extra);
      // EvalCtx temp allocator is used for udf args deep coping.
      OZ(extra->udf_fun_->process_add_func(tmp_alloc_g.get_allocator(),
                                          stored_row.cells(),
                                          aggr_info.param_exprs_,
                                          extra->udf_ctx_));
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("curr_row_results count is not 1", K(stored_row));
      } else if (!stored_row.cells()[0].is_null()) {
        aggr_cell.set_tiny_num_uint(stored_row.cells()[0].get_uint());
      } else {
        // if first value is null, set agg result to default value
        uint64_t init_val = aggr_fun == T_FUN_SYS_BIT_AND ? UINT_MAX_VAL[ObUInt64Type] : 0;
        aggr_cell.set_tiny_num_uint(init_val);
      }
      aggr_cell.set_tiny_num_used();
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun), K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::process_aggr_batch_result(
    const ObIArray<ObExpr *> *param_exprs,
    AggrCell &aggr_cell,
    const ObAggrInfo &aggr_info,
    const T &selector)
{
  int ret = OB_SUCCESS;
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  if (NULL != param_exprs && param_exprs->empty()) {
    if (T_FUN_COUNT == aggr_fun) {
      //count asterisk(*) case: null is counted
      uint16_t row_count = 0;
      for (auto it = selector.begin(); it < selector.end(); selector.next(it)) {
        ++row_count;
      }
      aggr_cell.add_row_count(row_count);
    } else if (T_FUN_GROUP_ID == aggr_fun) {
      // do nothing
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum is null", K(aggr_fun), K(ret));
    }
  } else {
    switch (aggr_fun) {
    case T_FUN_COUNT: {
      uint16_t row_count = 0;
      for (auto it = selector.begin(); it < selector.end(); selector.next(it)) {
        bool has_null = false;
        for (int64_t nth_param = 0; !has_null && nth_param < param_exprs->count(); ++nth_param) {
          ObDatumVector aggr_input_datums = param_exprs->at(nth_param)->locate_expr_datumvector(eval_ctx_);
          has_null = aggr_input_datums.at(selector.get_batch_index(it))->is_null();
        }
        if (!has_null) {
          ++row_count;
        }
      }
      aggr_cell.add_row_count(row_count);
      break;
    }
    case T_FUN_MAX: {
      ObDatumVector aggr_input_datums = param_exprs->at(0)->locate_expr_datumvector(eval_ctx_);
      ret = max_calc_batch(aggr_cell, aggr_cell.get_iter_result(),
                      aggr_input_datums,
                      aggr_info.expr_->basic_funcs_->null_first_cmp_,
                      aggr_info.is_number(), selector);
      break;
    }
    case T_FUN_MIN: {
      ObDatumVector aggr_input_datums = param_exprs->at(0)->locate_expr_datumvector(eval_ctx_);
      ret = min_calc_batch(aggr_cell, aggr_cell.get_iter_result(),
                      aggr_input_datums,
                      aggr_info.expr_->basic_funcs_->null_first_cmp_,
                      aggr_info.is_number(), selector);
      break;
    }
    case T_FUN_AVG: {
      LOG_WARN("should not reach here, T_FUN_AVG should be transformed to T_FUN_SUM/T_FUN_COUNT",
                K(aggr_fun));
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    case T_FUN_SUM:
    case T_FUN_COUNT_SUM: {
      ObDatumVector aggr_input_datums = param_exprs->at(0)->locate_expr_datumvector(eval_ctx_);
      ret = add_calc_batch(aggr_cell.get_iter_result(), aggr_input_datums,
                            aggr_cell, aggr_info, selector);
      break;
    }
    case T_FUN_GROUPING: {
      //do nothing
      break;
    }
    case T_FUN_GROUPING_ID: {
      //do nothing
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
      ObDatum *llc_bitmap = &aggr_cell.get_iter_result();
      if (!aggr_cell.get_is_evaluated()) {
        // init value firstly
        if (OB_FAIL(llc_init(aggr_cell))) {
          LOG_WARN("failed to llc init", K(ret));
        } else {
          aggr_cell.set_is_evaluated(true);
        }
      }
      if (OB_SUCC(ret)) {
        ret = approx_count_calc_batch(*llc_bitmap, param_exprs, selector);
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      if (1 != param_exprs->count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("The count of APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE is not 1",
          K(param_exprs->count()));
      } else {
        ObDatumVector arg_datums = param_exprs->at(0)->locate_expr_datumvector(eval_ctx_);
        ret = approx_count_merge_calc_batch(aggr_cell.get_iter_result(), aggr_cell, arg_datums,
                                            aggr_info.is_number(), selector);
      }
      break;
    }
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_MEDIAN:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT:
    case T_FUN_PL_AGG_UDF:
    case T_FUN_JSON_ARRAYAGG:
    case T_FUN_ORA_JSON_ARRAYAGG:
    case T_FUN_JSON_OBJECTAGG:
    case T_FUN_ORA_JSON_OBJECTAGG:
    case T_FUN_ORA_XMLAGG:
    {
      GroupConcatExtraResult *extra_info = NULL;
      if (OB_ISNULL(extra_info = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else {
        if (OB_FAIL(group_extra_aggr_calc_batch(param_exprs, aggr_cell, aggr_info, extra_info, selector))) {
          LOG_WARN("group extra calc failed", K(ret), K(aggr_fun));
        } else if (aggr_fun == T_FUN_JSON_ARRAYAGG || aggr_fun == T_FUN_JSON_OBJECTAGG) {
          int64_t len = param_exprs->count();
          if (OB_FAIL(extra_info->reserve_bool_mark_count(len))) {
            LOG_WARN("reserve_bool_mark_count failed", K(ret), K(len));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < len; i++) {
            ObExpr *tmp = NULL;
            if (OB_FAIL(param_exprs->at(i, tmp))) {
              LOG_WARN("fail to get param_exprs[i]", K(ret));
            } else {
              bool is_bool = (tmp->is_boolean_ == 1);
              if (OB_FAIL(extra_info->set_bool_mark(i, is_bool))) {
                LOG_WARN("fail to set_bool_mark", K(ret));
              }
            }
          }
        }
      }
      break;
    }
    case T_FUN_HYBRID_HIST: {
      HybridHistExtraResult *extra_info = NULL;
      if (OB_ISNULL(extra_info = static_cast<HybridHistExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is null", K(aggr_cell));
      } else if (OB_FAIL(selector.add_batch(param_exprs, extra_info, eval_ctx_))) {
        LOG_WARN("add batch failed");
      }
      break;
    }
    case T_FUN_TOP_FRE_HIST: {
      TopKFreHistExtraResult *extra_info = NULL;
      if (OB_ISNULL(extra_info = static_cast<TopKFreHistExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else {
        ObDatumVector arg_datums = param_exprs->at(0)->locate_expr_datumvector(eval_ctx_);
        if (!aggr_cell.get_is_evaluated()) {
          if (OB_FAIL(init_topk_fre_histogram_item(aggr_info, &extra_info->topk_fre_hist_))) {
            LOG_WARN("failed to init topk fre histogram", K(ret));
          } else {
            aggr_cell.set_is_evaluated(true);
          }
        }
        ret = top_fre_hist_calc_batch(aggr_info, extra_info, arg_datums, selector);
      }
      break;
    }
    case T_FUN_AGG_UDF: {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
      DllUdfExtra *extra = static_cast<DllUdfExtra *>(aggr_cell.get_extra());
      CK(NULL != extra);
      auto expr_cnt = aggr_info.param_exprs_.count();
      ObDatum *datums = static_cast<ObDatum *>(
          tmp_alloc_g.get_allocator().alloc(expr_cnt * sizeof(ObDatum)));
      // Reuse ObAggUdfFunction::process_add_func, UDF does NOT care performance
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end();
           selector.next(it)) {
        auto batch_idx = selector.get_batch_index(it);
        for (auto col = 0; col < expr_cnt; col++) {
          datums[col] = aggr_info.param_exprs_.at(col)->locate_expr_datum(eval_ctx_, batch_idx);
        }
        // EvalCtx temp allocator is used for udf args deep coping.
        OZ(extra->udf_fun_->process_add_func(tmp_alloc_g.get_allocator(),
                                             datums, aggr_info.param_exprs_,
                                             extra->udf_ctx_));
      }
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      ObDatumVector aggr_input_datums = param_exprs->at(0)->locate_expr_datumvector(eval_ctx_);
      ret = bitwise_calc_batch(aggr_input_datums, aggr_cell, aggr_fun, selector);
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun), K(ret));
      break;
    }
  }
  return ret;
}

int ObAggregateProcessor::process_aggr_result(const ObChunkDatumStore::StoredRow &stored_row,
    const ObIArray<ObExpr *> *param_exprs, AggrCell &aggr_cell, const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  if (NULL != param_exprs && param_exprs->empty()) {
    if (T_FUN_COUNT == aggr_fun) {
      if (!removal_info_.is_inv_aggr_) {
        aggr_cell.inc_row_count();
      } else {
        aggr_cell.dec_row_count();
      }
    } else if (T_FUN_GROUP_ID == aggr_fun) {
      //do nothing
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum is null", K(aggr_fun), K(ret));
    }
  } else {
    switch (aggr_fun) {
    case T_FUN_COUNT: {
      bool has_null = false;
      for (int64_t i = 0; !has_null && i < stored_row.cnt_; ++i) {
        has_null = stored_row.cells()[i].is_null();
      }
      if (!has_null) {
        if (!removal_info_.is_inv_aggr_) {
          aggr_cell.inc_row_count();
        } else {
          aggr_cell.dec_row_count();
        }
      }
      break;
    }
    case T_FUN_MAX: {
      if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("curr_row_results count is not 1", K(stored_row));
      } else if (ob_is_user_defined_sql_type(aggr_info.expr_->datum_meta_.type_)) {
        // other udt types not supported, xmltype does not have order or map member function
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
      } else if (!stored_row.cells()[0].is_null()) {
        ret = max_calc(aggr_cell, aggr_cell.get_iter_result(),
                       stored_row.cells()[0],
                       aggr_info.expr_->basic_funcs_->null_first_cmp_,
                       aggr_info.is_number());
      }
      break;
    }
    case T_FUN_MIN: {
      if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("curr_row_results count is not 1", K(stored_row));
      } else if (ob_is_user_defined_sql_type(aggr_info.expr_->datum_meta_.type_)) {
        // other udt types not supported, xmltype does not have order or map member function
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
      } else if (!stored_row.cells()[0].is_null()) {
        ret = min_calc(aggr_cell, aggr_cell.get_iter_result(),
                       stored_row.cells()[0],
                       aggr_info.expr_->basic_funcs_->null_first_cmp_,
                       aggr_info.is_number());
      }
      break;
    }
    case T_FUN_AVG: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected func avg", K(ret));
      break;
    }
    case T_FUN_SUM:
    case T_FUN_COUNT_SUM: {
      if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("curr_row_results count is not 1", K(stored_row));
      } else if (!stored_row.cells()[0].is_null()) {
        if (!removal_info_.is_inv_aggr_) {
          ret = add_calc(stored_row.cells()[0], aggr_cell, aggr_info);
        } else {
          ret = sub_calc(stored_row.cells()[0], aggr_cell, aggr_info);
        }
      } else {
        if (!removal_info_.is_inv_aggr_) {
          removal_info_.null_cnt_++;
        } else {
          removal_info_.null_cnt_--;
        }
      }
      break;
    }
    case T_FUN_GROUPING: {
      //do nothing
      break;
    }
    case T_FUN_GROUPING_ID: {
      //do nothing
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS: {
      bool has_null_cell = false;
      ObDatum *llc_bitmap = &aggr_cell.get_iter_result();
      uint64_t hash_value = 0;
      if (OB_FAIL(llc_calc_hash_value(stored_row,
                                      aggr_info.param_exprs_,
                                      has_null_cell,
                                      hash_value))) {
        LOG_WARN("fail to do hash", K(ret));
      } else if (has_null_cell) {
       /*do nothing*/
      } else {
        ret = llc_add_value(hash_value, llc_bitmap->get_string());
      }
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("curr_row_results count is not 1", K(stored_row));
      } else {
        ret = llc_add(aggr_cell.get_iter_result(), stored_row.cells()[0]);
      }
      break;
    }
    case T_FUN_GROUP_CONCAT:
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_CUME_DIST:
    case T_FUN_MEDIAN:
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_COUNT:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_WM_CONCAT:
    case T_FUN_WM_CONCAT:
    case T_FUN_PL_AGG_UDF:
    case T_FUN_JSON_ARRAYAGG:
    case T_FUN_ORA_JSON_ARRAYAGG:
    case T_FUN_JSON_OBJECTAGG:
    case T_FUN_ORA_JSON_OBJECTAGG:
    case T_FUN_ORA_XMLAGG:
    {
      GroupConcatExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (param_exprs == NULL && OB_FAIL(extra->add_row(stored_row))) {
        LOG_WARN("fail to add row", K(ret));
      } else if (param_exprs != NULL && OB_FAIL(extra->add_row(*param_exprs, eval_ctx_))) {
        LOG_WARN("fail to add row", K(ret));
      } else {
        LOG_DEBUG("succ to add row", K(stored_row), KPC(extra));
      }
      break;
    }
    case T_FUN_HYBRID_HIST: {
      HybridHistExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<HybridHistExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is null", K(aggr_cell));
      } else if (param_exprs == NULL && OB_FAIL(extra->add_sort_row(stored_row))) {
        LOG_WARN("fail to add row");
      } else if (param_exprs != NULL && OB_FAIL(extra->add_sort_row(*param_exprs, eval_ctx_))) {
        LOG_WARN("fail to add row");
      } else {
        LOG_DEBUG("succ to add row", K(stored_row), KPC(extra));
      }
      break;
    }
    case T_FUN_TOP_FRE_HIST: {
      TopKFreHistExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<TopKFreHistExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(stored_row.cnt_ != 1 ||
                      aggr_info.param_exprs_.count() != 1 ||
                      OB_ISNULL(aggr_info.param_exprs_.at(0)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(stored_row.cnt_), K(aggr_info));
      } else {
        ObObj obj;
        if (OB_FAIL(stored_row.cells()[0].to_obj(obj, aggr_info.param_exprs_.at(0)->obj_meta_))) {
          LOG_WARN("failed to obj", K(ret));
        } else if (OB_FAIL(extra->topk_fre_hist_.add_top_k_frequency_item(obj))) {
          LOG_WARN("failed to process row", K(ret));
        } else {/*do nothing*/}
      }
      break;
    }
    case T_FUN_AGG_UDF: {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
      DllUdfExtra *extra = static_cast<DllUdfExtra *>(aggr_cell.get_extra());
      CK(NULL != extra);
      // EvalCtx temp allocator is used for udf args deep coping.
      OZ(extra->udf_fun_->process_add_func(tmp_alloc_g.get_allocator(),
                                          stored_row.cells(),
                                          aggr_info.param_exprs_,
                                          extra->udf_ctx_));
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      if (OB_UNLIKELY(stored_row.cnt_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("curr_row_results count is not 1", K(stored_row));
      } else if (!stored_row.cells()[0].is_null()) {
        uint64_t left_uint = aggr_cell.get_tiny_num_uint();
        uint64_t right_uint = stored_row.cells()[0].get_uint();
        uint64_t res_uint = 0;
        if (aggr_fun == T_FUN_SYS_BIT_AND) {
          res_uint = left_uint & right_uint;
        } else if (aggr_fun == T_FUN_SYS_BIT_OR) {
          res_uint = left_uint | right_uint;
        } else if (aggr_fun == T_FUN_SYS_BIT_XOR) {
          res_uint = left_uint ^ right_uint;
        }
        aggr_cell.set_tiny_num_uint(res_uint);
        aggr_cell.set_tiny_num_used();
      }
      break;
    }
    default:
      LOG_WARN("unknown aggr function type", K(aggr_fun), K(ret));
      break;
    }
  }
  return ret;
}

int ObAggregateProcessor::extend_concat_str_buf(
  const ObString &pad_str,
  const ObCollationType cs_type,
  const int64_t pos,
  const int64_t group_concat_cur_row_num,
  int64_t &append_len,
  bool &buf_is_full)
{
  int ret = OB_SUCCESS;
  int64_t tmp_max_len = pad_str.length() + pos;
  if (0 == cur_concat_buf_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current concat buf len is 0", K(ret));
  } else if (tmp_max_len > cur_concat_buf_len_) {
    if (cur_concat_buf_len_ < concat_str_max_len_) {
      // 从1024开始，然后每次double来扩,且不能小于当前pad后的总内存大小
      cur_concat_buf_len_ = cur_concat_buf_len_ * 2 < tmp_max_len ?
                            next_pow2(tmp_max_len) :
                            cur_concat_buf_len_ * 2;
      if (cur_concat_buf_len_ > concat_str_max_len_) {
        cur_concat_buf_len_ = concat_str_max_len_;
      }
      char *tmp_concat_str_buf = nullptr;
      if (OB_ISNULL(tmp_concat_str_buf = static_cast<char *>(aggr_alloc_.alloc(cur_concat_buf_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fall to alloc buff", K(ret));
      } else {
        if (0 < pos) {
          MEMCPY(tmp_concat_str_buf, concat_str_buf_, pos);
        }
        concat_str_buf_ = tmp_concat_str_buf;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (pad_str.length() + pos > concat_str_max_len_) {
      append_len = concat_str_max_len_ - pos;
      buf_is_full = true;
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
        LOG_WARN("result of string concatenation is too long", K(ret),
                K(pad_str.length()), K(pos), K(concat_str_max_len_));
      } else {
        int64_t well_formed_len = 0;
        int32_t well_formed_error = 0;
        if (OB_FAIL(ObCharset::well_formed_len(cs_type,
                                               pad_str.ptr(),
                                               append_len,
                                               well_formed_len,
                                               well_formed_error))) {
          LOG_WARN("invalid string for charset", K(ret), K(cs_type), K(pad_str));
        } else {
          append_len = well_formed_len;
          LOG_USER_WARN(OB_ERR_CUT_VALUE_GROUP_CONCAT, group_concat_cur_row_num + 1);
        }
      }
    }
  }
  return ret;
}
int ObAggregateProcessor::collect_aggr_result(
  AggrCell &aggr_cell, const ObExpr *diff_expr,
  const ObAggrInfo &aggr_info, const int64_t cur_group_id,
  const int64_t max_group_cnt/* =INT64_MIN*/)
{
  int ret = OB_SUCCESS;
  ObDatum &result = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
  bool has_lob_header = aggr_info.expr_->obj_meta_.has_lob_header();
  const ObItemType aggr_fun = aggr_info.get_expr_type();
  switch (aggr_fun) {
    case T_FUN_COUNT: {
      if (lib::is_mysql_mode()) {
        result.set_int(aggr_cell.get_row_count());
      } else {
        ObNumber result_num;
        char local_buff[ObNumber::MAX_BYTE_LEN];
        ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN );
        if (OB_FAIL(result_num.from(aggr_cell.get_row_count(), local_alloc))) {
          LOG_WARN("fail to call from", K(ret));
        } else {
          result.set_number(result_num);
        }
      }
      break;
    }
    case T_FUN_COUNT_SUM:
    case T_FUN_SUM: {
      const ObObjTypeClass tc = ob_obj_type_class(aggr_info.get_first_child_type());
      if (OB_FAIL(aggr_cell.collect_result(tc, eval_ctx_, aggr_info))) {
        LOG_WARN("fail to collect_result", K(ret));
      } else {
      }
      break;
    }
    case T_FUN_AVG: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected func avg", K(ret));
      break;
    }
    case T_FUN_GROUPING: {
      int64_t new_value = aggr_cell.get_tiny_num_int();
      LOG_DEBUG("debug grouping", K(new_value), KP(diff_expr));
      if (diff_expr != NULL && diff_expr == aggr_info.param_exprs_.at(0)) {
        new_value = 1;
        aggr_cell.set_tiny_num_int(new_value);
        LOG_DEBUG("debug grouping", K(new_value), KP(diff_expr));
      }
      if (lib::is_mysql_mode()) {
        result.set_int(new_value);
      } else {
        ObNumber result_num;
        char local_buff[ObNumber::MAX_BYTE_LEN];
        ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN );
        if (OB_FAIL(result_num.from(new_value, local_alloc))) {
          LOG_WARN("fail to call from", K(ret));
        } else {
          result.set_number(result_num);
        }
      }
      break;
    }
    case T_FUN_GROUPING_ID: {
      uint64_t new_value = aggr_cell.get_tiny_num_uint();
      if (cur_group_id == max_group_cnt) {
        // last rollup, should calc it manually.
        // normal query only. Batch rollup shouldn't reach here
        new_value = 0;
        for (int64_t i = 0; i < aggr_info.grouping_idxs_.count(); i++) {
          new_value = new_value << 1;
          int64_t grouping_idx = aggr_info.grouping_idxs_.at(i);
          if (grouping_idx >= cur_group_id) {
            new_value++;
          }
        }
      }
      if (lib::is_mysql_mode()) {
        result.set_int(new_value);
      } else {
        ObNumber result_num;
        char local_buff[ObNumber::MAX_BYTE_LEN];
        ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
        if (OB_FAIL(result_num.from(new_value, local_alloc))) {
          LOG_WARN("fail to call from", K(ret));
        } else {
          result.set_number(result_num);
        }
      }
      break;
    }
    case T_FUN_GROUP_ID: {
      uint64_t new_value = aggr_cell.get_tiny_num_uint();
      if (cur_group_id == max_group_cnt) {
        //last rollup, should calc it manually
        bool match = false;
        for (int64_t i = 0; !match && i < aggr_info.group_idxs_.count(); i++) {
          if (cur_group_id == aggr_info.group_idxs_.at(i)) {
            new_value++;
            match = true;
          }
        }
        if (!match) {
          new_value = 0;
        }
      }
      if (lib::is_mysql_mode()) {
        result.set_int(new_value);
      } else {
        ObNumber result_num;
        char local_buff[ObNumber::MAX_BYTE_LEN];
        ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
        if (OB_FAIL(result_num.from(new_value, local_alloc))) {
          LOG_WARN("fail to call from", K(ret));
        } else {
          result.set_number(result_num);
        }
      }
      break;
    }
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: {
      ret = aggr_info.expr_->deep_copy_datum(eval_ctx_, aggr_cell.get_iter_result());
      break;
    }
    case T_FUN_APPROX_COUNT_DISTINCT: {
      int64_t tmp_result = OB_INVALID_COUNT;
      ObExprEstimateNdv::llc_estimate_ndv(tmp_result, aggr_cell.get_iter_result().get_string());
      if (tmp_result >= 0) {
        if (lib::is_mysql_mode()) {
          result.set_int(tmp_result);
        } else {
          char local_buff[ObNumber::MAX_BYTE_LEN];
          ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN);
          ObNumber result_num;
          if (OB_FAIL(result_num.from(tmp_result, local_alloc))) {
            LOG_WARN("failed to convert to number", K(ret));
          } else {
            result.set_number(result_num);
          }
        }
      }
      break;
    }

    case T_FUN_WM_CONCAT:
    case T_FUN_KEEP_WM_CONCAT: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_wm_concat_result(aggr_info, extra,
                                       T_FUN_KEEP_WM_CONCAT == aggr_fun, result))) {
        LOG_WARN("failed to get wm concat result", K(ret));
      } else {
      }
      break;
    }
    case T_FUN_ORA_XMLAGG: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_ora_xmlagg_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get xmlagg result", K(ret));
      } else {
      }
      break;
    }
    case T_FUN_JSON_ARRAYAGG: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_json_arrayagg_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get json_arrayagg result", K(ret));
      } else {
      }
      break;
    }

    case T_FUN_ORA_JSON_ARRAYAGG: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_ora_json_arrayagg_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get json_arrayagg result", K(ret));
      } else {
      }
      break;
    }

    case T_FUN_JSON_OBJECTAGG: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_json_objectagg_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get json_objectagg result", K(ret));
      } else {
      }
      break;
    }

    case T_FUN_ORA_JSON_OBJECTAGG: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_ora_json_objectagg_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get json_objectagg result", K(ret));
      } else {
      }
      break;
    }

    case T_FUN_GROUP_CONCAT: {
      GroupConcatExtraResult *extra = NULL;
      ObString sep_str;
      if (NULL == concat_str_buf_) {
        uint64_t concat_str_max_len = (lib::is_oracle_mode()
                                       ? OB_DEFAULT_GROUP_CONCAT_MAX_LEN_FOR_ORACLE
                                       : OB_DEFAULT_GROUP_CONCAT_MAX_LEN);
        if (!lib::is_oracle_mode()
            && OB_FAIL(eval_ctx_.exec_ctx_.get_my_session()->get_group_concat_max_len(concat_str_max_len))) {
          LOG_WARN("fail to get group concat max len", K(ret));
        } else if (0 != cur_concat_buf_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("first time concat buf len is not 0", K(cur_concat_buf_len_));
        } else {
          // 从1024开始迭代
          cur_concat_buf_len_ = concat_str_max_len > 1024 ? 1024 : concat_str_max_len;
          if (OB_ISNULL(concat_str_buf_ = static_cast<char *>(aggr_alloc_.alloc(cur_concat_buf_len_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fall to alloc buff", K(concat_str_max_len), K(ret));
          } else {
            concat_str_max_len_ = concat_str_max_len;
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(extra->empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is empty", K(ret), KPC(extra));
      } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
        // Group concat row may be iterated in rollup_process(), rewind here.
        LOG_WARN("rewind failed", KPC(extra), K(ret));
      } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
        LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
      } else {
        if (aggr_info.separator_expr_ != NULL) {
          if (OB_ISNULL(extra->get_separator_datum())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("seperator is nullptr", K(ret));
          } else {
            sep_str = extra->get_separator_datum()->get_string();
          }
        } else {
          if (lib::is_oracle_mode()) {
            sep_str = ObString::make_empty_string();
          } else {
            // 默认为逗号
            sep_str = ObCharsetUtils::get_const_str(aggr_info.expr_->datum_meta_.cs_type_, ',');
          }
        }
      }

      if (OB_SUCC(ret)) {
        int64_t pos = 0;
        bool buf_is_full = false;
        const ObChunkDatumStore::StoredRow *storted_row = NULL;
        int64_t group_concat_cur_row_num = 0;
        bool first_item_printed = false;
        const int64_t group_concat_param_count = aggr_info.group_concat_param_count_;
        const ObCollationType cs_type = aggr_info.expr_->datum_meta_.cs_type_;
        while(OB_SUCC(ret)
              && !buf_is_full
              && OB_SUCC(extra->get_next_row(storted_row))) {
          bool should_skip = false;
          for (int64_t i = 0; !should_skip && i < group_concat_param_count; ++i) {
            // 只要有一个cell为null，那么整个item都跳过
            should_skip = storted_row->cells()[i].is_null();
          }

          LOG_DEBUG("group concat iter", K(group_concat_cur_row_num), K(should_skip),
                    KPC(extra), K(group_concat_param_count), KPC(storted_row), K(sep_str));

          if (!should_skip) {
            if (group_concat_cur_row_num != 0) {
              int64_t append_len = sep_str.length();
              if (OB_FAIL(extend_concat_str_buf(
                  sep_str, cs_type, pos, group_concat_cur_row_num, append_len, buf_is_full))) {
                LOG_WARN("failed to extend concat str buf", K(ret));
              } else if (cur_concat_buf_len_ < append_len + pos) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("concat buf len is error", K(ret), K(cur_concat_buf_len_),
                  K(pos), K(append_len));
              } else if (0 < append_len) {
                MEMCPY(concat_str_buf_ + pos, sep_str.ptr(), append_len);
                pos += append_len;
                LOG_DEBUG("group concat iter append", K(sep_str), K(append_len), K(pos));
              }
            }
            for (int64_t i = 0; OB_SUCC(ret) && !buf_is_full && i < group_concat_param_count; ++i) {
              const ObString cell_string = storted_row->cells()[i].get_string();
              int64_t append_len = cell_string.length();
              if (OB_FAIL(extend_concat_str_buf(cell_string, cs_type, pos,
                  group_concat_cur_row_num, append_len, buf_is_full))) {
                LOG_WARN("failed to extend concat str buf", K(ret));
              } else if (cur_concat_buf_len_ < append_len + pos) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("concat buf len is error", K(ret), K(cur_concat_buf_len_),
                  K(pos), K(append_len), K(cell_string.length()));
              } else if (0 < append_len) {
                MEMCPY(concat_str_buf_ + pos, cell_string.ptr(), append_len);
                pos += append_len;
                LOG_DEBUG("group concat iter append", K(i), K(cell_string), K(append_len), K(pos));
              }
            }//end of for

            if (OB_SUCC(ret)) {
              ++group_concat_cur_row_num;
              first_item_printed |= (!first_item_printed);
            }
          }
        }//end of while

        if (ret != OB_ITER_END && ret != OB_SUCCESS) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          ret = OB_SUCCESS;
          ObDatum concat_result;
          if (first_item_printed) {
            concat_result.set_string(concat_str_buf_, pos);
          } else {
            concat_result.set_null(); //全部都跳过了，则设为null
          }
          OX(LOG_DEBUG("group concat finish ", K(group_concat_cur_row_num), K(pos),
              K(concat_result), KPC(extra)));
          ret = aggr_info.expr_->deep_copy_datum(eval_ctx_, concat_result);
        }
      }
      break;
    }
    case T_FUN_GROUP_RANK:
    case T_FUN_GROUP_DENSE_RANK:
    case T_FUN_GROUP_PERCENT_RANK:
    case T_FUN_GROUP_CUME_DIST: {//复用了GroupConcatCtx，因为仅仅只是最后的计算方法不一样
      GroupConcatExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(extra->empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is empty", K(ret), KPC(extra));
      } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
        // Group concat row may be iterated in rollup_process(), rewind here.
        LOG_WARN("rewind failed", KPC(extra), K(ret));
      } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
        LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
      } else {
        const ObChunkDatumStore::StoredRow *storted_row = NULL;
        int64_t rank_num = 0;
        bool need_check_order_equal = T_FUN_GROUP_DENSE_RANK == aggr_fun;
        ObChunkDatumStore::LastStoredRow prev_row(aggr_alloc_);
        int64_t total_sort_row_cnt = extra->get_row_count();
        bool is_first = true;
        while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
          if (NULL == storted_row) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else {
            ++ rank_num;
            bool find_target_rank = true;
            int comp_result = -2;
            bool is_all_equal = T_FUN_GROUP_CUME_DIST == aggr_fun;//use to cume dist(累积分布)
            bool need_continue = true;
            for (int64_t i = 0;
                 OB_SUCC(ret) && need_continue && i < aggr_info.sort_collations_.count();
                 ++i) {
              bool is_asc = false;
              uint32_t field_index = aggr_info.sort_collations_.at(i).field_idx_;
              if (OB_UNLIKELY(i >= storted_row->cnt_ || field_index >= storted_row->cnt_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get invalid argument", K(ret), K(i), K(storted_row->cnt_), K(field_index));
              } else if (OB_FAIL(compare_calc(storted_row->cells()[i],
                                              storted_row->cells()[field_index],
                                              aggr_info,
                                               i,
                                              comp_result,
                                              is_asc))) {
                LOG_WARN("failed to compare calc", K(ret));
              } else if (comp_result < -1 || comp_result > 1) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get invalid argument", K(ret), K(comp_result));
              } else if (0 == comp_result) {//equal(=)
                /*do nothing*/
              } else if (is_asc && 1 == comp_result) {//asc great(>)
                find_target_rank = false;
                is_all_equal = false;
                need_continue = false;
              } else if (is_asc && -1 == comp_result) {//asc less(<)
                find_target_rank = true;
                is_all_equal = false;
                need_continue = false;
              } else if (!is_asc && 1 == comp_result) {//desc great(>)
                find_target_rank = true;
                is_all_equal = false;
                need_continue = false;
              } else if (!is_asc && -1 == comp_result) {//desc less(<)
                find_target_rank = false;
                is_all_equal = false;
                need_continue = false;
              } else {/*do nothing*/}
            }
            if (OB_SUCC(ret)) {
              bool is_equal = false;
              if (need_check_order_equal) {
                if (is_first) {//第一次
                  if (OB_FAIL(prev_row.save_store_row(*storted_row))) {
                    LOG_WARN("failed to deep copy limit last rows", K(ret));
                  } else {
                    is_first = false;
                  }
                } else if (OB_FAIL(check_rows_equal(prev_row,
                                                    *storted_row,
                                                    aggr_info,
                                                    is_equal))) {
                  LOG_WARN("failed to is order by item equal with prev row", K(ret));
                } else if (is_equal) {
                  -- rank_num;
                } else {
                  if (OB_FAIL(prev_row.save_store_row(*storted_row))) {
                    LOG_WARN("failed to deep copy limit last rows", K(ret));
                  }
                }
                if (OB_SUCC(ret)) {
                  if (find_target_rank) {
                    break;
                  }
                }
              //cume dist要求是<=的总数，因此相等需要继续寻找
              } else if (find_target_rank && !is_all_equal) {
                break;
              }
            }
          }
        }
        if (ret != OB_ITER_END && ret != OB_SUCCESS) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          rank_num = ret == OB_ITER_END ? rank_num + 1 : rank_num;
          ret = OB_SUCCESS;
          ObNumber num_result;
          if (T_FUN_GROUP_RANK == aggr_fun || T_FUN_GROUP_DENSE_RANK == aggr_fun) {
            if (OB_FAIL(num_result.from(rank_num, aggr_alloc_))) {
              LOG_WARN("failed to create number", K(ret));
            }
          } else {
            ObNumber num;
            ObNumber num_total;
            rank_num = aggr_fun == T_FUN_GROUP_PERCENT_RANK ? rank_num - 1 : rank_num;
            total_sort_row_cnt = aggr_fun == T_FUN_GROUP_CUME_DIST ?
                                                    total_sort_row_cnt + 1 : total_sort_row_cnt;
            if (OB_FAIL(num.from(rank_num, aggr_alloc_))) {
              LOG_WARN("failed to create number", K(ret));
            } else if (OB_FAIL(num_total.from(total_sort_row_cnt, aggr_alloc_))) {
              LOG_WARN("failed to div number", K(ret));
            } else if (OB_FAIL(num.div(num_total, num_result, aggr_alloc_))) {
              LOG_WARN("failed to div number", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            result.set_number(num_result);
          }
        }
      }
      break;
    }
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_MEDIAN: {
      GroupConcatExtraResult *extra = NULL;
      if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
      } else if (OB_UNLIKELY(extra->empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra is empty", K(ret), KPC(extra));
      } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
        // Group concat row may be iterated in rollup_process(), rewind here.
        LOG_WARN("rewind failed", KPC(extra), K(ret));
      } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
        LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
      } else if (1 != aggr_info.sort_collations_.count() ||
                 aggr_info.sort_collations_.at(0).field_idx_ >=
                 aggr_info.param_exprs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sort_collations/param_exprs count", K(ret));
      } else {
        const int64_t param_idx = 0;
        const int64_t obj_idx = aggr_info.sort_collations_.at(0).field_idx_;
        const ObChunkDatumStore::StoredRow *storted_row = NULL;
        const int64_t total_row_count = extra->get_row_count();
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObChunkDatumStore::LastStoredRow prev_row(aggr_alloc_);
        ObNumber factor;
        bool need_linear_inter = false;
        int64_t not_null_start_loc = 0;
        int64_t dest_loc = 0;
        int64_t row_cnt = 0;
        while (OB_SUCC(ret) && 0 == not_null_start_loc
                            && OB_SUCC(extra->get_next_row(storted_row))) {
          if (OB_ISNULL(storted_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected group storted_row", K(ret), K(storted_row));
          } else {
            ++ row_cnt;
            if (!storted_row->cells()[obj_idx].is_null()) {
              not_null_start_loc = row_cnt;
            }
          }
        }
        if (ret != OB_ITER_END && ret != OB_SUCCESS) {
          LOG_WARN("fail to get next row", K(ret));
        } else if (ret == OB_ITER_END) {
          /* do nothing */
        } else if (T_FUN_MEDIAN == aggr_fun) {
            if (1 == (total_row_count - not_null_start_loc) % 2) {
              need_linear_inter = true;
              if (OB_FAIL(factor.from(ObNumber::get_positive_zero_dot_five(),
                                      allocator))) {
                LOG_WARN("failed to create number", K(ret));
              }
            }
            dest_loc = not_null_start_loc + (total_row_count - not_null_start_loc) / 2;
        } else if (OB_FAIL(get_percentile_param(aggr_info, storted_row->cells()[param_idx],
                                                not_null_start_loc, total_row_count,
                                                dest_loc, need_linear_inter,
                                                factor, allocator))) {
          LOG_WARN("get linear inter factor", K(factor));
        }
        while (OB_SUCC(ret) && row_cnt < dest_loc
                            && OB_SUCC(extra->get_next_row(storted_row))) {
          if (OB_ISNULL(storted_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else {
            ++ row_cnt;
          }
        }
        if (ret == OB_ITER_END && 0 == not_null_start_loc) {
          ret = OB_SUCCESS;
          result.set_null();
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to get dest loc row", K(ret), K(dest_loc), K(row_cnt));
        } else if (OB_FAIL(prev_row.save_store_row(*storted_row))) {
            LOG_WARN("fail to deep copy cur row", K(ret));
        } else if (need_linear_inter) {
          ObDatum tmp_datum;
          if (OB_FAIL(extra->get_next_row(storted_row))) {
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else if (OB_ISNULL(storted_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
          } else if (OB_FAIL(linear_inter_calc(aggr_info,
                                               prev_row.store_row_->cells()[obj_idx],
                                               storted_row->cells()[obj_idx],
                                               factor, tmp_datum))) {
            LOG_WARN("failed to calc linear inter", K(ret),
                                                    K(prev_row.store_row_->cells()[obj_idx]),
                                                    K(storted_row->cells()[obj_idx]),
                                                    K(factor));
          } else if (OB_FAIL(aggr_info.expr_->deep_copy_datum(eval_ctx_, tmp_datum))) {
            LOG_WARN("clone cell failed", K(ret), K(prev_row.store_row_->cells()[obj_idx]));
          } else {
            LOG_DEBUG("get median result", K(factor),
                                           K(prev_row.store_row_->cells()[obj_idx]),
                                           K(storted_row->cells()[obj_idx]),
                                           K(not_null_start_loc),
                                           K(dest_loc),
                                           K(need_linear_inter),
                                           K(result));
          }
        } else if (OB_FAIL(aggr_info.expr_->deep_copy_datum(eval_ctx_,
                                                    prev_row.store_row_->cells()[obj_idx]))) {
          LOG_WARN("clone cell failed", K(ret), K(prev_row.store_row_->cells()[obj_idx]));
        } else {
          LOG_DEBUG("get median result", K(prev_row.store_row_->cells()[obj_idx]),
                                         K(not_null_start_loc),
                                         K(dest_loc),
                                         K(need_linear_inter),
                                         K(result));
        }
      }
      break;
    }
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_SUM:
    case T_FUN_KEEP_COUNT: {
      if (OB_UNLIKELY(aggr_info.group_concat_param_count_ != 1 &&
                      aggr_info.group_concat_param_count_ != 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(aggr_info.group_concat_param_count_));
      } else {
        GroupConcatExtraResult *extra = NULL;
        if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
        } else if (OB_UNLIKELY(extra->empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extra is empty", K(ret), KPC(extra));
        //类似于group_concat的实现及计算,在rollup中我们可以rewind,这样更加高效
        } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
          LOG_WARN("rewind failed", KPC(extra), K(ret));
        } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
          LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
        } else {
          const ObChunkDatumStore::StoredRow *storted_row = NULL;
          ObChunkDatumStore::LastStoredRow first_row(aggr_alloc_);
          bool is_first = true;
          while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
            bool is_equal = false;
            if (NULL == storted_row) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("group sort row store is NULL", K(ret), K(storted_row));
            } else if (is_first && OB_FAIL(first_row.save_store_row(*storted_row))) {
              LOG_WARN("failed to deep copy limit last rows", K(ret));
            } else if (!is_first && OB_FAIL(check_rows_equal(first_row,
                                                             *storted_row,
                                                             aggr_info,
                                                             is_equal))) {
              LOG_WARN("failed to is order by item equal with prev row", K(ret));
            } else if (is_first || is_equal) {
              is_first = false;
              switch (aggr_fun) {
                case T_FUN_KEEP_MAX: {
                  if (!storted_row->cells()[0].is_null()) {
                    ret = max_calc(aggr_cell, aggr_cell.get_iter_result(),
                                   storted_row->cells()[0],
                                   aggr_info.expr_->basic_funcs_->null_first_cmp_,
                                   aggr_info.is_number());
                  }
                  break;
                }
                case T_FUN_KEEP_MIN: {
                  if (!storted_row->cells()[0].is_null()) {
                    ret = min_calc(aggr_cell, aggr_cell.get_iter_result(),
                                   storted_row->cells()[0],
                                   aggr_info.expr_->basic_funcs_->null_first_cmp_,
                                   aggr_info.is_number());
                  }
                  break;
                }
                case T_FUN_KEEP_SUM: {
                  if (!storted_row->cells()[0].is_null()) {
                    ret = add_calc(storted_row->cells()[0], aggr_cell, aggr_info);
                  }
                  break;
                }
                case T_FUN_KEEP_COUNT: {
                  if (aggr_info.group_concat_param_count_ == 0 ||
                      !storted_row->cells()[0].is_null()) {
                    aggr_cell.inc_row_count();
                  }
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("get unexpected aggr type", K(ret), K(aggr_fun));
                  break;
                }
              }
            } else {//不相等，结束
              break;
            }
          }
          if (ret != OB_ITER_END && ret != OB_SUCCESS) {
            LOG_WARN("fail to get next row", K(ret));
          } else {
            switch (aggr_fun) {
              case T_FUN_KEEP_MAX:
              case T_FUN_KEEP_MIN: {
                if (OB_FAIL(aggr_info.expr_->deep_copy_datum(eval_ctx_,
                                                                aggr_cell.get_iter_result()))) {
                  LOG_WARN("fail to deep copy datum", K(ret));
                } else {
                  ret = OB_SUCCESS;
                }
                break;
              }
              case T_FUN_KEEP_SUM: {
                const ObObjTypeClass tc = ob_obj_type_class(aggr_info.get_first_child_type());
                if (OB_FAIL(aggr_cell.collect_result(tc, eval_ctx_, aggr_info))) {
                  LOG_WARN("fail to collect_result", K(ret));
                } else {
                  ret = OB_SUCCESS;
                }
                break;
              }
              case T_FUN_KEEP_COUNT: {
                ObNumber result_num;
                char local_buff[ObNumber::MAX_BYTE_LEN];
                ObDataBuffer local_alloc(local_buff, ObNumber::MAX_BYTE_LEN );
                if (OB_FAIL(result_num.from(aggr_cell.get_row_count(), local_alloc))) {
                  LOG_WARN("fail to call from", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  result.set_number(result_num);
                }
                break;
              }
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected aggr type", K(ret), K(aggr_fun));
            }
          }
        }
      }
      break;
    }
    case T_FUN_TOP_FRE_HIST: {
      TopKFreHistExtraResult *extra = static_cast<TopKFreHistExtraResult *>(aggr_cell.get_extra());
      if (OB_ISNULL(extra)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra info is null", K(ret));
      } else if (OB_FAIL(get_top_k_fre_hist_result(extra->topk_fre_hist_, has_lob_header, result))) {
        LOG_WARN("failed to get topk fre hist result", K(ret));
      } else {
      }
      break;
    }
    case T_FUN_PL_AGG_UDF: {
      GroupConcatExtraResult *extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(get_pl_agg_udf_result(aggr_info, extra, result))) {
        LOG_WARN("failed to get_pl_agg_udf_result", K(ret));
      } else {
        LOG_TRACE("succeed to get pl agg udf result");
      }
      break;
    }
    case T_FUN_HYBRID_HIST: {
      HybridHistExtraResult *extra = static_cast<HybridHistExtraResult *>(aggr_cell.get_extra());
      if (OB_FAIL(compute_hybrid_hist_result(aggr_info, extra, result))) {
        LOG_WARN("failed to compute_hybrid_hist_result", K(ret));
      } else {
        LOG_TRACE("succeed to get pl agg udf result");
      }
      break;
    }
    case T_FUN_AGG_UDF: {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
      DllUdfExtra *extra = static_cast<DllUdfExtra *>(aggr_cell.get_extra());
      CK(NULL != extra);
      ObObj obj_res;
      // EvalCtx temp allocator is used for result obj
      OZ(extra->udf_fun_->process_origin_func(tmp_alloc_g.get_allocator(),
                                             obj_res,
                                             extra->udf_ctx_));
      if (OB_SUCC(ret)) {
        ObDatum &res = aggr_info.expr_->locate_datum_for_write(eval_ctx_);
        OZ(res.from_obj(obj_res));
        if (is_lob_storage(obj_res.get_type())) {
          OZ(ob_adjust_lob_datum(obj_res, aggr_info.expr_->obj_meta_,
                                eval_ctx_.exec_ctx_.get_allocator(), res));
        }
        OZ(aggr_info.expr_->deep_copy_datum(eval_ctx_, res));
      }
      OZ(extra->udf_fun_->process_clear_func(extra->udf_ctx_));
      // call udf deinit in ~DllUdfExtra()
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      result.set_uint(aggr_cell.get_tiny_num_uint());
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_fun));
      break;
  } // end switch

  aggr_info.expr_->set_evaluated_projected(eval_ctx_);
  return ret;
}

//
//查找dst_op, 过滤可能添加的sys cast,
//
int ObAggregateProcessor::search_op_expr(ObExpr *upper_expr,
                                         const ObItemType dst_op,
                                         ObExpr *&res_expr)
{
  int ret = OB_SUCCESS;
  ObExpr *tmp_expr = upper_expr;
  res_expr = NULL;
  while (OB_SUCC(ret) && OB_NOT_NULL(tmp_expr) && T_FUN_SYS_CAST == tmp_expr->type_) {
    if ( 2 != tmp_expr->arg_cnt_ ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sys cast arg cnt", K(ret), K(*tmp_expr));
    } else {
      LOG_DEBUG("search calc expr", K(dst_op), K(*tmp_expr));
      tmp_expr->get_eval_info(eval_ctx_).clear_evaluated_flag();
      tmp_expr = tmp_expr->args_[0];
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(tmp_expr) && dst_op == tmp_expr->type_) {
    tmp_expr->get_eval_info(eval_ctx_).clear_evaluated_flag();
    res_expr = tmp_expr;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail find dst_op", K(ret), K(dst_op));
  }
  return ret;
}

//
//填充linear_inter_expr输入参数并计算
//计算res = factor * (right - left) + left
//
int ObAggregateProcessor::linear_inter_calc(const ObAggrInfo &aggr_info,
                                            const ObDatum &prev_datum,
                                            const ObDatum &curr_datum,
                                            const ObNumber &factor,
                                            ObDatum &res)
{
  int ret = OB_SUCCESS;
  auto ctx = static_cast<LinearInterAggrFuncCtx *>(get_aggr_func_ctx(aggr_info));
  ObExpr *order_expr = aggr_info.param_exprs_.at(aggr_info.sort_collations_.at(0).field_idx_);
  res.set_null();
  if (OB_UNLIKELY(T_QUESTIONMARK == order_expr->type_)) {
    res.set_datum(curr_datum);
  } else if (NULL == ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate function ctx is NULL", K(ret), K(aggr_func_ctxs_.count()));
  } else {
    if (NULL == ctx->linear_inter_) {
      // setup runtime datum arithmetic
      ObRTDatumArith *arith = OB_NEWx(ObRTDatumArith, (&eval_ctx_.exec_ctx_.get_allocator()),
                                      eval_ctx_.exec_ctx_, *eval_ctx_.exec_ctx_.get_my_session());
      if (NULL == arith) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ObDatumMeta factor_meta;
        factor_meta.type_ = ObNumberType;
        factor_meta.cs_type_ = CS_TYPE_BINARY;
        factor_meta.scale_
            = ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale();
        factor_meta.precision_
            = ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision();

        if (OB_FAIL(arith->setup_datum_metas(factor_meta /* factor meta */,
                                             order_expr->datum_meta_ /* prev meta */,
                                             order_expr->datum_meta_ /* cur meta */))) {
          LOG_WARN("setup datum metas failed", K(ret));
        } else {
          auto factor_item = arith->ref(0);
          auto prev_item = arith->ref(1);
          auto cur_item = arith->ref(2);
          if (OB_FAIL(arith->generate(factor_item * (cur_item - prev_item) + prev_item))) {
            LOG_WARN("generate arithmetic expression failed", K(ret));
          } else if (NULL == arith->get_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no runtime expr generated", K(ret));
          } else if (order_expr->datum_meta_.type_ != arith->get_expr()->datum_meta_.type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result meta miss match", K(ret), K(*order_expr), K(*arith->get_expr()));
          }
        }

        if (OB_SUCC(ret)) {
          ctx->linear_inter_ = arith;
        } else {
          if (NULL != arith) {
            arith->~ObRTDatumArith();
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      char factor_nmb_buf[OBJ_DATUM_NUMBER_RES_SIZE];
      ObDatum factor_datum;
      factor_datum.ptr_ = factor_nmb_buf;
      factor_datum.set_number(factor);

      ObDatum *eval_res = NULL;
      if (OB_FAIL(ctx->linear_inter_->eval(eval_res, factor_datum, prev_datum, curr_datum))) {
        LOG_WARN("runtime datum arithmetic evaluate failed", K(ret));
      } else if (OB_ISNULL(eval_res)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eval result is NULL", K(ret));
      } else {
        res = *eval_res;
      }
    }
  }
  return ret;
}

//
//计算percentile_cont/percentile_disc输出行位置，若需要插值计算插值因数fator
//
int ObAggregateProcessor::get_percentile_param(const ObAggrInfo &aggr_info,
                                               const ObDatum &param,
                                               const int64_t not_null_start_loc,
                                               const int64_t total_row_count,
                                               int64_t &dest_loc,
                                               bool &need_linear_inter,
                                               ObNumber &factor,
                                               ObDataBuffer &allocator)
{
  int ret = OB_SUCCESS;
  need_linear_inter = false;
  int64_t scale = 0;
  int64_t int64_value = 0;
  char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN * 5];
  ObDataBuffer local_allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN * 5);
  const ObDatumMeta param_datum_meta = aggr_info.param_exprs_.at(0)->datum_meta_;
  ObNumber percentile;
  if (!ob_is_number_tc(param_datum_meta.type_)) {//deduce type时已经转换为number
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("percentile value invalid", K(ret), K(param_datum_meta), K(param));
  } else if (OB_FAIL(percentile.from(ObNumber(param.get_number()), local_allocator))) {
    LOG_WARN("failed to create number percentile", K(ret));
  } else if (percentile.is_negative()
             || 0 < percentile.compare(ObNumber::get_positive_one())) {//判断大于1或小于0
    ret = OB_ERR_PERCENTILE_VALUE_INVALID;
    LOG_WARN("invalid percentile value", K(ret), K(percentile));
  } else if (T_FUN_GROUP_PERCENTILE_DISC == aggr_info.get_expr_type()) {
    //
    //  PERCENTILE_DISC 计算过程
    //  rows 按照 null first 方式排序，计算时忽略 null
    //  percentile value (P) and the number of rows (N)
    //  dest_loc = ceil(N * P) + not_null_start_loc - 1
    //  特例：当 P = 0 时, 直接从 not_null_start_loc 开始读取
    //
    ObNumber row_count, rn;
    if (percentile.is_zero()) {
      dest_loc = not_null_start_loc;
    } else if (OB_FAIL(row_count.from(total_row_count - not_null_start_loc + 1,
                                      local_allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (OB_FAIL(percentile.mul(row_count, rn, local_allocator, false))) {
      LOG_WARN("failed to calc number mul", K(ret));
    } else if (OB_FAIL(rn.ceil(scale))) {
      LOG_WARN("failed to ceil number", K(ret));
    } else if (!rn.is_int64()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get int64 after ceil", K(ret));
    } else if (OB_FAIL(rn.cast_to_int64(int64_value))) {
      LOG_WARN("failed to cast to int64", K(ret), K(rn));
    } else {
      dest_loc = not_null_start_loc + int64_value - 1;
    }
  } else if (T_FUN_GROUP_PERCENTILE_CONT == aggr_info.get_expr_type()) {
    //
    //  PERCENTILE_CONT 计算过程
    //  rows 按照 null first 方式排序，计算时忽略 null
    //  percentile value (P) and the number of rows (N), row number RN = (1 + (P * (N - 1))
    //  CRN = CEILING(RN) and FRN = FLOOR(RN)
    //  If (CRN = FRN = RN) then the result is
    //    (value of expression from row at RN)
    //  Otherwise the result is
    //    (CRN - RN) * (value of expression for row at FRN) +
    //    (RN - FRN) * (value of expression for row at CRN)
    //    = (RN - FRN) * (row at CRN - row at FRN) + row at FRN
    //
    //  程序中 factor = (RN - FRN)
    //  FRN位置 dest_loc = not_null_start_loc + RN - 1
    //  需要插值时calc_linear_inter函数中计算 factor * (obj2 - obj1) + obj1
    //
    ObNumber row_count, rn, frn, res;
    if (OB_FAIL(row_count.from(total_row_count - not_null_start_loc, local_allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (OB_FAIL(percentile.mul(row_count, rn, local_allocator, false))) {
      LOG_WARN("failed to calc number mul", K(ret));
    } else if (OB_FAIL(frn.from(rn, local_allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (OB_FAIL(frn.floor(scale))) {
      LOG_WARN("failed to floor number", K(ret));
    } else if (OB_FAIL(rn.sub(frn, res, local_allocator))) {
      LOG_WARN("failed to calc number sub", K(ret));
    } else if (!frn.is_int64()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get int64 after floor", K(ret));
    } else if (OB_FAIL(frn.cast_to_int64(int64_value))) {
      LOG_WARN("failed to cast to int64", K(ret), K(rn));
    } else if (OB_FAIL(factor.from(res, allocator))) {
      LOG_WARN("failed to create number", K(ret));
    } else if (rn.is_integer()) {
      dest_loc = not_null_start_loc + int64_value;
    } else {
      need_linear_inter = true;
      dest_loc = not_null_start_loc + int64_value;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected aggr function type", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::max_calc(AggrCell &aggr_cell, ObDatum &base, const ObDatum &other,
    ObDatumCmpFuncType cmp_func, const bool is_number)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cmp_func)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_func is NULL", K(ret));
  } else if (!base.is_null() && !other.is_null()) {
    int cmp_ret = 0;
    if (OB_FAIL(cmp_func(base, other, cmp_ret))) {
      LOG_WARN("failed to compare", K(ret));
    } else if (cmp_ret < 0) {
      ret = clone_aggr_cell(aggr_cell, other, is_number);
      removal_info_.is_index_change_ = true;
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_aggr_cell(aggr_cell, other, is_number);
    removal_info_.is_index_change_ = true;
  } else {
    // nothing.
  }
  return ret;
}

int ObAggregateProcessor::min_calc(AggrCell &aggr_cell, ObDatum &base, const ObDatum &other,
    ObDatumCmpFuncType cmp_func, const bool is_number)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cmp_func)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_func is NULL", K(ret));
  } else if (!base.is_null() && !other.is_null()) {
    int cmp_ret = 0;
    if (OB_FAIL(cmp_func(base, other, cmp_ret))) {
      LOG_WARN("failed to compare", K(ret));
    } else if (cmp_ret > 0) {
      ret = clone_aggr_cell(aggr_cell, other, is_number);
      removal_info_.is_index_change_ = true;
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_aggr_cell(aggr_cell, other, is_number);
    removal_info_.is_index_change_ = true;
  } else {
    // nothing.
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::max_calc_batch(
  AggrCell &aggr_cell,
  ObDatum &dst, const ObDatumVector &src, ObDatumCmpFuncType cmp_func, const bool is_number,
  const T &selector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(cmp_func))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_func is NULL", K(ret));
  } else if (OB_UNLIKELY(!selector.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("selector is invalid", K(ret), K(selector.is_valid()));
  } else {
    ObDatum *max = nullptr;
    uint16_t i = 0; // row num in a batch
    int cmp_ret = 0;
    for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
      i = selector.get_batch_index(it);
      if (max && !src.at(i)->is_null()) {
        if (OB_FAIL(cmp_func(*max, *src.at(i), cmp_ret))) {
          LOG_WARN("failed to compare", K(ret));
        } else if (cmp_ret < 0) {
          max = src.at(i);
        }
      } else if (!src.at(i)->is_null()) {
        if (dst.is_null()) {
          max = src.at(i);
        } else if (OB_FAIL(cmp_func(dst, *src.at(i), cmp_ret))) {
          LOG_WARN("failed to compare", K(ret));
        } else if (cmp_ret < 0) {
          max = src.at(i);
        }
      }
    }
    if (OB_SUCC(ret) && max) {
      ret = clone_aggr_cell(aggr_cell, *(max), is_number);
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::min_calc_batch(
    AggrCell &aggr_cell,
    ObDatum &dst, const ObDatumVector &src, ObDatumCmpFuncType cmp_func, const bool is_number,
    const T &selector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(cmp_func))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cmp_func is NULL", K(ret));
  } else if (OB_UNLIKELY(!selector.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("selector is invalid", K(ret), K(selector.is_valid()));
  } else {
    ObDatum *min = nullptr;
    uint16_t i = 0; // row num in a batch
    int cmp_ret = 0;
    for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
      i = selector.get_batch_index(it);
      if (min && !src.at(i)->is_null()) {
        if (OB_FAIL(cmp_func(*min, *src.at(i), cmp_ret))) {
          LOG_WARN("failed to compare", K(ret));
        } else if (cmp_ret > 0) {
          min = src.at(i);
        }
      } else if (!src.at(i)->is_null()) {
        if (dst.is_null()) {
          min = src.at(i);
        } else if (OB_FAIL(cmp_func(dst, *src.at(i), cmp_ret))) {
          LOG_WARN("failed to compare", K(ret));
        } else if (cmp_ret > 0) {
          min = src.at(i);
        }
      }
    }
    if (OB_SUCC(ret) && min) {
      ret = clone_aggr_cell(aggr_cell, *(min), is_number);
    }
  }
  return ret;
}

int ObAggregateProcessor::prepare_add_calc(
  const ObDatum &first_value, AggrCell &aggr_cell, const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum &result_datum = aggr_cell.get_iter_result();
  switch (column_tc) {
    case ObIntTC: {
      aggr_cell.set_tiny_num_int(first_value.get_int());
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObUIntTC: {
      aggr_cell.set_tiny_num_uint(first_value.get_uint());
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObFloatTC:
    case ObDoubleTC: {
      ret = clone_aggr_cell(aggr_cell, first_value, false);
      break;
    }
    case ObNumberTC: {
      ret = clone_aggr_cell(aggr_cell, first_value, true);
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("bot support now", K(column_tc));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "rollup contain agg udfs");
    }
  }
  return ret;
}

// if use tiny_num, final result is tiny_num add iter_result.
// left from tiny_num, right from iter_value, result_datum from iter_result.
int ObAggregateProcessor::add_calc(
  const ObDatum &iter_value, AggrCell &aggr_cell, const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum &result_datum = aggr_cell.get_iter_result();
  switch (column_tc) {
    case ObIntTC: {
      int64_t left_int = aggr_cell.get_tiny_num_int();
      int64_t right_int = iter_value.get_int();
      int64_t sum_int = left_int + right_int;
      if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
        LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObNumber result_nmb;
        if (!result_datum.is_null()) {
          ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                  result_datum.get_number());
          result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
        }
        if (OB_FAIL(result_nmb.add(left_int, right_int, result_nmb, allocator))) {
          LOG_WARN("number add failed", K(ret));
        } else if (OB_FAIL(clone_number_cell(result_nmb, aggr_cell))) {
          LOG_WARN("clone_number_cell failed", K(ret));
        } else {
          aggr_cell.set_tiny_num_int(0);
        }
      } else {
        LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
        aggr_cell.set_tiny_num_int(sum_int);
      }
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObUIntTC: {
      uint64_t left_uint = aggr_cell.get_tiny_num_uint();
      uint64_t right_uint = iter_value.get_uint();
      uint64_t sum_uint = left_uint + right_uint;
      if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
        LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObNumber result_nmb;
        if (!result_datum.is_null()) {
          ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                          result_datum.get_number());
          result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
        }
        if (OB_FAIL(result_nmb.add(left_uint, right_uint, result_nmb, allocator))) {
          LOG_WARN("number add failed", K(ret));
        } else if (OB_FAIL(clone_number_cell(result_nmb, aggr_cell))) {
          LOG_WARN("clone_number_cell failed", K(ret));
        } else {
          aggr_cell.set_tiny_num_uint(0);
        }
      } else {
        LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
        aggr_cell.set_tiny_num_uint(sum_uint);
      }
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObFloatTC: {
      if (result_datum.is_null()) {
        ret = clone_aggr_cell(aggr_cell, iter_value, false);
      } else {
        float left_f = result_datum.get_float();
        float right_f = iter_value.get_float();
        if (OB_UNLIKELY(ObArithExprOperator::is_float_out_of_range(left_f + right_f))
            && !lib::is_oracle_mode()) {
          ret = OB_OPERATE_OVERFLOW;
          char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
          int64_t pos = 0;
          databuff_printf(expr_str,
                          OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                          pos,
                          "'(%e + %e)'", left_f, right_f);
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
          LOG_WARN("float out of range", K(left_f), K(right_f));
        } else {
          result_datum.set_float(left_f + right_f);
        }
      }
      break;
    }
    case ObDoubleTC: {
      if (result_datum.is_null()) {
        ret = clone_aggr_cell(aggr_cell, iter_value, false);
      } else {
        double left_d = result_datum.get_double();
        double right_d = iter_value.get_double();
        result_datum.set_double(left_d + right_d);
      }
      break;
    }
    case ObNumberTC: {
      ObNumber right_nmb(iter_value.get_number());
      removal_info_.is_out_of_range_ = right_nmb.fast_sum_agg_may_overflow();
      if (result_datum.is_null()) {
        ret = clone_aggr_cell(aggr_cell, iter_value, true);
      } else {
        ObNumber left_nmb(result_datum.get_number());
        removal_info_.is_out_of_range_ = removal_info_.is_out_of_range_ || left_nmb.fast_sum_agg_may_overflow();
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
        ObNumber result_nmb;
        if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, allocator, strict_mode))) {
          LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
        } else {
          ret = clone_number_cell(result_nmb, aggr_cell);
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(column_tc), K(ret));
    }
  }
  return ret;
}

// if use tiny_num, final result is tiny_num add iter_result.
// left_value from tiny_num, right_value from iter_value, result_datum from iter_result.
int ObAggregateProcessor::sub_calc(
  const ObDatum &iter_value, AggrCell &aggr_cell, const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum &result_datum = aggr_cell.get_iter_result();
  switch (column_tc) {
    case ObIntTC: {
      // If out of range, use result_datum sub right_value, not change left_value.
      // If not out of range, use left_value sub right_value, not change result_datum.
      int64_t left_int = aggr_cell.get_tiny_num_int();
      int64_t right_int = iter_value.get_int();
      int64_t dif_int = left_int - right_int;
      if (ObExprMinus::is_int_int_out_of_range(left_int, right_int, dif_int)) {
        LOG_DEBUG("int64_t sub overflow, will use number", K(left_int), K(right_int));
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        char int_buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObDataBuffer int_allocator(int_buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObNumber result_nmb;
        ObNumber right_num;
        if (!result_datum.is_null()) {
          ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                  result_datum.get_number());
          result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
        }
        if (OB_FAIL(right_num.from(right_int, int_allocator))) {
          LOG_WARN("number convert failed", K(ret));
        } else if (OB_FAIL(result_nmb.sub_v3(right_num, result_nmb, allocator))) {
          LOG_WARN("number sub failed", K(ret));
        } else if (OB_FAIL(clone_number_cell(result_nmb, aggr_cell))) {
          LOG_WARN("clone_number_cell failed", K(ret));
        } else {
          // maintain tiny num int
        }
      } else {
        LOG_DEBUG("int64_t sub does not overflow", K(left_int), K(right_int), K(dif_int));
        aggr_cell.set_tiny_num_int(dif_int);
      }
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObUIntTC: {
      // If out of range, get ULONG_MAX from result_datum add to left_value.
      //    Then left_value will turn to left_value + ULONG_MAX - right.
      // If not out of range, use left_value sub right_value, not change result_datum.
      uint64_t left_uint = aggr_cell.get_tiny_num_uint();
      uint64_t right_uint = iter_value.get_uint();
      uint64_t dif_uint = left_uint - right_uint;
      if (ObExprMinus::is_uint_uint_out_of_range(left_uint, right_uint, dif_uint)) {
        LOG_DEBUG("uint64_t sub overflow, will use number", K(left_uint), K(right_uint));
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        char uint_buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        ObDataBuffer uint_allocator(uint_buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        const bool strict_mode = false;
        ObNumber result_nmb;
        ObNumber uint_max_nmb;
        if (OB_UNLIKELY(result_datum.is_null())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("uint out of range", K(ret));
        } else {
          ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                          result_datum.get_number());
          result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(uint_max_nmb.from((uint64_t)ULONG_MAX, uint_allocator))) {
          LOG_WARN("number convert failed", K(ret));
        } else if (OB_FAIL(result_nmb.sub_v3(uint_max_nmb, result_nmb, allocator, strict_mode))) {
          LOG_WARN("number sub failed", K(ret));
        } else if (OB_FAIL(clone_number_cell(result_nmb, aggr_cell))) {
          LOG_WARN("clone_number_cell failed", K(ret));
        } else {
          aggr_cell.set_tiny_num_uint((uint64_t)ULONG_MAX - right_uint + left_uint);
        }
      } else {
        LOG_DEBUG("uint64_t sub does not overflow", K(left_uint), K(right_uint), K(dif_uint));
        aggr_cell.set_tiny_num_uint(dif_uint);
      }
      aggr_cell.set_tiny_num_used();
      break;
    }
    case ObNumberTC: {
      ObNumber right_nmb(iter_value.get_number());
      removal_info_.is_out_of_range_ = right_nmb.fast_sum_agg_may_overflow();
      if (result_datum.is_null()) {
        ret = clone_aggr_cell(aggr_cell, iter_value, true);
      } else {
        ObNumber left_nmb(result_datum.get_number());
        removal_info_.is_out_of_range_ = removal_info_.is_out_of_range_ || left_nmb.fast_sum_agg_may_overflow();
        char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
        ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
        const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
        ObNumber result_nmb;
        if (OB_FAIL(left_nmb.sub_v3(right_nmb, result_nmb, allocator, strict_mode))) {
          LOG_WARN("number sub failed", K(ret), K(left_nmb), K(right_nmb));
        } else {
          ret = clone_number_cell(result_nmb, aggr_cell);
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(column_tc), K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::number_accumulator(
    const ObDatumVector &src,
    ObDataBuffer &allocator1,
    ObDataBuffer &allocator2,
    ObNumber &result,
    uint32_t *sum_digits,
    bool &all_skip,
    const T &selector)
{
  int ret = OB_SUCCESS;
  ObNumber res;
  ObNumber sum;
  uint32_t normal_sum_path_counter = 0;
  uint32_t fast_sum_path_counter = 0;
  int64_t sum_frag_val = 0;
  int64_t sum_int_val = 0;
  // TODO zuojiao.hzj: add new number accumulator to avoid memory allocate
  char buf_ori_result[ObNumber::MAX_CALC_BYTE_LEN];
  ObDataBuffer allocator_ori_result(buf_ori_result, ObNumber::MAX_CALC_BYTE_LEN);
  ObNumber ori_result;
  bool may_overflow = false;
  bool ori_result_copied = false;
  uint16_t i = 0; // row num in a batch
  for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
    i = selector.get_batch_index(it);
    if (src.at(i)->is_null()) {
      continue;
    }
    all_skip = false;
    ObNumber src_num(src.at(i)->get_number());
    if (OB_UNLIKELY(src_num.is_zero())) {
      // do nothing
    } else if (src_num.d_.is_2d_positive_decimal()) {
      sum_frag_val += src_num.get_digits()[1];
      sum_int_val += src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_positive_fragment()) {
      sum_frag_val += src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_positive_integer()) {
      sum_int_val += src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_2d_negative_decimal()) {
      sum_frag_val -= src_num.get_digits()[1];
      sum_int_val -= src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_negative_fragment()) {
      sum_frag_val -= src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else if (src_num.d_.is_1d_negative_integer()) {
      sum_int_val -= src_num.get_digits()[0];
      ++fast_sum_path_counter;
    } else {
      if (OB_UNLIKELY(!ori_result_copied)) {
        // copy result to ori_result to fall back
        MEMSET(buf_ori_result, 0, sizeof(char) * ObNumber::MAX_CALC_BYTE_LEN);
        if (OB_FAIL(ori_result.deep_copy_v3(result, allocator_ori_result))) {
          LOG_WARN("deep copy number failed", K(ret));
        } else {
          ori_result_copied = true;
        }
      }
      if (OB_UNLIKELY(src_num.fast_sum_agg_may_overflow() && fast_sum_path_counter > 0)) {
        may_overflow = true;
        LOG_DEBUG("number accumulator may overflow, fall back to normal path",
                  K(src_num), K(sum_int_val), K(sum_frag_val));
        break;
      } else { // normal path
        ObDataBuffer &allocator = (normal_sum_path_counter % 2 == 0) ? allocator1 : allocator2;
        allocator.free();
        ret = result.add_v3(src_num, res, allocator, true, true);
        result = res;
        ++normal_sum_path_counter;
      }
    }
  }
  if (OB_UNLIKELY(may_overflow)) {
    fast_sum_path_counter = 0;
    normal_sum_path_counter = 0;
    result = ori_result;
    for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
      i = selector.get_batch_index(it);
      if (src.at(i)->is_null()) {
        continue;
      }
      ObNumber src_num(src.at(i)->get_number());
      if (OB_UNLIKELY(src_num.is_zero())) {
        // do nothing
      } else {
        ObDataBuffer &allocator = (normal_sum_path_counter % 2 == 0) ? allocator1 : allocator2;
        allocator.free();
        ret = result.add_v3(src_num, res, allocator, true, true);
        result = res;
        normal_sum_path_counter++;
      }
    }
    if (OB_SUCC(ret)) {
      result.assign(res.d_.desc_, res.get_digits());
    }
  } else if (OB_SUCC(ret) && !all_skip) {
    // construct sum result into number format
    const int64_t base = ObNumber::BASE;
    int64_t carry = 0;
    if (abs(sum_frag_val) >= base) {
      sum_int_val += sum_frag_val / base; // eg : (-21) / 10 = -2
      sum_frag_val = sum_frag_val % base; // eg : (-21) % 10 = -1
    }
    if (sum_int_val > 0 && sum_frag_val < 0) {
      sum_int_val -= 1;
      sum_frag_val += base;
    } else if (sum_int_val < 0 && sum_frag_val > 0) {
      sum_int_val += 1;
      sum_frag_val -= base;
    }
    if (abs(sum_int_val) >= base) {
      carry = sum_int_val / base; // eg : (-21) / 10 = -2
      sum_int_val = sum_int_val % base; // eg : (-21) % 10 = -1
    }
    if (0 == carry && 0 == sum_int_val && 0 == sum_frag_val) { // sum is zero
      sum.set_zero();
    } else if (carry >= 0 && sum_int_val >= 0 && sum_frag_val >= 0) { // sum is positive
      sum.d_.desc_ = NUM_DESC_2DIGITS_POSITIVE_DECIMAL;
      sum.d_.len_ -= (0 == sum_frag_val);
      if (carry > 0) {
        ++sum.d_.exp_;
        ++sum.d_.len_;
        sum.d_.len_ -= (sum_int_val == 0 && sum_frag_val == 0);
        // performance critical: set the tailing digits even they are 0, no overflow risk
        sum_digits[0] = static_cast<uint32_t>(carry);
        sum_digits[1] = static_cast<uint32_t>(sum_int_val);
        sum_digits[2] = static_cast<uint32_t>(sum_frag_val);
      } else { // 0 == carry
        if (0 == sum_int_val) {
          --sum.d_.exp_;
          --sum.d_.len_;
          sum_digits[0] = static_cast<uint32_t>(sum_frag_val);
        } else {
          sum_digits[0] = static_cast<uint32_t>(sum_int_val);
          sum_digits[1] = static_cast<uint32_t>(sum_frag_val);
        }
      }
    } else { // sum is negative
      sum.d_.desc_ = NUM_DESC_2DIGITS_NEGATIVE_DECIMAL;
      sum.d_.len_ -= (0 == sum_frag_val);
      // get abs of carry/sum_int_val/sum_frag_val
      carry = -carry;
      sum_int_val = -sum_int_val;
      sum_frag_val = -sum_frag_val;
      if (carry > 0) {
        --sum.d_.exp_; // notice here : different from postive
        ++sum.d_.len_;
        sum.d_.len_ -= (sum_int_val == 0 && sum_frag_val == 0);
        // performance critical: set the tailing digits even they are 0, no overflow risk
        sum_digits[0] = static_cast<uint32_t>(carry);
        sum_digits[1] = static_cast<uint32_t>(sum_int_val);
        sum_digits[2] = static_cast<uint32_t>(sum_frag_val);
      } else { // 0 == carry
        if (0 == sum_int_val) {
          ++sum.d_.exp_; // notice here : different from postive
          --sum.d_.len_;
          sum_digits[0] = static_cast<uint32_t>(sum_frag_val);
        } else {
          sum_digits[0] = static_cast<uint32_t>(sum_int_val);
          sum_digits[1] = static_cast<uint32_t>(sum_frag_val);
        }
      }
    }
    sum.assign(sum.d_.desc_, sum_digits);
    if (normal_sum_path_counter == 0 && result.is_zero()) {
      // all aggr result is filled in sum, just return sum
      if (sum.d_.len_ == 0) {
        result.set_zero();
      } else {
        result.assign(sum.d_.desc_, sum_digits);
      }
    } else if (sum.d_.len_ == 0) { // do nothing
    } else { // merge sum into aggr result
      ObDataBuffer &allocator = (normal_sum_path_counter % 2 == 0) ? allocator1 : allocator2;
      allocator.free();
      if (OB_FAIL(result.add_v3(sum, res, allocator, true, true))) {
        LOG_WARN("number_accumulator sum error", K(ret), K(sum), K(result));
      } else {
        result.assign(res.d_.desc_, res.get_digits());
      }
    }
  }

  LOG_DEBUG("number_accumulator done", K(ret), K(result),
            K(normal_sum_path_counter), K(fast_sum_path_counter));
  return ret;
}

int ObAggregateProcessor::init_group_extra_aggr_info(
  AggrCell &aggr_cell,
  const ObAggrInfo &aggr_info
)
{
  int ret = OB_SUCCESS;
  GroupConcatExtraResult *extra = NULL;
  if (OB_ISNULL(extra = static_cast<GroupConcatExtraResult *>(aggr_cell.get_extra()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_ is NULL", K(ret), K(aggr_cell));
  } else {
    extra->reuse_self();
    if (aggr_info.separator_expr_ != NULL) {
      ObDatum *separator_result = NULL;
      if (OB_UNLIKELY(!aggr_info.separator_expr_->obj_meta_.is_string_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr node is null", K(ret), KPC(aggr_info.separator_expr_));
      } else if (OB_FAIL(aggr_info.separator_expr_->eval(eval_ctx_, separator_result))) {
        LOG_WARN("eval failed", K(ret));
      } else {
        // prepare阶段解析分隔符，如果到collect阶段，则seperate_expr已经是下一组的值，导致结果错误
        int64_t pos = sizeof(ObDatum);
        int64_t len = pos + (separator_result->null_ ? 0 : separator_result->len_);
        char *buf = (char*)aggr_alloc_.alloc(len);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fall to alloc buff", K(len), K(ret));
        } else {
          ObDatum **separator_datum = const_cast<ObDatum**>(&extra->separator_datum_);
          *separator_datum = new (buf) ObDatum;
          if (OB_FAIL((*separator_datum)->deep_copy(*separator_result, buf, len, pos))) {
            LOG_WARN("failed to deep copy datum", K(ret), K(pos), K(len));
          } else {
            LOG_DEBUG("succ to calc separator", K(ret), KP(*separator_datum));
          }
        }
      }
    } else {
      /* nothing to do*/
    }
  }
  return ret;
}

int ObAggregateProcessor::ObBatchRowsSlice::add_batch(
  const ObIArray<ObExpr *> *param_exprs,
  ObSortOpImpl *unique_sort_op,
  GroupConcatExtraResult *extra_info,
  ObEvalCtx &eval_ctx
) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(unique_sort_op)) {
    int64_t stored_rows_count = 0;
    if (OB_FAIL(unique_sort_op->add_batch(
        *param_exprs, *brs_->skip_, end_pos_, begin_pos_, &stored_rows_count))) {
      LOG_WARN("failed to add batch", K(ret));
    }
  } else if (OB_NOT_NULL(extra_info->sort_op_)) {
    int64_t stored_rows_count = 0;
    if (OB_FAIL(extra_info->sort_op_->add_batch(
        *param_exprs, *brs_->skip_, end_pos_, begin_pos_, &stored_rows_count))) {
      LOG_WARN("failed to add batch", K(ret));
    } else {
      extra_info->row_count_ += stored_rows_count;
    }
  } else {
    int64_t stored_rows_count = 0;
    if (OB_FAIL(extra_info->row_store_.add_batch(
        *param_exprs, eval_ctx, *brs_->skip_, end_pos_, stored_rows_count, nullptr, begin_pos_))) {
      LOG_WARN("failed to add batch", K(ret));
    } else {
      extra_info->row_count_ += stored_rows_count;
    }
  }
  return ret;
}

int ObAggregateProcessor::ObBatchRowsSlice::add_batch(
  const ObIArray<ObExpr *> *param_exprs,
  HybridHistExtraResult *extra_info,
  ObEvalCtx &eval_ctx
) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(extra_info->sort_op_)) {
    int64_t stored_rows_count = 0;
    if (OB_FAIL(extra_info->sort_op_->add_batch(
        *param_exprs, *brs_->skip_, end_pos_, begin_pos_, &stored_rows_count))) {
      LOG_WARN("failed to add batch");
    } else {
      extra_info->sort_row_count_ += stored_rows_count;
    }
  }
  return ret;
}

int ObAggregateProcessor::ObSelector::add_batch(
  const ObIArray<ObExpr *> *param_exprs,
  ObSortOpImpl *unique_sort_op,
  GroupConcatExtraResult *extra_info,
  ObEvalCtx &eval_ctx
) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(unique_sort_op)) {
    if (OB_FAIL(unique_sort_op->add_batch(
        *param_exprs, *brs_->skip_, brs_->size_, selector_array_, count_))) {
      LOG_WARN("failed to add batch", K(ret));
    }
  } else if (OB_NOT_NULL(extra_info->sort_op_)) {
    if (OB_FAIL(extra_info->sort_op_->add_batch(
        *param_exprs, *brs_->skip_, brs_->size_, selector_array_, count_))) {
      LOG_WARN("failed to add batch", K(ret));
    } else {
      extra_info->row_count_ += count_;
    }
  } else {
    int64_t stored_rows_count = 0;
    if (OB_FAIL(extra_info->row_store_.add_batch(
        *param_exprs, eval_ctx, *brs_->skip_, brs_->size_, selector_array_, count_, nullptr))) {
      LOG_WARN("failed to add batch", K(ret));
    } else {
      extra_info->row_count_ += count_;
    }
  }
  return ret;
}

int ObAggregateProcessor::ObSelector::add_batch(
  const ObIArray<ObExpr *> *param_exprs,
  HybridHistExtraResult *extra_info,
  ObEvalCtx &eval_ctx
) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(extra_info->sort_op_)) {
    if (OB_FAIL(extra_info->sort_op_->add_batch(
        *param_exprs, *brs_->skip_, brs_->size_, selector_array_, count_))) {
      LOG_WARN("failed to add batch");
    } else {
      extra_info->sort_row_count_ += count_;
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::top_fre_hist_calc_batch(
  const ObAggrInfo &aggr_info,
  TopKFreHistExtraResult *extra_info,
  const ObDatumVector &arg_datums,
  const T &selector
)
{
  int ret = OB_SUCCESS;
  const ObObjMeta obj_meta = aggr_info.param_exprs_.at(0)->obj_meta_;
  for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
    uint64_t nth_row = selector.get_batch_index(it);
    ObDatum *datum = arg_datums.at(nth_row);
    if (datum->is_null()) {
      continue;
    }
    ObObj obj;
    if (OB_FAIL(datum->to_obj(obj, obj_meta))) {
      LOG_WARN("failed to obj", K(ret));
    } else if (OB_FAIL(extra_info->topk_fre_hist_.add_top_k_frequency_item(obj))) {
      LOG_WARN("failed to process row", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::group_extra_aggr_calc_batch(
  const ObIArray<ObExpr *> *param_exprs,
  AggrCell &aggr_cell,
  const ObAggrInfo &aggr_info,
  GroupConcatExtraResult *extra_info,
  const T &selector
)
{
  int ret = OB_SUCCESS;
  if (!aggr_cell.get_is_evaluated()) {
    if (OB_FAIL(init_group_extra_aggr_info(aggr_cell, aggr_info))) {
      LOG_WARN("failed to init group extra aggr info", K(ret));
    } else {
      aggr_cell.set_is_evaluated(true);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(selector.add_batch(param_exprs, nullptr, extra_info, eval_ctx_))) {
      LOG_WARN("failed to add batch", K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::approx_count_merge_calc_batch(
  ObDatum &dst,
  AggrCell &aggr_cell,
  const ObDatumVector &arg_datums,
  const bool is_number,
  const T &selector
)
{
  int ret = OB_SUCCESS;
  for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
    uint64_t nth_row = selector.get_batch_index(it);
    if (arg_datums.at(nth_row)->is_null()) {
      continue;
    }
    if (!aggr_cell.get_is_evaluated()) {
      // init firstly
      ret = clone_aggr_cell(aggr_cell, *arg_datums.at(nth_row), is_number);
      aggr_cell.set_is_evaluated(true);
    } else {
      ret = llc_add(dst, *arg_datums.at(nth_row));
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::approx_count_calc_batch(
  ObDatum &dst,
  const ObIArray<ObExpr *> *param_exprs,
  const T &selector
)
{
  int ret = OB_SUCCESS;
  int64_t nth_row = 0;
  for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
    uint64_t hash_value = 0;
    bool has_null_cell = false;
    for (int64_t nth_arg = 0; OB_SUCC(ret) && !has_null_cell && nth_arg < param_exprs->count(); ++nth_arg) {
      ObExpr *expr = param_exprs->at(nth_arg);
      ObDatumVector arg_datums = expr->locate_expr_datumvector(eval_ctx_);
      nth_row = selector.get_batch_index(it);
      if (arg_datums.at(nth_row)->is_null()) {
        has_null_cell = true;
      }
      OB_ASSERT(NULL != expr->basic_funcs_);
      ObExprHashFuncType hash_func = expr->basic_funcs_->default_hash_;
      if (OB_FAIL(hash_func(*arg_datums.at(nth_row), hash_value, hash_value))) {
        LOG_WARN("fail to do hash", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("debug approx_count_calc_batch", K(has_null_cell), K(dst));
      if (!has_null_cell) {
        ret = llc_add_value(hash_value, dst.get_string());
      }
    }
  }
  return ret;
}

template <typename T>
int ObAggregateProcessor::add_calc_batch(
    ObDatum &dst, const ObDatumVector &src, AggrCell &aggr_cell, const ObAggrInfo &aggr_info,
    const T &selector)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum &result_datum = dst;
  switch (column_tc) {
    case ObIntTC: {
      char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
      int64_t left_int  = 0;
      int64_t right_int = 0;
      int64_t sum_int   = 0;
      ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
      uint16_t i = 0; // row num in a batch
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
        i = selector.get_batch_index(it);
        if (src.at(i)->is_null()) {
          LOG_DEBUG("debug null", K(i));
          continue;
        }
        left_int  = aggr_cell.get_tiny_num_int();
        right_int = src.at(i)->get_int();
        sum_int   = left_int + right_int;
        ObNumber result_nmb;
        if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
          LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
          if (!result_datum.is_null()) {
            ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                    result_datum.get_number());
            result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
          }
          if (OB_FAIL(result_nmb.add(left_int, right_int, result_nmb, allocator))) {
            LOG_WARN("number add failed", K(ret));
          } else if (OB_FAIL(clone_number_cell(result_nmb, aggr_cell))) {
            LOG_WARN("clone_number_cell failed", K(ret));
          } else {
            aggr_cell.set_tiny_num_int(0);
            allocator.free();
          }
        } else {
          LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int), K(i));
          aggr_cell.set_tiny_num_int(sum_int);
          aggr_cell.set_tiny_num_used();
        }
      }
      break;
    }
    case ObUIntTC: {
      char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
      ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
      uint64_t left_uint  = 0;
      uint64_t right_uint = 0;
      uint64_t sum_uint   = 0;
      ObNumber result_nmb;
      uint16_t i = 0; // row num in a batch
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
        i = selector.get_batch_index(it);
        if (src.at(i)->is_null()) {
          continue;
        }
        left_uint  = aggr_cell.get_tiny_num_uint();
        right_uint = src.at(i)->get_uint();
        sum_uint   = left_uint + right_uint;
        if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
          LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
          if (!result_datum.is_null()) {
            ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                            result_datum.get_number());
            result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
          }
          if (OB_FAIL(result_nmb.add(left_uint, right_uint, result_nmb, allocator))) {
            LOG_WARN("number add failed", K(ret));
          } else if (OB_FAIL(clone_number_cell(result_nmb, aggr_cell))) {
            LOG_WARN("clone_number_cell failed", K(ret));
          } else {
            aggr_cell.set_tiny_num_uint(0);
            allocator.free();
          }
        } else {
          LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
          aggr_cell.set_tiny_num_uint(sum_uint);
          aggr_cell.set_tiny_num_used();
        }
      }
      break;
    }
    case ObFloatTC: {
      float left_f  = 0.0;
      float right_f = 0.0;
      uint16_t i = 0; // row num in a batch
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
        i = selector.get_batch_index(it);
        if (src.at(i)->is_null()) {
          continue;
        }
        if (result_datum.is_null()) {
          ret = clone_aggr_cell(aggr_cell, *src.at(i), false);
        } else {
          left_f  = result_datum.get_float();
          right_f = src.at(i)->get_float();
          if (OB_UNLIKELY(ObArithExprOperator::is_float_out_of_range(left_f + right_f))
              && !lib::is_oracle_mode()) {
            ret = OB_OPERATE_OVERFLOW;
            char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
            int64_t pos = 0;
            databuff_printf(expr_str,
                            OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                            pos,
                            "'(%e + %e)'", left_f, right_f);
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
            LOG_WARN("float out of range", K(left_f), K(right_f));
          } else {
            result_datum.set_float(left_f + right_f);
          }
        }
      }
      break;
    }
    case ObDoubleTC: {
      double left_d  = 0.0;
      double right_d = 0.0;
      uint16_t i = 0; // row num in a batch
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
        i = selector.get_batch_index(it);
        if (src.at(i)->is_null()) {
          continue;
        }
        if (result_datum.is_null()) {
          ret = clone_aggr_cell(aggr_cell, *src.at(i), false);
        } else {
          left_d  = result_datum.get_double();
          right_d = src.at(i)->get_double();
          result_datum.set_double(left_d + right_d);
        }
      }
      break;
    }
    case ObNumberTC: {
      ObNumber result_nmb;
      char buf_alloc1[ObNumber::MAX_CALC_BYTE_LEN];
      char buf_alloc2[ObNumber::MAX_CALC_BYTE_LEN];
      uint32_t sum_digits_buf[ObNumber::OB_CALC_BUFFER_SIZE];
      MEMSET(sum_digits_buf, 0, ObNumber::MAX_CALC_BYTE_LEN);
      ObDataBuffer allocator1(buf_alloc1, ObNumber::MAX_CALC_BYTE_LEN);
      ObDataBuffer allocator2(buf_alloc2, ObNumber::MAX_CALC_BYTE_LEN);
      bool all_skip = true;
      if (!result_datum.is_null()) {
          result_nmb.assign(result_datum.get_number_desc().desc_,
                      const_cast<uint32_t *>(result_datum.get_number_digits()));
      }
      if (OB_FAIL(number_accumulator(src, allocator1, allocator2, result_nmb,
                                     sum_digits_buf, all_skip, selector))) {
        LOG_WARN("number add failed", K(ret), K(result_nmb));
      } else if (!all_skip) {
        ret = clone_number_cell(result_nmb, aggr_cell);
      }
      LOG_DEBUG("number result", K(result_nmb));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(column_tc), K(ret));
    }
  }

  return ret;
}

template <typename T>
int ObAggregateProcessor::bitwise_calc_batch(
    const ObDatumVector &src,
    AggrCell &aggr_cell,
    const ObItemType &aggr_func,
    const T &selector)
{
  int ret = OB_SUCCESS;
  uint64_t cur_uint = 0;
  uint64_t res_uint = 0;
  // In batch mode, it will not call prepare_aggr_result() function to get initial value.
  // So we do alternative initialization here.
  if (aggr_cell.is_tiny_num_used()) {
    res_uint = aggr_cell.get_tiny_num_uint();
  } else if (aggr_func == T_FUN_SYS_BIT_AND) {
    res_uint = UINT_MAX_VAL[ObUInt64Type];
  }
  uint16_t i = 0; // row num in a batch
  switch (aggr_func) {
    case T_FUN_SYS_BIT_AND: {
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
        i = selector.get_batch_index(it);
        if (!src.at(i)->is_null()) {
          cur_uint = src.at(i)->get_uint();
          res_uint &= cur_uint;
        }
      }
      break;
    }
    case T_FUN_SYS_BIT_OR: {
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
        i = selector.get_batch_index(it);
        if (!src.at(i)->is_null()) {
          cur_uint = src.at(i)->get_uint();
          res_uint |= cur_uint;
        }
      }
      break;
    }
    case T_FUN_SYS_BIT_XOR: {
      for (auto it = selector.begin(); OB_SUCC(ret) && it < selector.end(); selector.next(it)) {
        i = selector.get_batch_index(it);
        if (!src.at(i)->is_null()) {
          cur_uint = src.at(i)->get_uint();
          res_uint ^= cur_uint;
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown aggr function type", K(aggr_func));
      break;
    }
  }
  aggr_cell.set_tiny_num_uint(res_uint);
  aggr_cell.set_tiny_num_used();
  return ret;
}

int ObAggregateProcessor::rollup_add_number_calc(ObDatum &aggr_result, AggrCell &aggr_cell)
{
  int ret = OB_SUCCESS;
  ObDatum &rollup_result = aggr_cell.get_iter_result();
  if (aggr_result.is_null()) {
    //do nothing
  } else if (rollup_result.is_null()) {
    if (OB_FAIL(clone_aggr_cell(aggr_cell, aggr_result, true))) {
      LOG_WARN("clone_cell failed", K(ret), K(aggr_result));
    }
  } else {
    char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
    const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
    ObNumber left_nmb(aggr_result.get_number());
    ObNumber right_nmb(rollup_result.get_number());
    ObNumber result_nmb;
    if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, allocator, strict_mode))) {
      LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
    } else if (OB_FAIL(clone_number_cell(result_nmb, aggr_cell))) {
      LOG_WARN("clone_number_cell failed", K(ret), K(result_nmb));
    }
  }
  return ret;
}

int ObAggregateProcessor::rollup_add_calc(
  AggrCell &aggr_cell, AggrCell &rollup_cell, const ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass column_tc = ob_obj_type_class(aggr_info.get_first_child_type());
  ObDatum &aggr_result = aggr_cell.get_iter_result();
  ObDatum &rollup_result = rollup_cell.get_iter_result();
  switch (column_tc) {
    case ObIntTC: {
      if (aggr_cell.is_tiny_num_used()) {
        int64_t left_int = aggr_cell.get_tiny_num_int();
        int64_t right_int = rollup_cell.get_tiny_num_int();
        int64_t sum_int = left_int + right_int;
        if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
          LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
          char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
          ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
          ObNumber result_nmb;
          if (!rollup_result.is_null()) {
            ObCompactNumber &cnum = const_cast<ObCompactNumber &>(rollup_result.get_number());
            result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
          }
          if (OB_FAIL(result_nmb.add(left_int, right_int, result_nmb, allocator))) {
            LOG_WARN("number add failed", K(ret));
          } else if (OB_FAIL(clone_number_cell(result_nmb, rollup_cell))) {
            LOG_WARN("clone_number_cell failed", K(ret));
          } else {
            rollup_cell.set_tiny_num_int(0);
          }
        } else {
          LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
          rollup_cell.set_tiny_num_int(sum_int);
          rollup_cell.set_tiny_num_used();
        }
      }
      if (OB_SUCC(ret)) {
        ret = rollup_add_number_calc(aggr_result, rollup_cell);
      }
      break;
    }
    case ObUIntTC: {
      if (aggr_cell.is_tiny_num_used()) {
        uint64_t left_uint = aggr_cell.get_tiny_num_uint();
        uint64_t right_uint = rollup_cell.get_tiny_num_uint();
        uint64_t sum_uint = left_uint + right_uint;
        if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
          LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
          char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
          ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
          ObNumber result_nmb;
          if (!rollup_result.is_null()) {
            ObCompactNumber &cnum = const_cast<ObCompactNumber &>(rollup_result.get_number());
            result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
          }
          if (OB_FAIL(result_nmb.add(left_uint, right_uint, result_nmb, allocator))) {
            LOG_WARN("number add failed", K(ret));
          } else if (OB_FAIL(clone_number_cell(result_nmb, rollup_cell))) {
            LOG_WARN("clone_number_cell failed", K(ret));
          } else {
            rollup_cell.set_tiny_num_uint(0);
          }
        } else {
          LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
          rollup_cell.set_tiny_num_uint(sum_uint);
          rollup_cell.set_tiny_num_used();
        }
      }
      if (OB_SUCC(ret)) {
        ret = rollup_add_number_calc(aggr_result, rollup_cell);
      }
      break;
    }
    case ObNumberTC: {
      ret = rollup_add_number_calc(aggr_result, rollup_cell);
      break;
    }
    case ObFloatTC: {
      if (aggr_result.is_null()) {
        //do nothing
      } else if (rollup_result.is_null()) {
        ret = clone_aggr_cell(rollup_cell, aggr_result, false);
      } else {
        float left_f = aggr_result.get_float();
        float right_f = rollup_result.get_float();
        if (OB_UNLIKELY(ObArithExprOperator::is_float_out_of_range(left_f + right_f))
            && !lib::is_oracle_mode()) {
          ret = OB_OPERATE_OVERFLOW;
          char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
          int64_t pos = 0;
          databuff_printf(expr_str,
                          OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                          pos,
                          "'(%e + %e)'", left_f, right_f);
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
          LOG_WARN("float out of range", K(left_f), K(right_f));
        } else {
          rollup_result.set_float(left_f + right_f);
        }
      }
      break;
    }
    case ObDoubleTC: {
      if (aggr_result.is_null()) {
        //do nothing
      } else if (rollup_result.is_null()) {
        ret = clone_aggr_cell(rollup_cell, aggr_result, false);
      } else {
        double left_d = aggr_result.get_double();
        double right_d = rollup_result.get_double();
        if (OB_UNLIKELY(ObArithExprOperator::is_double_out_of_range(left_d + right_d))
            && !lib::is_oracle_mode()) {
          ret = OB_OPERATE_OVERFLOW;
          char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
          int64_t pos = 0;
          databuff_printf(expr_str,
                          OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                          pos,
                          "'(%le + %le)'", left_d, right_d);
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
          LOG_WARN("double out of range", K(left_d), K(right_d), K(ret));
        } else {
          rollup_result.set_double(left_d + right_d);
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(column_tc), K(ret));
    }
  }
  return ret;
}

int ObAggregateProcessor::llc_add(ObDatum &result, const ObDatum &new_value)
{
  int ret = OB_SUCCESS;
  ObString res_buf = result.get_string();
  ObString left_buf = result.get_string();
  ObString right_buf = new_value.get_string();;
  if (OB_UNLIKELY(left_buf.length() < get_llc_size())
      || OB_UNLIKELY(right_buf.length() < get_llc_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer size don't match", K(left_buf.length()), K(right_buf.length()), K(ret));
  } else {
    //TODO::here we can use SSE
    for (int64_t i = 0; i < get_llc_size(); ++i) {
      res_buf.ptr()[i] = std::max(static_cast<uint8_t>(left_buf[i]),
                                  static_cast<uint8_t>(right_buf[i]));
    }
  }
  return ret;
}

int ObAggregateProcessor::get_llc_size()
{
  return sizeof(char ) * LLC_NUM_BUCKETS;
}

int ObAggregateProcessor::llc_add_value(const uint64_t value, const ObString &llc_bitmap_buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(llc_add_value(value, const_cast<char*>(llc_bitmap_buf.ptr()), llc_bitmap_buf.length()))) {
    LOG_WARN("fail to add value", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::llc_add_value(const uint64_t value, char *llc_bitmap_buf, int64_t size)
{
  int ret = OB_SUCCESS;
  uint64_t bucket_index = value >> (64 - LLC_BUCKET_BITS);
  uint64_t pmax = 0;
  if (0 == value << LLC_BUCKET_BITS) {
    // do nothing
  } else {
    pmax = ObExprEstimateNdv::llc_leading_zeros(value << LLC_BUCKET_BITS, 64 - LLC_BUCKET_BITS) + 1;
  }
  ObString::obstr_size_t llc_num_buckets = size;
  OB_ASSERT(size == get_llc_size());
  OB_ASSERT(ObExprEstimateNdv::llc_is_num_buckets_valid(llc_num_buckets));
  OB_ASSERT(llc_num_buckets > bucket_index);
  if (pmax > static_cast<uint8_t>(llc_bitmap_buf[bucket_index])) {
    // 理论上pmax不会超过65.
    llc_bitmap_buf[bucket_index] = static_cast<uint8_t>(pmax);
  }
  return ret;
}

int ObAggregateProcessor::llc_init(AggrCell &aggr_cell)
{
  int ret = OB_SUCCESS;
  char llc_bitmap_buf[sizeof(char ) * LLC_NUM_BUCKETS] = {};
  ObDatum src_datum;
  src_datum.set_string(llc_bitmap_buf, sizeof(char ) * LLC_NUM_BUCKETS);
  ret = clone_aggr_cell(aggr_cell, src_datum);
  LOG_DEBUG("llc init", K(aggr_cell));
  return ret;
}

int ObAggregateProcessor::llc_init_empty(ObExpr &expr, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  char *llc_bitmap_buf = NULL;
  const int64_t llc_bitmap_size = get_llc_size();
  if (OB_ISNULL(llc_bitmap_buf = expr.get_str_res_mem(eval_ctx_,
      llc_bitmap_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc", K(llc_bitmap_size), K(ret));
  } else {
    MEMSET(llc_bitmap_buf, 0, llc_bitmap_size);
    ObDatum &datum = expr.locate_datum_for_write(eval_ctx);
    expr.set_evaluated_projected(eval_ctx);
    datum.set_string(llc_bitmap_buf, llc_bitmap_size);
  }
  return ret;
}

int ObAggregateProcessor::llc_calc_hash_value(const ObChunkDatumStore::StoredRow &stored_row,
    const ObIArray<ObExpr *> &param_exprs, bool &has_null_cell, uint64_t &hash_value)
{
  int ret = OB_SUCCESS;
  has_null_cell = false;
  hash_value = 0;
  for (int64_t i = 0; !has_null_cell && i < stored_row.cnt_ && OB_SUCC(ret); ++i) {
    const ObExpr &expr = *param_exprs.at(i);
    const ObDatum &datum = stored_row.cells()[i];
    if (datum.is_null()) {
      has_null_cell = true;
    } else {
      OB_ASSERT(NULL != expr.basic_funcs_);
      ObExprHashFuncType hash_func = expr.basic_funcs_->default_hash_;
      if (OB_FAIL(hash_func(datum, hash_value, hash_value))) {
        LOG_WARN("failed to do hash", K(ret));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::compare_calc(const ObDatum &left_value,
                                       const ObDatum &right_value,
                                       const ObAggrInfo &aggr_info,
                                       int64_t index,
                                       int &compare_result,
                                       bool &is_asc)
{
  int ret = OB_SUCCESS;
  is_asc = false;
  if (OB_UNLIKELY(index >= aggr_info.sort_collations_.count() ||
                  index >= aggr_info.sort_cmp_funcs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(index), K(aggr_info.sort_collations_.count()),
                                     K(aggr_info.sort_cmp_funcs_.count()), K(ret));
  } else if (OB_FAIL(aggr_info.sort_cmp_funcs_.at(index).cmp_func_(left_value, right_value, compare_result))) {
    LOG_WARN("failed to cmp", K(ret), K(index), K(left_value), K(right_value));
  } else {
    is_asc = aggr_info.sort_collations_.at(index).is_ascending_;
  }
  return ret;
}

int ObAggregateProcessor::check_rows_equal(const ObChunkDatumStore::LastStoredRow &prev_row,
                                           const ObChunkDatumStore::StoredRow &cur_row,
                                           const ObAggrInfo &aggr_info,
                                           bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (aggr_info.sort_collations_.empty()) {
    // %sort_columns_ is empty if order by const value, set is_equal to true directly.
    // pre_sort_columns_.store_row_ is NULL here.
    is_equal = true;
  } else if (OB_ISNULL(prev_row.store_row_) ||
             OB_UNLIKELY(prev_row.store_row_->cnt_ != cur_row.cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(prev_row.store_row_), K(cur_row.cnt_));
  } else {
    is_equal = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < aggr_info.sort_collations_.count(); ++i) {
      uint32_t index = aggr_info.sort_collations_.at(i).field_idx_;
      if (OB_UNLIKELY(index >= prev_row.store_row_->cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid argument", K(ret), K(index), K(prev_row.store_row_->cnt_));
      } else {
        int cmp_ret = 0;
        if (OB_FAIL(aggr_info.sort_cmp_funcs_.at(i).cmp_func_(prev_row.store_row_->cells()[index],
                                                              cur_row.cells()[index],
                                                              cmp_ret))) {
          LOG_WARN("failed to cmp", K(ret), K(index));
        } else {
          is_equal = 0 == cmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObAggregateCalcFunc::add_calc(const ObDatum &left_value, const ObDatum &right_value,
    ObDatum &result_datum, const ObObjTypeClass type, ObIAllocator &out_allocator)
{
  int ret = OB_SUCCESS;
  if (left_value.is_null() && right_value.is_null()) {
    result_datum.set_null();
  } else {
    switch (type) {
      case ObIntTC: {
        if (left_value.is_null()) {
          result_datum.set_int(right_value.get_int());
        } else if (right_value.is_null()) {
          result_datum.set_int(left_value.get_int());
        } else {
          int64_t left_int = left_value.get_int();
          int64_t right_int = right_value.get_int();
          int64_t sum_int = left_int + right_int;
          if (ObExprAdd::is_int_int_out_of_range(left_int, right_int, sum_int)) {
            LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
            char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
            ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
            ObNumber result_nmb;
            if (!result_datum.is_null()) {
              ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                      result_datum.get_number());
              result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
            }
            if (OB_FAIL(result_nmb.add(left_int, right_int, result_nmb, allocator))) {
              LOG_WARN("number add failed", K(ret));
            } else if (OB_FAIL(clone_number_cell(result_nmb, result_datum, out_allocator))) {
              LOG_WARN("clone_number_cell failed", K(ret));
            }
          } else {
            LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
            result_datum.set_int(sum_int);
          }
        }
        break;
      }
      case ObUIntTC: {
        if (left_value.is_null()) {
          result_datum.set_uint(right_value.get_uint());
        } else if (right_value.is_null()) {
          result_datum.set_uint(left_value.get_uint());
        } else {
          uint64_t left_uint = left_value.get_uint();
          uint64_t right_uint = right_value.get_uint();
          uint64_t sum_uint = left_uint + right_uint;
          if (ObExprAdd::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
            LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
            char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
            ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
            ObNumber result_nmb;
            if (!result_datum.is_null()) {
              ObCompactNumber &cnum = const_cast<ObCompactNumber &>(
                                              result_datum.get_number());
              result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
            }
            if (OB_FAIL(result_nmb.add(left_uint, right_uint, result_nmb, out_allocator))) {
              LOG_WARN("number add failed", K(ret));
            } else if (OB_FAIL(clone_number_cell(result_nmb, result_datum, out_allocator))) {
              LOG_WARN("clone_number_cell failed", K(ret));
            }
          } else {
            LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
            result_datum.set_uint(sum_uint);
          }
        }
        break;
      }
      case ObFloatTC: {
        if (left_value.is_null()) {
          result_datum.set_float(right_value.get_float());
        } else if (right_value.is_null()) {
          result_datum.set_float(left_value.get_float());
        } else {
          float left_f = left_value.get_float();
          float right_f = right_value.get_float();
          if (OB_UNLIKELY(ObArithExprOperator::is_float_out_of_range(left_f + right_f))
              && !lib::is_oracle_mode()) {
            ret = OB_OPERATE_OVERFLOW;
            char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
            int64_t pos = 0;
            databuff_printf(expr_str,
                            OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                            pos,
                            "'(%e + %e)'", left_f, right_f);
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
            LOG_WARN("float out of range", K(left_f), K(right_f));
          } else {
            result_datum.set_float(left_f + right_f);
          }
        }
        break;
      }
      case ObDoubleTC: {
        if (left_value.is_null()) {
          result_datum.set_double(right_value.get_double());
        } else if (right_value.is_null()) {
          result_datum.set_double(left_value.get_double());
        } else {
          double left_d = left_value.get_double();
          double right_d = right_value.get_double();
          if (OB_UNLIKELY(ObArithExprOperator::is_double_out_of_range(left_d + right_d))
              && !lib::is_oracle_mode()) {
            ret = OB_OPERATE_OVERFLOW;
            char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
            int64_t pos = 0;
            databuff_printf(expr_str,
                            OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                            pos,
                            "'(%le + %le)'", left_d, right_d);
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, lib::is_oracle_mode() ? "BINARY_DOUBLE" : "DOUBLE", expr_str);
            LOG_WARN("double out of range", K(left_d), K(right_d), K(ret));
          } else {
            result_datum.set_double(left_d + right_d);
          }
        }
        break;
      }
      case ObNumberTC: {
        if (left_value.is_null()) {
          if (OB_FAIL(clone_number_cell(right_value.get_number(),
              result_datum, out_allocator))) {
            LOG_WARN("fail to clone number cell", K(ret));
          }
        } else if (right_value.is_null()) {
          if (OB_FAIL(clone_number_cell(left_value.get_number(),
              result_datum, out_allocator))) {
            LOG_WARN("fail to clone number cell", K(ret));
          }
        } else {
          char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN];
          ObDataBuffer allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN);
          const bool strict_mode = false; //this is tmp allocator, so we can ues non-strinct mode
          ObNumber left_nmb(left_value.get_number());
          ObNumber right_nmb(right_value.get_number());
          ObNumber result_nmb;
          if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, allocator, strict_mode))) {
            LOG_WARN("number add failed", K(ret), K(left_nmb), K(right_nmb));
          } else {
            ret = clone_number_cell(result_nmb, result_datum, out_allocator);
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(type), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObAggregateCalcFunc::clone_number_cell(const ObNumber &src_number,
    ObDatum &target_cell, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t need_size = ObNumber::MAX_BYTE_LEN;
  char *buff_ptr = NULL;
  if (OB_ISNULL(buff_ptr = static_cast<char*>(allocator.alloc(need_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
  }

  if (OB_SUCC(ret)) {
    target_cell.ptr_ = buff_ptr;
    target_cell.set_number(src_number);
    LOG_DEBUG("succ to clone cell", K(src_number), K(target_cell));
  }
  return ret;
}

// only for window function aggr cell
int ObAggregateProcessor::clone_cell_for_wf(ObDatum &target_cell, const ObDatum &src_cell,
    const bool is_number/*false*/)
{
  int ret = OB_SUCCESS;
  int64_t need_size = sizeof(int64_t) * 2 +
      (is_number ? number::ObNumber::MAX_BYTE_LEN : src_cell.len_);
  AggrCell aggr_cell;
  if (OB_FAIL(clone_cell(aggr_cell, need_size, &target_cell))) {
    SQL_LOG(WARN, "failed to clone cell", K(ret));
  } else {
    memcpy((char*)target_cell.ptr_, src_cell.ptr_, src_cell.len_);
    target_cell.pack_ = src_cell.pack_;
  }
  OX(SQL_LOG(DEBUG, "succ to clone cell", K(src_cell), K(target_cell), K(need_size)));
  return ret;
}

int ObAggregateProcessor::get_wm_concat_result(const ObAggrInfo &aggr_info,
                                               GroupConcatExtraResult *&extra,
                                               bool is_keep_group_concat,
                                               ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc;
  if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    // 默认为逗号
    ObString sep_str = ObCharsetUtils::get_const_str(aggr_info.expr_->datum_meta_.cs_type_, ',');
    ObChunkDatumStore::LastStoredRow first_row(aggr_alloc_);
    const ObChunkDatumStore::StoredRow *storted_row = NULL;
    bool is_first = true;
    bool need_continue = true;
    int64_t str_len = 0;
    char *buf = NULL;
    int64_t buf_len = 0;
    while (OB_SUCC(ret) && need_continue && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else if (is_keep_group_concat) {
        if (is_first && OB_FAIL(first_row.save_store_row(*storted_row))) {
          LOG_WARN("failed to deep copy limit last rows", K(ret));
        } else if (!is_first && OB_FAIL(check_rows_equal(first_row,
                                                         *storted_row,
                                                         aggr_info,
                                                         need_continue))) {
          LOG_WARN("failed to is order by item equal with prev row", K(ret));
        } else {
          is_first = false;
        }
      }
      if (OB_SUCC(ret) && need_continue) {
        if (storted_row->cells()[0].is_null()) {
        } else {
          const ObDatum &datum = storted_row->cells()[0];
          const ObDatumMeta &datum_meta = aggr_info.expr_->args_[0]->datum_meta_;
          const bool has_lob_header = aggr_info.expr_->args_[0]->obj_meta_.has_lob_header();
          ObString cell_string;
          ObTextStringIter text_iter(datum_meta.type_, datum_meta.cs_type_, datum.get_string(), has_lob_header);
          if (OB_FAIL(text_iter.init(0, NULL, &tmp_alloc))) {
            LOG_WARN("fail to init text reader", K(ret), K(text_iter));
          } else if (OB_FAIL(text_iter.get_full_data(cell_string))) {
            LOG_WARN("fail to get full data", K(ret), K(text_iter));
          } else if (cell_string.length() > 0) {
            int64_t append_len = cell_string.length() + sep_str.length() + str_len;
            if (OB_UNLIKELY(append_len > OB_MAX_PACKET_LENGTH)) {
              ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
              LOG_WARN("result of string concatenation is too long", K(ret), K(append_len),
                                                                     K(OB_MAX_PACKET_LENGTH));
            } else if (buf_len < append_len) {
              char *tmp_buf = NULL;
              buf_len = append_len * 2;
              if (OB_ISNULL(tmp_buf = static_cast<char*>(tmp_alloc.alloc(buf_len)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail alloc memory", K(tmp_buf), K(buf_len), K(ret));
              } else {
                // Note: buf may be nullptr, but str_len = 0 at that time, NO ISSUE
                MEMCPY(tmp_buf, buf, str_len);
                buf = tmp_buf;
              }
            }
            if (OB_SUCC(ret)) {
              MEMCPY(buf + str_len, cell_string.ptr(), cell_string.length());
              str_len += cell_string.length();
              MEMCPY(buf + str_len, sep_str.ptr(), sep_str.length());
              str_len += sep_str.length();
            }
          }
        }
      }
    }//end of while
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      str_len = str_len == 0 ? str_len : str_len - sep_str.length();
      if (str_len > 0) {
        ObString res_str;
        res_str.assign(buf, str_len);
        // see: ObRawExprDeduceType::visit T_FUN_KEEP_WM_CONCAT
        ObLobLocator *result = nullptr;
        char *total_buf = NULL;
        const int64_t total_buf_len = sizeof(ObLobLocator) + str_len; // Notice: using lob locator v1
        if (OB_ISNULL(total_buf = aggr_info.expr_->get_str_res_mem(eval_ctx_, total_buf_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(buf_len));
        } else if (FALSE_IT(result = reinterpret_cast<ObLobLocator *> (total_buf))) {
        } else if (OB_FAIL(result->init(res_str))) {
            LOG_WARN("Failed to init lob locator", K(ret), K(res_str), K(result));
        } else {
          concat_result.set_lob_locator(*result);
        }
      } else {
        concat_result.set_null(); //全部都跳过了，则设为null
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::init_topk_fre_histogram_item(
  const ObAggrInfo &aggr_info,
  ObTopKFrequencyHistograms *topk_fre_hist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FUN_TOP_FRE_HIST != aggr_info.get_expr_type()) ||
      OB_ISNULL(aggr_info.window_size_param_expr_) ||
      OB_ISNULL(aggr_info.item_size_param_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(aggr_info), K(topk_fre_hist));
  } else {
    ObDatum *window_size_result = NULL;
    ObDatum *item_size_result = NULL;
    int64_t window_size = 0;
    int64_t item_size = 0;
    if (OB_UNLIKELY(!aggr_info.window_size_param_expr_->obj_meta_.is_numeric_type() ||
                    !aggr_info.item_size_param_expr_->obj_meta_.is_numeric_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr node is null", K(ret), KPC(aggr_info.window_size_param_expr_),
                                    KPC(aggr_info.item_size_param_expr_));
    } else if (OB_FAIL(aggr_info.window_size_param_expr_->eval(eval_ctx_, window_size_result)) ||
               OB_FAIL(aggr_info.item_size_param_expr_->eval(eval_ctx_, item_size_result))) {
      LOG_WARN("eval failed", K(ret));
    } else if (OB_ISNULL(window_size_result) || OB_ISNULL(item_size_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(window_size_result), K(item_size_result));
    } else if (OB_FAIL(ObExprUtil::get_int_param_val(window_size_result, window_size)) ||
               OB_FAIL(ObExprUtil::get_int_param_val(item_size_result, item_size))) {
      LOG_WARN("failed to get int param val", K(*window_size_result), K(window_size),
                                              K(*item_size_result), K(item_size), K(ret));
    } else {
      topk_fre_hist->set_window_size(window_size);
      topk_fre_hist->set_item_size(item_size);
      topk_fre_hist->set_is_topk_hist_need_des_row(aggr_info.is_need_deserialize_row_);
    }
  }
  return ret;
}

int ObAggregateProcessor::get_pl_agg_udf_result(const ObAggrInfo &aggr_info,
                                                GroupConcatExtraResult *&extra,
                                                ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObPlAggUdfFunction pl_agg_udf_func;
  ObObjParam pl_agg_udf_obj;
  if (OB_ISNULL(extra) || OB_ISNULL(aggr_info.expr_) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(extra), K(aggr_info.expr_));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else if (OB_FAIL(pl_agg_udf_func.init(eval_ctx_.exec_ctx_.get_my_session(),
                                          &(eval_ctx_.exec_ctx_.get_allocator()),
                                          &eval_ctx_.exec_ctx_,
                                          aggr_info.pl_agg_udf_type_id_,
                                          aggr_info.pl_agg_udf_params_type_,
                                          aggr_info.pl_result_type_,
                                          pl_agg_udf_obj))) {
    LOG_WARN("failed to init pl agg udf func", K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *stored_row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(stored_row))) {
      ObObj *tmp_obj = NULL;
      common::ObArenaAllocator tmp_alloc;
      if (OB_ISNULL(stored_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(stored_row));
      } else if (OB_ISNULL(tmp_obj = static_cast<ObObj*>(tmp_alloc.alloc(
                                                            sizeof(ObObj) * (stored_row->cnt_))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(tmp_obj));
      } else if (OB_FAIL(convert_datum_to_obj(aggr_info, *stored_row, tmp_obj, stored_row->cnt_))) {
        LOG_WARN("failed to convert datum to obj", K(ret));
      } else if (OB_FAIL(pl_agg_udf_func.process_calc_pl_agg_udf(pl_agg_udf_obj,
                                                                 tmp_obj,
                                                                 stored_row->cnt_))) {
        LOG_WARN("failed to process calc pl agg udf", K(ret));
      } else {/*do nothing*/}
    }
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObObj result_obj;
      if (OB_FAIL(pl_agg_udf_func.process_get_pl_agg_udf_result(pl_agg_udf_obj, result_obj))) {
        LOG_WARN("failed to process get pl agg udf result", K(ret));
      } else if (ob_is_lob_tc(aggr_info.pl_result_type_.get_type())) {
        if (ob_is_lob_locator(result_obj.get_type())) {
          if (OB_FAIL(result.from_obj(result_obj, aggr_info.expr_->obj_datum_map_))) { // META，旧oblobtype，不涉及
            LOG_WARN("failed to convert obj to ObDatum", K(ret), K(result_obj));
          } else {
            LOG_TRACE("succeed to get pl agg udf result", K(result_obj), K(result));
          }
        } else {
          ObString str;
          if (!ob_is_string_or_lob_type(result_obj.get_type())) {
            ObObj dst_obj;
            ObCastCtx cast_ctx(&aggr_alloc_, NULL, CM_NONE, ObCharset::get_system_collation());
            if (OB_FAIL(ObObjCaster::to_type(ObLongTextType, cast_ctx, result_obj, dst_obj))) {
              LOG_WARN("failed to cast type", K(ret));
            } else if (OB_FAIL(dst_obj.get_string(str))) {
              LOG_WARN("failed to get string", K(ret), K(dst_obj));
            } else {/*do nothing*/}
          } else if (OB_FAIL(result_obj.get_string(str))) {
            LOG_WARN("failed to get string", K(ret), K(result_obj));
          }
          if (OB_SUCC(ret)) {
            ObLobLocator *lob_result = nullptr;
            char *total_buf = NULL;
            const int64_t total_buf_len = sizeof(ObLobLocator) + str.length();
            if (OB_ISNULL(total_buf = aggr_info.expr_->get_str_res_mem(eval_ctx_, total_buf_len))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(total_buf_len));
            } else if (FALSE_IT(lob_result = reinterpret_cast<ObLobLocator *> (total_buf))) {
            } else if (OB_FAIL(lob_result->init(str))) {
              LOG_WARN("Failed to init lob locator", K(ret), K(str), K(lob_result));
            } else {
              result.set_lob_locator(*lob_result);
              LOG_TRACE("succeed to get pl agg udf result", K(result_obj), K(result));
            }
          }
        }
      } else if (OB_FAIL(result.from_obj(result_obj, aggr_info.expr_->obj_datum_map_))) {
        LOG_WARN("failed to convert obj to ObDatum", K(ret), K(result_obj));
      } else if (is_lob_storage(result_obj.get_type()) &&
                 OB_FAIL(ob_adjust_lob_datum(result_obj, aggr_info.expr_->obj_meta_,
                                             aggr_info.expr_->obj_datum_map_,
                                             eval_ctx_.exec_ctx_.get_allocator(), result))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(result_obj.get_meta()),
                 K(aggr_info.expr_->obj_meta_));
      } else {
        LOG_TRACE("succeed to get pl agg udf result", K(result_obj), K(result));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::get_top_k_fre_hist_result(ObTopKFrequencyHistograms &top_k_fre_hist,
                                                    bool has_lob_header,
                                                    ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  if (top_k_fre_hist.has_bucket()) {
    //sort and reserve topk item
    if (OB_FAIL(top_k_fre_hist.create_topk_fre_items())) {
      LOG_WARN("failed to adjust frequency sort", K(ret));
    } else {
      char *buf = NULL;
      int64_t buf_size = top_k_fre_hist.get_serialize_size();
      int64_t buf_pos = 0;
      ObTextStringResult new_tmp_lob(ObLongTextType, has_lob_header, &aggr_alloc_);
      if (OB_FAIL(new_tmp_lob.init(buf_size))) {
        LOG_WARN("init tmp lob failed", K(ret), K(buf_size));
      } else if (OB_FAIL(new_tmp_lob.get_reserved_buffer(buf, buf_size))) {
        LOG_WARN("tmp lob append failed", K(ret), K(new_tmp_lob));
      } else if (OB_FAIL(top_k_fre_hist.serialize(buf, buf_size, buf_pos))) {
        LOG_WARN("fail serialize init task arg", KP(buf), K(buf_size), K(buf_pos), K(ret));
      } else if (OB_FAIL(new_tmp_lob.lseek(buf_pos, 0))) {
        LOG_WARN("temp lob lseek failed", K(ret), K(new_tmp_lob), K(buf_pos));
      } else {
        ObString lob_loc_str;
        new_tmp_lob.get_result_buffer(lob_loc_str);
        result_datum.set_string(lob_loc_str);
        LOG_TRACE("succeed to get topK fre hist result", K(result_datum),K(top_k_fre_hist));
      }
    }
  } else {
    result_datum.set_null();
    LOG_TRACE("succeed to get topK fre hist result", K(result_datum), K(top_k_fre_hist));
  }
  return ret;
}

int ObAggregateProcessor::convert_datum_to_obj(const ObAggrInfo &aggr_info,
                                               const ObChunkDatumStore::StoredRow &stored_row,
                                               ObObj *tmp_obj,
                                               int64_t obj_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj_cnt != stored_row.cnt_ || obj_cnt != aggr_info.param_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(obj_cnt), K(stored_row.cnt_),
                                     K(aggr_info.param_exprs_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_row.cnt_; ++i) {
      if (OB_FAIL(stored_row.cells()[i].to_obj(tmp_obj[i],
                                               aggr_info.param_exprs_.at(i)->obj_meta_))) {
        LOG_WARN("failed to obj", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObAggregateProcessor::compute_hybrid_hist_result(const ObAggrInfo &aggr_info,
                                                     HybridHistExtraResult *&extra,
                                                     ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObHybridHistograms hybrid_hist;
  ObDatum *bucket_num_result = NULL;
  int64_t bucket_num = 0;
  int64_t num_distinct = 0;
  int64_t null_count = 0;
  int64_t total_count = 0;
  int64_t pop_count = 0;
  int64_t pop_freq = 0;
  if (OB_ISNULL(extra) || OB_ISNULL(aggr_info.bucket_num_param_expr_) ||
     OB_UNLIKELY(extra->get_sort_row_count() == 0 ||
                 aggr_info.param_exprs_.count() != 1 ||
                 aggr_info.sort_collations_.count() != 1 ||
                 !aggr_info.bucket_num_param_expr_->obj_meta_.is_numeric_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(extra), K(aggr_info.param_exprs_.count()),
                                     K(aggr_info.sort_collations_.count()),
                                     K(aggr_info.bucket_num_param_expr_));
  } else if (OB_FAIL(aggr_info.bucket_num_param_expr_->eval(eval_ctx_, bucket_num_result))) {
    LOG_WARN("eval failed", K(ret));
  } else if (OB_ISNULL(bucket_num_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(bucket_num_result));
  } else if (OB_FAIL(ObExprUtil::get_int_param_val(bucket_num_result, bucket_num))) {
    LOG_WARN("failed to get int param val", K(*bucket_num_result), K(bucket_num), K(ret));
  } else if (bucket_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(bucket_num));
  } else if ( OB_FAIL(extra->finish_add_sort_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    ObChunkDatumStore::LastStoredRow prev_row(aggr_alloc_);
    const int64_t extra_size = sizeof(BucketDesc);
    int64_t repeat_count = 0;
    const ObChunkDatumStore::StoredRow *stored_row = NULL;
    const ObChunkDatumStore::StoredRow *mat_stored_row = NULL;
    // get null count
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row_from_sort(stored_row))) {
      if (OB_ISNULL(stored_row) || OB_UNLIKELY(stored_row->cnt_ != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(stored_row));
      } else if (stored_row->cells()[0].is_null()) {
        ++ null_count;
      } else if (OB_FAIL(prev_row.save_store_row(*stored_row))) {
        LOG_WARN("failed to deep copy limit last rows", K(ret));
      } else {
        repeat_count = 1;
        ++ num_distinct;
        break;
      }
    }
    total_count = extra->get_sort_row_count() - null_count;
    int64_t pop_threshold = total_count / bucket_num;
    // get all bucket node and store them into chunk datum store
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row_from_sort(stored_row))) {
      bool is_equal = false;
      if (OB_ISNULL(stored_row) || OB_UNLIKELY(stored_row->cnt_ != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(stored_row));
      } else if (OB_FAIL(check_rows_equal(prev_row, *stored_row, aggr_info, is_equal))) {
        LOG_WARN("failed to is order by item equal with prev row", K(ret));
      } else if (is_equal) {
        ++ repeat_count;
      } else if (OB_ISNULL(prev_row.store_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(prev_row.store_row_));
      } else if (OB_FAIL(extra->add_material_row(prev_row.store_row_->cells(),
                                                 prev_row.store_row_->cnt_,
                                                 extra_size, mat_stored_row))) {
        LOG_WARN("failed to add material row");
      } else if (OB_FAIL(prev_row.save_store_row(*stored_row))) {
        LOG_WARN("failed to deep copy limit last rows", K(ret));
      } else if (OB_ISNULL(mat_stored_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(mat_stored_row));
      } else {
        BucketDesc *desc = reinterpret_cast<BucketDesc*>(mat_stored_row->get_extra_payload());
        desc->ep_count_ = repeat_count;
        desc->is_pop_ = repeat_count > pop_threshold;
        if (desc->is_pop_) {
          pop_freq += repeat_count;
          ++ pop_count;
        }
        repeat_count = 1;
        ++ num_distinct;
      }
    }
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      bool has_lob_header = aggr_info.expr_->obj_meta_.has_lob_header();
      if (prev_row.store_row_ != nullptr) {
        if (OB_ISNULL(prev_row.store_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(prev_row.store_row_));
        } else if (OB_FAIL(extra->add_material_row(prev_row.store_row_->cells(),
                                                   prev_row.store_row_->cnt_,
                                                   extra_size, mat_stored_row))) {
          LOG_WARN("failed to add material row");
        } else if (OB_ISNULL(mat_stored_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(mat_stored_row));
        } else {
          BucketDesc *desc = reinterpret_cast<BucketDesc*>(mat_stored_row->get_extra_payload());
          desc->ep_count_ = repeat_count;
          desc->is_pop_ = repeat_count > pop_threshold;
          if (desc->is_pop_) {
            pop_freq += repeat_count;
            ++ pop_count;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(extra->finish_add_material_row())) {
        LOG_WARN("failed to finish add material row", K(ret));
      } else if (OB_FAIL(hybrid_hist.build_hybrid_hist(extra, &aggr_alloc_, bucket_num, total_count,
                                                       num_distinct, pop_count, pop_freq,
                                                       aggr_info.param_exprs_.at(0)->obj_meta_))) {
        LOG_WARN("failed to build hybrid hist", K(ret), K(&aggr_alloc_));
      } else if (OB_FAIL(get_hybrid_hist_result(&hybrid_hist, has_lob_header, result))) {
        LOG_WARN("failed to get hybrid hist result", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObAggregateProcessor::get_hybrid_hist_result(ObHybridHistograms *hybrid_hist,
                                                 bool has_lob_header,
                                                 ObDatum &result_datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hybrid_hist)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null hybrid histograms", K(ret));
  } else if (hybrid_hist->get_buckets().count() > 0) {
    char *buf = NULL;
    int64_t buf_size = hybrid_hist->get_serialize_size();
    int64_t buf_pos = 0;
    ObTextStringResult new_tmp_lob(ObLongTextType, has_lob_header, &aggr_alloc_);
    if (OB_FAIL(new_tmp_lob.init(buf_size))) {
      LOG_WARN("tmp lob init failed", K(ret), K(buf_size));
    } else if (OB_FAIL(new_tmp_lob.get_reserved_buffer(buf, buf_size))) {
      LOG_WARN("tmp lob append failed", K(ret), K(new_tmp_lob));
    } else if (OB_FAIL(hybrid_hist->serialize(buf, buf_size, buf_pos))) {
      LOG_WARN("fail serialize init task arg", KP(buf), K(buf_size), K(buf_pos), K(ret));
    } else if (OB_FAIL(new_tmp_lob.lseek(buf_pos, 0))) {
      LOG_WARN("temp lob lseek failed", K(ret), K(new_tmp_lob), K(buf_pos));
    } else {
      ObString lob_loc_str;
      new_tmp_lob.get_result_buffer(lob_loc_str);
      result_datum.set_string(lob_loc_str);
      LOG_TRACE("succeed to get hybrid hist result", K(result_datum), KPC(hybrid_hist));
    }
  } else {
    result_datum.set_null();
    LOG_TRACE("succeed to get hybrid hist result", K(result_datum), KPC(hybrid_hist));
  }
  return ret;
}

int ObAggregateProcessor::get_json_arrayagg_result(const ObAggrInfo &aggr_info,
                                                   GroupConcatExtraResult *&extra,
                                                   ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc;
  if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *storted_row = NULL;
    ObJsonArray json_array(&tmp_alloc);
    bool is_bool = false;
    if (OB_FAIL(extra->get_bool_mark(0, is_bool))) {
      LOG_WARN("get_bool info failed, may not distinguish between bool and int", K(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else {
        // get type
        ObObj *tmp_obj = NULL;
        if (OB_ISNULL(tmp_obj = static_cast<ObObj*>(tmp_alloc.alloc(
                                                    sizeof(ObObj) * (storted_row->cnt_))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(tmp_obj));
        } else if (OB_FAIL(convert_datum_to_obj(aggr_info, *storted_row, tmp_obj, storted_row->cnt_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (storted_row->cnt_ < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected column count", K(ret), K(storted_row));
        } else {
          ObObjType val_type = tmp_obj->get_type();
          ObCollationType cs_type = tmp_obj->get_collation_type();
          ObScale scale = tmp_obj->get_scale();
          scale = (val_type == ObBitType) ? aggr_info.param_exprs_.at(0)->datum_meta_.length_semantics_ : scale;
          ObIJsonBase *json_val = NULL;
          ObDatum converted_datum;
          converted_datum.set_datum(storted_row->cells()[0]);
          // convert string charset if needed
          if (ob_is_string_type(val_type) 
              && (ObCharset::charset_type_by_coll(cs_type) != CHARSET_UTF8MB4)) {
            ObString origin_str = converted_datum.get_string();
            ObString converted_str;
            if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type, converted_str, 
                                                             CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
              LOG_WARN("convert string collation failed", K(ret), K(cs_type), K(origin_str.length()));
            } else {
              converted_datum.set_string(converted_str);
              cs_type = CS_TYPE_UTF8MB4_BIN;
            }
          }

          // get json value
          if (OB_FAIL(ret)) {
          } else if (is_bool) {
            void *json_node_buf = tmp_alloc.alloc(sizeof(ObJsonBoolean));
            if (OB_ISNULL(json_node_buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed: allocate jsonboolean", K(ret));
            } else {
              ObJsonBoolean *bool_node = (ObJsonBoolean*)new(json_node_buf)ObJsonBoolean(converted_datum.get_bool());
              json_val = bool_node;
            }
          } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
            if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(converted_datum, val_type,
                                                                          &tmp_alloc, cs_type,
                                                                          json_val, false,
                                                                          tmp_obj->has_lob_header(),
                                                                          true))) {
              LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
            }
          } else {
            if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(converted_datum, val_type,
                                                                    &tmp_alloc, scale,
                                                                    eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info(),
                                                                    eval_ctx_.exec_ctx_.get_my_session(),
                                                                    json_val, false))) {
              LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(json_array.array_append(json_val))) {
            LOG_WARN("failed: json array append json value", K(ret));
          } else if (json_array.get_serialize_size() > OB_MAX_PACKET_LENGTH) {
            ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
            LOG_WARN("result of json_arrayagg is too long", K(ret), K(json_array.get_serialize_size()),
                                                            K(OB_MAX_PACKET_LENGTH));
          }
        }
      }
    }//end of while
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObString str;
      // output res
      if (OB_FAIL(json_array.get_raw_binary(str, &aggr_alloc_))) {
        LOG_WARN("get result binary failed", K(ret));
      } else {
        ObTextStringDatumResult text_result(ObJsonType, aggr_info.expr_->obj_meta_.has_lob_header(), &concat_result);
        if (OB_FAIL(text_result.init(str.length(), &aggr_alloc_))) {
          LOG_WARN("init lob result failed");
        } else if (OB_FAIL(text_result.append(str.ptr(), str.length()))) {
          LOG_WARN("failed to append realdata", K(ret), K(str), K(text_result));
        } else {
          text_result.set_result();
        }
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::get_ora_json_arrayagg_result(const ObAggrInfo &aggr_info,
                                                       GroupConcatExtraResult *&extra,
                                                       ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc;
  if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *storted_row = NULL;
    ObJsonBuffer json_array_buf(&aggr_alloc_);
    ObObj *tmp_obj = NULL;
    if (OB_FAIL(json_array_buf.append("["))) {
      LOG_WARN("fail to append curly brace", K(ret));
    }
    bool inited_tmp_obj = false;
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else {
        // get type
        if (!inited_tmp_obj && OB_ISNULL(tmp_obj = static_cast<ObObj*>(tmp_alloc.alloc(
                                                   sizeof(ObObj) * (storted_row->cnt_))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(tmp_obj));
        } else if (!inited_tmp_obj && FALSE_IT(inited_tmp_obj = true)) {
        } else if (OB_FAIL(convert_datum_to_obj(aggr_info, *storted_row, tmp_obj, storted_row->cnt_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (storted_row->cnt_ < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected column count", K(ret), K(storted_row));
        } else {
          ObObjType val_type = tmp_obj->get_type();
          const ObExpr *expr = aggr_info.param_exprs_.at(0);
          if (val_type == ObNumberType && aggr_info.format_json_) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(val_type));
          } else {
            if (ob_is_null(val_type) && !aggr_info.absent_on_null_) {
              continue;
            }
            ObCollationType cs_type = tmp_obj->get_collation_type();
            ObScale scale = tmp_obj->get_scale();
            ObIJsonBase *json_val = NULL;
            ObDatum converted_datum;
            converted_datum.set_datum(storted_row->cells()[0]);
            bool is_clob = ob_is_clob(val_type, cs_type);
            // convert string charset if neededd
            if ((ob_is_string_tc(val_type) || is_clob || ob_is_json(val_type) || ob_is_raw(val_type))
                && (ObCharset::charset_type_by_coll(cs_type) != CHARSET_UTF8MB4)) {
              ObString origin_str;
              if (ob_is_lob_locator(val_type)) {
                const ObLobLocator &lob_locator = converted_datum.get_lob_locator();
                origin_str.assign(const_cast<char *>(lob_locator.get_payload_ptr()),
                    static_cast<ObString::obstr_size_t>(lob_locator.payload_size_));
                val_type = ObVarcharType;
              } else if (tmp_obj->is_lob_storage()) {
                origin_str = converted_datum.get_string();
                if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc, val_type,
                                                        cs_type, tmp_obj->has_lob_header(), origin_str))) {
                  LOG_WARN("fail to get real data.", K(ret), K(origin_str));
                } else {
                  val_type = ObVarcharType;
                }
              } else {
                origin_str = converted_datum.get_string();
              }
              ObString converted_str;
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type, converted_str,
                                                                   CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
                LOG_WARN("convert string collation failed", K(ret), K(cs_type), K(origin_str.length()));
              } else {
                converted_datum.set_string(converted_str);
                cs_type = CS_TYPE_UTF8MB4_BIN;
              }
            }
            ObEvalCtx ctx(eval_ctx_);
            if (OB_FAIL(ret)) {
            } else if (ObJsonExprHelper::is_convertible_to_json(val_type) || ob_is_raw(val_type)) {

              if (OB_FAIL(ObJsonExprHelper::transform_convertible_2String(*expr, ctx, converted_datum, val_type,
                                                                          cs_type, json_array_buf,
                                                                          tmp_obj->has_lob_header(),
                                                                          aggr_info.format_json_,
                                                                          aggr_info.strict_json_, 0))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
              }
            } else {
              if (OB_FAIL(ObJsonExprHelper::transform_scalar_2String(ctx, converted_datum,
                                                                    val_type, scale,
                                                                    eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info(),
                                                                    json_array_buf))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (json_array_buf.length() > OB_MAX_PACKET_LENGTH) {
              ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
              LOG_WARN("result of json_arrar is too long", K(ret), K(json_array_buf.length()),
                                                                  K(OB_MAX_PACKET_LENGTH));
            } else if (json_array_buf.append(",")) {
              LOG_WARN("fail to append comma", K(ret));
            }
          }
        }
      }
    }//end of while
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ParseNode parse_node;
      parse_node.value_ = aggr_info.returning_type_;
      ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
      ObCollationType obj_cs_type = static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]);
      if (json_array_buf.length() > 1) {
        char *end_of_obj = json_array_buf.ptr() + json_array_buf.length() - 1;
        *end_of_obj = ']';
      } else if (OB_FAIL(json_array_buf.append("]"))) {
        LOG_WARN("fail to append curly brace", K(ret));
      }

      int32_t dst_len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
      ObString res_str(json_array_buf.length(), json_array_buf.ptr());
      ObJsonNode* json_node = NULL;
      uint32_t parse_flag = ObJsonParser::JSN_STRICT_FLAG;

      ADD_FLAG_IF_NEED(!aggr_info.strict_json_, parse_flag, ObJsonParser::JSN_RELAXED_FLAG);
      ADD_FLAG_IF_NEED(aggr_info.with_unique_keys_, parse_flag, ObJsonParser::JSN_UNIQUE_FLAG);

      if (obj_type == ObJsonType) {
        ADD_FLAG_IF_NEED(true, parse_flag, ObJsonParser::JSN_UNIQUE_FLAG);
      }
      if (obj_type == ObJsonType && OB_FAIL(ObJsonParser::get_tree(&tmp_alloc, res_str, json_node, parse_flag))) {
        LOG_WARN("fail to get json base", K(ret), K(res_str));
      } else if (ob_is_string_type(obj_type) || ob_is_lob_locator(obj_type) || ob_is_raw(obj_type)) {
        if (ob_is_string_type(obj_type) || ob_is_raw(obj_type)) {
          if (obj_type == ObVarcharType && res_str.length()  > dst_len) {
            char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
            if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
              LOG_WARN("dst_len fail to string.", K(ret));
            }
            ret = OB_OPERATE_OVERFLOW;
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, res_ptr, "json_arrayagg");
          } else if (ob_is_string_type(obj_type)) {
            if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(*aggr_info.expr_, eval_ctx_, concat_result, res_str, &aggr_alloc_))) {
              LOG_WARN("fail to pack res result.", K(ret));
            }
          } else {
            concat_result.set_string(res_str);
          }
        } else {
          ObLobLocator *result = nullptr;
          char *total_buf = NULL;
          const int64_t total_buf_len = sizeof(ObLobLocator) + res_str.length();
          if (OB_ISNULL(total_buf = aggr_info.expr_->get_str_res_mem(eval_ctx_, total_buf_len))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(total_buf_len));
          } else if (FALSE_IT(result = reinterpret_cast<ObLobLocator *> (total_buf))) {
          } else if (OB_FAIL(result->init(res_str))) {
              LOG_WARN("Failed to init lob locator", K(ret), K(res_str), K(result));
          } else {
            concat_result.set_lob_locator(*result);
          }
        }
      } else if (ob_is_json(obj_type)) {
        ObString raw_binary_str;
        if (OB_FAIL(json_node->get_raw_binary(raw_binary_str, &aggr_alloc_))) {
          LOG_WARN("get result binary failed", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(*aggr_info.expr_, eval_ctx_, concat_result, raw_binary_str, &aggr_alloc_))) {
          LOG_WARN("fail to pack res result.", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unpexcted returning type", K(ret), K(obj_type));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::get_json_objectagg_result(const ObAggrInfo &aggr_info,
                                                    GroupConcatExtraResult *&extra,
                                                    ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  const int col_num = 2;
  common::ObArenaAllocator tmp_alloc(ObModIds::OB_SQL_AGGR_FUNC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *storted_row = NULL;
    ObJsonObject json_object(&tmp_alloc);
    ObObj tmp_obj[col_num];
    bool is_bool = false;
    if (OB_FAIL(extra->get_bool_mark(1, is_bool))) {
      LOG_WARN("get_bool info failed, may not distinguish between bool and int", K(ret));
    }
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else if (storted_row->cnt_ != col_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected column count", K(ret), K(storted_row->cnt_));
      } else {
        // get obj
        if (OB_FAIL(convert_datum_to_obj(aggr_info, *storted_row, tmp_obj, storted_row->cnt_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (tmp_obj[0].get_type() == ObNullType) {
          ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
          LOG_WARN("null type for json_objectagg key");
        } else if (ob_is_string_type(tmp_obj[0].get_type()) 
                   && tmp_obj[0].get_collation_type() == CS_TYPE_BINARY) {
          // not support binary charset as mysql 
          LOG_WARN("unsuport json string type with binary charset", 
              K(tmp_obj[0].get_type()), K(tmp_obj[0].get_collation_type()));
          ret = OB_ERR_INVALID_JSON_CHARSET;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
        } else if (NULL == tmp_obj[0].get_string_ptr()) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("unexpected null result", K(ret), K(tmp_obj[0]));
        } else {
          ObObjType val_type0 = tmp_obj[0].get_type();
          ObCollationType cs_type0 = tmp_obj[0].get_collation_type();
          ObObjType val_type1 = tmp_obj[1].get_type();
          ObScale scale1 = tmp_obj[1].get_scale();
          bool has_lob_header1 = tmp_obj[1].has_lob_header();
          scale1 = (val_type1 == ObBitType) ? aggr_info.param_exprs_.at(1)->datum_meta_.length_semantics_ : scale1;
          ObCollationType cs_type1 = tmp_obj[1].get_collation_type();
          ObString key_string = tmp_obj[0].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc, tmp_obj[0], key_string))) {
            LOG_WARN("fail to read key string", K(ret), K(tmp_obj[0]));
          } else if (ObCharset::charset_type_by_coll(cs_type0) != CHARSET_UTF8MB4) {
            ObString converted_key_str;
            if (OB_FAIL(ObExprUtil::convert_string_collation(key_string, cs_type0, converted_key_str, 
                                                                  CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
              LOG_WARN("convert key string collation failed", K(ret), K(cs_type0), K(key_string.length()));
            } else {
              key_string = converted_key_str;
            }
          }

          // get key and value, and append to json_object
          ObString key_data;
          ObIJsonBase *json_val = NULL;
          if (OB_SUCC(ret) && OB_FAIL(deep_copy_ob_string(tmp_alloc, key_string, key_data))) {
            LOG_WARN("fail copy string", K(ret), K(key_string.length()));
          } else {
            ObDatum converted_datum;
            converted_datum.set_datum(storted_row->cells()[1]);
            if (ob_is_string_type(val_type1) 
            && (ObCharset::charset_type_by_coll(cs_type1) != CHARSET_UTF8MB4)) {
              ObString origin_str = converted_datum.get_string();
              ObString converted_str;
              if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type1, converted_str, 
                                                              CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
                LOG_WARN("convert string collation failed", K(ret), K(cs_type1), K(origin_str.length()));
              } else {
                converted_datum.set_string(converted_str);
                cs_type1 = CS_TYPE_UTF8MB4_BIN;
              }
            }

            if (OB_FAIL(ret)) {
            } else if (is_bool) {
              void *json_node_buf = tmp_alloc.alloc(sizeof(ObJsonBoolean));
              if (OB_ISNULL(json_node_buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed: allocate jsonboolean", K(ret));
              } else {
                ObJsonBoolean *bool_node = (ObJsonBoolean*)new(json_node_buf)ObJsonBoolean(storted_row->cells()[1].get_bool());
                json_val = bool_node;
              }
            } else if (ObJsonExprHelper::is_convertible_to_json(val_type1)) {
              if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(converted_datum, val_type1,
                                                                            &tmp_alloc, cs_type1,
                                                                            json_val, false,
                                                                            has_lob_header1, true))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
              }
            } else {
              if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(converted_datum, val_type1,
                                                                      &tmp_alloc, scale1,
                                                                      eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info(),
                                                                      eval_ctx_.exec_ctx_.get_my_session(),
                                                                      json_val, false))) {
                LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(json_object.object_add(key_data, json_val))) {
              LOG_WARN("failed: json object add json value", K(ret));
            } else if (json_object.get_serialize_size() > OB_MAX_PACKET_LENGTH) {
              ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
              LOG_WARN("result of json_objectagg is too long", K(ret), K(json_object.get_serialize_size()),
                                                                K(OB_MAX_PACKET_LENGTH));
            }
          }
        }
      }
    }//end of while
    if (ret == OB_ERR_JSON_DOCUMENT_NULL_KEY) {
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
    }
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObString str;
      // output res
      if (OB_FAIL(json_object.get_raw_binary(str, &aggr_alloc_))) {
        LOG_WARN("get result binary failed", K(ret));
      } else {
        ObTextStringDatumResult text_result(ObJsonType, aggr_info.expr_->obj_meta_.has_lob_header(), &concat_result);
        if (OB_FAIL(text_result.init(str.length(), &aggr_alloc_))) {
          LOG_WARN("init lob result failed");
        } else if (OB_FAIL(text_result.append(str.ptr(), str.length()))) {
          LOG_WARN("failed to append realdata", K(ret), K(str), K(text_result));
        } else {
          text_result.set_result();
        }
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::get_ora_xmlagg_result(const ObAggrInfo &aggr_info,
                                                GroupConcatExtraResult *&extra,
                                                ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  ret = OB_NOT_SUPPORTED;
  return ret;
}


int ObAggregateProcessor::check_key_valid(common::hash::ObHashSet<ObString> &view_key_names, const ObString &key_name)
{
  INIT_SUCC(ret);

  if (OB_HASH_EXIST == view_key_names.exist_refactored(key_name)) {
    ret = OB_ERR_DUPLICATE_KEY;
    LOG_WARN("duplicate key", K(ret));
  } else if (OB_FAIL(view_key_names.set_refactored(key_name, 0))) {
    LOG_WARN("store key to vector failed", K(ret), K(view_key_names.size()));
  }
  return ret;
}

int ObAggregateProcessor::get_ora_json_objectagg_result(const ObAggrInfo &aggr_info,
                                                        GroupConcatExtraResult *&extra,
                                                        ObDatum &concat_result)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc(ObModIds::OB_SQL_AGGR_FUNC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  const int64_t MAX_BUCKET_NUM = 1024;
  common::hash::ObHashSet<ObString> view_key_names;
  if (OB_FAIL(view_key_names.create(MAX_BUCKET_NUM))) {
    LOG_WARN("init hash failed", K(ret), K(MAX_BUCKET_NUM));
  } else if (OB_ISNULL(extra) || OB_UNLIKELY(extra->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unpexcted null", K(ret), K(extra));
  } else if (extra->is_iterated() && OB_FAIL(extra->rewind())) {
    // Group concat row may be iterated in rollup_process(), rewind here.
    LOG_WARN("rewind failed", KPC(extra), K(ret));
  } else if (!extra->is_iterated() && OB_FAIL(extra->finish_add_row())) {
    LOG_WARN("finish_add_row failed", KPC(extra), K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *storted_row = NULL;
    ObJsonBuffer json_object_buf(&aggr_alloc_);
    if (OB_FAIL(json_object_buf.append("{"))) {
      LOG_WARN("fail to append curly brace", K(ret));
    }
    bool inited_tmp_obj = false;
    ObObj *tmp_obj = NULL;
    while (OB_SUCC(ret) && OB_SUCC(extra->get_next_row(storted_row))) {
      if (OB_ISNULL(storted_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(storted_row));
      } else {
        // get obj
        if (!inited_tmp_obj && OB_ISNULL(tmp_obj = static_cast<ObObj*>(tmp_alloc.alloc(
                                                   sizeof(ObObj) * (storted_row->cnt_))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(tmp_obj));
        } else if (!inited_tmp_obj && FALSE_IT(inited_tmp_obj = true)) {
        } else if (OB_FAIL(convert_datum_to_obj(aggr_info, *storted_row, tmp_obj, storted_row->cnt_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (tmp_obj[0].get_type() == ObNullType) {
          ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
          LOG_WARN("null type for json_objectagg key");
        } else if (ob_is_string_type(tmp_obj[0].get_type())
                   && tmp_obj[0].get_collation_type() == CS_TYPE_BINARY) {
          // not support binary charset as mysql
          LOG_WARN("unsuport json string type with binary charset",
              K(tmp_obj[0].get_type()), K(tmp_obj[0].get_collation_type()));
          ret = OB_ERR_INVALID_JSON_CHARSET;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
        } else if (NULL == tmp_obj[0].get_string_ptr()) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("unexpected null result", K(ret), K(tmp_obj[0]));
        } else {
          ObObjType val_type1 = tmp_obj[1].get_type();
          const ObExpr *expr = aggr_info.param_exprs_.at(1);
          if (val_type1 == ObNumberType && aggr_info.format_json_) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(val_type1));
          } else {
            if (ob_is_null(val_type1) && (aggr_info.absent_on_null_ >= 1)) {
              continue;
            }
            ObObjType val_type0 = tmp_obj[0].get_type();
            ObCollationType cs_type0 = tmp_obj[0].get_collation_type();
            ObScale scale1 = tmp_obj[1].get_scale();
            ObCollationType cs_type1 = tmp_obj[1].get_collation_type();
            ObString key_string = tmp_obj[0].get_string();
            if (OB_SUCC(ret) && ObCharset::charset_type_by_coll(cs_type0) != CHARSET_UTF8MB4) {
              ObString converted_key_str;
              if (OB_FAIL(ObExprUtil::convert_string_collation(key_string, cs_type0, converted_key_str,
                                                              CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
                LOG_WARN("convert key string collation failed", K(ret), K(cs_type0), K(key_string.length()));
              } else {
                key_string = converted_key_str;
              }
            }

            // get key and value, and append to json_object
            if (OB_SUCC(ret) && aggr_info.with_unique_keys_
                && OB_FAIL(check_key_valid(view_key_names, key_string))) {
              LOG_WARN("duplicate key", K(ret), K(key_string));
            }
            ObJsonString ob_str(key_string.ptr(), key_string.length());
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(ob_str.print(json_object_buf, true))) {
              LOG_WARN("fail to print json node", K(ret));
            } else if (OB_FAIL(json_object_buf.append(":"))) {
              LOG_WARN("fail to append colon", K(ret));
            } else {
              ObDatum converted_datum;
              converted_datum.set_datum(storted_row->cells()[1]);
              bool is_clob = !ob_is_clob(val_type1, cs_type1);
              // convert string charset if neededd
              if ((ob_is_string_tc(val_type1) || is_clob || ob_is_json(val_type1) || ob_is_raw(val_type1))
                  && (ObCharset::charset_type_by_coll(cs_type1) != CHARSET_UTF8MB4)) {
                ObString origin_str;
                if (ob_is_lob_locator(val_type1)) {
                  const ObLobLocator &lob_locator = converted_datum.get_lob_locator();
                  origin_str.assign(const_cast<char *>(lob_locator.get_payload_ptr()),
                      static_cast<ObString::obstr_size_t>(lob_locator.payload_size_));
                  val_type1 = ObVarcharType;
                } else if (tmp_obj->is_lob_storage()) {
                  origin_str = converted_datum.get_string();
                  if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc, val_type1,
                                                          cs_type1, tmp_obj[1].has_lob_header(), origin_str))) {
                    LOG_WARN("fail to get real data.", K(ret), K(origin_str));
                  } else {
                    val_type1 = ObVarcharType;
                  }
                } else {
                  origin_str = converted_datum.get_string();
                }
                ObString converted_str;
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(ObExprUtil::convert_string_collation(origin_str, cs_type1, converted_str,
                                                                    CS_TYPE_UTF8MB4_BIN, tmp_alloc))) {
                  LOG_WARN("convert string collation failed", K(ret), K(cs_type1), K(origin_str.length()));
                } else {
                  converted_datum.set_string(converted_str);
                  cs_type1 = CS_TYPE_UTF8MB4_BIN;
                }
              }

              ObIJsonBase *json_val = NULL;
              ObString val_data;
              ObEvalCtx ctx(eval_ctx_);
              if (OB_FAIL(ret)) {
              } else if (ObJsonExprHelper::is_convertible_to_json(val_type1) || ob_is_raw(val_type1)) {
                if (OB_FAIL(ObJsonExprHelper::transform_convertible_2String(*expr, ctx, converted_datum, val_type1,
                                                                            cs_type1, json_object_buf,
                                                                            tmp_obj[1].has_lob_header(),
                                                                            aggr_info.format_json_,
                                                                            aggr_info.strict_json_, 0))) {
                  LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
                }
              } else {
                if (OB_FAIL(ObJsonExprHelper::transform_scalar_2String(ctx, converted_datum,
                                                                      val_type1, scale1,
                                                                      eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info(),
                                                                      json_object_buf))) {
                  LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type1));
                }
              }

              if (OB_FAIL(ret)) {
              } else if (json_object_buf.length() > OB_MAX_PACKET_LENGTH) {
                ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
                LOG_WARN("result of json_objectagg is too long", K(ret), K(json_object_buf.length()),
                                                                K(OB_MAX_PACKET_LENGTH));
              } else if (json_object_buf.append(",")) {
                LOG_WARN("fail to append comma", K(ret));
              }
            }
          }
        }
      }
    }//end of while
    if (ret == OB_ERR_JSON_DOCUMENT_NULL_KEY) {
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
    }
    if (ret != OB_ITER_END && ret != OB_SUCCESS) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS;
      ParseNode parse_node;
      parse_node.value_ = aggr_info.returning_type_;
      ObObjType obj_type = aggr_info.expr_->obj_meta_.get_type();
      ObCollationType obj_cs_type = aggr_info.expr_->obj_meta_.get_collation_type();
      if (json_object_buf.length() > 1) {
        char *end_of_obj = json_object_buf.ptr() + json_object_buf.length() - 1;
        *end_of_obj = '}';
      } else if (OB_FAIL(json_object_buf.append("}"))) {
        LOG_WARN("fail to append curly brace", K(ret));
      }
      ObString res_str(json_object_buf.length(), json_object_buf.ptr());
      ObJsonNode* json_node = NULL;
      int32_t dst_len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
      uint32_t parse_flag = ObJsonParser::JSN_STRICT_FLAG;

      ADD_FLAG_IF_NEED(!aggr_info.strict_json_, parse_flag, ObJsonParser::JSN_RELAXED_FLAG);
      ADD_FLAG_IF_NEED(aggr_info.with_unique_keys_, parse_flag, ObJsonParser::JSN_UNIQUE_FLAG);

      if (obj_type == ObJsonType) {
        ADD_FLAG_IF_NEED(true, parse_flag, ObJsonParser::JSN_UNIQUE_FLAG);
      }
      if (obj_type == ObJsonType && OB_FAIL(ObJsonParser::get_tree(&tmp_alloc, res_str, json_node, parse_flag))) {
        LOG_WARN("fail to get json base", K(ret));
      } else if (ob_is_string_type(obj_type) || ob_is_lob_locator(obj_type) || ob_is_raw(obj_type)) {
        if (ob_is_string_type(obj_type) || ob_is_raw(obj_type)) {
          if (obj_type == ObVarcharType && res_str.length()  > dst_len) {
            char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
            if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
              LOG_WARN("dst_len fail to string.", K(ret));
            }
            ret = OB_OPERATE_OVERFLOW;
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, res_ptr, "json_objectagg");
          } else if (ob_is_string_type(obj_type)) {
            if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(*aggr_info.expr_, eval_ctx_, concat_result, res_str, &aggr_alloc_))) {
              LOG_WARN("fail to pack res result.", K(ret));
            }
          } else {
            concat_result.set_string(res_str);
          }
        } else {
          ObLobLocator *result = nullptr;
          char *total_buf = NULL;
          const int64_t total_buf_len = sizeof(ObLobLocator) + res_str.length();
          if (OB_ISNULL(total_buf = aggr_info.expr_->get_str_res_mem(eval_ctx_, total_buf_len))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(total_buf_len));
          } else if (FALSE_IT(result = reinterpret_cast<ObLobLocator *> (total_buf))) {
          } else if (OB_FAIL(result->init(res_str))) {
              LOG_WARN("Failed to init lob locator", K(ret), K(res_str), K(result));
          } else {
            concat_result.set_lob_locator(*result);
          }
        }
      } else if (ob_is_json(obj_type)) {
        ObString raw_binary_str;
        if (OB_FAIL(json_node->get_raw_binary(raw_binary_str, &aggr_alloc_))) {
          LOG_WARN("get result binary failed", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(*aggr_info.expr_, eval_ctx_, concat_result, raw_binary_str, &aggr_alloc_))) {
          LOG_WARN("fail to pack res result.", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unpexcted returning type", K(ret), K(obj_type));
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::single_row_agg(GroupRow &group_row, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (!support_fast_single_row_agg_) {
    group_row.reuse();
    if (OB_FAIL(prepare(group_row))) {
      LOG_WARN("failed to prepare group row", K(ret));
    } else if (OB_FAIL(collect_group_row(&group_row))) {
      LOG_WARN("failed to collect group by row", K(ret));
    }
  } else if (OB_FAIL(fast_single_row_agg(eval_ctx, aggr_infos_))) {
    LOG_WARN("failed to fill result", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::single_row_agg_batch(GroupRow **group_row, ObEvalCtx &eval_ctx, const int64_t batch_size, const ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(group_row) && OB_NOT_NULL(skip));
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
  batch_info_guard.set_batch_size(batch_size);
  if (OB_FAIL(ret)) {
  } else if (!support_fast_single_row_agg_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip->at(i)) {
        continue;
      }
      if (OB_ISNULL(group_row[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group row is not init", K(ret), K(i));
      } else {
        group_row[i]->reuse();
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(prepare(*group_row[i]))) {
          LOG_WARN("failed to prepare group row", K(ret));
        } else if (OB_FAIL(collect_group_row(group_row[i]))) {
          LOG_WARN("failed to collect group by row", K(ret));
        }
      }
    }
  } else if (OB_FAIL(fast_single_row_agg_batch(eval_ctx, batch_size, skip))) {
    LOG_WARN("failed to fill result", K(ret));
  }
  return ret;
}

int ObAggregateProcessor::fast_single_row_agg(
    ObEvalCtx &eval_ctx, ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  if (!aggr_info.is_implicit_first_aggr()) {
    for (int64_t j = 0; OB_SUCC(ret) && j < aggr_info.param_exprs_.count(); ++j) {
      ObDatum *tmp_res = nullptr;
      if (OB_FAIL(aggr_info.param_exprs_.at(j)->eval(eval_ctx, tmp_res))) {
        LOG_WARN("failed to eval param expr", K(ret), K(j));
      }
    }
    if (OB_SUCC(ret)) {
      const ObItemType aggr_fun = aggr_info.get_expr_type();
      ObDatum &result = aggr_info.expr_->locate_datum_for_write(eval_ctx);
      switch (aggr_fun) {
        case T_FUN_COUNT: {
          bool has_null = false;
          for (int64_t j = 0; !has_null && j < aggr_info.param_exprs_.count(); ++j) {
            has_null = aggr_info.param_exprs_.at(j)->locate_expr_datum(eval_ctx).is_null();
          }
          if (lib::is_mysql_mode()) {
            result.set_int(has_null ? 0 : 1);
          } else {
            result.set_number(has_null ? ObNumber::get_zero() : ObNumber::get_positive_one());
          }
          break;
        }
        case T_FUN_SUM: {
          const ObObjTypeClass tc = ob_obj_type_class(aggr_info.get_first_child_type());
          if ((ObIntTC == tc || ObUIntTC == tc) && !aggr_info.param_exprs_.at(0)->locate_expr_datum(eval_ctx).is_null()) {
            ObNumStackAllocator<2> tmp_alloc;
            ObNumber result_nmb;
            if (ObIntTC == tc) {
              if (OB_FAIL(result_nmb.from(aggr_info.param_exprs_.at(0)->locate_expr_datum(eval_ctx).get_int(), tmp_alloc))) {
                  LOG_WARN("create number from int failed", K(ret), K(result_nmb), K(tc));
                }
            } else {
              if (OB_FAIL(result_nmb.from(aggr_info.param_exprs_.at(0)->locate_expr_datum(eval_ctx).get_uint(), tmp_alloc))) {
                LOG_WARN("create number from int failed", K(ret), K(result_nmb), K(tc));
              }
            }
            OX (result.set_number(result_nmb));
          } else {
            result.set_datum(aggr_info.param_exprs_.at(0)->locate_expr_datum(eval_ctx));
          }
          break;
        }
        case T_FUN_SYS_BIT_AND:
        case T_FUN_SYS_BIT_OR:
        case T_FUN_SYS_BIT_XOR:
        case T_FUN_MAX:
        case T_FUN_MIN: {
          result.set_datum(aggr_info.param_exprs_.at(0)->locate_expr_datum(eval_ctx));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown aggr function type", K(ret), K(aggr_fun), K(*aggr_info.expr_));
        }
      }
    }
  }
  return ret;
}

int ObAggregateProcessor::fast_single_row_agg(ObEvalCtx &eval_ctx, ObIArray<ObAggrInfo> &aggr_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos.count(); ++i) {
    ObAggrInfo &aggr_info = aggr_infos.at(i);
    if (OB_FAIL(fast_single_row_agg(eval_ctx, aggr_info))) {
      LOG_WARN("unknown aggr function type", K(ret),
               K(aggr_info.get_expr_type()), K(*aggr_info.expr_));
    }
  }
  return ret;
}

int ObAggregateProcessor::fast_single_row_agg_batch(ObEvalCtx &eval_ctx, const int64_t batch_size, const ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(skip));
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_infos_.count(); ++i) {
    ObAggrInfo &aggr_info = aggr_infos_.at(i);
    if (aggr_info.is_implicit_first_aggr()) {
      continue;
    }
    const ObItemType aggr_fun = aggr_info.get_expr_type();
    switch (aggr_fun) {
      case T_FUN_COUNT: {
        bool has_null[batch_size];
        MEMSET(has_null, false, sizeof(bool) * batch_size);
        ObDatum *result = aggr_info.expr_->locate_datums_for_update(eval_ctx, batch_size);
        for (int64_t j = 0; j < aggr_info.param_exprs_.count(); ++j) {
          ObDatumVector param_vec = aggr_info.param_exprs_.at(j)->locate_expr_datumvector(eval_ctx);
          for (int64_t batch_idx = 0; batch_idx < batch_size; ++batch_idx) {
            if (skip->at(batch_idx) || has_null[batch_idx]) {
              continue;
            }
            has_null[batch_idx] = param_vec.at(batch_idx)->is_null();
          }
        }
        if (lib::is_mysql_mode()) {
          for (int64_t batch_idx = 0; batch_idx < batch_size; ++batch_idx) {
            if (skip->at(batch_idx)) {
              continue;
            }
            result[batch_idx].set_int(has_null[batch_idx] ? 0 : 1);
          }
        } else {
          for (int64_t batch_idx = 0; OB_SUCC(ret) && batch_idx < batch_size; ++batch_idx) {
            if (skip->at(batch_idx)) {
              continue;
            }
            result[batch_idx].set_number(has_null[batch_idx] ? ObNumber::get_zero() : ObNumber::get_positive_one());
          }
        }
        break;
      }
      case T_FUN_SUM: {
        const ObObjTypeClass tc = ob_obj_type_class(aggr_info.get_first_child_type());
        ObDatum *result = aggr_info.expr_->locate_datums_for_update(eval_ctx, batch_size);
        ObDatumVector param_vec = aggr_info.param_exprs_.at(0)->locate_expr_datumvector(eval_ctx);
        if (ObIntTC == tc) {
          for (int64_t batch_idx = 0; OB_SUCC(ret) && batch_idx < batch_size; ++batch_idx) {
            if (skip->at(batch_idx)) {
              continue;
            }
            ObNumStackAllocator<2> tmp_alloc;
            ObNumber result_nmb;
            if (param_vec.at(batch_idx)->is_null()) {
              result[batch_idx].set_null();
            } else if (OB_FAIL(result_nmb.from(param_vec.at(batch_idx)->get_int(), tmp_alloc))) {
              LOG_WARN("create number from int failed", K(ret), K(result_nmb), K(tc));
            } else {
              result[batch_idx].set_number(result_nmb);
            }
          }
        } else if (ObUIntTC == tc) {
          for (int64_t batch_idx = 0; OB_SUCC(ret) && batch_idx < batch_size; ++batch_idx) {
            if (skip->at(batch_idx)) {
              continue;
            }
            ObNumStackAllocator<2> tmp_alloc;
            ObNumber result_nmb;
            if (param_vec.at(batch_idx)->is_null()) {
              result[batch_idx].set_null();
            } else if (OB_FAIL(result_nmb.from(param_vec.at(batch_idx)->get_uint64(), tmp_alloc))) {
              LOG_WARN("create number from int failed", K(ret), K(result_nmb), K(tc));
            } else {
              result[batch_idx].set_number(result_nmb);
            }
          }
        } else {
          for (int64_t batch_idx = 0; batch_idx < batch_size; ++batch_idx) {
            if (skip->at(batch_idx)) {
              continue;
            }
            result[batch_idx].set_datum(*param_vec.at(batch_idx));
          }
        }
        break;
      }
      case T_FUN_MAX:
      case T_FUN_MIN: {
        ObDatum *result = aggr_info.expr_->locate_batch_datums(eval_ctx);
        ObDatumVector param_vec = aggr_info.param_exprs_.at(0)->locate_expr_datumvector(eval_ctx);
        for (int64_t batch_idx = 0; batch_idx < batch_size; ++batch_idx) {
          if (skip->at(batch_idx)) {
            continue;
          }
          result[batch_idx].set_datum(*param_vec.at(batch_idx));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown aggr function type", K(ret), K(aggr_fun), K(*aggr_info.expr_));
      }
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
