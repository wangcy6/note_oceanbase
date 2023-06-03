/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_CG_SERVICE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_CG_SERVICE_H_
#include "ob_table_scan_executor.h"
#include "ob_table_insert_executor.h"
#include "ob_table_delete_executor.h"
#include "ob_table_update_executor.h"
#include "ob_table_insert_up_executor.h"
#include "ob_table_replace_executor.h"
#include "ob_table_lock_executor.h"
#include "ob_table_context.h"
#include "share/table/ob_table.h" // for ObTableQuery
#include "sql/engine/dml/ob_conflict_checker.h" // for ObConflictCheckerCtdef
#include "ob_table_executor_factory.h"

namespace oceanbase
{
namespace table
{
// 构造表达式的静态类
class ObTableExprCgService
{
public:
  // 构造表达式
  static int generate_exprs(ObTableCtx &ctx,
                             common::ObIAllocator &allocator,
                             ObExprFrameInfo &expr_frame_info);
  // 基于table schema构造全表列原生列引用表达式和生成列表达式
  static int generate_column_raw_exprs(ObTableCtx &ctx);
  // 构造更新需要的表达式
  static int generate_update_raw_exprs(ObTableCtx &ctx);
  // 基于原生表达式生成表达式内存布局
  static int generate_expr_frame_info(ObTableCtx &ctx,
                                        common::ObIAllocator &allocator,
                                        ObExprFrameInfo &expr_frame_info);
  // 基于内存表达式内存布局申请内存(这里只是申请了frame内存，dml场景还需要初始化)
  static int alloc_exprs_memory(ObTableCtx &ctx, ObExprFrameInfo &expr_frame_info);
  static int refresh_insert_exprs_frame(ObTableCtx &ctx,
                                        const common::ObIArray<sql::ObExpr *> &exprs,
                                        const ObTableEntity &entity);
  static int refresh_delete_exprs_frame(ObTableCtx &ctx,
                                        const common::ObIArray<sql::ObExpr *> &exprs,
                                        const ObTableEntity &entity);
  static int refresh_replace_exprs_frame(ObTableCtx &ctx,
                                         const common::ObIArray<sql::ObExpr *> &exprs,
                                         const ObTableEntity &entity);
  static int refresh_update_exprs_frame(ObTableCtx &ctx,
                                        const common::ObIArray<sql::ObExpr *> &old_row,
                                        const common::ObIArray<sql::ObExpr *> &new_row,
                                        const common::ObIArray<sql::ObExpr *> &full_assign_row,
                                        const ObTableCtx::ObAssignIds &assign_ids,
                                        const ObTableEntity &entity);
  static int refresh_insert_up_exprs_frame(ObTableCtx &ctx,
                                           const common::ObIArray<sql::ObExpr *> &ins_new_row,
                                           const common::ObIArray<sql::ObExpr *> &delta_exprs,
                                           const ObTableEntity &entity);
  static int refresh_generated_column_related_frame(ObTableCtx &ctx,
                                                    const common::ObIArray<sql::ObExpr *> &old_row,
                                                    const common::ObIArray<sql::ObExpr *> &full_assign_row,
                                                    const ObTableCtx::ObAssignIds &assign_ids,
                                                    const ObColumnSchemaV2 &col_schema);
  static int generate_assign_exprs(ObTableCtx &ctx,
                                   const ObTableCtx::ObAssignIds &assign_ids,
                                   common::ObIArray<sql::ObRawExpr *> &assign_exprs);
private:
  static int init_datum_param_store(ObTableCtx &ctx,
                                    int64_t capacity);
  static int refresh_exprs_frame(ObTableCtx &ctx,
                                 const common::ObIArray<sql::ObExpr *> &exprs,
                                 const ObTableEntity &entity);
  static int refresh_rowkey_exprs_frame(ObTableCtx &ctx,
                                        const common::ObIArray<sql::ObExpr *> &exprs,
                                        const common::ObIArray<ObObj> &rowkey);
  static int refresh_properties_exprs_frame(ObTableCtx &ctx,
                                            const common::ObIArray<sql::ObExpr *> &exprs,
                                            const ObTableEntity &entity);
  static int refresh_assign_exprs_frame(ObTableCtx &ctx,
                                        const common::ObIArray<sql::ObExpr *> &old_rows,
                                        const common::ObIArray<sql::ObExpr *> &new_rows,
                                        const common::ObIArray<sql::ObExpr *> &full_assign_rows,
                                        const ObTableCtx::ObAssignIds &assign_ids,
                                        const ObTableEntity &entity);
  static int refresh_delta_exprs_frame(ObTableCtx &ctx,
                                       const common::ObIArray<sql::ObExpr *> &delta_exprs,
                                       const ObTableEntity &entity);
  static int generate_full_assign_raw_exprs(ObTableCtx &ctx);
private:
  // 通过column_name在表达式数组获取列引用表达式
  static ObRawExpr* get_ref_raw_expr(const common::ObIArray<sql::ObRawExpr *> &all_exprs,
                                     const common::ObString &col_name);
  // 构造生成列表达式
  static int build_generated_column_expr(ObTableCtx &ctx,
                                         sql::ObColumnRefRawExpr &col_expr,
                                         const common::ObString &expr_str,
                                         const common::ObIArray<sql::ObRawExpr *> &exprs);
  // 处理生成列表达式
  static int resolve_generated_column_expr(ObTableCtx &ctx);
  // 构造列引用原生表达式
  static int generate_column_ref_raw_expr(ObTableCtx &ctx,
                                          const ObColumnSchemaV2 &col_schema,
                                          sql::ObColumnRefRawExpr *&col_ref_expr);
  static int write_datum(common::ObIAllocator &allocator,
                         const sql::ObExpr &expr,
                         sql::ObEvalCtx &eval_ctx,
                         const ObObj &obj);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableExprCgService);
};

class ObTableLocCgService
{
public:
  static int generate_table_loc_meta(const ObTableCtx &ctx,
                                     sql::ObDASTableLocMeta &loc_meta,
                                     bool is_lookup = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLocCgService);
};

class ObTableDmlCgService
{
public:
  static int generate_insert_ctdef(ObTableCtx &ctx,
                                   ObIAllocator &allocator,
                                   ObTableInsCtDef &ins_ctdef);
  static int generate_update_ctdef(ObTableCtx &ctx,
                                   ObIAllocator &allocator,
                                   const ObTableCtx::ObAssignIds &assign_ids,
                                   ObTableUpdCtDef &upd_ctdef);
  static int generate_delete_ctdef(ObTableCtx &ctx,
                                   ObIAllocator &allocator,
                                   ObTableDelCtDef &del_ctdef);
  static int generate_replace_ctdef(ObTableCtx &ctx,
                                    ObIAllocator &allocator,
                                    ObTableReplaceCtDef &replace_ctdef);
  static int generate_insert_up_ctdef(ObTableCtx &ctx,
                                      ObIAllocator &allocator,
                                      const ObTableCtx::ObAssignIds &assign_ids,
                                      ObTableInsUpdCtDef &ins_up_ctdef);
  static int generate_lock_ctdef(ObTableCtx &ctx,
                                 ObTableLockCtDef &lock_ctdef);
  static int generate_conflict_checker_ctdef(ObTableCtx &ctx,
                                             ObIAllocator &allocator,
                                             sql::ObConflictCheckerCtdef &conflict_checker_ctdef);
private:
  static int generate_base_ctdef(ObTableCtx &ctx,
                                 ObTableDmlBaseCtDef &base_ctdef,
                                 common::ObIArray<sql::ObRawExpr*> &old_row,
                                 common::ObIArray<sql::ObRawExpr*> &new_row);
  static int generate_column_ids(const common::ObIArray<sql::ObRawExpr*> &exprs,
                                 common::ObIArray<uint64_t> &column_ids);
  static int generate_das_ins_ctdef(ObTableCtx &ctx,
                                    uint64_t index_tid,
                                    sql::ObDASInsCtDef &das_ins_ctdef,
                                    const common::ObIArray<sql::ObRawExpr*> &new_row);
  static int generate_das_upd_ctdef(ObTableCtx &ctx,
                                    uint64_t index_tid,
                                    const common::ObIArray<sql::ObRawExpr *> &assign_exprs,
                                    sql::ObDASUpdCtDef &das_upd_ctdef,
                                    const common::ObIArray<sql::ObRawExpr*> &old_row,
                                    const common::ObIArray<sql::ObRawExpr*> &new_row,
                                    const common::ObIArray<sql::ObRawExpr*> &full_row);
  static int generate_das_del_ctdef(ObTableCtx &ctx,
                                    uint64_t index_tid,
                                    sql::ObDASDelCtDef &das_del_ctdef,
                                    const common::ObIArray<sql::ObRawExpr*> &old_row);
  static int generate_das_lock_ctdef(ObTableCtx &ctx,
                                     uint64_t index_tid,
                                     sql::ObDASLockCtDef &das_lock_ctdef,
                                     const common::ObIArray<sql::ObRawExpr*> &old_row);
  static int generate_updated_column_ids(const common::ObIArray<sql::ObRawExpr *> &assign_exprs,
                                         const common::ObIArray<uint64_t> &column_ids,
                                         common::ObIArray<uint64_t> &updated_column_ids);
  static int generate_upd_assign_infos(ObTableCtx &ctx,
                                       ObIAllocator &allocator,
                                       const common::ObIArray<sql::ObRawExpr *> &assign_exprs,
                                       ObTableUpdCtDef &udp_ctdef);
  static int generate_das_base_ctdef(uint64_t index_tid,
                                     ObTableCtx &ctx,
                                     sql::ObDASDMLBaseCtDef &base_ctdef);
  static int generate_column_info(ObTableID index_tid,
                                  ObTableCtx &ctx,
                                  sql::ObDASDMLBaseCtDef &base_ctdef);
  static int convert_table_param(ObTableCtx &ctx,
                                 sql::ObDASDMLBaseCtDef &base_ctdef);
  static int generate_projector(const common::ObIArray<uint64_t> &dml_column_ids,
                                const common::ObIArray<uint64_t> &storage_column_ids,
                                const common::ObIArray<sql::ObRawExpr*> &old_row,
                                const common::ObIArray<sql::ObRawExpr*> &new_row,
                                const common::ObIArray<sql::ObRawExpr*> &full_row,
                                sql::ObDASDMLBaseCtDef &das_ctdef);
  static int generate_related_ins_ctdef(ObTableCtx &ctx,
                                        ObIAllocator &allocator,
                                        const common::ObIArray<sql::ObRawExpr*> &new_row,
                                        sql::DASInsCtDefArray &ins_ctdefs);
  static int generate_related_upd_ctdef(ObTableCtx &ctx,
                                        ObIAllocator &allocator,
                                        const common::ObIArray<sql::ObRawExpr *> &assign_exprs,
                                        const common::ObIArray<sql::ObRawExpr*> &old_row,
                                        const common::ObIArray<sql::ObRawExpr*> &new_row,
                                        const common::ObIArray<sql::ObRawExpr*> &full_row,
                                        sql::DASUpdCtDefArray &upd_ctdefs);
  static int generate_related_del_ctdef(ObTableCtx &ctx,
                                        ObIAllocator &allocator,
                                        const common::ObIArray<sql::ObRawExpr*> &old_row,
                                        sql::DASDelCtDefArray &del_ctdefs);
  static int get_rowkey_exprs(ObTableCtx &ctx,
                              common::ObIArray<sql::ObRawExpr*> &rowkey_exprs);
  static int generate_table_rowkey_info(ObTableCtx &ctx,
                                        ObTableInsCtDef &ins_ctdef);
  static int generate_tsc_ctdef(ObTableCtx &ctx,
                                sql::ObDASScanCtDef &tsc_ctdef);
  static int generate_single_constraint_info(ObTableCtx &ctx,
                                             const share::schema::ObTableSchema &index_schema,
                                             const uint64_t table_id,
                                             sql::ObUniqueConstraintInfo &constraint_info);
  static int generate_constraint_infos(ObTableCtx &ctx,
                                       common::ObIArray<sql::ObUniqueConstraintInfo> &cst_infos);
  static int generate_constraint_ctdefs(ObTableCtx &ctx,
                                        ObIAllocator &allocator,
                                        sql::ObRowkeyCstCtdefArray &cst_ctdefs);
  static int replace_exprs_with_dependant(const common::ObIArray<sql::ObRawExpr *> &src_exprs,
                                          common::ObIArray<sql::ObRawExpr *> &dst_exprs);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDmlCgService);
};

class ObTableSpecCgService
{
public:
  // given operation type, generate spec tree
  template<int TYPE>
  static int generate(common::ObIAllocator &alloc,
                      ObTableCtx &ctx,
                      ObTableApiSpec *&root_spec)
  {
    int ret = OB_SUCCESS;
    ObTableApiSpec *spec = nullptr;
    if (TYPE <= TABLE_API_EXEC_INVALID || TYPE >= TABLE_API_EXEC_MAX) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "input TYPE is invalid", K(ret), K(TYPE));
    } else if (TYPE == TABLE_API_EXEC_UPDATE) {
      ret = ObTableSpecCgService::generate_with_child
          <TABLE_API_EXEC_UPDATE, TABLE_API_EXEC_SCAN>(alloc, ctx, root_spec);
    } else if (TYPE == TABLE_API_EXEC_DELETE) {
      ret = ObTableSpecCgService::generate_with_child
          <TABLE_API_EXEC_DELETE, TABLE_API_EXEC_SCAN>(alloc, ctx, root_spec);
    } else if (TYPE == TABLE_API_EXEC_LOCK) {
      ret = ObTableSpecCgService::generate_with_child
          <TABLE_API_EXEC_LOCK, TABLE_API_EXEC_SCAN>(alloc, ctx, root_spec);
    } else if (OB_FAIL(ObTableExecutorFactory::generate_spec(alloc,
                                                            static_cast<ObTableExecutorType>(TYPE),
                                                            ctx,
                                                            spec))) {
      SERVER_LOG(WARN, "fail to generate spec", K(ret));
    } else {
      root_spec = spec;
    }

    return ret;
  }
public:
  static int generate_spec(common::ObIAllocator &alloc,
                           ObTableCtx &ctx,
                           ObTableApiScanSpec &spec);

  static int generate_spec(common::ObIAllocator &alloc,
                           ObTableCtx &ctx,
                           ObTableApiInsertSpec &spec);

  static int generate_spec(common::ObIAllocator &alloc,
                           ObTableCtx &ctx,
                           ObTableApiUpdateSpec &spec);

  static int generate_spec(common::ObIAllocator &alloc,
                           ObTableCtx &ctx,
                           ObTableApiDelSpec &spec);

  static int generate_spec(common::ObIAllocator &alloc,
                           ObTableCtx &ctx,
                           ObTableApiInsertUpSpec &spec);

  static int generate_spec(common::ObIAllocator &alloc,
                           ObTableCtx &ctx,
                           ObTableApiReplaceSpec &spec);

  static int generate_spec(common::ObIAllocator &alloc,
                           ObTableCtx &ctx,
                           ObTableApiLockSpec &spec);

private:
  template<int FATHER_TYPE, int CHILD_TYPE>
  static int generate_with_child(common::ObIAllocator &alloc,
                                 ObTableCtx &ctx,
                                 ObTableApiSpec *&root_spec)
  {
    int ret = OB_SUCCESS;
    ObTableApiSpec *child_spec = nullptr;
    if (FATHER_TYPE <= TABLE_API_EXEC_INVALID || FATHER_TYPE >= TABLE_API_EXEC_MAX ||
        CHILD_TYPE <= TABLE_API_EXEC_INVALID || CHILD_TYPE >= TABLE_API_EXEC_MAX) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid type", K(ret), K(FATHER_TYPE), K(CHILD_TYPE));
    } else if (OB_FAIL(ObTableExecutorFactory::generate_spec(
                        alloc, static_cast<ObTableExecutorType>(CHILD_TYPE), ctx, child_spec))) {
      SERVER_LOG(WARN, "fail to generate scan spec", K(ret));
    } else {
      ObTableApiSpec *father_spec = nullptr;
      if (OB_FAIL(ObTableExecutorFactory::generate_spec(
                    alloc, static_cast<ObTableExecutorType>(FATHER_TYPE), ctx, father_spec))) {
        SERVER_LOG(WARN, "fail to generate update spec", K(ret));
      } else {
        father_spec->set_child(child_spec);
        child_spec->set_parent(father_spec);
        root_spec = father_spec;
      }
    }

    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableSpecCgService);
};

class ObTableTscCgService
{
public:
  ObTableTscCgService() {}
  virtual ~ObTableTscCgService() {}
  static int generate_tsc_ctdef(const ObTableCtx &ctx,
                                ObIAllocator &allocator,
                                ObTableApiScanCtDef &tsc_ctdef);
  static int generate_table_loc_meta(const ObTableCtx &ctx,
                                     sql::ObDASTableLocMeta &loc_meta,
                                     bool is_lookup);
  static int generate_das_result_output(sql::ObDASScanCtDef &das_tsc_ctdef,
                                        const common::ObIArray<uint64_t> &output_cids);
private:
  static int generate_das_tsc_ctdef(const ObTableCtx &ctx,
                                    ObIAllocator &allocator,
                                    sql::ObDASScanCtDef &das_tsc_ctdef);
  static int replace_gen_col_exprs(const ObTableCtx &ctx,
                                  common::ObIArray<sql::ObRawExpr*> &access_exprs);
  static int generate_output_exprs(const ObTableCtx &ctx,
                                   common::ObIArray<sql::ObExpr *> &output_exprs);
  static int generate_access_ctdef(const ObTableCtx &ctx,
                                   ObIAllocator &allocator,
                                   sql::ObDASScanCtDef &das_tsc_ctdef);
  static int generate_table_param(const ObTableCtx &ctx,
                                  sql::ObDASScanCtDef &das_tsc_ctdef);
  static OB_INLINE bool is_in_array(const common::ObIArray<sql::ObRawExpr*> &array,
                                    const sql::ObRawExpr *expr)
  {
    bool is_in = false;
    for (int64_t i = 0; i < array.count() && !is_in; i++) {
      if (array.at(i) == expr) {
        is_in = true;
      }
    }
    return is_in;
  }
  static int generate_rt_exprs(const ObTableCtx &ctx,
                               ObIAllocator &allocator,
                               const common::ObIArray<sql::ObRawExpr *> &src,
                               common::ObIArray<sql::ObExpr *> &dst);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableTscCgService);
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_CG_SERVICE_H_ */