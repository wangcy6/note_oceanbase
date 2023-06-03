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

#ifndef STORAGE_COMPACTION_OB_TABLET_MERGE_CTX_H_
#define STORAGE_COMPACTION_OB_TABLET_MERGE_CTX_H_

#include "share/scheduler/ob_dag_scheduler.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "ob_partition_parallel_merge_ctx.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction/ob_partition_merge_progress.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/scn.h"
<<<<<<< HEAD
=======
#include "storage/ob_tenant_tablet_stat_mgr.h"
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

namespace oceanbase
{

namespace storage
{
struct ObTabletCreateSSTableParam;
class ObStorageSchema;
}

namespace blocksstable
{
class ObSSTable;
struct ObSSTableMergeRes;
}

namespace compaction
{
// used for record output macro blocks
class ObTabletMergeInfo
{
public:
  ObTabletMergeInfo();
  virtual ~ObTabletMergeInfo();

  int init(const ObTabletMergeCtx &ctx, bool need_check = true);
  int add_macro_blocks(const int64_t idx,
                       blocksstable::ObMacroBlocksWriteCtx *blocks_ctx,
                       const ObSSTableMergeInfo &sstable_merge_info);
  int add_bloom_filter(blocksstable::ObMacroBlocksWriteCtx &bloom_filter_blocks_ctx);
  int prepare_index_builder(const ObDataStoreDesc &desc);
  int create_sstable(ObTabletMergeCtx &ctx);
  ObSSTableMergeInfo &get_sstable_merge_info() { return sstable_merge_info_; }
  blocksstable::ObSSTableIndexBuilder *get_index_builder() const { return index_builder_; }
  void destroy();
  int get_data_macro_block_count(int64_t &macro_block_count);
  TO_STRING_KV(K_(is_inited), K_(sstable_merge_info), KP_(index_builder));

private:
  static int build_create_sstable_param(const ObTabletMergeCtx &ctx,
                                        const blocksstable::ObSSTableMergeRes &res,
                                        const blocksstable::MacroBlockId &bf_macro_id,
                                        ObTabletCreateSSTableParam &param);
  int new_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx *&ctx);

  static int record_start_tx_scn_for_tx_data(const ObTabletMergeCtx &ctx, ObTabletCreateSSTableParam &param);
private:
  bool is_inited_;
  common::ObSpinLock lock_;
  ObArray<blocksstable::ObMacroBlocksWriteCtx *> block_ctxs_;
  blocksstable::ObMacroBlocksWriteCtx *bloom_filter_block_ctx_;
  blocksstable::MacroBlockId bloomfilter_block_id_;
  ObSSTableMergeInfo sstable_merge_info_;
  common::ObArenaAllocator allocator_;
  blocksstable::ObSSTableIndexBuilder *index_builder_;
};

struct ObSchemaMergeCtx
{
  ObSchemaMergeCtx(ObIAllocator &allocator);
  ~ObSchemaMergeCtx()
  {
    destroy();
  }
  OB_INLINE void destroy()
  {
    if (allocated_storage_schema_ && nullptr != storage_schema_) {
      storage_schema_->~ObStorageSchema();
      allocator_.free((void *)storage_schema_);
      allocated_storage_schema_ = false;
      storage_schema_ = nullptr;
    }
  }
  int deep_copy(const ObSchemaMergeCtx &input_ctx);

  common::ObIAllocator &allocator_;
  int64_t base_schema_version_;
  int64_t schema_version_;
  bool allocated_storage_schema_;
  const ObStorageSchema *storage_schema_; // schema for all merge

  TO_STRING_KV(K_(base_schema_version), K_(schema_version), KPC_(storage_schema));
};

class ObCompactionTimeGuard : public common::occam::ObOccamTimeGuard
{
public:
  enum ObTabletCompactionEvent{
    DAG_WAIT_TO_SCHEDULE = 0,
    COMPACTION_POLICY,
    GET_TABLE_SCHEMA,
    CALC_PROGRESSIVE_PARAM,
    PRE_PROCESS_TX_TABLE,
    GET_PARALLEL_RANGE,
    EXECUTE,
    CREATE_SSTABLE,
    UPDATE_TABLET,
    RELEASE_MEMTABLE,
    SCHEDULE_OTHER_COMPACTION,
    DAG_FINISH,
    COMPACTION_EVENT_MAX
  };
  const static char *ObTabletCompactionEventStr[];
  static const char *get_comp_event_str(enum ObTabletCompactionEvent event);
public:
  ObCompactionTimeGuard();
  virtual ~ObCompactionTimeGuard();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void add_time_guard(const ObCompactionTimeGuard &other);
  ObCompactionTimeGuard & operator=(const ObCompactionTimeGuard &other);
  OB_INLINE bool is_empty() const { return 0 == idx_; }
  OB_INLINE uint32_t get_specified_cost_time(const int64_t line) {
    uint32_t ret_val = 0;
    for (int64_t idx = 0; idx < idx_; ++idx) {
      if (line_array_[idx] == line) {
        ret_val = click_poinsts_[idx];
        break;
      }
    }
    return ret_val;
  }

private:
  static const int64_t COMPACTION_WARN_THRESHOLD_RATIO = 60 * 1000L * 1000L; // 1 min
  static constexpr float COMPACTION_SHOW_PERCENT_THRESHOLD = 0.1;
  static const int64_t COMPACTION_SHOW_TIME_THRESHOLD = 1 * 1000L * 1000L; // 1s
};

struct ObTabletMergeCtx
{
  ObTabletMergeCtx(ObTabletMergeDagParam &param, common::ObIAllocator &allocator);
  virtual ~ObTabletMergeCtx();
  void destroy();
  virtual bool is_valid() const;
  bool need_full_checksum() const { return is_full_merge_; }
  bool need_rewrite_macro_block(const blocksstable::ObMacroBlockDesc &macro_desc) const;
  int64_t get_storage_format_work_version() const { return param_.merge_version_; }
  int init_parallel_merge();
  int init_merge_progress(bool is_major);
  int get_merge_range(int64_t parallel_idx, blocksstable::ObDatumRange &merge_range);

  int inner_init_for_mini(bool &skip_rest_operation);
  int inner_init_for_medium();
  int init_get_medium_compaction_info(const int64_t medium_snapshot, ObGetMergeTablesResult &result);
  int get_specified_medium_compaction_info_from_memtable(
    ObIAllocator &allocator,
    const int64_t medium_snapshot,
    ObMediumCompactionInfo &info);
  int get_schema_and_gene_from_result(const ObGetMergeTablesResult &get_merge_table_result);
  int get_storage_schema_and_gene_from_result(const ObGetMergeTablesResult &get_merge_table_result);
  int get_storage_schema_to_merge(const ObTablesHandleArray &merge_tables_handle, const bool get_schema_on_memtable = true);

  int try_swap_tablet_handle(const ObTablesHandleArray &tables_handle);
public:
  int get_medium_compaction_info_to_store();

  int get_basic_info_from_result(const ObGetMergeTablesResult &get_merge_table_result);
  int cal_minor_merge_param();
  int cal_major_merge_param(const ObGetMergeTablesResult &get_merge_table_result);
  int init_merge_info();
  int prepare_index_tree();
  int prepare_merge_progress();
  int generate_participant_table_info(char *buf, const int64_t buf_len) const;
  int generate_macro_id_list(char *buf, const int64_t buf_len) const;
  void collect_running_info();
  int update_tablet_directly(const ObGetMergeTablesResult &get_merge_table_result);
  int update_tablet_or_release_memtable(const ObGetMergeTablesResult &get_merge_table_result);

  OB_INLINE int64_t get_concurrent_cnt() const { return parallel_merge_ctx_.get_concurrent_cnt(); }
  ObITable::TableType get_merged_table_type() const;
  ObTabletMergeInfo& get_merge_info() { return merge_info_; }
  const ObStorageSchema *get_schema() const { return schema_ctx_.storage_schema_; }
  int64_t get_compaction_scn() const {
    return
<<<<<<< HEAD
        is_multi_version_minor_merge(param_.merge_type_) ?
=======
        is_multi_version_merge(param_.merge_type_) ?
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
            scn_range_.end_scn_.get_val_for_tx() : sstable_version_range_.snapshot_version_;
  }

  typedef common::ObSEArray<ObGetMergeTablesResult, ObPartitionMergePolicy::OB_MINOR_PARALLEL_INFO_ARRAY_SIZE> MinorParallelResultArray;
  static const int64_t LARGE_VOLUME_DATA_ROW_COUNT_THREASHOLD = 1000L * 1000L; // 100w
  static const int64_t LARGE_VOLUME_DATA_MACRO_COUNT_THREASHOLD = 300L;
  // 1. init in dag
  ObTabletMergeDagParam &param_;
  common::ObIAllocator &allocator_;

  // 2. filled in ObPartitionStore::get_merge_tables
  ObVersionRange sstable_version_range_;// version range for new sstable
  share::ObScnRange scn_range_;
  share::SCN merge_scn_;
  int64_t create_snapshot_version_;

  storage::ObTablesHandleArray tables_handle_;
  storage::ObTableHandleV2 merged_table_handle_;

  // 3. filled in ObTabletMergeCtx::get_schemas_to_merge
  ObSchemaMergeCtx schema_ctx_;

  // 4. filled in ObTabletMergePrepareTask::cal_minior_merge_param
  bool is_full_merge_;               // full merge or increment merge
  bool is_tenant_major_merge_;
  storage::ObMergeLevel merge_level_;
  ObTabletMergeInfo merge_info_;

  ObParallelMergeCtx parallel_merge_ctx_;

  ObLSHandle ls_handle_;
  ObTabletHandle tablet_handle_;

  int16_t sstable_logic_seq_;

  int64_t progressive_merge_num_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_step_;
  bool schedule_major_;

  // we would push up last_replay_log_ts if the corresponding memtable has been merged,
  // but this memtable may not be released due to the warming-up table_store
  // if a new index is created, the schedule will also trigger a mini merge for it with the old frozen memtable
  // now we get a table store with old end_scn within the pg which has a larger last_replay_log_ts
  // so we need use last_replay_log_ts to prevent such useless mini merge happening
  int64_t read_base_version_; // use for major merge
  ObBasicTabletMergeDag *merge_dag_;
  compaction::ObPartitionMergeProgress *merge_progress_;
  compaction::ObICompactionFilter *compaction_filter_;
  ObCompactionTimeGuard time_guard_;
  int64_t rebuild_seq_;
  uint64_t data_version_;
  ObMediumCompactionInfoList merge_list_;
  ObTransNodeDMLStat tnode_stat_; // collect trans node dml stat on memtable, only worked in mini compaction.

  TO_STRING_KV(K_(param), K_(sstable_version_range), K_(create_snapshot_version),
               K_(is_full_merge), K_(merge_level),
               K_(progressive_merge_num),
               K_(parallel_merge_ctx), K_(schema_ctx),
               "tables_handle count", tables_handle_.get_count(),
               K_(progressive_merge_round),
               K_(progressive_merge_step),
               K_(tables_handle), K_(schedule_major),
               K_(scn_range), K_(merge_scn), K_(read_base_version),
               K_(ls_handle), K_(tablet_handle),
               KPC_(merge_progress),
               KPC_(compaction_filter), K_(time_guard), K_(rebuild_seq), K_(data_version), K_(merge_list));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMergeCtx);
};

} // namespace compaction
} // namespace oceanbase

#endif
