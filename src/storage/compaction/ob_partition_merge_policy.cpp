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

#define USING_LOG_PREFIX STORAGE
#include "ob_partition_merge_policy.h"
#include "share/ob_debug_sync_point.h"
#include "share/ob_debug_sync.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/scn.h"
<<<<<<< HEAD
=======
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace blocksstable;

namespace oceanbase
{
namespace compaction
{

// keep order with ObMergeType
ObPartitionMergePolicy::GetMergeTables ObPartitionMergePolicy::get_merge_tables[MERGE_TYPE_MAX]
  = { ObPartitionMergePolicy::get_minor_merge_tables,
      ObPartitionMergePolicy::get_hist_minor_merge_tables,
      ObAdaptiveMergePolicy::get_meta_merge_tables,
      ObPartitionMergePolicy::get_mini_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
    };


int ObPartitionMergePolicy::get_neighbour_freeze_info(
    const int64_t snapshot_version,
    const ObITable *last_major,
    ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
<<<<<<< HEAD
  ObTenantFreezeInfoMgr *freeze_info_mgr = nullptr;
  if (OB_ISNULL(freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null freeze_info_mgr", K(ret));
  } else if (OB_SUCC(freeze_info_mgr->get_neighbour_major_freeze(snapshot_version, freeze_info))) {
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    LOG_WARN("Failed to get freeze info, use snapshot_gc_ts instead", K(ret), K(snapshot_version));
    ret = OB_SUCCESS;
    freeze_info.reset();
    freeze_info.next.freeze_scn.set_max();
    if (OB_NOT_NULL(last_major)) {
      if (OB_FAIL(freeze_info.prev.freeze_scn.convert_for_tx(last_major->get_snapshot_version()))) {
        LOG_WARN("failed to convert scn", K(ret), K(last_major));
      }
=======
  if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_neighbour_major_freeze(snapshot_version, freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("Failed to get freeze info, use snapshot_gc_ts instead", K(ret), K(snapshot_version));
      ret = OB_SUCCESS;
      freeze_info.reset();
      freeze_info.next.freeze_version = INT64_MAX;
      if (OB_NOT_NULL(last_major)) {
        freeze_info.prev.freeze_version = last_major->get_snapshot_version();
      }
    } else {
      LOG_WARN("Failed to get neighbour major freeze info", K(ret), K(snapshot_version));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_medium_merge_tables(
  const ObGetMergeTablesParam &param,
  ObLS &ls,
  const ObTablet &tablet,
  ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  result.reset();
  result.merge_version_ = param.merge_version_;
  result.suggest_merge_type_ = param.merge_type_;
  DEBUG_SYNC(BEFORE_GET_MAJOR_MGERGE_TABLES);

  if (OB_UNLIKELY(!table_store.is_valid() || !param.is_valid() || !is_major_merge_type(param.merge_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(table_store), K(param));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(table_store.get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("major sstable not exist", K(ret), K(table_store));
  } else if (OB_FAIL(base_table->get_frozen_schema_version(result.base_schema_version_))) {
    LOG_WARN("failed to get frozen schema version", K(ret));
  } else if (OB_FAIL(result.handle_.add_table(base_table))) {
    LOG_WARN("failed to add base_table to result", K(ret));
  } else if (base_table->get_snapshot_version() >= param.merge_version_) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("medium merge already finished", K(ret), KPC(base_table), K(result));
  } else if (OB_UNLIKELY(tablet.get_snapshot_version() < param.merge_version_)) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("tablet is not ready to schedule medium merge", K(ret), K(tablet), K(param));
  } else if (OB_UNLIKELY(tablet.get_multi_version_start() > param.merge_version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet haven't kept medium snapshot", K(ret), K(tablet), K(param));
  } else {
    const ObSSTableArray &minor_tables = table_store.get_minor_sstables();
    bool start_add_table_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count_; ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
      // TODO: add right boundary for major
      } else if (!start_add_table_flag && minor_tables[i]->get_upper_trans_version() >= base_table->get_snapshot_version()) {
        start_add_table_flag = true;
      }
      if (OB_SUCC(ret) && start_add_table_flag) {
        if (OB_FAIL(result.handle_.add_table(minor_tables[i]))) {
          LOG_WARN("failed to add table", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(result.handle_.check_continues(nullptr))) {
      LOG_WARN("failed to check continues for major merge", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(base_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null base table", K(ret), K(tablet));
  } else {
    result.version_range_.base_version_ = base_table->get_upper_trans_version();
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
    result.version_range_.snapshot_version_ = param.merge_version_;
    if (OB_FAIL(get_multi_version_start(param.merge_type_, ls, tablet, result.version_range_))) {
      LOG_WARN("failed to get multi version_start", K(ret));
    } else {
      result.read_base_version_ = base_table->get_snapshot_version();
      result.create_snapshot_version_ = base_table->get_meta().get_basic_meta().create_snapshot_version_;
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;

  ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
  int64_t merge_inc_base_version = tablet.get_snapshot_version();
  const ObMergeType merge_type = param.merge_type_;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObSEArray<ObTableHandleV2, MAX_MEMSTORE_CNT> memtable_handles;
  result.reset();
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);

  if (MINI_MERGE != merge_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_UNLIKELY(nullptr == tablet.get_memtable_mgr() || !table_store.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null memtable mgr from tablet or invalid table store", K(ret), K(tablet), K(table_store));
  } else if (table_store.get_minor_sstables().count() >= MAX_SSTABLE_CNT_IN_STORAGE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("Too many sstables, delay mini merge until sstable count falls below MAX_SSTABLE_CNT",
              K(ret), K(table_store), K(tablet));
    // add compaction diagnose info
    ObPartitionMergePolicy::diagnose_table_count_unsafe(MINI_MERGE, tablet);
  } else if (OB_FAIL(tablet.get_memtable_mgr()->get_all_memtables(memtable_handles))) {
    LOG_WARN("failed to get all memtables from memtable mgr", K(ret));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               table_store.get_major_sstables().get_boundary_table(true),
                                               freeze_info))) {
    LOG_WARN("failed to get next major freeze", K(ret), K(merge_inc_base_version), K(table_store));
  } else if (OB_FAIL(find_mini_merge_tables(param, freeze_info, ls, tablet, memtable_handles, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to find mini merge tables", K(ret), K(freeze_info));
    }
  } else if (!result.update_tablet_directly_
      && OB_FAIL(deal_with_minor_result(merge_type, ls, tablet, result))) {
    LOG_WARN("failed to deal with minor merge result", K(ret));
  }

  return ret;
}

int ObPartitionMergePolicy::find_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    const ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info,
    ObLS &ls,
    const storage::ObTablet &tablet,
    ObIArray<ObTableHandleV2> &memtable_handles,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  // TODO: @dengzhi.ldz, remove max_snapshot_version, merge all forzen memtables
  // Keep max_snapshot_version currently because major merge must be done step by step
<<<<<<< HEAD
  int64_t max_snapshot_version = freeze_info.next.freeze_scn.get_val_for_tx();
=======
  int64_t max_snapshot_version = freeze_info.next.freeze_version;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  const SCN &clog_checkpoint_scn = tablet.get_clog_checkpoint_scn();

  // Freezing in the restart phase may not satisfy end >= last_max_sstable,
  // so the memtable cannot be filtered by scn
  // can only take out all frozen memtable
  ObIMemtable *memtable = nullptr;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool need_update_snapshot_version = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_handles.count(); ++i) {
    if (OB_ISNULL(memtable = static_cast<ObIMemtable *>(memtable_handles.at(i).get_table()))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret), K(tablet));
    } else if (OB_UNLIKELY(memtable->is_active_memtable())) {
      LOG_DEBUG("skip active memtable", K(i), KPC(memtable), K(memtable_handles));
      break;
    } else if (!memtable->can_be_minor_merged()) {
      FLOG_INFO("memtable cannot mini merge now", K(ret), K(i), KPC(memtable), K(max_snapshot_version), K(memtable_handles), K(param));
      break;
    } else if (memtable->get_end_scn() <= clog_checkpoint_scn) {
<<<<<<< HEAD
      if (!tablet_id.is_special_merge_tablet()) {
        if (static_cast<ObMemtable *>(memtable)->get_is_force_freeze()
            && memtable->get_snapshot_version() > tablet.get_tablet_meta().snapshot_version_) {
          contain_force_freeze_memtable = true;
        }
=======
      if (!tablet_id.is_special_merge_tablet() &&
          memtable->get_snapshot_version() > tablet.get_tablet_meta().snapshot_version_) {
        need_update_snapshot_version = true;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
      } else {
        LOG_DEBUG("memtable wait to release", K(param), KPC(memtable));
        continue;
      }
    } else if (result.handle_.get_count() > 0) {
      if (result.scn_range_.end_scn_ < memtable->get_start_scn()) {
        FLOG_INFO("scn range  not continues, reset previous minor merge tables",
                  "last_end_scn", result.scn_range_.end_scn_, KPC(memtable), K(tablet));
        // mini merge always use the oldest memtable to dump
        break;
      } else if (memtable->get_snapshot_version() > max_snapshot_version) {
        // This judgment is only to try to prevent cross-major mini merge,
        // but when the result is empty, it can still be added
        LOG_INFO("max_snapshot_version is reached, no need find more tables",
                 K(max_snapshot_version), KPC(memtable));
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.handle_.add_table(memtable))) {
        LOG_WARN("Failed to add memtable", KPC(memtable), K(ret));
      } else {
        // update end_scn/snapshot_version
        if (1 == result.handle_.get_count()) {
          result.scn_range_.start_scn_ = memtable->get_start_scn();
        }
        result.scn_range_.end_scn_ = memtable->get_end_scn();
        result.version_range_.snapshot_version_ = MAX(memtable->get_snapshot_version(), result.version_range_.snapshot_version_);
      }
    }
  } // end for
  if (OB_FAIL(ret)) {
  } else {
    result.suggest_merge_type_ = param.merge_type_;
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
    if (result.handle_.empty()) {
      ret = OB_NO_NEED_MERGE;
    } else if (result.scn_range_.end_scn_ <= clog_checkpoint_scn) {
<<<<<<< HEAD
      if (contain_force_freeze_memtable) {
=======
      if (need_update_snapshot_version) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
        result.update_tablet_directly_ = true;
        result.version_range_.multi_version_start_ = 0; // set multi_version_start to pass tablet::init check
        LOG_INFO("meet empty force freeze memtable, could update tablet directly", K(ret), K(result));
      } else {
        ret = OB_NO_NEED_MERGE;
      }
    } else if (OB_FAIL(refine_mini_merge_result(tablet, result))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to refine mini merge result", K(ret), K(tablet_id));
      }
    }

    if (OB_SUCC(ret) && result.version_range_.snapshot_version_ > max_snapshot_version) {
      result.schedule_major_ = true;
    }
  }

  return ret;
}

int ObPartitionMergePolicy::deal_with_minor_result(
    const ObMergeType &merge_type,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  if (result.handle_.empty()) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("no need to minor merge", K(ret), K(merge_type), K(result));
  } else if (OB_UNLIKELY(!result.scn_range_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument to check result", K(ret), K(result));
<<<<<<< HEAD
  } else if (OB_FAIL(result.handle_.check_continues(merge_type == BUF_MINOR_MERGE ? nullptr : &result.scn_range_))) {
=======
  } else if (OB_FAIL(result.handle_.check_continues(&result.scn_range_))) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (OB_FAIL(get_multi_version_start(merge_type, ls, tablet, result.version_range_))) {
    LOG_WARN("failed to get kept multi_version_start", K(ret), K(merge_type), K(tablet));
  } else {
    result.schema_version_ = tablet.get_storage_schema().schema_version_;
    if (MINI_MERGE == merge_type) {
      ObITable *table = NULL;
      result.base_schema_version_ = result.schema_version_;
      for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
        if (OB_ISNULL(table = result.handle_.get_table(i)) || !table->is_memtable()) {
          ret = OB_ERR_SYS;
          LOG_ERROR("get unexpected table", KPC(table), K(ret));
        } else {
          result.schema_version_ = MAX(result.schema_version_, reinterpret_cast<ObIMemtable *>(table)->get_max_schema_version());
        }
      }
    } else {
      if (OB_FAIL(result.handle_.get_table(0)->get_frozen_schema_version(result.base_schema_version_))) {
        LOG_WARN("failed to get frozen schema version", K(ret), K(result));
      }
    }
    if (OB_SUCC(ret)) {
      result.version_range_.base_version_ = 0;
      if (OB_SUCC(ret) && !is_mini_merge(merge_type)) {
        const ObTabletTableStore &table_store = tablet.get_table_store();
        if (OB_FAIL(table_store.get_recycle_version(result.version_range_.multi_version_start_, result.version_range_.base_version_))) {
          LOG_WARN("Fail to get table store recycle version", K(ret), K(result.version_range_), K(table_store));
        }
      }
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  int64_t min_snapshot_version = 0;
  int64_t max_snapshot_version = 0;
  const ObMergeType merge_type = param.merge_type_;
  result.reset();
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);

  // no need to distinguish data tablet and tx tablet, all minor tables included
  if (OB_UNLIKELY(!is_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(merge_type));
  } else if (tablet.is_ls_inner_tablet()) {
    min_snapshot_version = 0;
    max_snapshot_version = INT64_MAX;
  } else if (OB_FAIL(get_boundary_snapshot_version(tablet, min_snapshot_version, max_snapshot_version))) {
    LOG_WARN("fail to calculate boundary version", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_minor_merge_tables(param,
                                             min_snapshot_version,
                                             max_snapshot_version,
                                             ls,
                                             tablet,
                                             result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get minor merge tables", K(ret), K(max_snapshot_version));
    }
  }

  return ret;
}

int ObPartitionMergePolicy::get_boundary_snapshot_version(
    const ObTablet &tablet,
    int64_t &min_snapshot,
    int64_t &max_snapshot,
    const bool check_table_cnt)
{
  int ret = OB_SUCCESS;
  int64_t merge_inc_base_version = tablet.get_snapshot_version();
  ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObITable *last_major_table = table_store.get_major_sstables().get_boundary_table(true);

  if (OB_UNLIKELY(tablet.is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for special tablet", K(ret), K(tablet));
  } else if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(table_store));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               last_major_table,
                                               freeze_info))) {
    LOG_WARN("failed to get freeze info", K(ret), K(merge_inc_base_version), K(table_store));
  } else if (check_table_cnt && table_store.get_table_count() >= OB_UNSAFE_TABLE_CNT) {
    max_snapshot = INT64_MAX;
    if (table_store.get_table_count() >= OB_EMERGENCY_TABLE_CNT) {
      min_snapshot = 0;
    } else if (OB_NOT_NULL(last_major_table)) {
      min_snapshot = last_major_table->get_snapshot_version();
    }
  } else {
<<<<<<< HEAD
    min_snapshot = freeze_info.prev.freeze_scn.get_val_for_tx();
    max_snapshot = freeze_info.next.freeze_scn.get_val_for_tx();
=======
    if (OB_NOT_NULL(last_major_table)) {
      min_snapshot = max(last_major_table->get_snapshot_version(), freeze_info.prev.freeze_version);
    } else {
      min_snapshot = freeze_info.prev.freeze_version;
    }
    max_snapshot = freeze_info.next.freeze_version;

    int64_t max_medium_scn = 0;
    if (OB_FAIL(tablet.get_max_medium_snapshot(max_medium_scn))) {
      LOG_WARN("failed to get medium from memtables", K(ret));
    } else {
      min_snapshot = max(min_snapshot, max_medium_scn);
    }
    LOG_DEBUG("get boundary snapshot", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_, K(table_store), K(min_snapshot), K(max_snapshot),
        K(tablet.get_medium_compaction_info_list()), K(max_medium_scn), KPC(last_major_table),
        K(freeze_info));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  }
  return ret;
}

int ObPartitionMergePolicy::find_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t min_snapshot_version,
    const int64_t max_snapshot_version,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.reset_handle_and_range();
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObTablesHandleArray minor_tables;
  int64_t minor_compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;

  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("unexpected table store", K(ret), K(table_store));
  } else if (OB_FAIL(table_store.get_mini_minor_sstables(minor_tables))) {
    LOG_WARN("failed to get mini minor sstables", K(ret), K(table_store));
  } else {
    ObSSTable *table = nullptr;
    bool found_greater = false;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        minor_compact_trigger = tenant_config->minor_compact_trigger;
      }
    }

    ObSEArray<ObSSTable*, 16> minor_merge_candidates;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.get_count(); ++i) {
      if (OB_ISNULL(table = static_cast<ObSSTable *>(minor_tables.get_table(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not null", K(ret), K(i), K(table_store));
      } else if (!found_greater && table->get_upper_trans_version() <= min_snapshot_version) {
        continue;
      } else {
        found_greater = true;
<<<<<<< HEAD
        if (result.handle_.get_count() > 0) {
          if (result.scn_range_.end_scn_ < table->get_start_scn()) {
            LOG_INFO("log ts not continues, reset previous minor merge tables",
                     K(i), "last_end_scn", result.scn_range_.end_scn_, KPC(table));
            result.reset_handle_and_range();
          } else if (HISTORY_MINI_MINOR_MERGE == param.merge_type_ && table->get_upper_trans_version() > max_snapshot_version) {
            break;
          } else if (table_store.get_table_count() < OB_UNSAFE_TABLE_CNT &&
                     table->get_max_merged_trans_version() > max_snapshot_version) {
            LOG_INFO("max_snapshot_version reached, stop find more tables", K(param), K(max_snapshot_version), KPC(table));
            break;
          }
        }
        if (OB_FAIL(result.handle_.add_table(table))) {
          LOG_WARN("Failed to add table", KPC(table), K(ret));
        } else {
          if (1 == result.handle_.get_count()) {
            result.scn_range_.start_scn_ = table->get_start_scn();
          }
          result.scn_range_.end_scn_ = table->get_end_scn();
=======
        if (0 == minor_merge_candidates.count()) {
        } else if (is_history_minor_merge(param.merge_type_) && table->get_upper_trans_version() > max_snapshot_version) {
          break;
        } else if (table_store.get_table_count() < OB_UNSAFE_TABLE_CNT &&
            table->get_max_merged_trans_version() > max_snapshot_version) {
          LOG_INFO("max_snapshot_version reached, stop find more tables", K(param), K(max_snapshot_version), KPC(table));
          break;
        }
        if (OB_FAIL(minor_merge_candidates.push_back(table))) {
          LOG_WARN("failed to add table", K(ret));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
        }
      }
    }

    int64_t left_border = 0;
    int64_t right_border = minor_merge_candidates.count();
    if (OB_FAIL(ret)) {
    } else if (MINOR_MERGE != param.merge_type_) {
    } else if (OB_FAIL(refine_minor_merge_tables(tablet, minor_merge_candidates, left_border, right_border))) {
      LOG_WARN("failed to adjust mini minor merge tables", K(ret));
    }

    for (int64_t i = left_border; OB_SUCC(ret) && i < right_border; ++i) {
      ObSSTable *table = minor_merge_candidates.at(i);
      if (result.handle_.get_count() > 0 && result.scn_range_.end_scn_ < table->get_start_scn()) {
        LOG_INFO("log ts not continues, reset previous minor merge tables",
                "last_end_log_ts", result.scn_range_.end_scn_, KPC(table));
        result.reset_handle_and_range();
      }
      if (OB_FAIL(result.handle_.add_table(table))) {
        LOG_WARN("Failed to add table", K(ret), KPC(table));
      } else {
        if (1 == result.handle_.get_count()) {
          result.scn_range_.start_scn_ = table->get_start_scn();
        }
        result.scn_range_.end_scn_ = table->get_end_scn();
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.suggest_merge_type_ = param.merge_type_;
    if (OB_FAIL(refine_minor_merge_result(minor_compact_trigger, result))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to refine_minor_merge_result", K(ret));
      }
    } else if (FALSE_IT(result.version_range_.snapshot_version_ = tablet.get_snapshot_version())) {
    } else {
      if (OB_FAIL(deal_with_minor_result(param.merge_type_, ls, tablet, result))) {
        LOG_WARN("Failed to deal with minor merge result", K(ret), K(param), K(result));
      } else {
        FLOG_INFO("succeed to get minor merge tables", K(min_snapshot_version), K(max_snapshot_version),
            K(result), K(tablet));
      }
    }
  } else if (OB_NO_NEED_MERGE == ret && table_store.get_minor_sstables().count() >= DIAGNOSE_TABLE_CNT_IN_STORAGE) {
      ADD_SUSPECT_INFO(MINOR_MERGE,
                       tablet.get_tablet_meta().ls_id_,
                       tablet.get_tablet_meta().tablet_id_,
                       "can't schedule minor merge.",
                       K(min_snapshot_version), K(max_snapshot_version),
                       "mini_sstable_cnt", table_store.get_minor_sstables().count());
  }
  return ret;
}

int ObPartitionMergePolicy::refine_minor_merge_tables(
    const ObTablet &tablet,
    const ObIArray<ObSSTable *> &merge_tables,
    int64_t &left_border,
    int64_t &right_border)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObITable *meta_table = tablet.get_table_store().get_extend_sstable(ObTabletTableStore::META_MAJOR);
  int64_t covered_by_meta_table_cnt = 0;
  left_border = 0;
  right_border = merge_tables.count();

  if (tablet_id.is_special_merge_tablet()) {
  } else if (merge_tables.count() < 2 || nullptr == meta_table) {
    // do nothing
  } else {
    // no need meta merge, but exist meta_sstable
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_tables.count(); ++i) {
      if (OB_ISNULL(merge_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(i), K(merge_tables));
      } else if (merge_tables.at(i)->get_upper_trans_version() > meta_table->get_snapshot_version()) {
        break;
      } else {
        ++covered_by_meta_table_cnt;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (covered_by_meta_table_cnt * 2 >= merge_tables.count()) {
    right_border = covered_by_meta_table_cnt;
  } else {
    left_border = covered_by_meta_table_cnt;
  }
  return ret;
}

int ObPartitionMergePolicy::get_hist_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  int64_t max_snapshot_version = 0;
  result.reset();

  if (OB_UNLIKELY(!is_history_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_FAIL(deal_hist_minor_merge(tablet, max_snapshot_version))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to deal hist minor merge", K(ret));
    }
<<<<<<< HEAD
  } else if (OB_FAIL(find_mini_minor_merge_tables(param, 0, max_snapshot_version, multi_version_start, tablet, result))) {
    LOG_WARN("failed to get minor tables for hist minor merge", K(ret));
  }
  return ret;
}

int ObPartitionMergePolicy::get_buf_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  UNUSEDx(param, multi_version_start, tablet, result);
  return ret;
}

int ObPartitionMergePolicy::find_buf_minor_merge_tables(
    const storage::ObTablet &tablet,
    ObGetMergeTablesResult *result)
{
  int ret = OB_SUCCESS;
  UNUSEDx(tablet, result);
  return ret;
}

int ObPartitionMergePolicy::find_buf_minor_base_table(ObITable *last_major_table, ObITable *&buf_minor_base_table)
{
  int ret = OB_SUCCESS;
  UNUSEDx(last_major_table, buf_minor_base_table);
  return ret;
}

int ObPartitionMergePolicy::add_buf_minor_merge_result(ObITable *table, ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  UNUSEDx(table, result);
  return ret;
}

int ObPartitionMergePolicy::get_major_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  result.reset();
  result.merge_version_ = param.merge_version_;
  result.suggest_merge_type_ = MAJOR_MERGE;
  DEBUG_SYNC(BEFORE_GET_MAJOR_MGERGE_TABLES);

  if (OB_UNLIKELY(!table_store.is_valid() || !param.is_major_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(table_store), K(param));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(table_store.get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("major sstable not exist", K(ret), K(table_store));
  } else if (base_table->get_snapshot_version() >= result.merge_version_) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("major merge already finished", K(ret), KPC(base_table), K(result));
  } else if (OB_FAIL(base_table->get_frozen_schema_version(result.base_schema_version_))) {
    LOG_WARN("failed to get frozen schema version", K(ret));
  } else if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_behind_snapshot_version,
      base_table->get_snapshot_version(), freeze_info))) {
    LOG_WARN("failed to get freeze info", K(ret), K(base_table->get_snapshot_version()));
  } else if (OB_FAIL(result.handle_.add_table(base_table))) {
    LOG_WARN("failed to add base_table to result", K(ret));
  } else if (base_table->get_snapshot_version() >= freeze_info.freeze_scn.get_val_for_tx()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected sstable with snapshot_version bigger than next freeze_scn",
             K(ret), K(freeze_info), KPC(base_table), K(tablet));
  } else {
    const ObSSTableArray &minor_tables = table_store.get_minor_sstables();
    bool start_add_table_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count_; ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
      // TODO: add right boundary for major
      } else if (!start_add_table_flag && minor_tables[i]->get_upper_trans_version() > base_table->get_snapshot_version()) {
        start_add_table_flag = true;
      }
      if (OB_SUCC(ret) && start_add_table_flag) {
        if (OB_FAIL(result.handle_.add_table(minor_tables[i]))) {
          LOG_WARN("failed to add table", K(ret));
        } else {
          result.scn_range_.end_scn_ = minor_tables[i]->get_key().get_end_scn();
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (result.handle_.get_count() < 2) { // fix issue 42746719
        if (OB_UNLIKELY(NULL == result.handle_.get_table(0) || !result.handle_.get_table(0)->is_major_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the only table must be major sstable", K(ret), K(result), K(table_store));
        }
      } else if (OB_FAIL(result.handle_.check_continues(nullptr))) {
        LOG_WARN("failed to check continues for major merge", K(ret), K(result));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(base_table)) {
    const int64_t major_snapshot = MAX(base_table->get_snapshot_version(), freeze_info.freeze_scn.get_val_for_tx());
    result.read_base_version_ = base_table->get_snapshot_version();
    result.version_range_.snapshot_version_ = major_snapshot;
    result.create_snapshot_version_ = base_table->get_meta().get_basic_meta().create_snapshot_version_;
    result.schema_version_ = freeze_info.schema_version;
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
    if (multi_version_start < result.version_range_.multi_version_start_) {
      LOG_WARN("cannot reserve multi_version_start", "old multi_version_start", result.version_range_.multi_version_start_,
               K(multi_version_start));
    } else if (multi_version_start < result.version_range_.snapshot_version_) {
      result.version_range_.multi_version_start_ = multi_version_start;
      LOG_TRACE("succ reserve multi_version_start", K(result.version_range_));
    } else {
      result.version_range_.multi_version_start_ = result.version_range_.snapshot_version_;
      LOG_TRACE("no need keep multi version", K(result.version_range_));
    }
  }
  return ret;
}

// may need rewrite for checkpoint_mgr
int ObPartitionMergePolicy::check_need_mini_merge(
    const storage::ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  bool can_merge = false;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObSEArray<ObITable *, MAX_MEMSTORE_CNT> memtables;
  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tablet is unexpectedly invalid", K(ret), K(tablet));
  } else if (OB_FAIL(tablet.get_memtables(memtables, false/*need_active*/))) {
    LOG_WARN("failed to get all memtables from table store", K(ret));
  } else if (!memtables.empty()) {
    ObSSTable *latest_sstable = static_cast<ObSSTable*>(get_latest_sstable(table_store));
    ObIMemtable *first_frozen_memtable = static_cast<ObIMemtable*>(memtables.at(0));
    ObIMemtable *last_frozen_memtable = static_cast<ObIMemtable*>(memtables.at(memtables.count() - 1));
    if (OB_NOT_NULL(first_frozen_memtable)) {
      need_merge = true;
      if (first_frozen_memtable->can_be_minor_merged()) {
        can_merge = true;
        if (OB_NOT_NULL(latest_sstable)
            && (latest_sstable->get_end_scn() >= last_frozen_memtable->get_end_scn()
                && tablet.get_snapshot_version() >= last_frozen_memtable->get_snapshot_version())) {
          need_merge = false;
          LOG_ERROR("unexpected sstable", K(ret), KPC(latest_sstable), KPC(last_frozen_memtable));
        }
      } else if (REACH_TENANT_TIME_INTERVAL(30 * 1000 * 1000)) {
        LOG_INFO("memtable can not minor merge",
                 "memtable end_scn", first_frozen_memtable->get_end_scn(),
                 "memtable timestamp", first_frozen_memtable->get_timestamp());

        const ObStorageSchema &storage_schema = tablet.get_storage_schema();
        ADD_SUSPECT_INFO(MINI_MERGE,
            ls_id, tablet_id,
            "memtable can not minor merge",
            "memtable end_scn",
            first_frozen_memtable->get_end_scn(),
            "memtable timestamp",
            first_frozen_memtable->get_timestamp());
      }
      if (need_merge && !check_table_count_safe(table_store)) { // check table_store count
        can_merge = false;
        LOG_ERROR("table count is not safe for mini merge", K(tablet_id));
        // add compaction diagnose info
        diagnose_table_count_unsafe(MINI_MERGE, tablet);
      }
#ifdef ERRSIM
      // TODO@hanhui: fix this errsim later
      ret = E(EventTable::EN_COMPACTION_DIAGNOSE_TABLE_STORE_UNSAFE_FAILED) ret;
      if (OB_FAIL(ret)) {
         ret = OB_SUCCESS;
         need_merge = false;
         diagnose_table_count_unsafe(MINI_MERGE, tablet); // ignore failure
         LOG_INFO("check table count with errsim", K(tablet_id));
      }
#endif
      if (need_merge && !can_merge) {
        need_merge = false;
        if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_INFO("check_need_mini_merge which cannot merge", K(tablet_id), K(need_merge), K(can_merge),
              K(latest_sstable), K(first_frozen_memtable));
        }
      }
    }
  }

  if (OB_SUCC(ret) && need_merge) {
    LOG_DEBUG("check mini merge", K(ls_id), "tablet_id", tablet_id.id(), K(need_merge),
              K(can_merge), K(table_store));
  }
  return ret;
}

int ObPartitionMergePolicy::check_need_mini_minor_merge(
    const ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  int64_t min_snapshot_version = 0;
  int64_t max_snapshot_version = 0;
  int64_t minor_sstable_count = 0;
  int64_t need_merge_mini_count = 0;
  need_merge = false;
  int64_t mini_minor_threshold = DEFAULT_MINOR_COMPACT_TRIGGER;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t delay_merge_schedule_interval = 0;
  ObTablesHandleArray minor_tables;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      mini_minor_threshold = tenant_config->minor_compact_trigger;
      delay_merge_schedule_interval = tenant_config->_minor_compaction_interval;
    }
  } // end of ObTenantConfigGuard
  if (table_store.get_minor_sstables().count_ <= mini_minor_threshold) {
    // total number of mini sstable is less than threshold + 1
  } else if (tablet.is_ls_tx_data_tablet()) {
    min_snapshot_version = 0;
    max_snapshot_version = INT64_MAX;
  } else if (OB_FAIL(get_boundary_snapshot_version(tablet, min_snapshot_version, max_snapshot_version))) {
    LOG_WARN("failed to calculate boundary version", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_store.get_mini_minor_sstables(minor_tables))) {
    LOG_WARN("failed to get mini minor sstables", K(ret), K(table_store));
  } else {
    int64_t minor_check_snapshot_version = 0;
    bool found_greater = false;

    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.get_count(); ++i) {
      ObSSTable *table = static_cast<ObSSTable *>(minor_tables.get_table(i));
      if (OB_UNLIKELY(table->is_buf_minor_sstable())) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Unexpected buf minor sstable", K(ret), K(table_store));
      } else if (!found_greater && table->get_upper_trans_version() <= min_snapshot_version) {
        continue;
      } else if (table->get_max_merged_trans_version() > max_snapshot_version) {
        break;
      }
      found_greater = true;
      minor_sstable_count++;
      if (table->is_mini_sstable()) {
        if (mini_minor_threshold == need_merge_mini_count++) {
          minor_check_snapshot_version = table->get_max_merged_trans_version();
        }
      } else if (need_merge_mini_count > 0 || minor_sstable_count - need_merge_mini_count > 1) {
        // chaos order with mini and minor sstable OR more than one minor sstable
        // need compaction except data replica
        need_merge_mini_count = mini_minor_threshold + 1;
        break;
      }
    } // end of for

    if (OB_SUCC(ret)) {
      // GCONF.minor_compact_trigger means the maximum number of the current L0 sstable,
      // the compaction will be scheduled when it be exceeded
      // If minor_compact_trigger = 0, it means that all L0 sstables should be merged into L1 as soon as possible
      if (minor_sstable_count <= 1) {
        // only one minor sstable exist, no need to do mini minor merge
      } else if (table_store.get_table_count() >= MAX_SSTABLE_CNT_IN_STORAGE - RESERVED_STORE_CNT_IN_STORAGE) {
        need_merge = true;
        LOG_INFO("table store has too many sstables, need to compaction", K(table_store));
      } else if (need_merge_mini_count <= mini_minor_threshold) {
        // no need merge
      } else {
        if (delay_merge_schedule_interval > 0 && minor_check_snapshot_version > 0) {
          // delays the compaction scheduling
          int64_t current_time = ObTimeUtility::current_time();
          if (minor_check_snapshot_version + delay_merge_schedule_interval < current_time) {
            // need merge
            need_merge = true;
          }
        } else {
          need_merge = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_merge) {
    LOG_DEBUG("check mini minor merge", "ls_id", tablet.get_tablet_meta().ls_id_,
             K(tablet_id), K(need_merge), K(table_store));
  }
  if (OB_SUCC(ret) && !need_merge && table_store.get_minor_sstables().count() >= DIAGNOSE_TABLE_CNT_IN_STORAGE) {
    ADD_SUSPECT_INFO(MINOR_MERGE,
                     tablet.get_tablet_meta().ls_id_, tablet_id,
                     "can't schedule minor merge",
                     K(min_snapshot_version), K(max_snapshot_version), K(need_merge_mini_count),
                     K(minor_sstable_count), "mini_sstable_cnt", table_store.get_minor_sstables().count());
  }
  return ret;
}

int ObPartitionMergePolicy::check_need_buf_minor_merge(
    const ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_NO_NEED_MERGE;
  need_merge = false;
  UNUSED(tablet);
  return ret;
}

int ObPartitionMergePolicy::check_need_hist_minor_merge(
    const storage::ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  const int64_t hist_threashold = cal_hist_minor_merge_threshold();
  int64_t max_snapshot_version = 0;
  need_merge = false;
  if (OB_FAIL(deal_hist_minor_merge(tablet, max_snapshot_version))) {
=======
  } else if (OB_FAIL(find_minor_merge_tables(param, 0/*min_snapshot*/,
      max_snapshot_version, ls, tablet, result))) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get minor tables for hist minor merge", K(ret));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::deal_hist_minor_merge(
    const ObTablet &tablet,
    int64_t &max_snapshot_version)
{
  int ret = OB_SUCCESS;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const int64_t hist_threshold = cal_hist_minor_merge_threshold();
  ObITable *first_major_table = nullptr;
  max_snapshot_version = 0;

  if (!table_store.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected invalid table store", K(ret), K(table_store));
  } else if (table_store.get_minor_sstables().count_ < hist_threshold) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_ISNULL(first_major_table = table_store.get_major_sstables().get_boundary_table(false))) {
    // index table during building, need compat with continuous multi version
<<<<<<< HEAD
    if (0 == (max_snapshot_version = freeze_info_mgr->get_latest_frozen_scn().get_val_for_tx())) {
=======
    if (0 == (max_snapshot_version = MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version())) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
      // no freeze info found, wait normal mini minor to free sstable
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No freeze range to do hist minor merge for buiding index", K(ret), K(table_store));
    }
  } else {
    ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
    ObSEArray<ObTenantFreezeInfoMgr::FreezeInfo, 8> freeze_infos;
    if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_behind_major_snapshot(
                first_major_table->get_snapshot_version(),
                freeze_infos))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_NO_NEED_MERGE;
      } else {
        LOG_WARN("Failed to get freeze infos behind major version", K(ret), KPC(first_major_table));
      }
    } else if (freeze_infos.count() <= 1) {
      // only one major freeze found, wait normal mini minor to reduce table count
      ret = OB_NO_NEED_MERGE;
      LOG_DEBUG("No enough freeze range to do hist minor merge", K(ret), K(freeze_infos));
    } else {
      int64_t table_cnt = 0;
      int64_t min_minor_version = 0;
      int64_t max_minor_version = 0;
      if (OB_FAIL(get_boundary_snapshot_version(tablet, min_minor_version, max_minor_version))) {
        LOG_WARN("fail to calculate boundary version", K(ret));
      } else {
        ObSSTable *table = nullptr;
        const ObSSTableArray &minor_tables = table_store.get_minor_sstables();
        for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count_; ++i) {
          if (OB_ISNULL(table = static_cast<ObSSTable*>(minor_tables[i]))) {
            ret = OB_ERR_SYS;
            LOG_ERROR("table must not null", K(ret), K(i), K(table_store));
          } else if (table->get_upper_trans_version() <= min_minor_version) {
            table_cnt++;
          } else {
            break;
          }
        }

        if (OB_SUCC(ret)) {
          if (1 < table_cnt) {
            max_snapshot_version = min_minor_version;
          } else {
            ret = OB_NO_NEED_MERGE;
          }
        }
      }
    }
  }
  return ret;
}

<<<<<<< HEAD
int ObPartitionMergePolicy::check_need_major_merge(
    const storage::ObTablet &tablet,
    int64_t &merge_version,
    bool &need_merge,
    bool &can_merge,
    bool &need_force_freeze)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  can_merge = false;
  need_force_freeze = false;
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  int64_t last_sstable_snapshot = tablet.get_snapshot_version();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  int64_t major_sstable_version = 0;
  bool is_tablet_data_status_complete = true;   //ha status

  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tablet is unexpectedly invalid", K(ret), K(tablet));
  } else {
    ObSSTable *latest_major_sstable = static_cast<ObSSTable*>(table_store.get_major_sstables().get_boundary_table(true/*last*/));
    if (OB_NOT_NULL(latest_major_sstable)) {
      major_sstable_version = latest_major_sstable->get_snapshot_version();
      if (major_sstable_version < merge_version) {
        need_merge = true;
      }
    }
    if (need_merge) {
      if (!tablet.get_tablet_meta().ha_status_.is_data_status_complete()) {
        can_merge = false;
        is_tablet_data_status_complete = false;
        LOG_INFO("tablet data status incomplete, can not merge", K(ret), K(tablet_id));
      } else if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_behind_snapshot_version, major_sstable_version, freeze_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get freeze info", K(ret), K(merge_version), K(major_sstable_version));
        } else {
          can_merge = false;
          ret = OB_SUCCESS;
          LOG_INFO("can't get freeze info after snapshot", K(ret), K(merge_version), K(major_sstable_version));
        }
      } else {
        can_merge = last_sstable_snapshot >= freeze_info.freeze_scn.get_val_for_tx();
        if (!can_merge) {
          LOG_TRACE("tablet need merge, but cannot merge now", K(tablet_id), K(merge_version), K(last_sstable_snapshot), K(freeze_info));
        }
      }

      if (OB_SUCC(ret) && !can_merge && is_tablet_data_status_complete) {
        ObTabletMemtableMgr *memtable_mgr = nullptr;
        memtable::ObMemtable *last_frozen_memtable = nullptr;
        if (OB_ISNULL(memtable_mgr = static_cast<ObTabletMemtableMgr *>(tablet.get_memtable_mgr()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable mgr is unexpected null", K(ret), K(tablet));
        } else if (OB_ISNULL(last_frozen_memtable = memtable_mgr->get_last_frozen_memtable())) {
          // no frozen memtable, need force freeze
          need_force_freeze = true;
        } else {
          need_force_freeze = last_frozen_memtable->get_snapshot_version() < freeze_info.freeze_scn.get_val_for_tx();
          if (!need_force_freeze) {
            FLOG_INFO("tablet no need force freeze", K(ret), K(tablet_id), K(merge_version), K(freeze_info), KPC(last_frozen_memtable));
          }
        }
      }
    }
    if (need_merge && !can_merge && REACH_TENANT_TIME_INTERVAL(60L * 1000L * 1000L)) {
      LOG_INFO("check_need_major_merge", K(ret), "ls_id", tablet.get_tablet_meta().ls_id_, K(tablet_id),
               K(need_merge), K(can_merge), K(need_force_freeze), K(merge_version), K(freeze_info),
               K(is_tablet_data_status_complete));
      const ObStorageSchema &storage_schema = tablet.get_storage_schema();
      ADD_SUSPECT_INFO(MAJOR_MERGE, tablet.get_tablet_meta().ls_id_, tablet_id,
                       "need major merge but can't merge now",
                       K(merge_version), K(freeze_info), K(last_sstable_snapshot), K(need_force_freeze),
                       K(is_tablet_data_status_complete));
    }
  }
  return ret;
}

=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
int ObPartitionMergePolicy::diagnose_minor_dag(
    ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObTabletMergeExecuteDag dag;
  ObDiagnoseTabletCompProgress progress;
  if (OB_FAIL(ObCompactionDiagnoseMgr::diagnose_dag(
          merge_type,
          ls_id,
          tablet_id,
          ObVersion::MIN_VERSION,
          dag,
          progress))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to init dag", K(ret), K(ls_id), K(tablet_id));
    } else {
      // no minor merge dag
      ret = OB_SUCCESS;
    }
  } else if (progress.is_valid()) { // dag exist
    ADD_COMPACTION_INFO_PARAM(buf, buf_len, "minor_merge_progress", progress);
  }
  return ret;
}

int ObPartitionMergePolicy::diagnose_table_count_unsafe(
    const ObMergeType merge_type,
    const storage::ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t buf_len = ObScheduleSuspectInfoMgr::EXTRA_INFO_LEN;
  char tmp_str[buf_len] = "\0";
  const ObStorageSchema &storage_schema = tablet.get_storage_schema();
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const ObSSTableArray &major_tables = table_store.get_major_sstables();
  const ObSSTableArray &minor_tables = table_store.get_minor_sstables();

  // add sstable info
  const int64_t major_table_count = major_tables.count_;
  const int64_t minor_table_count = minor_tables.count_;
  ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, K(major_table_count), K(minor_table_count));

  if (OB_TMP_FAIL(ObCompactionDiagnoseMgr::check_system_compaction_config(tmp_str, buf_len))) {
    LOG_WARN("failed to check system compaction config", K(tmp_ret), K(tmp_str));
  }

  // check min_reserved_snapshot
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t min_reserved_snapshot = 0;
  int64_t min_merged_snapshot = INT64_MAX;
  ObString snapshot_from_str;
  const ObSSTable *major_sstable = static_cast<const ObSSTable *>(major_tables.get_boundary_table(false/*last*/));
  if (OB_ISNULL(major_sstable)) {
    const ObSSTable *minor_sstable = static_cast<const ObSSTable *>(minor_tables.get_boundary_table(false/*last*/));
    ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, "no major sstable. first_minor_start_scn = ", minor_sstable->get_start_scn());
  } else if (FALSE_IT(min_merged_snapshot = major_sstable->get_snapshot_version())) {
  } else if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(diagnose_min_reserved_snapshot,
      tablet_id,
      min_merged_snapshot,
      min_reserved_snapshot,
      snapshot_from_str))) {
    LOG_WARN("failed to get min reserved snapshot", K(ret), K(tablet_id));
  } else if (snapshot_from_str.length() > 0) {
    ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, "snapstho_type", snapshot_from_str, K(min_reserved_snapshot));
  }

  // check have minor merge DAG
  if (OB_TMP_FAIL(diagnose_minor_dag(MINOR_MERGE, ls_id, tablet_id, tmp_str, buf_len))) {
    LOG_WARN("failed to diagnose minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(tmp_str));
  }
  if (OB_TMP_FAIL(diagnose_minor_dag(HISTORY_MINOR_MERGE, ls_id, tablet_id, tmp_str, buf_len))) {
    LOG_WARN("failed to diagnose history minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(tmp_str));
  }

  // add suspect info
  ADD_SUSPECT_INFO(merge_type, ls_id, tablet_id,
      "sstable count is not safe", "extra_info", tmp_str);
  return ret;
}

int ObPartitionMergePolicy::refine_mini_merge_result(
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObITable *last_table = nullptr;
  const ObTabletTableStore &table_store = tablet.get_table_store();

  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table store not valid", K(ret), K(table_store));
  } else if (OB_ISNULL(last_table = table_store.get_minor_sstables().get_boundary_table(true/*last*/))) {
    // no minor sstable, skip to cut memtable's boundary
  } else if (result.scn_range_.start_scn_ > last_table->get_end_scn()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected uncontinuous scn_range in mini merge",
              K(ret), K(result), KPC(last_table), K(table_store), K(tablet));
  } else if (result.scn_range_.start_scn_ < last_table->get_end_scn()
      && !tablet.get_tablet_meta().tablet_id_.is_special_merge_tablet()) {
    // fix start_scn to make scn_range continuous in migrate phase for issue 42832934
    if (result.scn_range_.end_scn_ <= last_table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("No need mini merge memtable which is covered by existing sstable",
               K(ret), K(result), KPC(last_table), K(table_store), K(tablet));
    } else {
      result.scn_range_.start_scn_ = last_table->get_end_scn();
      FLOG_INFO("Fix mini merge result scn range", K(ret), K(result), KPC(last_table), K(table_store), K(tablet));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::refine_minor_merge_result(
    const int64_t minor_compact_trigger,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObMergeType &merge_type = result.suggest_merge_type_;
  if (result.handle_.get_count() <= MAX(minor_compact_trigger, 1)) {
    ret = OB_NO_NEED_MERGE;
    LOG_DEBUG("minor refine, no need to do minor merge", K(result));
    result.handle_.reset();
  } else if (OB_UNLIKELY(!is_minor_merge_type(merge_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected merge type to refine merge tables", K(result), K(ret));
  } else if (0 == minor_compact_trigger || result.handle_.get_count() >= OB_UNSAFE_TABLE_CNT) {
    // no refine
  } else {
    ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> mini_tables;
    ObITable *table = NULL;
    ObSSTable *sstable = NULL;
    int64_t large_sstable_cnt = 0;
    int64_t large_sstable_row_cnt = 0;
    int64_t mini_sstable_row_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      if (OB_ISNULL(table = result.handle_.get_table(i)) || !table->is_minor_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("get unexpected table", KP(table), K(ret));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable*>(table))) {
      } else {
        if (sstable->get_meta().get_basic_meta().row_count_ > OB_LARGE_MINOR_SSTABLE_ROW_COUNT) { // large sstable
          ++large_sstable_cnt;
          large_sstable_row_cnt += sstable->get_meta().get_basic_meta().row_count_;
          if (mini_tables.count() > minor_compact_trigger) {
            break;
          } else {
            mini_tables.reset();
            continue;
          }
        } else {
          mini_sstable_row_cnt += sstable->get_meta().get_basic_meta().row_count_;
        }
        if (OB_FAIL(mini_tables.push_back(table))) {
          LOG_WARN("Failed to push mini minor table into array", K(ret));
        }
      }
    } // end of for

    int64_t size_amplification_factor = OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        size_amplification_factor = tenant_config->_minor_compaction_amplification_factor;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (large_sstable_cnt > 1
        || mini_tables.count() <= minor_compact_trigger
        || mini_sstable_row_cnt > (large_sstable_row_cnt * size_amplification_factor / 100)) {
      // no refine, use current result to compaction
    } else if (mini_tables.count() != result.handle_.get_count()) {
      // reset the merge result, mini sstable merge into a new mini sstable
      result.reset_handle_and_range();
      for (int64_t i = 0; OB_SUCC(ret) && i < mini_tables.count(); i++) {
        ObITable *table = mini_tables.at(i);
        if (OB_UNLIKELY(0 != i && table->get_start_scn() != result.scn_range_.end_scn_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexepcted table array", K(ret), K(i), KPC(table), K(mini_tables));
        } else if (OB_FAIL(result.handle_.add_table(table))) {
          LOG_WARN("Failed to add table to minor merge result", KPC(table), K(ret));
        } else {
          if (1 == result.handle_.get_count()) {
            result.scn_range_.start_scn_ = table->get_start_scn();
          }
          result.scn_range_.end_scn_ = table->get_end_scn();
        }
<<<<<<< HEAD
      } // end of ObTenantConfigGuard
      if (1 == result.handle_.get_count()) {
        LOG_INFO("minor refine, only one sstable, no need to do mini minor merge", K(result));
        result.handle_.reset();
      } else if (HISTORY_MINI_MINOR_MERGE == merge_type) {
        // use minor merge to do history mini minor merge and skip other checks
      } else if (0 == minor_compact_trigger || mini_tables.count() <= 1 || minor_sstable_count > 1) {
        merge_type = MINOR_MERGE;
      } else if (minor_sstable_count == 0 && mini_sstable_size > OB_MIN_MINOR_SSTABLE_ROW_COUNT) {
        merge_type = MINOR_MERGE;
        LOG_INFO("minor refine, mini minor merge sstable refine to minor merge",
                 K(minor_sstable_size), K(mini_sstable_size), K(OB_MIN_MINOR_SSTABLE_ROW_COUNT), K(result));
      } else if (minor_sstable_count == 1
                 && mini_sstable_size > (minor_sstable_size * size_amplification_factor / 100)) {
        merge_type = MINOR_MERGE;
        LOG_INFO("minor refine, mini minor merge sstable refine to minor merge", K(minor_sstable_size),
                 K(mini_sstable_size), K(size_amplification_factor), K(result));
      } else {
        // reset the merge result, mini sstable merge into a new mini sstable
        result.reset_handle_and_range();
        for (int64_t i = 0; OB_SUCC(ret) && i < mini_tables.count(); i++) {
          ObITable *table = mini_tables.at(i);
          if (OB_FAIL(result.handle_.add_table(table))) {
            LOG_WARN("Failed to add table to minor merge result", KPC(table), K(ret));
          } else {
            if (1 == result.handle_.get_count()) {
              result.scn_range_.start_scn_ = table->get_start_scn();
            }
            result.scn_range_.end_scn_ = table->get_end_scn();
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("minor refine, mini minor merge sstable refine info", K(minor_sstable_size),
                   K(mini_sstable_size), K(result));
        }
=======
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("minor refine, mini minor merge sstable refine info", K(result));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
      }
    }
  }
  return ret;
}

// call this func means have serialized medium compaction clog = medium_snapshot
int ObPartitionMergePolicy::check_need_medium_merge(
    ObLS &ls,
    storage::ObTablet &tablet,
    const int64_t medium_snapshot,
    bool &need_merge,
    bool &can_merge,
    bool &need_force_freeze)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  can_merge = false;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTabletTableStore &table_store = tablet.get_table_store();
  ObITable *last_major = table_store.get_major_sstables().get_boundary_table(true/*last*/);
  const bool is_tablet_data_status_complete = tablet.get_tablet_meta().ha_status_.is_data_status_complete();
  if (nullptr == last_major) {
    // no major, no medium
  } else {
    need_merge = last_major->get_snapshot_version() < medium_snapshot;
    if (need_merge
        && is_tablet_data_status_complete
        && ObTenantTabletScheduler::check_weak_read_ts_ready(medium_snapshot, ls)) {
      can_merge = tablet.get_snapshot_version() >= medium_snapshot;
      if (!can_merge) {
        ObTabletMemtableMgr *memtable_mgr = nullptr;
        ObTableHandleV2 memtable_handle;
        memtable::ObMemtable *last_frozen_memtable = nullptr;
        if (OB_ISNULL(memtable_mgr = static_cast<ObTabletMemtableMgr *>(tablet.get_memtable_mgr()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable mgr is unexpected null", K(ret), K(tablet));
        } else if (OB_FAIL(memtable_mgr->get_last_frozen_memtable(memtable_handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) { // no frozen memtable, need force freeze
            need_force_freeze = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get last frozen memtable", K(ret));
          }
        } else if (OB_FAIL(memtable_handle.get_data_memtable(last_frozen_memtable))) {
          LOG_WARN("failed to get last frozen memtable", K(ret));
        } else {
          need_force_freeze = last_frozen_memtable->get_snapshot_version() < medium_snapshot;
          if (!need_force_freeze) {
            LOG_INFO("tablet no need force freeze", K(ret), K(tablet_id), K(medium_snapshot), KPC(last_frozen_memtable));
          }
        }
      }
    }
  }

  if (need_merge && !can_merge && REACH_TENANT_TIME_INTERVAL(60L * 1000L * 1000L)) {
    LOG_INFO("check_need_medium_merge", K(ret), "ls_id", tablet.get_tablet_meta().ls_id_, K(tablet_id),
             K(need_merge), K(can_merge), K(medium_snapshot), K(is_tablet_data_status_complete));
    ADD_SUSPECT_INFO(MAJOR_MERGE, tablet.get_tablet_meta().ls_id_, tablet_id,
                     "need major merge but can't merge now",
                     K(medium_snapshot), K(is_tablet_data_status_complete), K(need_force_freeze),
                     "max_serialized_medium_scn", tablet.get_tablet_meta().max_serialized_medium_scn_);
  }
  return ret;
}

int64_t ObPartitionMergePolicy::cal_hist_minor_merge_threshold()
{
  int64_t compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    compact_trigger = tenant_config->minor_compact_trigger;
  }
  return MIN((1 + compact_trigger) * OB_HIST_MINOR_FACTOR, MAX_TABLE_CNT_IN_STORAGE / 2);
}

int ObPartitionMergePolicy::get_multi_version_start(
    const ObMergeType merge_type,
    ObLS &ls,
    const ObTablet &tablet,
    ObVersionRange &result_version_range)
{
  int ret = OB_SUCCESS;
  int64_t expect_multi_version_start = 0;
  if (tablet.is_ls_inner_tablet()) {
    result_version_range.multi_version_start_ = INT64_MAX;
  } else if (OB_FAIL(ObTablet::get_kept_multi_version_start(ls, tablet, expect_multi_version_start))) {
    if (is_mini_merge(merge_type) || OB_TENANT_NOT_EXIST == ret) {
      expect_multi_version_start = tablet.get_multi_version_start();
      FLOG_INFO("failed to get multi_version_start, use multi_version_start on tablet", K(ret),
          K(merge_type), K(expect_multi_version_start));
      ret = OB_SUCCESS; // clear errno
    } else {
      LOG_WARN("failed to get kept multi_version_start", K(ret),
          "tablet_id", tablet.get_tablet_meta().tablet_id_);
    }
  }
  if (OB_SUCC(ret) && !tablet.is_ls_inner_tablet()) {
    // update multi_version_start
    if (expect_multi_version_start < result_version_range.multi_version_start_) {
      LOG_WARN("cannot reserve multi_version_start", "multi_version_start", result_version_range.multi_version_start_,
               K(expect_multi_version_start));
    } else if (expect_multi_version_start < result_version_range.snapshot_version_) {
      result_version_range.multi_version_start_ = expect_multi_version_start;
      LOG_DEBUG("succ reserve multi_version_start", "multi_version_start", result_version_range.multi_version_start_,
                K(expect_multi_version_start));
    } else {
      result_version_range.multi_version_start_ = result_version_range.snapshot_version_;
      LOG_DEBUG("no need keep multi version", "multi_version_start", result_version_range.multi_version_start_,
                K(expect_multi_version_start));
    }
  }
  return ret;
}


int add_table_with_check(ObGetMergeTablesResult &result, ObITable *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table));
  } else if (OB_UNLIKELY(!result.handle_.empty()
      && table->get_start_scn() > result.scn_range_.end_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log ts range is not continues", K(ret), K(result), KPC(table));
  } else if (OB_FAIL(result.handle_.add_table(table))) {
    LOG_WARN("failed to add table", K(ret), KPC(table));
  } else {
    if (1 == result.handle_.get_count()) {
      result.scn_range_.start_scn_ = table->get_start_scn();
    }
    result.scn_range_.end_scn_ = table->get_end_scn();
  }
  return ret;
}

int ObPartitionMergePolicy::generate_input_result_array(
    const ObGetMergeTablesResult &input_result,
    ObMinorExecuteRangeMgr &minor_range_mgr,
    int64_t &fixed_input_table_cnt,
    ObIArray<ObGetMergeTablesResult> &input_result_array)
{
  int ret = OB_SUCCESS;
  fixed_input_table_cnt = 0;
  input_result_array.reset();
  ObGetMergeTablesResult tmp_result;

  if (minor_range_mgr.exe_range_array_.empty()) {
    if (OB_FAIL(input_result_array.push_back(input_result))) {
      LOG_WARN("failed to add input result", K(ret), K(input_result));
    } else {
      fixed_input_table_cnt += input_result.handle_.get_count();
    }
  } else if (OB_FAIL(tmp_result.copy_basic_info(input_result))) {
    LOG_WARN("failed to copy basic info", K(ret), K(input_result));
  } else {
    const ObIArray<ObITable *> &table_array = input_result.handle_.get_tables();
    ObITable *table = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_array.count(); ++idx) {
      if (OB_ISNULL(table = table_array.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is unexpected null", K(ret), K(idx), K(table_array));
      } else if (minor_range_mgr.in_execute_range(table)) {
        if (tmp_result.handle_.get_count() < 2) {
        } else if (OB_FAIL(input_result_array.push_back(tmp_result))) {
          LOG_WARN("failed to add tmp result", K(ret), K(tmp_result));
        } else {
          fixed_input_table_cnt += tmp_result.handle_.get_count();
        }
        tmp_result.handle_.reset();
        tmp_result.scn_range_.reset();
      } else if (OB_FAIL(add_table_with_check(tmp_result, table))) {
        LOG_WARN("failed to add table into result", K(ret), K(tmp_result), KPC(table));
      }
    }

    if (OB_FAIL(ret) || tmp_result.handle_.get_count() < 2) {
    } else if (OB_FAIL(input_result_array.push_back(tmp_result))) {
      LOG_WARN("failed to add tmp result", K(ret), K(tmp_result));
    } else {
      fixed_input_table_cnt += tmp_result.handle_.get_count();
    }
  }
  return ret;
}

int ObPartitionMergePolicy::split_parallel_minor_range(
    const int64_t table_count_threshold,
    const ObGetMergeTablesResult &input_result,
    ObIArray<ObGetMergeTablesResult> &parallel_result)
{
  int ret = OB_SUCCESS;
  const int64_t input_table_cnt = input_result.handle_.get_count();
  ObGetMergeTablesResult tmp_result;
  if (input_table_cnt < table_count_threshold) {
    // if there are no running minor dags, then the input_table_cnt must be greater than threshold.
  } else if (input_table_cnt < OB_MINOR_PARALLEL_SSTABLE_CNT_TRIGGER) {
    if (OB_FAIL(parallel_result.push_back(input_result))) {
      LOG_WARN("failed to add input result", K(ret), K(input_result));
    }
  } else if (OB_FAIL(tmp_result.copy_basic_info(input_result))) {
    LOG_WARN("failed to copy basic info", K(ret), K(input_result));
  } else {
    const int64_t parallel_dag_cnt = MAX(1, input_table_cnt / OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG);
    const int64_t parallel_table_cnt = input_table_cnt / parallel_dag_cnt;
    const ObIArray<ObITable *> &table_array = input_result.handle_.get_tables();
    ObITable *table = nullptr;

    int64_t start = 0;
    int64_t end = 0;
    for (int64_t seq = 0; OB_SUCC(ret) && seq < parallel_dag_cnt; ++seq) {
      start = parallel_table_cnt * seq;
      end = (parallel_dag_cnt - 1 == seq) ? table_array.count() : end + parallel_table_cnt;
      for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
        if (OB_ISNULL(table = table_array.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table is unexpected null", K(ret), K(i), K(table_array));
        } else if (OB_FAIL(add_table_with_check(tmp_result, table))) {
          LOG_WARN("failed to add table into result", K(ret), K(tmp_result), KPC(table));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(parallel_result.push_back(tmp_result))) {
        LOG_WARN("failed to add tmp result", K(ret), K(tmp_result));
      } else {
        LOG_DEBUG("success to push result", K(ret), K(tmp_result), K(parallel_result));
        tmp_result.handle_.reset();
        tmp_result.scn_range_.reset();
      }
    }
  }
  return ret;
}

int ObPartitionMergePolicy::generate_parallel_minor_interval(
    const int64_t minor_compact_trigger,
    const ObGetMergeTablesResult &input_result,
    ObMinorExecuteRangeMgr &minor_range_mgr,
    ObIArray<ObGetMergeTablesResult> &parallel_result)
{
  int ret = OB_SUCCESS;
  parallel_result.reset();
  ObSEArray<ObGetMergeTablesResult, 2> input_result_array;
  int64_t fixed_input_table_cnt = 0;

  if (!storage::is_minor_merge(input_result.suggest_merge_type_)) {
    ret = OB_NO_NEED_MERGE;
  } else if (input_result.handle_.get_count() < minor_compact_trigger) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(generate_input_result_array(input_result, minor_range_mgr, fixed_input_table_cnt, input_result_array))) {
    LOG_WARN("failed to generate input result into array", K(ret), K(input_result));
  } else if (fixed_input_table_cnt < minor_compact_trigger) {
    // the quantity of table that should be merged is smaller than trigger, wait for the existing minor tasks to finish.
    ret = OB_NO_NEED_MERGE;
  }

  /*
   * When existing minor dag, we should ensure that the quantity of tables per parallel dag is a reasonable value:
   * 1. If compact_trigger is small, minor merge should be easier to schedule, we should lower the threshold;
   * 2. If compact_trigger is big, we should upper the threshold to prevent the creation of dag frequently.
   */
  int64_t exist_dag_cnt = minor_range_mgr.exe_range_array_.count();
  int64_t table_count_threshold = (0 == exist_dag_cnt)
                                ? minor_compact_trigger
                                : OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG + (OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG / 2) * (exist_dag_cnt - 1);
  for (int64_t i = 0; OB_SUCC(ret) && i < input_result_array.count(); ++i) {
    if (OB_FAIL(split_parallel_minor_range(table_count_threshold, input_result_array.at(i), parallel_result))) {
      LOG_WARN("failed to split parallel minor range", K(ret), K(input_result_array.at(i)));
    }
  }
  return ret;
}


/*************************************** ObMinorExecuteRangeMgr ***************************************/
bool compareScnRange(share::ObScnRange &a, share::ObScnRange &b)
{
  return a.end_scn_ < b.end_scn_;
}

int ObMinorExecuteRangeMgr::get_merge_ranges(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletMergeDagParam param;
  param.merge_type_ = MINOR_MERGE;
  param.merge_version_ = ObVersion::MIN_VERSION;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.for_diagnose_ = true;

  if (OB_FAIL(MTL(ObTenantDagScheduler*)->get_minor_exe_dag_info(param, exe_range_array_))) {
    LOG_WARN("failed to get minor exe dag info", K(ret));
  } else if (OB_FAIL(sort_ranges())) {
    LOG_WARN("failed to sort ranges", K(ret), K(param));
  }
  return ret;
}

int ObMinorExecuteRangeMgr::sort_ranges()
{
  int ret = OB_SUCCESS;
  std::sort(exe_range_array_.begin(), exe_range_array_.end(), compareScnRange);
  for (int i = 1; OB_SUCC(ret) && i < exe_range_array_.count(); ++i) {
    if (OB_UNLIKELY(!exe_range_array_.at(i).is_valid()
        || (exe_range_array_.at(i - 1).start_scn_.get_val_for_tx() > 0 // except meta major merge range
            && exe_range_array_.at(i).start_scn_.get_val_for_tx() > 0
            && exe_range_array_.at(i).start_scn_ < exe_range_array_.at(i - 1).end_scn_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected minor ranges", K(ret), K(i), K(exe_range_array_));
    }
  }
  return ret;
}

bool ObMinorExecuteRangeMgr::in_execute_range(const ObITable *table) const
{
  bool bret = false;
  if (exe_range_array_.count() > 0 && OB_NOT_NULL(table)) {
    for (int i = 0; i < exe_range_array_.count(); ++i) {
      if (table->get_end_scn() <= exe_range_array_.at(i).end_scn_
          && table->get_end_scn() > exe_range_array_.at(i).start_scn_) {
        bret = true;
        LOG_DEBUG("in execute range", KPC(table), K(i), K(exe_range_array_.at(i)));
        break;
      }
    }
  }
  return bret;
}


/*************************************** ObAdaptiveMergePolicy ***************************************/
const char * ObAdaptiveMergeReasonStr[] = {
  "NONE",
  "LOAD_DATA_SCENE",
  "TOMBSTONE_SCENE",
  "INEFFICIENT_QUERY",
  "FREQUENT_WRITE"
};

const char* ObAdaptiveMergePolicy::merge_reason_to_str(const int64_t merge_reason)
{
  STATIC_ASSERT(static_cast<int64_t>(INVALID_REASON) == ARRAYSIZEOF(ObAdaptiveMergeReasonStr),
                "adaptive merge reason str len is mismatch");
  const char *str = "";
  if (merge_reason >= INVALID_REASON || merge_reason < NONE) {
    str = "invalid_merge_reason";
  } else {
    str = ObAdaptiveMergeReasonStr[merge_reason];
  }
  return str;
}

bool ObAdaptiveMergePolicy::is_valid_merge_reason(const AdaptiveMergeReason &reason)
{
  return reason > AdaptiveMergeReason::NONE &&
         reason < AdaptiveMergeReason::INVALID_REASON;
}

int ObAdaptiveMergePolicy::get_meta_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const ObStorageSchema &storage_schema = tablet.get_storage_schema();
  result.reset();

  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(table_store));
  } else if (OB_UNLIKELY(META_MAJOR_MERGE != merge_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_FAIL(find_meta_major_tables(tablet, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("Failed to find minor merge tables", K(ret));
    }
  } else if (OB_FAIL(result.handle_.check_continues(nullptr))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (FALSE_IT(result.schema_version_ = storage_schema.schema_version_)) {
  } else if (FALSE_IT(result.suggest_merge_type_ = META_MAJOR_MERGE)) {
  } else if (FALSE_IT(result.version_range_.snapshot_version_ =
      MIN(tablet.get_snapshot_version(), result.version_range_.snapshot_version_))) {
    // choose version should less than tablet::snapshot
  } else if (OB_FAIL(ObPartitionMergePolicy::get_multi_version_start(
      param.merge_type_, ls, tablet, result.version_range_))) {
    LOG_WARN("failed to get multi version_start", K(ret));
  } else if (OB_FAIL(result.handle_.get_table(0)->get_frozen_schema_version(result.base_schema_version_))) {
    LOG_WARN("failed to get frozen schema version", K(ret), K(result));
  } else {
    FLOG_INFO("succeed to get meta major merge tables", K(result), K(table_store));
  }
  return ret;
}

int ObAdaptiveMergePolicy::find_meta_major_tables(
    const storage::ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  int64_t min_snapshot = 0;
  int64_t max_snapshot = 0;
  int64_t base_row_cnt = 0;
  int64_t inc_row_cnt = 0;
  int64_t tx_determ_table_cnt = 0;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObITable *last_major = table_store.get_major_sstables().get_boundary_table(true);
  ObITable *last_minor = table_store.get_minor_sstables().get_boundary_table(true);
  ObITable *base_table = table_store.get_extend_sstable(ObTabletTableStore::META_MAJOR);
  const ObSSTableArray &minor_tables = table_store.get_minor_sstables();

  if (!table_store.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletTableStore is not valid", K(ret), K(table_store));
  } else if (nullptr == last_minor || nullptr == last_major) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("no minor/major sstable to do meta major merge", K(ret), KPC(last_minor), KPC(last_major));
  } else if (OB_FAIL(ObPartitionMergePolicy::get_boundary_snapshot_version(
      tablet, min_snapshot, max_snapshot, false/*check_table_cnt*/))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("Failed to find meta merge base table", K(ret), KPC(last_major), KPC(last_major), KPC(base_table));
    }
  } else if (FALSE_IT(base_table = nullptr == base_table ? last_major : base_table)) {
  } else if (base_table->get_snapshot_version() < min_snapshot || max_snapshot != INT64_MAX) {
    // max_snapshot == INT64_MAX means there's no next freeze_info
    ret = OB_NO_NEED_MERGE;
    LOG_DEBUG("no need meta merge when the tablet is doing major merge", K(ret), K(min_snapshot), K(max_snapshot), KPC(base_table));
  } else if (OB_FAIL(add_meta_merge_result(base_table, result, true/*update_snapshot*/))) {
    LOG_WARN("failed to add base table to meta merge result", K(ret), KPC(base_table), K(result));
  } else {
    ++tx_determ_table_cnt; // inc for base_table
    bool found_undeterm_table = false;
    base_row_cnt = static_cast<ObSSTable *>(base_table)->get_meta().get_row_count();
    ObITable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      if (OB_ISNULL(table = minor_tables[i]) || !table->is_multi_version_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected table", K(ret), K(i), K(table_store));
      } else if (table->get_upper_trans_version() <= base_table->get_snapshot_version()) {
        // skip minor sstable which has been merged
        continue;
      } else if (!found_undeterm_table && table->is_trans_state_deterministic()) {
        ++tx_determ_table_cnt;
        inc_row_cnt += static_cast<ObSSTable *>(table)->get_meta().get_row_count();
      } else {
        found_undeterm_table = true;
      }

      if (FAILEDx(add_meta_merge_result(table, result, !found_undeterm_table))) {
        LOG_WARN("failed to add minor table to meta merge result", K(ret));
      }
    } // end of for

    if (OB_FAIL(ret)) {
    } else if (tx_determ_table_cnt < 2) {
      ret = OB_NO_NEED_MERGE;
      if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000/*60s*/)) {
        LOG_INFO("no enough table for meta merge", K(ret), K(result), K(table_store));
      }
    } else if (inc_row_cnt < TRANS_STATE_DETERM_ROW_CNT_THRESHOLD
      || inc_row_cnt < INC_ROW_COUNT_PERCENTAGE_THRESHOLD * base_row_cnt) {
      ret = OB_NO_NEED_MERGE;
      if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000/*60s*/)) {
        LOG_INFO("found sstable could merge is not enough", K(ret), K(inc_row_cnt), K(base_row_cnt));
      }
    } else if (result.version_range_.snapshot_version_ < tablet.get_multi_version_start()) {
      ret = OB_NO_NEED_MERGE;
      if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000/*60s*/)) {
        LOG_INFO("chosen snapshot is abandoned", K(ret), K(result), K(tablet.get_multi_version_start()));
      }
    }

#ifdef ERRSIM
  if (OB_NO_NEED_MERGE == ret) {
    if (tablet.get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
      ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_COMPACTION) ret;
      LOG_INFO("errsim", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_);
      if (OB_FAIL(ret)) {
        FLOG_INFO("set schedule medium with errsim", "tablet_id", tablet.get_tablet_meta().tablet_id_);
        ret = OB_SUCCESS;
      }
    }
  }
#endif

  }
  return ret;
}

int ObAdaptiveMergePolicy::find_base_table_and_inc_version(
    ObITable *last_major_table,
    ObITable *last_minor_table,
    ObITable *&meta_base_table,
    int64_t &merge_inc_version)
{
  int ret = OB_SUCCESS;
  // find meta base table
  if (OB_NOT_NULL(last_major_table)) {
    if (OB_ISNULL(meta_base_table)) {
      meta_base_table = last_major_table;
    } else if (OB_UNLIKELY(meta_base_table->get_snapshot_version() <= last_major_table->get_snapshot_version())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta major table covered by major", K(ret), KPC(meta_base_table), KPC(last_major_table));
    }
  }

  // find meta merge inc version
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(last_major_table) && OB_NOT_NULL(last_minor_table)) {
    merge_inc_version = MAX(last_major_table->get_snapshot_version(), last_minor_table->get_max_merged_trans_version());
  } else if (OB_NOT_NULL(last_major_table)) {
    merge_inc_version = last_major_table->get_snapshot_version();
  } else if (OB_NOT_NULL(last_minor_table)){
    merge_inc_version = last_minor_table->get_max_merged_trans_version();
  }

  if (OB_SUCC(ret) && (NULL == meta_base_table || merge_inc_version <= 0)) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("cannot meta merge with null base table or inc version", K(ret), K(meta_base_table), K(merge_inc_version));
  }
  return ret;
}

int ObAdaptiveMergePolicy::add_meta_merge_result(
    ObITable *table,
    ObGetMergeTablesResult &result,
    const bool update_snapshot_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table));
  } else if (OB_FAIL(result.handle_.add_table(table))) {
    LOG_WARN("failed to add table", K(ret), KPC(table));
  } else if (table->is_meta_major_sstable() || table->is_major_sstable()) {
    result.version_range_.base_version_ = 0;
    result.version_range_.multi_version_start_ = table->get_snapshot_version();
    result.version_range_.snapshot_version_ = table->get_snapshot_version();
    result.create_snapshot_version_ = static_cast<ObSSTable *>(table)->get_meta().get_basic_meta().create_snapshot_version_;
  } else if (update_snapshot_flag) {
    int64_t max_snapshot = MAX(result.version_range_.snapshot_version_, table->get_max_merged_trans_version());
    result.version_range_.multi_version_start_ = max_snapshot;
    result.version_range_.snapshot_version_ = max_snapshot;
    result.scn_range_.end_scn_ = table->get_end_scn();
  }
  return ret;
}

int ObAdaptiveMergePolicy::get_adaptive_merge_reason(
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTabletStat tablet_stat;
  reason = AdaptiveMergeReason::NONE;

  if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->get_latest_tablet_stat(ls_id, tablet_id, tablet_stat))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get latest tablet stat", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_TMP_FAIL(check_inc_sstable_row_cnt_percentage(tablet, reason))) {
      LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id));
    }
  } else {
    if (OB_TMP_FAIL(check_tombstone_situation(tablet_stat, tablet, reason))) {
      LOG_WARN("failed to check tombstone scene", K(tmp_ret), K(ls_id), K(tablet_id));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_load_data_situation(tablet_stat, tablet, reason))) {
      LOG_WARN("failed to check load data scene", K(tmp_ret), K(ls_id), K(tablet_id));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_inc_sstable_row_cnt_percentage(tablet, reason))) {
      LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_ineffecient_read(tablet_stat, tablet, reason))) {
      LOG_WARN("failed to check ineffecient read", K(tmp_ret), K(ls_id), K(tablet_id));
    }

    if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000 /*10s*/)) {
      LOG_INFO("Check tablet adaptive merge reason", K(reason), K(tablet_stat)); // TODO tmp log, remove later
    }
  }
  return ret;
}

int ObAdaptiveMergePolicy::check_inc_sstable_row_cnt_percentage(
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObSSTable *last_major = static_cast<ObSSTable *>(tablet.get_table_store().get_major_sstables().get_boundary_table(true));
  int64_t base_row_count = nullptr != last_major ? last_major->get_meta().get_basic_meta().row_count_ : 0;
  int64_t inc_row_count = 0;
  const ObSSTableArray &minor_sstables = tablet.get_table_store().get_minor_sstables();
  ObSSTable *sstable = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < minor_sstables.count(); ++i) {
    if (OB_ISNULL(sstable = static_cast<ObSSTable *>(minor_sstables.get_table(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is null", K(ret), K(i));
    } else {
      inc_row_count += sstable->get_meta().get_basic_meta().row_count_;
    }
  }
  if ((inc_row_count > INC_ROW_COUNT_THRESHOLD) ||
      (base_row_count > BASE_ROW_COUNT_THRESHOLD &&
      (inc_row_count * 100 / base_row_count) > LOAD_DATA_SCENE_THRESHOLD)) {
    reason = AdaptiveMergeReason::FREQUENT_WRITE;
  }
  LOG_DEBUG("check_sstable_data_situation", K(ret), K(ls_id), K(tablet_id), K(reason),
      K(base_row_count), K(inc_row_count));
  return ret;
}

int ObAdaptiveMergePolicy::check_load_data_situation(
    const ObTabletStat &tablet_stat,
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  reason = AdaptiveMergeReason::NONE;
  if (!tablet.is_valid() || !tablet_stat.is_valid()
      || ls_id.id() != tablet_stat.ls_id_ || tablet_id.id() != tablet_stat.tablet_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(tablet), K(tablet_stat));
  } else if (tablet_stat.is_hot_tablet() && tablet_stat.is_insert_mostly()) {
    reason = AdaptiveMergeReason::LOAD_DATA_SCENE;
  }
  LOG_DEBUG("check_load_data_situation", K(ret), K(ls_id), K(tablet_id), K(reason), K(tablet_stat));
  return ret;
}

int ObAdaptiveMergePolicy::check_tombstone_situation(
    const ObTabletStat &tablet_stat,
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  reason = AdaptiveMergeReason::NONE;

  if (!tablet.is_valid() || !tablet_stat.is_valid()
      || ls_id.id() != tablet_stat.ls_id_ || tablet_id.id() != tablet_stat.tablet_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(tablet), K(tablet_stat));
  } else if (tablet_stat.is_hot_tablet() && (tablet_stat.is_update_mostly() || tablet_stat.is_delete_mostly())) {
    reason = AdaptiveMergeReason::TOMBSTONE_SCENE;
  }
  LOG_DEBUG("check_tombstone_situation", K(ret), K(ls_id), K(tablet_id), K(reason), K(tablet_stat));
  return ret;
}

int ObAdaptiveMergePolicy::check_ineffecient_read(
    const ObTabletStat &tablet_stat,
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  reason = AdaptiveMergeReason::NONE;

  if (!tablet.is_valid() || !tablet_stat.is_valid() ||
      ls_id.id() != tablet_stat.ls_id_ || tablet_id.id() != tablet_stat.tablet_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(tablet), K(tablet_stat));
  } else if (!tablet_stat.is_hot_tablet()) {
  } else if (tablet_stat.is_inefficient_scan() || tablet_stat.is_inefficient_insert()
          || tablet_stat.is_inefficient_pushdown()) {
    reason = AdaptiveMergeReason::INEFFICIENT_QUERY;
  }
  LOG_DEBUG("check_ineffecient_read", K(ret), K(ls_id), K(tablet_id), K(reason), K(tablet_stat));
  return ret;
}


} /* namespace compaction */
} /* namespace oceanbase */
