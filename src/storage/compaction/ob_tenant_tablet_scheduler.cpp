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
#include "ob_tenant_tablet_scheduler.h"
#include "storage/ob_bloom_filter_task.h"
#include "ob_tablet_merge_task.h"
#include "ob_partition_merge_policy.h"
#include "ob_schedule_dag_func.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_debug_sync.h"
#include "share/ob_thread_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "observer/ob_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/memtable/ob_memtable.h"
#include "ob_tenant_freeze_info_mgr.h"
#include "ob_tenant_compaction_progress.h"
#include "ob_server_compaction_event_history.h"
<<<<<<< HEAD
#include "share/scn.h"
=======
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "share/scn.h"
#include "storage/ddl/ob_ddl_merge_task.h"
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

namespace oceanbase
{
using namespace compaction;
using namespace common;
using namespace share;

namespace storage
{

ObFastFreezeChecker::ObFastFreezeChecker()
  : enable_fast_freeze_(false)
{
}

ObFastFreezeChecker::~ObFastFreezeChecker()
{
}

void ObFastFreezeChecker::reset()
{
  enable_fast_freeze_ = false;
}

void ObFastFreezeChecker::reload_config(const bool enable_fast_freeze)
{
  enable_fast_freeze_ = enable_fast_freeze;
}

int ObFastFreezeChecker::check_need_fast_freeze(
    const ObTablet &tablet,
    bool &need_fast_freeze)
{
  int ret = OB_SUCCESS;
  need_fast_freeze = false;
  ObTableHandleV2 table_handle;
  memtable::ObMemtable *memtable = nullptr;

  if (!enable_fast_freeze_) {
  } else if (OB_FAIL(tablet.get_active_memtable(table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get active memtable", K(ret));
    }
  } else if (OB_FAIL(table_handle.get_data_memtable(memtable))) {
    LOG_WARN("failed to get memtalbe", K(ret), K(table_handle));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null memtable", K(ret), KPC(memtable));
  } else if (!memtable->is_active_memtable()) {
  } else if (ObTimeUtility::current_time() < memtable->get_mt_stat().frozen_time_ + FAST_FREEZE_INTERVAL_US) {
    if (REACH_TENANT_TIME_INTERVAL(120 * 1000 * 1000)) {
      LOG_INFO("[FastFreeze] no need to check fast freeze now", K(tablet));
    }
  } else if (OB_FAIL(check_hotspot_need_fast_freeze(*memtable, need_fast_freeze))) {
    LOG_WARN("failed to check hotspot need fast freeze", K(ret));
  }
  return ret;
}

int ObFastFreezeChecker::check_hotspot_need_fast_freeze(
    const memtable::ObMemtable &memtable,
    bool &need_fast_freeze)
{
  int ret = OB_SUCCESS;
  need_fast_freeze = false;
  if (!memtable.is_active_memtable()) {
  } else {
    need_fast_freeze = memtable.has_hotspot_row();
    if (need_fast_freeze && REACH_TENANT_TIME_INTERVAL(120 * 1000 * 1000)) {
      LOG_INFO("[FastFreeze] current tablet has hotspot row, need fast freeze", K(memtable));
    }
  }
  return ret;
}

void ObTenantTabletScheduler::MergeLoopTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->schedule_all_tablets_minor())) {
    LOG_WARN("Fail to merge all partition", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("MergeLoopTask", K(cost_ts));
}

void ObTenantTabletScheduler::MediumLoopTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->schedule_all_tablets_medium())) {
    LOG_WARN("Fail to merge all partition", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("MediumLoopTask", K(cost_ts));
}

void ObTenantTabletScheduler::SSTableGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  // use tenant config to loop minor && medium task
  MTL(ObTenantTabletScheduler *)->reload_tenant_config();

  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(MTL(ObTenantTabletScheduler *)->update_upper_trans_version_and_gc_sstable())) {
    LOG_WARN("Fail to update upper_trans_version and gc sstable", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("SSTableGCTask", K(cost_ts));
}

constexpr ObMergeType ObTenantTabletScheduler::MERGE_TYPES[];

ObTenantTabletScheduler::ObTenantTabletScheduler()
 : is_inited_(false),
   major_merge_status_(false),
   is_stop_(false),
   merge_loop_tg_id_(0),
   medium_loop_tg_id_(0),
   sstable_gc_tg_id_(0),
   schedule_interval_(0),
   bf_queue_(),
   frozen_version_lock_(),
   frozen_version_(INIT_COMPACTION_SCN),
   merged_version_(INIT_COMPACTION_SCN),
   inner_table_merged_scn_(INIT_COMPACTION_SCN),
   schedule_stats_(),
   merge_loop_task_(),
   medium_loop_task_(),
   sstable_gc_task_(),
   fast_freeze_checker_(),
   enable_adaptive_compaction_(false),
   minor_ls_tablet_iter_(false/*is_major*/),
   medium_ls_tablet_iter_(true/*is_major*/),
   error_tablet_cnt_(0),
   ls_locality_cache_()
{
  STATIC_ASSERT(static_cast<int64_t>(NO_MAJOR_MERGE_TYPE_CNT) == ARRAYSIZEOF(MERGE_TYPES), "merge type array len is mismatch");
}

ObTenantTabletScheduler::~ObTenantTabletScheduler()
{
  destroy();
}

void ObTenantTabletScheduler::destroy()
{
  stop();
  wait();
  TG_DESTROY(merge_loop_tg_id_);
  TG_DESTROY(medium_loop_tg_id_);
  TG_DESTROY(sstable_gc_tg_id_);
  bf_queue_.destroy();
  frozen_version_ = 0;
  merged_version_ = 0;
  inner_table_merged_scn_ = 0;
  schedule_stats_.reset();
  merge_loop_tg_id_ = 0;
  medium_loop_tg_id_ = 0;
  sstable_gc_tg_id_ = 0;
  schedule_interval_ = 0;
  minor_ls_tablet_iter_.reset();
  medium_ls_tablet_iter_.reset();
  ls_locality_cache_.reset();
  is_inited_ = false;
  LOG_INFO("The ObTenantTabletScheduler destroy");
}

int ObTenantTabletScheduler::init()
{
  int ret = OB_SUCCESS;
  int64_t schedule_interval = DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      schedule_interval = tenant_config->ob_compaction_schedule_interval;
      enable_adaptive_compaction_ = tenant_config->_enable_adaptive_compaction;
      fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
    }
  } // end of ObTenantConfigGuard
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantTabletScheduler has inited", K(ret));
  } else if (FALSE_IT(bf_queue_.set_run_wrapper(MTL_CTX()))) {
  } else if (OB_FAIL(bf_queue_.init(BLOOM_FILTER_LOAD_BUILD_THREAD_CNT,
                                    "BFBuildTask",
                                    BF_TASK_QUEUE_SIZE,
                                    BF_TASK_MAP_SIZE,
                                    BF_TASK_TOTAL_LIMIT,
                                    BF_TASK_HOLD_LIMIT,
                                    BF_TASK_PAGE_SIZE,
                                    MTL_ID(),
                                    "bf_queue"))) {
    LOG_WARN("Fail to init bloom filter queue", K(ret));
  } else if (OB_FAIL(ls_locality_cache_.init(MTL_ID(), GCTX.sql_proxy_))) {
    LOG_WARN("failed to init ls locality cache", K(ret), KP(GCTX.sql_proxy_));
  } else {
    schedule_interval_ = schedule_interval;
    is_inited_ = true;
  }

  return ret;
}

int ObTenantTabletScheduler::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MergeLoop, merge_loop_tg_id_))) {
    LOG_WARN("failed to create merge loop thread", K(ret));
  } else if (OB_FAIL(TG_START(merge_loop_tg_id_))) {
    LOG_WARN("failed to start minor merge scan thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(merge_loop_tg_id_, merge_loop_task_, schedule_interval_, repeat))) {
    LOG_WARN("Fail to schedule minor merge scan task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MediumLoop, medium_loop_tg_id_))) {
    LOG_WARN("failed to create medium loop thread", K(ret));
  } else if (OB_FAIL(TG_START(medium_loop_tg_id_))) {
    LOG_WARN("failed to start medium merge scan thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(medium_loop_tg_id_, medium_loop_task_, schedule_interval_, repeat))) {
    LOG_WARN("Fail to schedule medium merge scan task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTableGC, sstable_gc_tg_id_))) {
    LOG_WARN("failed to create merge loop thread", K(ret));
  } else if (OB_FAIL(TG_START(sstable_gc_tg_id_))) {
    LOG_WARN("failed to start sstable gc thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(sstable_gc_tg_id_, sstable_gc_task_, SSTABLE_GC_INTERVAL, repeat))) {
    LOG_WARN("Fail to schedule sstable gc task", K(ret));
  }
  return ret;
}

int ObTenantTabletScheduler::reload_tenant_config()
{
  int ret = OB_SUCCESS;
  int64_t merge_schedule_interval = DEFAULT_COMPACTION_SCHEDULE_INTERVAL;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      merge_schedule_interval = tenant_config->ob_compaction_schedule_interval;
      enable_adaptive_compaction_ = tenant_config->_enable_adaptive_compaction;
      fast_freeze_checker_.reload_config(tenant_config->_ob_enable_fast_freeze);
    }
  } // end of ObTenantConfigGuard
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (is_stop_) {
    // do nothing
  } else if (schedule_interval_ != merge_schedule_interval) {
    if (OB_FAIL(restart_schedule_timer_task(merge_schedule_interval, medium_loop_tg_id_, medium_loop_task_))) {
      LOG_WARN("failed to reload new merge schedule interval", K(merge_schedule_interval));
    } else if (OB_FAIL(restart_schedule_timer_task(merge_schedule_interval, merge_loop_tg_id_, merge_loop_task_))) {
      LOG_WARN("failed to reload new merge schedule interval", K(merge_schedule_interval));
    } else {
      schedule_interval_ = merge_schedule_interval;
      LOG_INFO("succeeded to reload new merge schedule interval", K(merge_schedule_interval));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::mtl_init(ObTenantTabletScheduler* &scheduler)
{
  return scheduler->init();
}

void ObTenantTabletScheduler::stop()
{
  is_stop_ = true;
  TG_STOP(merge_loop_tg_id_);
  TG_STOP(medium_loop_tg_id_);
  TG_STOP(sstable_gc_tg_id_);
  stop_major_merge();
}

void ObTenantTabletScheduler::wait()
{
  TG_WAIT(merge_loop_tg_id_);
  TG_WAIT(medium_loop_tg_id_);
  TG_WAIT(sstable_gc_tg_id_);
}

int ObTenantTabletScheduler::try_remove_old_table(ObLS &ls)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = ls.get_ls_id();
  // only need NORMAL tablet here
  ObLSTabletIterator tablet_iter(ObTabletCommon::DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US);

  if (OB_FAIL(ls.build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), K(ls));
  }

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  common::ObTabletID tablet_id;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get tablet", K(ret), K(tenant_id), K(ls_id), K(tablet_handle));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
    } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
    } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
    } else if (tablet_id.is_special_merge_tablet()) {
    } else {
      int64_t multi_version_start = 0;
      bool need_remove = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObTablet::get_kept_multi_version_start(ls, *tablet, multi_version_start))) {
        LOG_WARN("failed to get multi version start", K(tmp_ret), K(tablet_id));
      } else if (OB_TMP_FAIL(tablet->check_need_remove_old_table(multi_version_start, need_remove))) {
        LOG_WARN("failed to check need remove old store", K(tmp_ret), K(multi_version_start), K(tablet_id));
      } else if (need_remove) {
        const ObStorageSchema &storage_schema = tablet->get_storage_schema();
        const int64_t rebuild_seq = ls.get_rebuild_seq();
        ObUpdateTableStoreParam param(tablet->get_snapshot_version(), multi_version_start, &storage_schema, rebuild_seq);
        ObTabletHandle new_tablet_handle; // no use here
        if (OB_TMP_FAIL(ls.update_tablet_table_store(tablet_id, param, new_tablet_handle))) {
          LOG_WARN("failed to update table store", K(tmp_ret), K(param), K(tenant_id), K(ls_id), K(tablet_id));
        } else {
          FLOG_INFO("success to remove old table in table store", K(tmp_ret), K(tenant_id), K(ls_id),
              K(tablet_id), K(multi_version_start), KPC(tablet));
        }
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::update_upper_trans_version_and_gc_sstable()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls iterator", K(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_iter_guard.get_ptr()->get_next(ls))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), KP(ls_iter_guard.get_ptr()));
      }
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else {
      (void) ls->try_update_uppder_trans_version();
      (void) try_remove_old_table(*ls);
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_all_tablets_minor()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  int64_t schedule_tablet_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (!minor_ls_tablet_iter_.is_valid() && OB_FAIL(minor_ls_tablet_iter_.build_iter())) {
    LOG_WARN("failed to init iterator", K(ret));
  } else {
    LOG_INFO("start schedule all tablet minor merge", K(minor_ls_tablet_iter_));
  }

  while (OB_SUCC(ret) && schedule_tablet_cnt < SCHEDULE_TABLET_BATCH_CNT) {
    if (OB_FAIL(minor_ls_tablet_iter_.get_next_ls(ls_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), K(minor_ls_tablet_iter_));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else {
      const ObLSID &ls_id = ls->get_ls_id();
      if (OB_TMP_FAIL(schedule_ls_minor_merge(ls_handle, schedule_tablet_cnt))) {
        minor_ls_tablet_iter_.skip_cur_ls();
        if (!schedule_ignore_error(tmp_ret)) {
          LOG_WARN("failed to schedule ls minor merge", K(tmp_ret), K(ls_id));
        }
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::check_ls_compaction_finish(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->check_ls_compaction_dag_exist(ls_id, exist))) {
    LOG_WARN("failed to check ls compaction dag", K(ret), K(ls_id));
  } else if (exist) {
    // the compaction dag exists, need retry later.
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_build_bloomfilter(
    const uint64_t table_id,
    const blocksstable::MacroBlockId &macro_id,
    const int64_t prefix_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || prefix_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(macro_id), K(prefix_len));
  } else {
    ObBloomFilterBuildTask task(MTL_ID(), table_id, macro_id, prefix_len);
    if (OB_FAIL(bf_queue_.add_task(task))) {
      if (OB_LIKELY(OB_EAGAIN == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Failed to add bloomfilter build task", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_load_bloomfilter(const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid macro block id", K(ret), K(macro_id));
  } else {
    ObBloomFilterLoadTask task(MTL_ID(), macro_id);
    if (OB_FAIL(bf_queue_.add_task(task))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Failed to add bloomfilter load task", K(ret));
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_merge(const int64_t broadcast_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(broadcast_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(broadcast_version), K(ret));
  } else if (broadcast_version <= get_frozen_version()) {
  } else {
    {
      obsys::ObRLockGuard frozen_version_guard(frozen_version_lock_);
      frozen_version_ = broadcast_version;
    }

    LOG_INFO("schedule merge major version", K(broadcast_version));
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->add_progress(broadcast_version))) {
      LOG_WARN("failed to add progress", K(tmp_ret), K(broadcast_version));
    }
    clear_error_tablet_cnt();

    schedule_stats_.start_merge(); // set all statistics
    ADD_COMPACTION_EVENT(
        MTL_ID(),
        MAJOR_MERGE,
        broadcast_version,
        ObServerCompactionEvent::RECEIVE_BROADCAST_SCN,
        schedule_stats_.start_timestamp_,
        "last_merged_version",
        merged_version_);
  }
  return ret;
}

bool ObTenantTabletScheduler::check_weak_read_ts_ready(
    const int64_t &merge_version,
    ObLS &ls)
{
  bool is_ready_for_compaction = false;
  SCN weak_read_scn;

  if (FALSE_IT(weak_read_scn = ls.get_ls_wrs_handler()->get_ls_weak_read_ts())) {
  } else if (weak_read_scn.get_val_for_tx() < merge_version) {
    FLOG_INFO("current slave_read_ts is smaller than freeze_ts, try later",
              "ls_id", ls.get_ls_id(), K(merge_version), K(weak_read_scn));
  } else {
    is_ready_for_compaction = true;
  }
  return is_ready_for_compaction;
}

void ObTenantTabletScheduler::stop_major_merge()
{
  if (major_merge_status_) {
    LOG_INFO("major merge has been paused!");
    major_merge_status_ = false;
  }
}

void ObTenantTabletScheduler::resume_major_merge()
{
  if (!major_merge_status_) {
    LOG_INFO("major merge has been resumed!");
    major_merge_status_ = true;
  }
}

int64_t ObTenantTabletScheduler::get_frozen_version() const
{
  obsys::ObRLockGuard frozen_version_guard(frozen_version_lock_);
  return frozen_version_;
}

bool ObTenantTabletScheduler::check_tx_table_ready(ObLS &ls, const SCN &check_scn)
{
  int ret = OB_SUCCESS;
  bool tx_table_ready = false;
  SCN max_decided_scn;
  if (OB_FAIL(ls.get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to get max decided log_ts", K(ret), "ls_id", ls.get_ls_id());
  } else if (check_scn <= max_decided_scn) {
    tx_table_ready = true;
    LOG_INFO("tx table ready", "sstable_end_scn", check_scn, K(max_decided_scn));
  }

  return tx_table_ready;
}

int ObTenantTabletScheduler::check_ls_state(ObLS &ls, bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  if (ls.is_deleted()) {
    LOG_INFO("ls is deleted", K(ret), K(ls));
  } else if (ls.is_offline()) {
    LOG_INFO("ls is offline", K(ret), K(ls));
  } else {
    need_merge = true;
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_merge_dag(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version,
    const bool is_tenant_major_merge)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.merge_type_ = merge_type;
  param.merge_version_ = merge_snapshot_version;
  param.is_tenant_major_merge_ = is_tenant_major_merge;
  if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to schedule tablet merge dag", K(ret));
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_meta_major_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid()) || OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_handle), K(tablet_handle));
  } else {
    const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
    LOG_INFO("start try to schedule tablet meta major merge", K(ls_id), K(tablet_id), K(tablet_handle)); // tmp log, remove later

    ObGetMergeTablesParam param;
    ObGetMergeTablesResult result;

    const ObTabletTableStore &table_store = tablet_handle.get_obj()->get_table_store();
    ObITable *last_major = table_store.get_major_sstables().get_boundary_table(true/*last*/);
    ObAdaptiveMergePolicy::AdaptiveMergeReason adaptive_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE;
    int64_t max_sync_medium_scn = 0;

    if (OB_FAIL(tablet_handle.get_obj()->get_max_sync_medium_scn(max_sync_medium_scn))) {
      LOG_WARN("failed to get max sync medium snapshot", K(ret), K(ls_id), K(tablet_id));
    } else if (tablet_handle.get_obj()->get_medium_compaction_info_list().size() > 0
        || nullptr == last_major
        || max_sync_medium_scn > last_major->get_snapshot_version()) {
      // do nothing
    } else if (OB_FAIL(ObAdaptiveMergePolicy::get_adaptive_merge_reason(*tablet_handle.get_obj(), adaptive_merge_reason))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get meta merge priority", K(ret), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (ObAdaptiveMergePolicy::is_valid_merge_reason(adaptive_merge_reason)) {
      LOG_INFO("start schedule meta merge", K(*tablet_handle.get_obj())); // tmp log, remove later
      param.merge_type_ = META_MAJOR_MERGE;
      if (OB_FAIL(ObAdaptiveMergePolicy::get_meta_merge_tables(
              param,
              *ls_handle.get_ls(),
              *tablet_handle.get_obj(),
              result))) {
        if (OB_NO_NEED_MERGE == ret) {
          ret = OB_SUCCESS;
          LOG_DEBUG("tablet no need meta merge", K(ret), K(param), K(tablet_id));
        } else {
          LOG_WARN("failed to get meta merge tables", K(ret), K(param), K(tablet_id));
        }
      } else if (tablet_handle.get_obj()->get_multi_version_start() > result.merge_version_) {
        ret = OB_NO_NEED_MERGE;
        LOG_WARN("multi version data is discarded, no need meta merge", K(ret),
          K(ls_id), K(tablet_id), K(result.merge_version_));
      } else {
        ObTabletMergeDagParam dag_param(META_MAJOR_MERGE, ls_id, tablet_id);
        if (OB_FAIL(schedule_merge_execute_dag<ObTabletMergeExecuteDag>(dag_param, ls_handle, tablet_handle, result))) {
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            LOG_WARN("failed to schedule tablet meta merge dag", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

template <class T>
int ObTenantTabletScheduler::schedule_tablet_minor_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
  const int64_t schedule_type_cnt = tablet_id.is_special_merge_tablet() ? TX_TABLE_NO_MAJOR_MERGE_TYPE_CNT : NO_MAJOR_MERGE_TYPE_CNT;
  ObGetMergeTablesParam param;
  ObGetMergeTablesResult result;
  for (int i = 0; OB_SUCC(ret) && i < schedule_type_cnt; ++i) {
    param.merge_type_ = MERGE_TYPES[i];
    if (OB_FAIL(ObPartitionMergePolicy::get_merge_tables[MERGE_TYPES[i]](
            param,
            *ls_handle.get_ls(),
            *tablet_handle.get_obj(),
            result))) {
      if (OB_NO_NEED_MERGE == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("tablet no need merge", K(ret), "merge_type", MERGE_TYPES[i], K(tablet_id), K(tablet_handle));
      } else {
        LOG_WARN("failed to check need merge", K(ret), "merge_type", MERGE_TYPES[i], K(tablet_handle));
      }
    } else {
      int64_t minor_compact_trigger = ObPartitionMergePolicy::DEFAULT_MINOR_COMPACT_TRIGGER;
      {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
        if (tenant_config.is_valid()) {
          minor_compact_trigger = tenant_config->minor_compact_trigger;
        }
      }

      ObMinorExecuteRangeMgr minor_range_mgr;
      MinorParallelResultArray parallel_results;
      if (OB_FAIL(minor_range_mgr.get_merge_ranges(ls_id, tablet_id))) {
        LOG_WARN("failed to get merge range", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(ObPartitionMergePolicy::generate_parallel_minor_interval(minor_compact_trigger, result, minor_range_mgr, parallel_results))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to generate parallel minor dag", K(ret), K(result));
        } else {
          ret = OB_SUCCESS;
          LOG_DEBUG("tablet no need merge", K(ret), "merge_type", MERGE_TYPES[i], K(ls_id), K(tablet_id), K(result));
        }
      } else if (parallel_results.empty()) {
        LOG_DEBUG("parallel results is empty, cannot schedule parallel minor merge", K(ls_id), K(tablet_id),
            K(result), K(minor_range_mgr.exe_range_array_));
      } else {
        ObTabletMergeDagParam dag_param(MERGE_TYPES[i], ls_id, tablet_id);
        for (int k = 0; OB_SUCC(ret) && k < parallel_results.count(); ++k) {
          if (OB_UNLIKELY(parallel_results.at(k).handle_.get_count() <= 1)) {
            LOG_WARN("invalid parallel result", K(ret), K(k), K(parallel_results));
          } else if (OB_FAIL(schedule_merge_execute_dag<T>(dag_param, ls_handle, tablet_handle, parallel_results.at(k)))) {
            LOG_WARN("failed to schedule minor execute dag", K(ret), K(k), K(parallel_results.at(k)));
          } else {
            LOG_INFO("success to schedule tablet minor merge", K(ret), K(ls_id), K(tablet_id),
              "table_cnt", parallel_results.at(k).handle_.get_count(),
              "merge_scn_range", parallel_results.at(k).scn_range_, "merge_type", MERGE_TYPES[i]);
          }
        } // end of for
      }
    }
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_tablet_ddl_major_merge(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle kv_mgr_handle;
  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(tablet_handle));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (kv_mgr_handle.is_valid()) {
    ObDDLTableMergeDagParam param;
    if (OB_FAIL(kv_mgr_handle.get_obj()->get_ddl_major_merge_param(tablet_handle.get_obj()->get_tablet_meta(), param))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to get ddl major merge param", K(ret));
      }
    } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    }
  }
  return ret;
}

// for minor dag, only hold tables_handle(sstable + ref), should not hold tablet(memtable)
template <class T>
int ObTenantTabletScheduler::schedule_merge_execute_dag(
    const ObTabletMergeDagParam &param,
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle,
    const ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  T *merge_exe_dag = nullptr;
  const bool emergency = tablet_handle.get_obj()->get_tablet_meta().tablet_id_.is_ls_inner_tablet();

  if (result.handle_.get_count() > 1
        && !ObTenantTabletScheduler::check_tx_table_ready(
        *ls_handle.get_ls(),
        result.scn_range_.end_scn_)) {
    ret = OB_EAGAIN;
    LOG_INFO("tx table is not ready. waiting for max_decided_log_ts ...", KR(ret),
        "merge_scn", result.scn_range_.end_scn_);
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->alloc_dag(merge_exe_dag))) {
    LOG_WARN("failed to alloc dag", K(ret));
  } else if (OB_FAIL(merge_exe_dag->direct_init_ctx(
          param,
          tablet_handle.get_obj()->get_tablet_meta().compat_mode_,
          result,
          ls_handle))) {
    LOG_WARN("failed to init dag", K(ret), K(result));
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->add_dag(merge_exe_dag, emergency))) {
    LOG_WARN("failed to add dag", K(ret), KPC(merge_exe_dag));
  } else {
    LOG_INFO("success to scheudle tablet minor execute dag", K(ret), KP(merge_exe_dag), K(emergency));
  }
  if (OB_FAIL(ret) && nullptr != merge_exe_dag) {
    MTL(share::ObTenantDagScheduler *)->free_dag(*merge_exe_dag);
  }
  return ret;
}

int ObTenantTabletScheduler::schedule_ls_minor_merge(
    ObLSHandle &ls_handle,
    int64_t &schedule_tablet_cnt)
{
  int ret = OB_SUCCESS;
  ObLSTabletIterator tablet_iter(ObTabletCommon::DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US);
  bool need_merge = false;
  bool need_fast_freeze = false;
  ObLS &ls = *ls_handle.get_ls();
  const ObLSID &ls_id = ls.get_ls_id();
  if (OB_FAIL(check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls));
  } else if (!need_merge) {
    // no need to merge, do nothing
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObTabletID tablet_id;
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    int tmp_ret = OB_SUCCESS;
    bool schedule_minor_flag = true;
    while (OB_SUCC(ret) && schedule_minor_flag && schedule_tablet_cnt < SCHEDULE_TABLET_BATCH_CNT) { // loop all tablet in ls
      bool tablet_merge_finish = false;
      if (OB_FAIL(minor_ls_tablet_iter_.get_next_tablet(ls_handle, tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_LS_NOT_EXIST != ret) {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
      } else if (tablet_id.is_special_merge_tablet()) {
        // schedule minor merge for special tablet
        if (tablet_id.is_mini_and_minor_merge_tablet()
            && OB_TMP_FAIL(schedule_tablet_minor_merge<ObTxTableMinorExecuteDag>(
                ls_handle,
                tablet_handle))) {
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            schedule_minor_flag = false;
          } else if (OB_EAGAIN != tmp_ret) {
            LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_id));
          }
        }
      } else { // data tablet
        schedule_tablet_cnt++;
        if (OB_TMP_FAIL(schedule_tablet_minor_merge<ObTabletMergeExecuteDag>(ls_handle, tablet_handle))) {
          if (OB_SIZE_OVERFLOW == tmp_ret) {
            schedule_minor_flag = false;
          } else if (OB_EAGAIN != tmp_ret) {
            LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_id));
          }
        }

        if (OB_TMP_FAIL(schedule_tablet_ddl_major_merge(tablet_handle))) {
          if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
            LOG_WARN("failed to schedule tablet ddl merge", K(tmp_ret), K(ls_id), K(tablet_handle));
          }
        }

        if (OB_SUCC(ret)) {
          need_fast_freeze = false;
          if (!fast_freeze_checker_.need_check()) {
          } else if (OB_TMP_FAIL(fast_freeze_checker_.check_need_fast_freeze(*tablet_handle.get_obj(), need_fast_freeze))) {
            LOG_WARN("failed to check need fast freeze", K(tmp_ret), K(tablet_handle));
          } else if (need_fast_freeze) {
            if (OB_TMP_FAIL(ls.tablet_freeze(tablet_id, true/*is_sync*/))) {
              LOG_WARN("failt to freeze tablet", K(tmp_ret), K(tablet_id));
            }
          }
        }
      }
    } // end of while
  } // else
  return ret;
}

int ObTenantTabletScheduler::schedule_ls_medium_merge(
    int64_t &merge_version,
    ObLSHandle &ls_handle,
    bool &all_ls_weak_read_ts_ready,
    int64_t &schedule_tablet_cnt)
{
  int ret = OB_SUCCESS;
  bool need_merge = false;
  ObLS &ls = *ls_handle.get_ls();
  const ObLSID &ls_id = ls.get_ls_id();
  ObLSRestoreStatus restore_status;
  if (OB_FAIL(check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls));
  } else if (!need_merge) {
    // no need to merge, do nothing
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(ls.get_ls_meta().get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(ls));
  } else if (OB_UNLIKELY(!restore_status.is_restore_none())) {
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("ls is in restore status, should not loop tablet to schedule", K(ret), K(ls));
    }
    ret = OB_STATE_NOT_MATCH;
  } else {
    ObTabletID tablet_id;
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    int tmp_ret = OB_SUCCESS;
    bool is_leader = false;
    bool could_major_merge = false;
    const int64_t major_frozen_scn = get_frozen_version();
    ObRole role = INVALID_ROLE;
    ObLSLocality ls_locality;
    if (MTL(ObTenantTabletScheduler *)->could_major_merge_start()) {
      could_major_merge = true;
    } else if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("major merge should not schedule", K(ret), K(merge_version));
    }
    // check weak_read_ts
    if (merge_version >= 0) {
      if (check_weak_read_ts_ready(merge_version, ls)) { // weak read ts ready
        if (OB_FAIL(ObMediumCompactionScheduleFunc::get_palf_role(ls_id, role))) {
          if (OB_LS_NOT_EXIST != ret) {
            LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
          }
        } else if (is_leader_by_election(role)) {
          is_leader = true;
          if (OB_FAIL(ls_locality_cache_.get_ls_locality(ls_id, ls_locality))) {
            LOG_WARN("failed to get ls locality", K(ret), K(ls_id));
          } else if (ls_locality.is_valid()) {
            if (!ls_locality.check_exist(GCTX.self_addr())) {
              ADD_SUSPECT_INFO(MEDIUM_MERGE, ls_id, ObTabletID(INT64_MAX),
                "maybe bad case: ls leader is not in ls locality", "leader_addr", GCTX.self_addr(), K(ls_locality));
            } else {
              DEL_SUSPECT_INFO(MEDIUM_MERGE, ls_id, ObTabletID(INT64_MAX));
            }
          }
        }
      } else {
        all_ls_weak_read_ts_ready = false;
      }
    }

    while (OB_SUCC(ret) && schedule_tablet_cnt < SCHEDULE_TABLET_BATCH_CNT) { // loop all tablet in ls
      bool tablet_merge_finish = false;
      if (OB_FAIL(medium_ls_tablet_iter_.get_next_tablet(ls_handle, tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_LS_NOT_EXIST != ret) {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (FALSE_IT(tablet_id = tablet->get_tablet_meta().tablet_id_)) {
      } else if (tablet_id.is_special_merge_tablet()) { // data tablet
        // do nothing
      } else {
        ++schedule_tablet_cnt; // inc tablet cnt

        ObMediumCompactionScheduleFunc func(ls, tablet_handle);
        ObITable *latest_major = tablet->get_table_store().get_major_sstables().get_boundary_table(true/*last*/);
        if (OB_NOT_NULL(latest_major) && latest_major->get_snapshot_version() >= merge_version) {
          tablet_merge_finish = true;
          schedule_stats_.finish_cnt_++;

          if (tablet->get_tablet_meta().report_status_.need_report()) {
            if (OB_TMP_FAIL(GCTX.ob_service_->submit_tablet_update_task(MTL_ID(), ls_id, tablet_id))) {
              LOG_WARN("failed to submit tablet update task to report", K(tmp_ret), K(MTL_ID()), K(tablet_id));
            } else if (OB_TMP_FAIL(ls.get_tablet_svr()->update_tablet_report_status(tablet_id))) {
              LOG_WARN("failed to update tablet report status", K(tmp_ret), K(MTL_ID()), K(tablet_id));
            }
          }
        }
        LOG_DEBUG("schedule tablet medium", K(ret), K(ls_id), K(tablet_id), K(tablet_merge_finish),
            KPC(latest_major), K(merge_version), K(role), K(is_leader));
        bool could_schedule_next_medium = true;
        bool check_medium_finish = false;
        if (!is_leader || OB_ISNULL(latest_major)) {
          // follower or no major: do nothing
          could_schedule_next_medium = false;
        } else if (tablet->get_medium_compaction_info_list().need_check_finish()) { // need check finished
          ObTabletHandle new_handle;
          if (OB_TMP_FAIL(func.check_medium_finish(ls_locality))) {
            LOG_WARN("failed to check medium finish", K(tmp_ret), K(ls_id), K(tablet_id));
          } else if (FALSE_IT(check_medium_finish = true)) {
          } else if (FALSE_IT(func.get_tablet_handle(new_handle))) {
          } else if (ObTimeUtility::fast_current_time() * 1000 <
              tablet->get_medium_compaction_info_list().get_wait_check_medium_scn() + WAIT_MEDIUM_CHECK_THRESHOLD) {
            // need wait 10 mins before schedule meta major
          } else if (enable_adaptive_compaction_ && OB_TMP_FAIL(schedule_tablet_meta_major_merge(ls_handle, new_handle))) {
            if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
              LOG_WARN("failed to schedule tablet merge", K(tmp_ret), K(ls_id), K(tablet_id));
            }
          }
        }
        if (could_schedule_next_medium && could_major_merge
          && (!tablet_merge_finish || enable_adaptive_compaction_ || check_medium_finish)
          && OB_TMP_FAIL(func.schedule_next_medium_for_leader(
            tablet_merge_finish ? 0 : merge_version, schedule_stats_))) { // schedule another round
          LOG_WARN("failed to schedule next medium", K(tmp_ret), K(ls_id), K(tablet_id));
        } else {
          schedule_stats_.schedule_cnt_++;
        }

        if (could_major_merge && OB_TMP_FAIL(ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
            ls,
            *tablet,
            major_frozen_scn,
            true/*scheduler_called*/))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("failed to schedule medium", K(tmp_ret), K(ls_id), K(tablet_id));
          }
        }
        medium_ls_tablet_iter_.update_merge_finish(tablet_merge_finish);
      }
    } // end of while
  } // else
  return ret;
}

int ObTenantTabletScheduler::schedule_all_tablets_medium()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantTabletScheduler has not been inited", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", K(ret));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    // do nothing, should not loop tablets
    if (REACH_TENANT_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("compat_version is smaller than DATA_VERSION_4_1_0_0, cannot schedule medium", K(compat_version));
      ADD_SUSPECT_INFO(MEDIUM_MERGE, share::ObLSID(INT64_MAX), ObTabletID(INT64_MAX),
        "invalid data version to schedule all tablets medium",
        K(compat_version), "DATA_VERSION_4_1_0_0", DATA_VERSION_4_1_0_0);
    }
  } else if (!medium_ls_tablet_iter_.is_valid() && OB_FAIL(medium_ls_tablet_iter_.build_iter())) {
    LOG_WARN("failed to init iterator", K(ret));
  } else {
    bool all_ls_weak_read_ts_ready = true;
    bool check_report_scn_flag = false;
    int64_t merge_version = get_frozen_version();
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    int64_t schedule_tablet_cnt = 0;
    LOG_INFO("start schedule all tablet merge", K(merge_version), K(medium_ls_tablet_iter_));

    if (INIT_COMPACTION_SCN == merge_version) {
      merge_version = 0;
    } else if (merge_version > merged_version_) {
      if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress(merge_version,
          share::ObIDag::DAG_STATUS_NODE_RUNNING))) {
        LOG_WARN("failed to update progress", K(tmp_ret), K(merge_version));
      }
    }

    if (REACH_TENANT_TIME_INTERVAL(CHECK_REPORT_SCN_INTERVAL)) {
      check_report_scn_flag = true;
    }
    if (REACH_TENANT_TIME_INTERVAL(CHECK_LS_LOCALITY_INTERVAL)) {
      if (OB_TMP_FAIL(ls_locality_cache_.refresh_ls_locality())) {
        LOG_WARN("failed to refresh ls locality", K(tmp_ret));
        ADD_SUSPECT_INFO(MEDIUM_MERGE, share::ObLSID(INT64_MAX), ObTabletID(INT64_MAX),
          "refresh ls locality cache failed", "errno", tmp_ret);
      }
    }
#ifdef ERRSIM
    check_report_scn_flag = true;
#endif

    while (OB_SUCC(ret) && schedule_tablet_cnt < SCHEDULE_TABLET_BATCH_CNT) {
      if (OB_FAIL(medium_ls_tablet_iter_.get_next_ls(ls_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get ls", K(ret), K(ls_handle));
        }
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret), K(ls));
      } else if (OB_TMP_FAIL(schedule_ls_medium_merge(
                     merge_version, ls_handle,
                     all_ls_weak_read_ts_ready, schedule_tablet_cnt))) {
        medium_ls_tablet_iter_.skip_cur_ls(); // for any errno, skip cur ls
        medium_ls_tablet_iter_.update_merge_finish(false);
        if (OB_SIZE_OVERFLOW == tmp_ret) {
          break;
        } else if (!schedule_ignore_error(tmp_ret)) {
          LOG_WARN("failed to schedule ls merge", K(tmp_ret), KPC(ls));
        }
      } else {
        // loop tablet_meta table to update smaller report_scn because of migration
        if (check_report_scn_flag) {
          (void) update_report_scn_as_ls_leader(*ls);
        }
      }
    } // end while

    if (!medium_ls_tablet_iter_.tenant_merge_finish()) { // wait major compaction
      if (all_ls_weak_read_ts_ready) { // check schedule Timer Task
        if (schedule_stats_.add_weak_read_ts_event_flag_ && medium_ls_tablet_iter_.is_scan_finish()) { // all ls scan finish
          schedule_stats_.add_weak_read_ts_event_flag_ = false;
          ADD_COMPACTION_EVENT(
              MTL_ID(),
              MAJOR_MERGE,
              merge_version,
              ObServerCompactionEvent::WEAK_READ_TS_READY,
              ObTimeUtility::fast_current_time(),
              "check_weak_read_ts_cnt", schedule_stats_.check_weak_read_ts_cnt_ + 1);
        }
      } else {
        schedule_stats_.check_weak_read_ts_cnt_++;
      }

      if (medium_ls_tablet_iter_.is_scan_finish() && REACH_TENANT_TIME_INTERVAL(ADD_LOOP_EVENT_INTERVAL)) {
        ADD_COMPACTION_EVENT(
            MTL_ID(),
            MAJOR_MERGE,
            merge_version,
            ObServerCompactionEvent::SCHEDULER_LOOP,
            ObTimeUtility::fast_current_time(),
            "schedule_stats",
            schedule_stats_);
      }
    }

    if (OB_SUCC(ret) && medium_ls_tablet_iter_.tenant_merge_finish() && merge_version > merged_version_) {
      merged_version_ = merge_version;
      LOG_INFO("all tablet major merge finish", K(merged_version_), K(merge_version));
      DEL_SUSPECT_INFO(MEDIUM_MERGE, share::ObLSID(INT64_MAX), ObTabletID(INT64_MAX));
      if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->update_progress(
          merge_version,
          share::ObIDag::DAG_STATUS_FINISH))) {
        LOG_WARN("failed to finish progress", K(tmp_ret), K(merge_version));
      }

      const int64_t current_time = ObTimeUtility::fast_current_time();
      ADD_COMPACTION_EVENT(
          MTL_ID(),
          MAJOR_MERGE,
          merge_version,
          ObServerCompactionEvent::TABLET_COMPACTION_FINISHED,
          current_time,
          "cost_time",
          current_time - schedule_stats_.start_timestamp_);
    }

    LOG_INFO("finish schedule all tablet merge", K(merge_version), K(schedule_stats_),
        "tenant_merge_finish", medium_ls_tablet_iter_.tenant_merge_finish(),
        K(merged_version_), K(schedule_tablet_cnt));
    if (medium_ls_tablet_iter_.is_scan_finish()) {
      schedule_stats_.clear_tablet_cnt();
    }
  }
  return ret;
}

int ObTenantTabletScheduler::restart_schedule_timer_task(
  const int64_t schedule_interval,
  const int64_t tg_id,
  common::ObTimerTask &timer_task)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_FAIL(TG_TASK_EXIST(tg_id, timer_task, is_exist))) {
    LOG_ERROR("failed to check merge schedule task exist", K(ret));
  } else if (is_exist) {
    if (OB_FAIL(TG_WAIT_R(tg_id))) {
      LOG_WARN("failed to wait schedule task before restart", K(ret), K(tg_id), K(schedule_interval));
    } else if (OB_FAIL(TG_STOP_R(tg_id))) {
      LOG_WARN("failed to stop schedule task before restart", K(ret), K(tg_id), K(schedule_interval));
    } else if (OB_FAIL(TG_START(tg_id))) {
      LOG_WARN("failed to restart schedule task", K(ret), K(tg_id), K(schedule_interval));
    }
  }

  if (FAILEDx(TG_SCHEDULE(tg_id, timer_task, schedule_interval, true/*repeat*/))) {
    LOG_WARN("Fail to schedule minor merge scan task", K(ret));
  }
  return ret;
}

int ObTenantTabletScheduler::get_min_dependent_schema_version(int64_t &min_schema_version)
{
  int ret = OB_SUCCESS;
  min_schema_version = OB_INVALID_VERSION;
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  if (OB_FAIL(MTL(storage::ObTenantFreezeInfoMgr*)->get_min_dependent_freeze_info(freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("freeze info is not exist", K(ret));
    } else {
      LOG_WARN("failed to get freeze info", K(ret));
    }
  } else {
    min_schema_version = freeze_info.schema_version;
  }
  return ret;
}

int ObTenantTabletScheduler::update_report_scn_as_ls_leader(ObLS &ls)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObRole role = INVALID_ROLE;
  const int64_t major_merged_scn = get_inner_table_merged_scn();
  bool need_merge = false;
  if (OB_FAIL(check_ls_state(ls, need_merge))) {
    LOG_WARN("failed to check ls state", K(ret), K(ls_id));
  } else if (!need_merge) {
    // do nothing
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_palf_role(ls_id, role))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
    }
  } else if (is_leader_by_election(role)) {
    ObSEArray<ObTabletID, 200> tablet_id_array;
    if (OB_FAIL(ls.get_tablet_svr()->get_all_tablet_ids(true/*except_ls_inner_tablet*/, tablet_id_array))) {
      LOG_WARN("failed to get tablet id", K(ret), K(ls_id));
    } else if (major_merged_scn > INIT_COMPACTION_SCN
        && OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_unequal_report_scn_tablet(
          MTL_ID(), ls_id, major_merged_scn, tablet_id_array))) {
      LOG_WARN("failed to get unequal report scn", K(ret), K(ls_id), K(major_merged_scn));
    }
  }
  return ret;
}


// ------------------- ObCompactionScheduleIterator -------------------- //
int ObCompactionScheduleIterator::build_iter()
{
  int ret = OB_SUCCESS;
  ls_ids_.reuse();
  if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(ls_ids_))) {
    LOG_WARN("failed to get all ls id", K(ret));
  } else {
    ls_idx_ = -1;
    tablet_idx_ = 0;
    tablet_ids_.reuse();
    scan_finish_ = false;
    merge_finish_ = true;
    LOG_TRACE("build iter", K(ret), K(ls_ids_));
  }
  return ret;
}

int ObCompactionScheduleIterator::get_next_ls(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (-1 == ls_idx_
    || tablet_idx_ >= tablet_ids_.count()) { // tablet iter end, need get next ls
    ++ls_idx_;
    tablet_ids_.reuse();
  }
  do {
    if (ls_idx_ >= ls_ids_.count()) {
      scan_finish_ = true;
      ret = OB_ITER_END;
    } else if (OB_FAIL((MTL(storage::ObLSService *)->get_ls(ls_ids_[ls_idx_], ls_handle, mod_)))) {
      if (OB_LS_NOT_EXIST == ret) {
        LOG_TRACE("ls not exist", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
        skip_cur_ls();
      } else {
        LOG_WARN("failed to get ls", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
      }
    }
  } while (OB_LS_NOT_EXIST == ret);
  return ret;
}

void ObCompactionScheduleIterator::reset()
{
  mod_ = ObLSGetMod::INVALID_MOD;
  timeout_us_ = 0;
  ls_idx_ = 0;
  tablet_idx_ = 0;
  ls_ids_.reuse();
  tablet_ids_.reuse();
  scan_finish_ = false;
  merge_finish_ = false;
}

bool ObCompactionScheduleIterator::is_valid() const
{
  return ls_ids_.count() > 0 && ls_idx_ >= 0
    && (ls_idx_ < ls_ids_.count() - 1
      || (ls_idx_ == ls_ids_.count() - 1 && tablet_idx_ < tablet_ids_.count()));
    // have remain ls or have remain tablet
}

int ObCompactionScheduleIterator::get_next_tablet(
  ObLSHandle &ls_handle,
  ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_handle is invalid", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
  } else if (tablet_ids_.empty()) {
    if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_all_tablet_ids(
            is_major_/*except_ls_inner_tablet*/, tablet_ids_))) {
      LOG_WARN("failed to get tablet ids", K(ret));
    } else {
      tablet_idx_ = 0; // for new ls, set tablet_idx_ = 0
      LOG_TRACE("build iter", K(ret), K_(ls_idx), "ls_id", ls_ids_[ls_idx_], K(tablet_ids_));
    }
  }
  if (OB_SUCC(ret)) {
    do {
      if (tablet_idx_ >= tablet_ids_.count()) {
        ret = OB_ITER_END;
      } else {
        const common::ObTabletID &tablet_id = tablet_ids_.at(tablet_idx_);
        if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, timeout_us_))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            tablet_idx_++;
          } else {
            LOG_WARN("fail to get tablet", K(ret), K(tablet_idx_), K(tablet_id), K_(timeout_us));
          }
        } else {
          tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
          tablet_idx_++;
        }
      }
    } while (OB_TABLET_NOT_EXIST == ret);
  }
  return ret;
}
int64_t ObCompactionScheduleIterator::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_idx), K_(ls_ids), K_(tablet_idx), K(tablet_ids_.count()));
  if (is_valid()) {
    J_COMMA();
    J_KV("cur_ls", ls_ids_.at(ls_idx_));
    if (!tablet_ids_.empty() && tablet_idx_ < tablet_ids_.count()) {
      J_COMMA();
      J_KV("next_tablet", tablet_ids_.at(tablet_idx_));
    }
  }
  J_OBJ_END();
  return pos;
}

} // namespace storage
} // namespace oceanbase
