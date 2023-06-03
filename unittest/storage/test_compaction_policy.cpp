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
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define private public
#define protected public

#include "storage/ob_i_table.h"
#include "storage/schema_utils.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "observer/ob_safe_destroy_thread.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"
#include "storage/test_dml_common.h"
#include "storage/mockcontainer/mock_ob_iterator.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "share/scn.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace share;
using namespace compaction;

namespace memtable
{

  bool ObMemtable::can_be_minor_merged()
  {
    return is_tablet_freeze_;
  }

}

namespace unittest
{

class TestCompactionPolicy: public ::testing::Test
{
public:
  static void generate_table_key(
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    ObITable::TableKey &table_key);
  static int mock_sstable(
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    const int64_t max_merged_trans_version,
    const int64_t upper_trans_version,
    ObTableHandleV2 &table_handle);
  static int mock_memtable(
    const int64_t start_log_ts,
    const int64_t end_log_ts,
    const int64_t snapshot_version,
    ObTablet &tablet,
    ObTableHandleV2 &table_handle);
  static int mock_tablet(
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    ObTabletHandle &tablet_handle);
  static int mock_table_store(
    ObTabletHandle &tablet_handle,
    common::ObIArray<ObTableHandleV2> &major_tables,
    common::ObIArray<ObTableHandleV2> &minor_tables);
  static int batch_mock_sstables(
    const char *key_data,
    common::ObIArray<ObTableHandleV2> &major_tables,
    common::ObIArray<ObTableHandleV2> &minor_tables);
  static int batch_mock_memtables(
    const char *key_data,
    ObTabletHandle &tablet_handle,
    common::ObIArray<ObTableHandleV2> &memtables);
  static int batch_mock_tables(
    const char *key_data,
    common::ObIArray<ObTableHandleV2> &major_tables,
    common::ObIArray<ObTableHandleV2> &minor_tables,
    common::ObIArray<ObTableHandleV2> &memtables,
    ObTabletHandle &tablet_handle);
  static int prepare_freeze_info(
    const int64_t snapshot_gc_ts,
    common::ObIArray<ObTenantFreezeInfoMgr::FreezeInfo> &freeze_infos,
    common::ObIArray<share::ObSnapshotInfo> &snapshots);

  void prepare_schema(share::schema::ObTableSchema &table_schema);
  int prepare_medium_list(
      const char *snapshot_list,
      ObTabletHandle &tablet_handle);
  int construct_array(
      const char *snapshot_list,
      ObIArray<int64_t> &array);
  int check_result_tables_handle(const char *end_log_ts_list, const ObGetMergeTablesResult &result);

public:
  TestCompactionPolicy();
  ~TestCompactionPolicy() = default;
  int prepare_tablet(
    const char *key_data,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version);
public:
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static constexpr int64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 9001;
  static constexpr int64_t TEST_TABLET_ID = 2323233;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObTenantFreezeInfoMgr *freeze_info_mgr_;
  ObTabletHandle tablet_handle_;
  ObSEArray<ObTableHandleV2, 4> major_tables_;
  ObSEArray<ObTableHandleV2, 4> minor_tables_;
  ObSEArray<ObTableHandleV2, 4> memtables_;
  ObMediumCompactionInfo medium_info_;
  ObSEArray<int64_t, 10> array_;
  ObArenaAllocator allocator_;
};

TestCompactionPolicy::TestCompactionPolicy()
  : ls_id_(TEST_LS_ID),
    tablet_id_(TEST_TABLET_ID),
    freeze_info_mgr_(nullptr),
    tablet_handle_(),
    major_tables_(),
    minor_tables_(),
    memtables_()
{
}

void TestCompactionPolicy::SetUp()
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  ret = t3m->init();
  ASSERT_EQ(OB_SUCCESS, ret);

  share::schema::ObTableSchema table_schema;
  prepare_schema(table_schema);

  medium_info_.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
  medium_info_.medium_snapshot_ = 100;
  medium_info_.data_version_ = 100;

  medium_info_.storage_schema_.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL);
}

void TestCompactionPolicy::TearDown()
{
  tablet_handle_.reset();
  major_tables_.reset();
  minor_tables_.reset();
  memtables_.reset();

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();

  ObTenantFreezeInfoMgr *freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != freeze_info_mgr);
  freeze_info_mgr->info_list_[0].reset();
  freeze_info_mgr->info_list_[1].reset();
  freeze_info_mgr->snapshots_[0].reset();
  freeze_info_mgr->snapshots_[1].reset();
  freeze_info_mgr->snapshot_gc_ts_ = 0;
}

void TestCompactionPolicy::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  SAFE_DESTROY_INSTANCE.init();
  SAFE_DESTROY_INSTANCE.start();

  // ls service cannot service before ObServerCheckpointSlogHandler starts running
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  if (OB_SUCCESS != ret) {
    LOG_ERROR("[FATAL ERROR] failed to create ls", K(ret));
  }
}

void TestCompactionPolicy::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  ret = t3m->init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID), false);
  ASSERT_EQ(OB_SUCCESS, ret);

  SAFE_DESTROY_INSTANCE.stop();
  SAFE_DESTROY_INSTANCE.wait();
  SAFE_DESTROY_INSTANCE.destroy();

  MockTenantModuleEnv::get_instance().destroy();
}

void TestCompactionPolicy::generate_table_key(
    const ObITable::TableType &type,
    const int64_t start_scn,
    const int64_t end_scn,
    ObITable::TableKey &table_key)
{
  table_key.reset();
  table_key.tablet_id_ = TEST_TABLET_ID;
  table_key.table_type_ = type;
  if (type == ObITable::TableType::MAJOR_SSTABLE) {
    table_key.version_range_.base_version_ = start_scn;
    table_key.version_range_.snapshot_version_ = end_scn;
  } else {
    table_key.scn_range_.start_scn_.convert_for_tx(start_scn);
    table_key.scn_range_.end_scn_.convert_for_tx(end_scn);
  }
}

int TestCompactionPolicy::mock_sstable(
  const ObITable::TableType &type,
  const int64_t start_scn,
  const int64_t end_scn,
  const int64_t max_merged_trans_version,
  const int64_t upper_trans_version,
  ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);

  ObITable::TableKey table_key;
  generate_table_key(type, start_scn, end_scn, table_key);

  ObTabletID tablet_id;
  tablet_id = TEST_TABLET_ID;
  ObTabletCreateSSTableParam param;
  if (OB_FAIL(ObTabletCreateDeleteHelper::build_create_sstable_param(table_schema, tablet_id, 100, param))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else {
    param.table_key_ = table_key;
    param.max_merged_trans_version_ = max_merged_trans_version;
  }

  ObSSTable *sstable = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, table_handle))) {
    LOG_WARN("failed to create sstable", K(param));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  } else {
    sstable->meta_.basic_meta_.max_merged_trans_version_ = max_merged_trans_version;
    sstable->meta_.basic_meta_.upper_trans_version_ = upper_trans_version;
  }
  return ret;
}

int TestCompactionPolicy::mock_memtable(
    const int64_t start_scn,
    const int64_t end_scn,
    const int64_t snapshot_version,
    ObTablet &tablet,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(tablet.memtable_mgr_);

  ObITable::TableKey table_key;
  int64_t end_border = -1;
  if (0 == end_scn || INT64_MAX == end_scn) {
    end_border = OB_MAX_SCN_TS_NS;
  } else {
    end_border = end_scn;
  }

  generate_table_key(ObITable::DATA_MEMTABLE, start_scn, end_border, table_key);
  ObMemtable *memtable = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  if (OB_FAIL(t3m->acquire_memtable(table_handle))) {
    LOG_WARN("failed to acquire memtable", K(ret));
  } else if (OB_ISNULL(memtable = static_cast<ObMemtable*>(table_handle.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get memtable", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(mt_mgr->ls_->get_ls_id(), ls_handle, ObLSGetMod::DATA_MEMTABLE_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else if (OB_FAIL(memtable->init(table_key, ls_handle, mt_mgr->freezer_, mt_mgr, 0, mt_mgr->freezer_->get_freeze_clock()))) {
    LOG_WARN("failed to init memtable", K(ret));
  } else if (OB_FAIL(mt_mgr->add_memtable_(table_handle))) {
    LOG_WARN("failed to add memtable to mgr", K(ret));
  } else if (OB_FAIL(memtable->add_to_data_checkpoint(mt_mgr->freezer_->get_ls_data_checkpoint()))) {
    LOG_WARN("add to data_checkpoint failed", K(ret), KPC(memtable));
    mt_mgr->clean_tail_memtable_();
  } else if (OB_MAX_SCN_TS_NS != end_border) { // frozen memtable
    SCN snapshot_scn;
    snapshot_scn.convert_for_tx(snapshot_version);
    memtable->snapshot_version_ = snapshot_scn;
    memtable->write_ref_cnt_ = 0;
    memtable->unsynced_cnt_ = 0;
    memtable->is_tablet_freeze_ = true;
    memtable->state_ = ObMemtableState::MINOR_FROZEN;
    memtable->set_resolve_active_memtable_left_boundary(true);
    memtable->set_frozen();
    memtable->location_ = storage::checkpoint::ObFreezeCheckpointLocation::PREPARE;
  }

  return ret;
}

int TestCompactionPolicy::mock_tablet(
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id = ObLSID(TEST_LS_ID);
  ObTabletID tablet_id = ObTabletID(TEST_TABLET_ID);
  ObTabletID empty_tablet_id;
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  const lib::Worker::CompatMode &compat_mode = lib::Worker::CompatMode::MYSQL;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tablet = nullptr;

  ObTableHandleV2 table_handle;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  ObTabletTableStoreFlag table_store_flag;
  table_store_flag.set_without_major_sstable();

  if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null t3m", K(ret));
  } else if (OB_FAIL(t3m->del_tablet(key))) {
    LOG_WARN("failed to del tablet", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls handle", K(ret));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tablet(key, tablet_handle))) {
    LOG_WARN("failed to acquire tablet", K(ret), K(key));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(tablet->init(ls_id, tablet_id, tablet_id, empty_tablet_id, empty_tablet_id,
      SCN::min_scn(), snapshot_version, table_schema, compat_mode, table_store_flag, table_handle, ls_handle.get_ls()->get_freezer()))) {
    LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(snapshot_version),
              K(table_schema), K(compat_mode));
  } else {
    tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_logservice(clog_checkpoint_ts);
    tablet->tablet_meta_.snapshot_version_ = snapshot_version;
  }
  return ret;
}

int TestCompactionPolicy::construct_array(
    const char *snapshot_list,
    ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(snapshot_list)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(snapshot_list));
  } else {
    array.reset();
    std::string copy(snapshot_list);
    char *org = const_cast<char *>(copy.c_str());
    static const char *delim = " ";
    char *s = std::strtok(org, delim);
    if (NULL != s) {
      array.push_back(atoi(s));
      while (NULL != (s= strtok(NULL, delim))) {
        array.push_back(atoi(s));
      }
    }
  }
  return ret;
}

int TestCompactionPolicy::prepare_medium_list(
    const char *snapshot_list,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet &tablet = *tablet_handle.get_obj();
  construct_array(snapshot_list, array_);
  tablet.medium_info_list_.reset_list();
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); ++i) {
    medium_info_.medium_snapshot_ = array_.at(i);
    ret = tablet.medium_info_list_.add_medium_compaction_info(medium_info_);
  }
  return ret;
}

int TestCompactionPolicy::check_result_tables_handle(
    const char *end_log_ts_list,
    const ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  construct_array(end_log_ts_list, array_);
  if (array_.count() != result.handle_.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "table count is not equal", K(ret), K(array_), K(result.handle_));
  }
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); ++i) {
    if (array_.at(i) != result.handle_.get_table(i)->get_end_scn().get_val_for_tx()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "table is not equal", K(ret), K(i), K(array_.at(i)), KPC(result.handle_.get_table(i)));
    }
  }
  return ret;
}

int TestCompactionPolicy::mock_table_store(
    ObTabletHandle &tablet_handle,
    common::ObIArray<ObTableHandleV2> &major_table_handles,
    common::ObIArray<ObTableHandleV2> &minor_table_handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, 8> major_tables;
  for (int64_t i = 0; OB_SUCC(ret) && i < major_table_handles.count(); ++i) {
    if (OB_FAIL(major_tables.push_back(major_table_handles.at(i).get_table()))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  ObSEArray<ObITable *, 8> minor_tables;
  for (int64_t i = 0; OB_SUCC(ret) && i < minor_table_handles.count(); ++i) {
    if (OB_FAIL(minor_tables.push_back(minor_table_handles.at(i).get_table()))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }

  ObTablet &tablet = *tablet_handle.get_obj();
  ObTabletTableStore &table_store = tablet.table_store_;
  if (OB_SUCC(ret) && major_tables.count() > 0) {
    if (OB_FAIL(table_store.major_tables_.init_and_copy(*tablet.allocator_, major_tables))) {
      LOG_WARN("failed to init major tables", K(ret));
    }
  }

  if (OB_SUCC(ret) && minor_tables.count() > 0) {
    if (OB_FAIL(table_store.minor_tables_.init_and_copy(*tablet.allocator_, minor_tables))) {
      LOG_WARN("failed to init major tables", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_sstables(
  const char *key_data,
  common::ObIArray<ObTableHandleV2> &major_tables,
  common::ObIArray<ObTableHandleV2> &minor_tables)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    ObITable::TableType table_type = (type == 10) ? ObITable::MAJOR_SSTABLE : ((type == 11) ? ObITable::MINOR_SSTABLE : ObITable::MINI_SSTABLE);
    if (OB_FAIL(mock_sstable(table_type, cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), cells[4].get_int(), table_handle))) {
      LOG_WARN("failed to mock sstable", K(ret));
    } else if (ObITable::MAJOR_SSTABLE == table_type) {
      if (OB_FAIL(major_tables.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    } else if (OB_FAIL(minor_tables.push_back(table_handle))) {
      LOG_WARN("failed to add table", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_memtables(
  const char *key_data,
  ObTabletHandle &tablet_handle,
  common::ObIArray<ObTableHandleV2> &memtables)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    assert(0 == type);
    if (OB_FAIL(mock_memtable(cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), *tablet_handle.get_obj(), table_handle))) {
      LOG_WARN("failed to mock memtable", K(ret));
    } else if (OB_FAIL(memtables.push_back(table_handle))) {
      LOG_WARN("failed to add memtable", K(ret));
    }
  }
  return ret;
}

int TestCompactionPolicy::batch_mock_tables(
  const char *key_data,
  common::ObIArray<ObTableHandleV2> &major_tables,
  common::ObIArray<ObTableHandleV2> &minor_tables,
  common::ObIArray<ObTableHandleV2> &memtables,
  ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);

  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  for (int64_t i = 0; i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    ObTableHandleV2 table_handle;
    const int64_t type = cells[0].get_int();
    if (0 == type) {
      if (OB_FAIL(mock_memtable(cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), *tablet_handle.get_obj(), table_handle))) {
        LOG_WARN("failed to mock memtable", K(ret));
      } else if (OB_FAIL(memtables.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    } else {
      ObITable::TableType table_type = (type == 10) ? ObITable::MAJOR_SSTABLE : ((type == 11) ? ObITable::MINOR_SSTABLE : ObITable::MINI_SSTABLE);
      if (OB_FAIL(mock_sstable(table_type, cells[1].get_int(), cells[2].get_int(), cells[3].get_int(), cells[4].get_int(), table_handle))) {
        LOG_WARN("failed to mock sstable", K(ret));
      } else if (ObITable::MAJOR_SSTABLE == table_type) {
        if (OB_FAIL(major_tables.push_back(table_handle))) {
          LOG_WARN("failed to add table", K(ret));
        }
      } else if (OB_FAIL(minor_tables.push_back(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }
  return ret;
}

int TestCompactionPolicy::prepare_tablet(
    const char *key_data,
    const int64_t clog_checkpoint_ts,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  tablet_handle_.reset();
  major_tables_.reset();
  minor_tables_.reset();
  memtables_.reset();

  if (OB_UNLIKELY(clog_checkpoint_ts <= 0 || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(snapshot_version));
  } else if (OB_FAIL(mock_tablet(clog_checkpoint_ts, snapshot_version, tablet_handle_))) {
    LOG_WARN("failed to mock tablet", K(ret));
  } else if (OB_ISNULL(key_data)) {
  } else if (OB_FAIL(batch_mock_tables(key_data, major_tables_, minor_tables_, memtables_, tablet_handle_))) {
    LOG_WARN("failed to batch mock tables", K(ret));
  } else if (OB_FAIL(mock_table_store(tablet_handle_, major_tables_, minor_tables_))) {
    LOG_WARN("failed to mock table store", K(ret));
  }
  return ret;
}

int TestCompactionPolicy::prepare_freeze_info(
  const int64_t snapshot_gc_ts,
  common::ObIArray<ObTenantFreezeInfoMgr::FreezeInfo> &freeze_infos,
  common::ObIArray<share::ObSnapshotInfo> &snapshots)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  bool changed = false;
<<<<<<< HEAD
  SCN min_major_snapshot = SCN::max_scn();
=======
  int64_t min_major_snapshot = INT64_MAX;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

  if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr is unexpected null", K(ret));
<<<<<<< HEAD
  } else if (OB_FAIL(mgr->update_info(snapshot_gc_ts, freeze_infos, snapshots, min_major_snapshot, changed))) {
=======
  } else if (OB_FAIL(mgr->update_info(snapshot_gc_ts,
                                      freeze_infos,
                                      snapshots,
                                      min_major_snapshot,
                                      changed))) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    LOG_WARN("failed to update info", K(ret));
  }
  return ret;
}

class FakeLS : public storage::ObLS
{
public:
  FakeLS() {
    ls_meta_.tenant_id_ = 1001;
    ls_meta_.ls_id_ = ObLSID(100);
  }
  int64_t get_min_reserved_snapshot() { return 10; }
};


static const int64_t TENANT_ID = 1;
static const int64_t TABLE_ID = 7777;
static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
static const int64_t TEST_COLUMN_CNT = 6;

void TestCompactionPolicy::prepare_schema(share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 16 * 1024;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = TABLE_ID;
  share::schema::ObColumnSchemaV2 column;

  //generate data table schema
  table_schema.reset();
  ret = table_schema.set_table_name("test_merge_multi_version");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  const int64_t column_ids[] = {16,17,20,21,22,23,24,29};
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = ObIntType;
    const int64_t column_id = column_ids[i];

    if (i == 1) {
      obj_type = ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema));
}

TEST_F(TestCompactionPolicy, basic_create_sstable)
{
  int ret = OB_SUCCESS;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObTableHandleV2 major_table_handle;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MAJOR_SSTABLE, 0, 100, 100, 100, major_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 mini_table_handle;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MINI_SSTABLE, 100, 120, 120, 120, mini_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableHandleV2 minor_table_handle;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MINOR_SSTABLE, 120, 180, 180, INT64_MAX, minor_table_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestCompactionPolicy, basic_create_tablet)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_NE(nullptr, t3m);

  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(100, 100, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tablet_handle.is_valid());

  ObTablet &tablet = *tablet_handle.get_obj();
  ObTabletTableStore &table_store = tablet.get_table_store();
  ASSERT_EQ(true, table_store.is_valid());
  ASSERT_TRUE(nullptr != tablet.memtable_mgr_);
}

TEST_F(TestCompactionPolicy, basic_create_memtable)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(100, 100, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tablet_handle.is_valid());

  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(tablet_handle.get_obj()->memtable_mgr_);
  ASSERT_EQ(0, mt_mgr->get_memtable_count_());
  ObTableHandleV2 frozen_memtable;
  ret = TestCompactionPolicy::mock_memtable(1, 100, 100, *tablet_handle.get_obj(), frozen_memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, mt_mgr->get_memtable_count_());
  ObMemtable *frozen_mt = static_cast<ObMemtable *>(frozen_memtable.get_table());
  ASSERT_EQ(true, frozen_mt->is_in_prepare_list_of_data_checkpoint());

  ASSERT_EQ(true, frozen_mt->can_be_minor_merged());

  ObTableHandleV2 active_memtable;
  ret = TestCompactionPolicy::mock_memtable(100, INT64_MAX, INT64_MAX, *tablet_handle.get_obj(), active_memtable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, mt_mgr->get_memtable_count_());
  ObMemtable *active_mt = static_cast<ObMemtable *>(active_memtable.get_table());
  ASSERT_EQ(false, active_mt->can_be_minor_merged());
}

TEST_F(TestCompactionPolicy, basic_create_table_store)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ret = TestCompactionPolicy::mock_tablet(100, 100, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSEArray<ObTableHandleV2, 4> major_tables;
  ObTableHandleV2 major_table_handle_1;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MAJOR_SSTABLE, 0, 1, 1, 1, major_table_handle_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major_table_handle_1));

  ObTableHandleV2 major_table_handle_2;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MAJOR_SSTABLE, 0, 100, 100, 100, major_table_handle_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major_table_handle_2));

  ObTableHandleV2 major_table_handle_3;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MAJOR_SSTABLE, 0, 150, 150, 150, major_table_handle_3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major_table_handle_3));


  ObSEArray<ObTableHandleV2, 4> minor_tables;
  ObTableHandleV2 minor_table_handle_1;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MINI_SSTABLE, 100, 150, 150, 160, minor_table_handle_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(minor_table_handle_1));

  ObTableHandleV2 minor_table_handle_2;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MINI_SSTABLE, 150, 200, 190, 200, minor_table_handle_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(minor_table_handle_2));

  ObTableHandleV2 minor_table_handle_3;
  ret = TestCompactionPolicy::mock_sstable(ObITable::MINI_SSTABLE, 200, 350, 350, INT64_MAX, minor_table_handle_3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, minor_tables.push_back(minor_table_handle_3));

  ret = TestCompactionPolicy::mock_table_store(tablet_handle, major_tables, minor_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("Print tablet", KPC(tablet_handle.get_obj()));
}

TEST_F(TestCompactionPolicy, basic_batch_create_sstable)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            100        100        100      \n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n";

  ObArray<ObTableHandleV2> major_tables;
  ObArray<ObTableHandleV2> minor_tables;
  ret = TestCompactionPolicy::batch_mock_sstables(key_data, major_tables, minor_tables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, major_tables.count());
  ASSERT_EQ(2, minor_tables.count());
}

TEST_F(TestCompactionPolicy, basic_prepare_tablet)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            100        100        100      \n"
      "11            1            80         80         120      \n"
      "11            80           150        150        500      \n"
      "0             150          200        180        180      \n"
      "0             200          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletTableStore &table_store = tablet_handle_.get_obj()->table_store_;
  ASSERT_EQ(2, table_store.major_tables_.count());
  ASSERT_EQ(2, table_store.minor_tables_.count());

  ObTabletMemtableMgr *mt_mgr = static_cast<ObTabletMemtableMgr *>(tablet_handle_.get_obj()->memtable_mgr_);
  ASSERT_EQ(2, mt_mgr->get_memtable_count_());
}

TEST_F(TestCompactionPolicy, check_mini_merge_basic)
{
  int ret = OB_SUCCESS;
  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            160        150        150      \n"
      "11            160          300        300        300      \n"
      "0             1            160        150        150      \n"
      "0             160          200        210        210      \n"
      "0             200          300        300        300      \n"
      "0             300          0          0          0        \n";

  ret = prepare_tablet(key_data, 150, 150);
  ASSERT_EQ(OB_SUCCESS, ret);

  FakeLS ls;
  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINI_MERGE;
  ObGetMergeTablesResult result;
  tablet_handle_.get_obj()->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(300);
  tablet_handle_.get_obj()->tablet_meta_.snapshot_version_ = 300;
  ret = ObPartitionMergePolicy::get_mini_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
  ASSERT_EQ(result.update_tablet_directly_, false);
}

TEST_F(TestCompactionPolicy, check_minor_merge_basic)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<ObTenantFreezeInfoMgr::FreezeInfo> freeze_info;
  common::ObArray<share::ObSnapshotInfo> snapshots;
  SCN scn;
  scn.convert_for_tx(1);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info, snapshots);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, result.handle_.get_count());
}

TEST_F(TestCompactionPolicy, check_no_need_minor_merge)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<ObTenantFreezeInfoMgr::FreezeInfo> freeze_info;
  common::ObArray<share::ObSnapshotInfo> snapshots;
  SCN scn;
  scn.convert_for_tx(1);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));
  scn.convert_for_tx(320);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));
  scn.convert_for_tx(400);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info, snapshots);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          310        310        310      \n"
      "11            310          375        375        375      \n";

  ret = prepare_tablet(key_data, 375, 375);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, check_major_merge_basic)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<ObTenantFreezeInfoMgr::FreezeInfo> freeze_info;
  common::ObArray<share::ObSnapshotInfo> snapshots;
  SCN scn;
  scn.convert_for_tx(1);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));
  scn.convert_for_tx(340);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info, snapshots);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            1            150        150        150      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          350        350        350      \n";

  ret = prepare_tablet(key_data, 350, 350);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.merge_version_ = 350;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_medium_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, result.handle_.get_count());
}

TEST_F(TestCompactionPolicy, check_no_need_major_merge)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<ObTenantFreezeInfoMgr::FreezeInfo> freeze_info;
  common::ObArray<share::ObSnapshotInfo> snapshots;
  SCN scn;
  scn.convert_for_tx(1);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));
  scn.convert_for_tx(340);
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(scn, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info, snapshots);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "10            0            340        340        340      \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          340        340        340      \n";

  ret = prepare_tablet(key_data, 340, 340);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.merge_version_ = 340;
  ObGetMergeTablesResult result;
  FakeLS ls;
  ret = ObPartitionMergePolicy::get_medium_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);
}

TEST_F(TestCompactionPolicy, test_minor_with_medium)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_TRUE(nullptr != mgr);

  common::ObArray<ObTenantFreezeInfoMgr::FreezeInfo> freeze_info;
  common::ObArray<share::ObSnapshotInfo> snapshots;
  share::SCN scn;
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(1, 1, 0)));
  ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObTenantFreezeInfoMgr::FreezeInfo(140, 1, 0)));

  ret = TestCompactionPolicy::prepare_freeze_info(500, freeze_info, snapshots);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *key_data =
      "table_type    start_scn    end_scn    max_ver    upper_ver\n"
      "10            0            1          1          1        \n"
      "11            150          200        200        200      \n"
      "11            200          250        250        250      \n"
      "11            250          300        300        300      \n"
      "11            300          340        340        340      \n";

  ret = prepare_tablet(key_data, 340, 340);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObGetMergeTablesParam param;
  param.merge_type_ = ObMergeType::MINOR_MERGE;
  param.merge_version_ = 0;
  ObGetMergeTablesResult result;
  FakeLS ls;

  prepare_medium_list("240", tablet_handle_);
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_result_tables_handle("250, 300, 340", result));

  prepare_medium_list("150", tablet_handle_);
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, check_result_tables_handle("200, 250, 300, 340", result));

  prepare_medium_list("300", tablet_handle_);
  ret = ObPartitionMergePolicy::get_minor_merge_tables(param, ls, *tablet_handle_.get_obj(), result);
  ASSERT_EQ(OB_NO_NEED_MERGE, ret);

}

} //unittest
} //oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_compaction_policy.log");
  OB_LOGGER.set_file_name("test_compaction_policy.log");
  OB_LOGGER.set_log_level("DEBUG");
  CLOG_LOG(INFO, "begin unittest: test_compaction_policy");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
