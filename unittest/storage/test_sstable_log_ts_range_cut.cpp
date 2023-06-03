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

#include <gtest/gtest.h>
#include "storage/blocksstable/ob_data_buffer.h"
#define protected public
#define private public
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/ob_i_table.h"
#include "storage/ob_storage_struct.h"
#include "share/scn.h"
<<<<<<< HEAD
=======
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace omt;
using namespace share;
namespace unittest
{

class TestMockSSTable
{
public:
  static void generate_mock_sstable(
      const int64_t start_log_ts,
      const int64_t end_log_ts,
      ObSSTable &sstable)
  {
    sstable.key_.table_type_ = ObITable::MINOR_SSTABLE;
    sstable.key_.tablet_id_ = 1;
    sstable.key_.scn_range_.start_scn_.convert_for_gts(start_log_ts);
    sstable.key_.scn_range_.end_scn_.convert_for_gts(end_log_ts);
<<<<<<< HEAD
=======
    sstable.meta_.basic_meta_.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    sstable.meta_.basic_meta_.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    sstable.valid_for_reading_ = true;
    sstable.meta_.basic_meta_.status_ = SSTABLE_WRITE_BUILDING;
    sstable.meta_.data_root_info_.addr_.set_none_addr();
    sstable.meta_.macro_info_.macro_meta_info_.addr_.set_none_addr();
    sstable.meta_.basic_meta_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  }
};


class TestSSTableScnRangeCut : public ::testing::Test
{
public:
  TestSSTableScnRangeCut()
    : tenant_id_(1),
      t3m_(nullptr),
      tenant_base_(1)
  { }
  ~TestSSTableScnRangeCut() {}
  void SetUp()
  {
    t3m_ = OB_NEW(ObTenantMetaMemMgr, ObModIds::TEST, 1);
    t3m_->init();
    tenant_base_.set(t3m_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  }
  void TearDown()
  {
    t3m_->~ObTenantMetaMemMgr();
    t3m_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
private:
  const uint64_t tenant_id_;
  ObTenantMetaMemMgr *t3m_;
  ObTenantBase tenant_base_;
  ObTabletTableStore tablet_table_store_;
  DISALLOW_COPY_AND_ASSIGN(TestSSTableScnRangeCut);
};

//normal condition
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [100, 200)
//sstable3 log_ts: [200,300)
<<<<<<< HEAD
TEST(ObTabletTableStore, sstable_scn_range_no_cross_and_continue)
=======

TEST_F(TestSSTableScnRangeCut, sstable_scn_range_no_cross_and_continue)
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(100, 200, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(200, 300, sstable3);
  ObArray<ObITable *> minor_sstables;
  ObTablesHandleArray tables_handle;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
<<<<<<< HEAD
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables);
=======
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables, tables_handle);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  ASSERT_EQ(OB_SUCCESS, ret);
  tables_handle.meta_mem_mgr_ = nullptr;

<<<<<<< HEAD
  ASSERT_EQ(0, sstable1.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, sstable1.key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, sstable2.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, sstable2.key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, sstable3.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(300, sstable3.key_.scn_range_.end_scn_.get_val_for_inner_table_field());
=======
  ASSERT_EQ(0, tables_handle.get_table(0)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, tables_handle.get_table(0)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, tables_handle.get_table(1)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, tables_handle.get_table(1)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, tables_handle.get_table(2)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(300, tables_handle.get_table(2)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
}


//sstable log ts is not continue with other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [200, 300)
//sstable3 log_ts: [300,500)

<<<<<<< HEAD
TEST(ObTabletTableStore, sstable_scn_range_is_not_continue)
=======
TEST_F(TestSSTableScnRangeCut, sstable_scn_range_is_not_continue)
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(200, 300, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(300, 500, sstable3);

  ObArray<ObITable *> minor_sstables;
  ObTablesHandleArray tables_handle;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
<<<<<<< HEAD
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables);
=======
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables, tables_handle);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  tables_handle.meta_mem_mgr_ = nullptr;
}


//sstable log ts contain other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [0, 200)
//sstable3 log_ts: [200,500)

<<<<<<< HEAD
TEST(ObTabletTableStore, sstable_scn_range_contain)
=======
TEST_F(TestSSTableScnRangeCut, sstable_scn_range_contain)
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(0, 200, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(200, 500, sstable3);

  ObArray<ObITable *> minor_sstables;
  ObTablesHandleArray tables_handle;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
<<<<<<< HEAD
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables);
=======
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables, tables_handle);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  ASSERT_EQ(OB_SUCCESS, ret);
  tables_handle.meta_mem_mgr_ = nullptr;

<<<<<<< HEAD
  ASSERT_EQ(0, sstable1.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, sstable1.key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, sstable2.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, sstable2.key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, sstable3.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(500, sstable3.key_.scn_range_.end_scn_.get_val_for_inner_table_field());
=======
  ASSERT_EQ(0, tables_handle.get_table(0)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, tables_handle.get_table(0)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, tables_handle.get_table(1)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, tables_handle.get_table(1)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, tables_handle.get_table(2)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(500, tables_handle.get_table(2)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

}

//sstable log ts contain other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [50, 200)
//sstable3 log_ts: [200,500)

<<<<<<< HEAD
TEST(ObTabletTableStore, sstable_scn_range_has_overlap)
=======
TEST_F(TestSSTableScnRangeCut, sstable_scn_range_has_overlap)
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(50, 200, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(200, 500, sstable3);

  ObArray<ObITable *> minor_sstables;
  ObTablesHandleArray tables_handle;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
<<<<<<< HEAD
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables);
=======
  ret = tablet_table_store.cut_ha_sstable_scn_range_(minor_sstables, tables_handle);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  ASSERT_EQ(OB_SUCCESS, ret);
  tables_handle.meta_mem_mgr_ = nullptr;

<<<<<<< HEAD
  ASSERT_EQ(0, sstable1.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, sstable1.key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, sstable2.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, sstable2.key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, sstable3.key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(500, sstable3.key_.scn_range_.end_scn_.get_val_for_inner_table_field());
=======
  ASSERT_EQ(0, tables_handle.get_table(0)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, tables_handle.get_table(0)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, tables_handle.get_table(1)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, tables_handle.get_table(1)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, tables_handle.get_table(2)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(500, tables_handle.get_table(2)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
}


}//blocksstable
}//oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_log_ts_range_cut.log*");
  OB_LOGGER.set_file_name("test_sstable_log_ts_range_cut.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40L << 30);
  return RUN_ALL_TESTS();
}
