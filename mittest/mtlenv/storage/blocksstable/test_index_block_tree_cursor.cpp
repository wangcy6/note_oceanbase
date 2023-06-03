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
#define private public
#define protected public

#include "storage/blocksstable/ob_index_block_tree_cursor.h"
#include "storage/blocksstable/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{

class TestIndexBlockTreeCursor : public TestIndexBlockDataPrepare
{
public:
  TestIndexBlockTreeCursor();
  virtual ~TestIndexBlockTreeCursor() {}
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
};

TestIndexBlockTreeCursor::TestIndexBlockTreeCursor()
  : TestIndexBlockDataPrepare("Test index block tree cursor")
{
}

void TestIndexBlockTreeCursor::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestIndexBlockTreeCursor::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestIndexBlockTreeCursor::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestIndexBlockTreeCursor::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

TEST_F(TestIndexBlockTreeCursor, test_path)
{
  ObIndexBlockTreePath tree_path;
  ObIndexBlockTreePathItem *curr_item;
  ASSERT_EQ(OB_NOT_INIT, tree_path.get_next_item_ptr(curr_item));
  ASSERT_EQ(OB_SUCCESS, tree_path.init());
  ASSERT_EQ(OB_SUCCESS, tree_path.get_next_item_ptr(curr_item));
  for (int64_t i = 0; i <= ObIndexBlockTreePath::PathItemStack::MAX_TREE_FIX_BUF_LENGTH + 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, tree_path.push(curr_item));
    ASSERT_EQ(OB_SUCCESS, tree_path.get_next_item_ptr(curr_item));
  }
  for (int64_t i = 0; i <= ObIndexBlockTreePath::PathItemStack::MAX_TREE_FIX_BUF_LENGTH + 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, tree_path.pop(curr_item));
    ASSERT_EQ(OB_SUCCESS, tree_path.get_next_item_ptr(curr_item));
  }
  ASSERT_EQ(OB_ERR_UNEXPECTED, tree_path.pop(curr_item));
}

TEST_F(TestIndexBlockTreeCursor, test_normal)
{
  STORAGE_LOG(INFO, "normal test start");
  uint64_t tenant_id = table_schema_.get_tenant_id();
  ObIndexBlockTreeCursor tree_cursor;
  ASSERT_EQ(OB_SUCCESS, tree_cursor.init(sstable_, allocator_, &tablet_handle_.get_obj()->get_index_read_info()));

  const int64_t query_row_seed = max_row_seed_ - 5;
  const int64_t large_query_row_seed = max_row_seed_ + 1;
  const int64_t small_query_row_seed = 0;
  ObDatumRow query_row;
  ASSERT_EQ(OB_SUCCESS, query_row.init(allocator_, TEST_COLUMN_CNT + 1));
  row_generate_.get_next_row(query_row_seed, query_row);
  ObDatumRowkey query_rowkey;
  query_rowkey.assign(query_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  STORAGE_LOG(INFO, "Query rowkey", K(query_row));
  const ObIndexBlockRowParser *idx_row_parser = nullptr;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  bool is_beyond_range = false;
  ASSERT_EQ(OB_SUCCESS, tree_cursor.drill_down(
      query_rowkey, ObIndexBlockTreeCursor::MACRO, is_beyond_range));
  ASSERT_FALSE(is_beyond_range);
  ASSERT_EQ(OB_SUCCESS, tree_cursor.get_idx_parser(idx_row_parser));
  ASSERT_EQ(OB_SUCCESS, idx_row_parser->get_header(idx_row_header));
  ASSERT_TRUE(idx_row_header->is_macro_node());
  STORAGE_LOG(DEBUG, "Show index row", K(tree_cursor.row_), K(tree_cursor.curr_path_item_));
  ObArray<ObDatumRowkey> endkeys;
  ObArray<ObMicroIndexInfo> index_infos;
  ObIndexBlockTreePathItem hold_item;
  ObObj *allocated_buf = nullptr;
  ObDatumRange all_range;
  all_range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, tree_cursor.get_child_micro_infos(
      all_range, allocator_, endkeys, index_infos, hold_item));
  STORAGE_LOG(DEBUG, "Endkeys: ", K(endkeys));
  STORAGE_LOG(DEBUG, "Micro index infos:", K(index_infos));


  ASSERT_EQ(OB_SUCCESS, tree_cursor.pull_up_to_root());
  ASSERT_EQ(OB_SUCCESS, tree_cursor.drill_down(
      query_rowkey, ObIndexBlockTreeCursor::LEAF, is_beyond_range));
  ASSERT_FALSE(is_beyond_range);

  ASSERT_EQ(OB_SUCCESS, tree_cursor.pull_up_to_root());
  ASSERT_EQ(OB_SUCCESS, tree_cursor.drill_down(
      query_rowkey, ObIndexBlockTreeCursor::LEAF, is_beyond_range));
  ASSERT_FALSE(is_beyond_range);
  ASSERT_EQ(OB_SUCCESS, tree_cursor.idx_row_parser_.get_header(idx_row_header));
  ASSERT_TRUE(idx_row_header->is_data_block());
  ASSERT_EQ(OB_ITER_END, tree_cursor.drill_down());

  // Query Rowkey larger than sstable range
  row_generate_.get_next_row(large_query_row_seed, query_row);
  query_rowkey.assign(query_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  STORAGE_LOG(DEBUG, "Large query rowkey", K(query_rowkey));
  ASSERT_EQ(OB_SUCCESS, tree_cursor.pull_up_to_root());
  STORAGE_LOG(DEBUG, "Root block item", KPC(tree_cursor.curr_path_item_));
  ASSERT_EQ(OB_SUCCESS, tree_cursor.drill_down(
      query_rowkey, ObIndexBlockTreeCursor::LEAF, is_beyond_range));
  ASSERT_TRUE(is_beyond_range);

  tree_cursor.idx_row_parser_.get_header(idx_row_header);
  STORAGE_LOG(DEBUG, "Large query rowkey cursor", K(tree_cursor.row_), KPC(tree_cursor.curr_path_item_), KPC(idx_row_header));

  // Query Rowkey smaller than sstable range
  row_generate_.get_next_row(small_query_row_seed, query_row);
  query_rowkey.assign(query_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  ASSERT_EQ(OB_SUCCESS, tree_cursor.pull_up_to_root());
  ASSERT_EQ(OB_SUCCESS, tree_cursor.drill_down(
      query_rowkey, ObIndexBlockTreeCursor::MACRO, is_beyond_range));
  ASSERT_FALSE(is_beyond_range);
  int tmp_ret = OB_SUCCESS;
  int cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    ASSERT_EQ(OB_SUCCESS, tmp_ret);
    tree_cursor.idx_row_parser_.get_header(idx_row_header);
    STORAGE_LOG(DEBUG, "Show curr macro row", K(tree_cursor.row_), KPC(idx_row_header), K(cnt));
    tmp_ret = tree_cursor.move_forward(false);
    ++cnt;
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
}

TEST_F(TestIndexBlockTreeCursor, test_macro_iter)
{
  ObMicroBlockData root_block;
  sstable_.get_index_tree_root(tablet_handle_.get_obj()->get_index_read_info(), root_block);
  ASSERT_TRUE(nullptr != root_block.get_extra_buf());
  ObIndexBlockMacroIterator macro_iter;
  MacroBlockId macro_block_id;
  int tmp_ret = OB_SUCCESS;
  int64_t cnt = 0;
  ObDatumRange iter_range;
  iter_range.set_whole_range();

  // reverse whole scan
  ASSERT_EQ(OB_SUCCESS, macro_iter.open(
      sstable_, iter_range, tablet_handle_.get_obj()->get_index_read_info(), allocator_, true, true));
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = macro_iter.get_next_macro_block(macro_block_id);
    STORAGE_LOG(DEBUG, "Reverse get next macro block", K(tmp_ret), K(cnt),
        K(macro_block_id), K(macro_iter.micro_endkeys_.at(macro_iter.micro_endkeys_.count() - 1)));
    if (OB_SUCCESS == tmp_ret) {
      ++cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(cnt, data_macro_block_cnt_);

  // sequential whole scan
  macro_iter.reset();
  ASSERT_EQ(OB_SUCCESS, macro_iter.open(
      sstable_,
      iter_range,
      tablet_handle_.get_obj()->get_index_read_info(),
      allocator_,
      false,
      true));
  cnt = 0;
  tmp_ret = OB_SUCCESS;
  ObMacroBlockDesc macro_desc;
  while (OB_SUCCESS == tmp_ret) {
    macro_desc.reset();
    tmp_ret = macro_iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == tmp_ret) {
      ++cnt;
      const ObIArray<blocksstable::ObMicroIndexInfo> &index_infos = macro_iter.get_micro_index_infos();
      int64_t offset = 0;
      for (int64_t i = 0; i < index_infos.count(); ++i) {
        const ObMicroIndexInfo &info = index_infos.at(i);
        if (0 == i) {
          offset = info.get_block_offset() + info.get_block_size();
        } else {
          ASSERT_EQ(info.get_block_offset(), offset);
          offset += info.get_block_size();
        }
        ASSERT_EQ(info.get_macro_id(), macro_desc.macro_block_id_);
      }
      STORAGE_LOG(DEBUG, "Show Macro block descriptor", K(macro_desc), K(cnt));
      ASSERT_TRUE(macro_desc.is_valid());
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(cnt, data_macro_block_cnt_);

  // scan range lower than sstable first rowkey
  ObDatumRow row;
  ObDatumRowkey first_macro_endkey;
  MacroBlockId first_macro_id;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(min_row_seed_ - 1, row));
  macro_iter.reset();
  iter_range.reset();
  iter_range.start_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  iter_range.end_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  iter_range.border_flag_.set_inclusive_start();
  iter_range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, macro_iter.open(
      sstable_, iter_range, tablet_handle_.get_obj()->get_index_read_info(), allocator_, false, true));
  ASSERT_EQ(OB_SUCCESS, macro_iter.get_next_macro_block(macro_desc));
  ASSERT_TRUE(macro_desc.is_valid());
  ASSERT_TRUE(macro_desc.range_.get_start_key().is_min_rowkey());
  // deep copy first endkey
  int64_t copy_size = macro_desc.range_.end_key_.get_deep_copy_size();
  char *key_buf = reinterpret_cast<char *>(allocator_.alloc(copy_size));
  ASSERT_NE(nullptr, key_buf);
  macro_desc.range_.end_key_.deep_copy(first_macro_endkey, key_buf, copy_size);
  first_macro_id = macro_desc.macro_block_id_;
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next_macro_block(macro_desc));

  // scan range with start key equal to endkey
  iter_range.reset();
  macro_iter.reset();
  iter_range.start_key_ = first_macro_endkey;
  iter_range.end_key_ = first_macro_endkey;
  iter_range.border_flag_.set_inclusive_start();
  iter_range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, macro_iter.open(
      sstable_, iter_range, tablet_handle_.get_obj()->get_index_read_info(), allocator_, false, true));
  ASSERT_EQ(OB_SUCCESS, macro_iter.get_next_macro_block(macro_desc));
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next_macro_block(macro_desc));

  // scan major sstable with start key is not multi-version rowkey and left bound is not inclusive
  iter_range.reset();
  macro_iter.reset();
  iter_range.start_key_.assign(first_macro_endkey.datums_, TEST_ROWKEY_COLUMN_CNT);
  iter_range.end_key_.set_max_rowkey();
  iter_range.border_flag_.unset_inclusive_start();
  iter_range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, macro_iter.open(
      sstable_, iter_range, tablet_handle_.get_obj()->get_index_read_info(), allocator_, false, true));
  ASSERT_EQ(OB_SUCCESS, macro_iter.get_next_macro_block(macro_desc));
  ASSERT_NE(macro_desc.macro_block_id_, first_macro_id);
}

TEST_F(TestIndexBlockTreeCursor, test_bare_micro_block_iterator)
{
  ObIndexBlockMacroIterator macro_iter;
  ObDatumRange iter_range;
  iter_range.set_whole_range();
  uint64_t tenant_id = table_schema_.get_tenant_id();

  ASSERT_EQ(OB_SUCCESS, macro_iter.open(
      sstable_,
      iter_range,
      tablet_handle_.get_obj()->get_index_read_info(),
      allocator_,
      false,
      true));

  MacroBlockId macro_block_id;
  ASSERT_EQ(OB_SUCCESS, macro_iter.get_next_macro_block(macro_block_id));

  ObMacroBlockReadInfo read_info;
  ObMacroBlockHandle macro_handle;
  read_info.macro_block_id_ = macro_block_id;
  read_info.offset_ = 0;
  read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  ASSERT_EQ(OB_SUCCESS, ObBlockManager::async_read_block(read_info, macro_handle));
  ASSERT_EQ(OB_SUCCESS, macro_handle.wait(DEFAULT_IO_WAIT_TIME_MS));

  ObMicroBlockBareIterator micro_bare_iter;
  ObMicroBlockData micro_data;
  ASSERT_EQ(OB_SUCCESS,
      micro_bare_iter.open(macro_handle.get_buffer(), macro_handle.get_data_size(), true, true));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = micro_bare_iter.get_next_micro_block_data(micro_data);
  }
  ASSERT_EQ(tmp_ret, OB_ITER_END);
}

} // end blocksstable
} // end oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_block_tree_cursor.log*");
  OB_LOGGER.set_file_name("test_index_block_tree_cursor.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
