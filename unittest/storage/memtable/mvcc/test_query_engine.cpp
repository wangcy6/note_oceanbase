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

#include "storage/memtable/mvcc/ob_query_engine.h"

#include "storage/memtable/ob_memtable_key.h"
#include "lib/atomic/ob_atomic.h"

#include "../utils_rowkey_builder.h"
#include "../utils_mod_allocator.h"

#include <gtest/gtest.h>
#include <thread>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::keybtree;
using namespace oceanbase::memtable;
using ObQueryEngineIterator = ObQueryEngine::Iterator<BtreeIterator<ObStoreRowkeyWrapper, ObMvccRow *>>;

TEST(TestObQueryEngine, get_and_set_table_index_node)
{
  // This is a concurrency scene, use multi-thread test.
  constexpr uint64_t TABLE_COUNT_LIMIT = (1 << 14);
  constexpr int64_t THREAD_COUNT = 20;
  ObModAllocator allocator;
  ObQueryEngine qe(allocator);
  ObQueryEngine::TableIndex *table_index = nullptr;
  uint64_t counter = 0;
  int ret = OB_SUCCESS;

  ret = qe.init(1);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::thread threads[THREAD_COUNT];
  for (int64_t i = 0; i < THREAD_COUNT; ++i) {
    threads[i] = std::thread([&]() {
      int ret = OB_SUCCESS;
      ObQueryEngine::TableIndex *tmp_ptr = nullptr;
      if (OB_FAIL(qe.set_table_index(i, tmp_ptr))) {
        TRANS_LOG(WARN, "", KR(ret));
      } else {
        ATOMIC_AAF(&counter, 1);
        ObQueryEngine::TableIndex *cmp_ptr = nullptr;
        EXPECT_NE(nullptr, tmp_ptr);
        EXPECT_EQ(OB_SUCCESS, qe.get_table_index(cmp_ptr));
        EXPECT_EQ(tmp_ptr, cmp_ptr);
      }
    });
  }

  for (int64_t i = 0; i < THREAD_COUNT; ++i) {
    threads[i].join();
  }

  EXPECT_EQ(counter, THREAD_COUNT);
}

TEST(TestObQueryEngine, smoke_test)
{
  static const int64_t R_COUNT = 6;

  int ret = OB_SUCCESS;
  ObModAllocator allocator;
  ObQueryEngine qe(allocator);
  ObMemtableKey *mtk[R_COUNT];
  ObMvccTransNode tdn[R_COUNT];
  ObMvccRow mtv[R_COUNT];

  auto init_mtv = [&](ObMvccRow &mtv, ObMvccTransNode &node) {
    mtv.list_head_ = &node;
  };
  auto test_set_and_get = [&](ObMemtableKey *mtk, ObMvccRow &mtv) {
    int ret = OB_SUCCESS;
    ret = qe.set(mtk, &mtv);
    EXPECT_EQ(OB_SUCCESS, ret);
    ObMemtableKey t;
    ObMvccRow *v = nullptr;
    ret = qe.get(mtk, v, &t);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(v, &mtv);
  };
  auto test_ensure = [&](ObMemtableKey *mtk, ObMvccRow &mtv) {
    int ret = OB_SUCCESS;
    ret = qe.ensure(mtk, &mtv);
    EXPECT_EQ(OB_SUCCESS, ret);
  };
  auto test_scan = [&](int64_t start, bool include_start, int64_t end, bool include_end) {
    ObIQueryEngineIterator *iter = nullptr;
    ret = qe.scan(mtk[start], !include_start, mtk[end], !include_end, 1, iter);
    bool skip_purge_memtable = false;
    EXPECT_EQ(OB_SUCCESS, ret);
    if (start <= end) {
      for (int64_t i = (include_start ? start : (start + 1)); i <= (include_end ? end : (end - 1)); i++) {
        ret = iter->next(skip_purge_memtable);
        EXPECT_EQ(OB_SUCCESS, ret);
        assert(0 == mtk[i]->compare(*iter->get_key()));
        EXPECT_EQ(&mtv[i], iter->get_value());
      }
    } else {
      for (int64_t i = (include_start ? start : (start - 1)); i >= (include_end ? end : (end + 1)); i--) {
        ret = iter->next(skip_purge_memtable);
        EXPECT_EQ(OB_SUCCESS, ret);
        assert(0 == mtk[i]->compare(*iter->get_key()));       
        EXPECT_EQ(&mtv[i], iter->get_value());
      }
    }
    ret = iter->next(skip_purge_memtable);
    EXPECT_EQ(OB_ITER_END, ret);
    // if QueryEngine::Iterator returnes ITER_END, inner iter will be freed.
    ret = iter->next(skip_purge_memtable);
    EXPECT_EQ(OB_ITER_END, ret);
  };

  ret = qe.init(1);
  EXPECT_EQ(OB_SUCCESS, ret);

  INIT_MTK(allocator, mtk[0], V("aaaa", 4), I(1024), N("1234567890.01234567890"));
  INIT_MTK(allocator, mtk[1], V("aaaa", 4), I(1024), N("1234567890.01234567891"));
  INIT_MTK(allocator, mtk[2], V("aaaa", 4), I(2048), N("1234567890.01234567890"));
  INIT_MTK(allocator, mtk[3], V("aaaa", 4), I(2048), N("1234567890.01234567891"));
  INIT_MTK(allocator, mtk[4], V("bbbb", 4), I(2048), N("1234567890.01234567890"));
  INIT_MTK(allocator, mtk[5], V("bbbb", 4), I(2048), N("1234567890.01234567891"));

  init_mtv(mtv[0], tdn[0]);
  init_mtv(mtv[1], tdn[1]);
  init_mtv(mtv[2], tdn[2]);
  init_mtv(mtv[3], tdn[3]);
  init_mtv(mtv[4], tdn[4]);
  init_mtv(mtv[5], tdn[5]);

  test_set_and_get(mtk[0], mtv[0]);
  test_set_and_get(mtk[1], mtv[1]);
  test_set_and_get(mtk[2], mtv[2]);
  test_set_and_get(mtk[3], mtv[3]);
  test_set_and_get(mtk[4], mtv[4]);
  test_set_and_get(mtk[5], mtv[5]);

  // every hashtable will be inited with 128, every set has 1/1024 percent of expanding(1024).
  // keys with different table_id will be inserted into different btree.
  assert(128 == qe.hash_size() % (1 << 10));
  assert(R_COUNT >= (qe.hash_size() - 128) / (1 << 10));

  test_ensure(mtk[0], mtv[0]);
  test_ensure(mtk[1], mtv[1]);
  test_ensure(mtk[2], mtv[2]);
  test_ensure(mtk[3], mtv[3]);
  test_ensure(mtk[4], mtv[4]);
  test_ensure(mtk[5], mtv[5]);

  EXPECT_EQ(R_COUNT, qe.btree_size());

  test_scan(0, true,  5, true);
  test_scan(0, false, 5, true);
  test_scan(0, true,  5, false);
  test_scan(0, false, 5, false);

  test_scan(5, true,  0, true);
  test_scan(5, false, 0, true);
  test_scan(5, true,  0, false);
  test_scan(5, false, 0, false);

  test_scan(1, true,  4, true);
  test_scan(1, false, 4, true);
  test_scan(1, true,  4, false);
  test_scan(1, false, 4, false);

  test_scan(4, true,  1, true);
  test_scan(4, false, 1, true);
  test_scan(4, true,  1, false);
  test_scan(4, false, 1, false);

  test_scan(0, true,  0, true);
  test_scan(1, true,  1, true);
  test_scan(2, true,  2, true);
  test_scan(3, true,  3, true);
  test_scan(4, true,  4, true);
  test_scan(5, true,  5, true);

  test_scan(0, false,  0, false);
  test_scan(1, false,  1, false);
  test_scan(2, false,  2, false);
  test_scan(3, false,  3, false);
  test_scan(4, false,  4, false);
  test_scan(5, false,  5, false);
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_query_engine.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
