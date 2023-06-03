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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_px_resource_analyzer.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{
class SchedOrderGenerator {
public:
  SchedOrderGenerator() = default;
  ~SchedOrderGenerator() = default;
  int generate(DfoInfo &root, ObIArray<DfoInfo *> &edges);
};
}
}

// 后序遍历 dfo tree 即为调度顺序
// 用 edges 数组表示这种顺序
// 注意：
// 1. root 节点本身不会记录到 edge 中
// 2. 不需要做 normalize，因为我们这个文件只负责估计线程数，不做也可以得到相同结果
int SchedOrderGenerator::generate(
    DfoInfo &root,
    ObIArray<DfoInfo *> &edges)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root.get_child_count(); ++i) {
    DfoInfo *child = NULL;
    if (OB_FAIL(root.get_child(i, child))) {
      LOG_WARN("fail get child dfo", K(i), K(root), K(ret));
    } else if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(generate(*child, edges))) {
      LOG_WARN("fail do generate edge", K(*child), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(edges.push_back(&root))) {
      LOG_WARN("fail add edge to array", K(ret));
    }
  }
  return ret;
}



// ===================================================================================
// ===================================================================================
// ===================================================================================
// ===================================================================================



int DfoInfo::add_child(DfoInfo *child)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(child_dfos_.push_back(child))) {
    LOG_WARN("fail push back child to array", K(ret));
  }
  return ret;
}

int DfoInfo::get_child(int64_t idx, DfoInfo *&child)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= child_dfos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child idx unexpected", K(idx), "cnt", child_dfos_.count(), K(ret));
  } else if (OB_FAIL(child_dfos_.at(idx, child))) {
    LOG_WARN("fail get element", K(idx), "cnt", child_dfos_.count(), K(ret));
  }
  return ret;
}




// ===================================================================================
// ===================================================================================
// ===================================================================================
// ===================================================================================



ObPxResourceAnalyzer::ObPxResourceAnalyzer()
  : dfo_allocator_(CURRENT_CONTEXT->get_malloc_allocator())
{
  dfo_allocator_.set_label("PxResourceAnaly");
}

// entry function
int ObPxResourceAnalyzer::analyze(
    ObLogicalOperator &root_op,
    int64_t &max_parallel_thread_count,
    int64_t &max_parallel_group_count,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  // 本函数用于分析一个 PX 计划至少需要预留多少组线程才能被调度成功
  //
  // 需要考虑如下场景：
  //  1. multiple px
  //  2. subplan filter rescan (所有被标记为 rescanable 的 exchange 节点都是 QC)
  //  3. bushy tree scheduling
  //
  // 算法：
  // 1. 按照 dfo 调度算法生成调度顺序
  // 2. 然后模拟调度，每调度一对 dfo，就将 child 设置为 done，然后统计当前时刻多少个未完成 dfo
  // 3. 如此继续调度，直至所有 dfo 调度完成
  //
  // ref:
  ObArray<PxInfo> px_trees;
  if (OB_FAIL(convert_log_plan_to_nested_px_tree(px_trees, root_op))) {
    LOG_WARN("fail convert log plan to nested px tree", K(ret));
  } else if (OB_FAIL(walk_through_px_trees(px_trees,
                                           max_parallel_thread_count,
                                           max_parallel_group_count,
                                           max_parallel_thread_map,
                                           max_parallel_group_map))) {
    LOG_WARN("fail calc max parallel thread group count for resource reservation", K(ret));
  }
  reset_px_tree(px_trees);
  return ret;
}

void ObPxResourceAnalyzer::reset_px_tree(ObIArray<PxInfo> &px_trees)
{
  for (int i = 0; i < px_trees.count(); ++i) {
    px_trees.at(i).reset_dfo();
  }
}

int ObPxResourceAnalyzer::convert_log_plan_to_nested_px_tree(
    ObIArray<PxInfo> &px_trees,
    ObLogicalOperator &root_op)
{
  int ret = OB_SUCCESS;
  // 算法逻辑上分为两步走：
  // 1. qc 切分：顶层 qc ，subplan filter  右侧 qc
  // 2. 各个 qc 分别算并行度（zigzag, left-deep, right-deep, bushy）
  //
  // 具体实现上，两件事情并在一个流程里做，更简单些
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
      static_cast<const ObLogExchange *>(&root_op)->is_px_consumer()) {
    // 当前 exchange 是一个 QC，将下面的所有子计划抽象成一个 dfo tree
    if (OB_FAIL(create_dfo_tree(px_trees, static_cast<ObLogExchange &>(root_op)))) {
      LOG_WARN("fail create dfo tree", K(ret));
    }
  } else {
    int64_t num = root_op.get_num_of_child();
    for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
      if (nullptr == root_op.get_child(child_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
      } else if (OB_FAIL(SMART_CALL(convert_log_plan_to_nested_px_tree(
                  px_trees, *root_op.get_child(child_idx))))) {
        LOG_WARN("fail split px tree", K(child_idx), K(num), K(ret));
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::create_dfo_tree(
    ObIArray<PxInfo> &px_trees,
    ObLogExchange &root_op)
{
  int ret = OB_SUCCESS;
  // 以 root_op 为根节点创建一个 dfo tree
  // root_op 的类型一定是 EXCHANGE OUT DIST

  // 在向下遍历构造 dfo tree 时，如果遇到 subplan filter 右侧的 exchange，
  // 则将其也转化成一个独立的 dfo tree
  PxInfo px_info;
  DfoInfo *root_dfo = nullptr;
  px_info.root_op_ = &root_op;
  ObLogicalOperator *child = root_op.get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exchange out op should always has a child",
             "type", root_op.get_type(), KP(child), K(ret));

  } else if (log_op_def::LOG_EXCHANGE != child->get_type() ||
             static_cast<const ObLogExchange *>(child)->is_px_consumer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect a px producer below qc op", "type", root_op.get_type(), K(ret));
  } else if (OB_FAIL(do_split(px_trees, px_info, *child, root_dfo))) {
    LOG_WARN("fail split dfo for current dfo tree", K(ret));
  } else if (OB_FAIL(px_trees.push_back(px_info))) { // 先遇到的 px 后进入 px_tree，无妨
    LOG_WARN("fail push back root dfo to dfo tree collector", K(ret));
    px_info.reset_dfo();
  }
  return ret;
}


int ObPxResourceAnalyzer::do_split(
    ObIArray<PxInfo> &px_trees,
    PxInfo &px_info,
    ObLogicalOperator &root_op,
    DfoInfo *parent_dfo)
{
  int ret = OB_SUCCESS;
  // 遇到 subplan filter 右边的 exchange，都将它转成独立的 px tree，并终止向下遍历
  // 算法：
  //  1. 如果当前节点不是 TRANSMIT 算子，则递归遍历它的每一个 child
  //  2. 如果当前是 TRANSMIT 算子，则建立一个 dfo，同时记录它的父 dfo，并继续向下遍历
  //     如果没有父 dfo，说明它是 px root，记录到 px trees 中
  //  3. TODO: 对于 subplan filter rescan 的考虑

  // 算法分为两步走：
  // 1. qc 切分：顶层 qc ，subplan filter  右侧 qc
  // 2. 各个 qc 分别算并行度（zigzag, left-deep, right-deep, bushy）
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
      static_cast<const ObLogExchange&>(root_op).is_px_consumer() &&
      static_cast<const ObLogExchange&>(root_op).is_rescanable()) {
    if (OB_FAIL(convert_log_plan_to_nested_px_tree(px_trees,root_op))) {
      LOG_WARN("fail create qc for rescan op", K(ret));
    }
  } else {
    if (OB_SUCC(ret)) {
      if (log_op_def::LOG_JOIN_FILTER == root_op.get_type()) {
        if (OB_NOT_NULL(parent_dfo)) {
          ObLogJoinFilter &log_join_filter = static_cast<ObLogJoinFilter &>(root_op);
          if (log_join_filter.is_create_filter() && !parent_dfo->force_bushy()) {
            parent_dfo->force_bushy_ = true;
          }
        }
      }
    }
    if (log_op_def::LOG_EXCHANGE == root_op.get_type() &&
        static_cast<const ObLogExchange&>(root_op).is_px_producer()) {
      DfoInfo *dfo = nullptr;
      if (OB_FAIL(create_dfo(dfo,  static_cast<const ObLogExchange&>(root_op).get_parallel()))) {
        LOG_WARN("fail create dfo", K(ret));
      } else {
        if (OB_FAIL(dfo->location_addr_.create(hash::cal_next_prime(10), "PxResourceBucket", "PxResourceNode"))) {
          LOG_WARN("fail to create hash set", K(ret));
        } else if (OB_FAIL(get_dfo_addr_set(root_op, dfo->location_addr_))) {
          LOG_WARN("get addr_set failed", K(ret));
          dfo->reset();
        } else {
          if (nullptr == parent_dfo) {
            px_info.root_dfo_ = dfo;
          } else {
            parent_dfo->add_child(dfo);
          }
          dfo->set_parent(parent_dfo);
          parent_dfo = dfo;
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t num = root_op.get_num_of_child();
      for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
        if (OB_ISNULL(root_op.get_child(child_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
        } else if (OB_FAIL(SMART_CALL(do_split(
                    px_trees,
                    px_info,
                    *root_op.get_child(child_idx),
                    parent_dfo)))) {
          LOG_WARN("fail split px tree", K(child_idx), K(num), K(ret));
        }
      }
      if (parent_dfo->location_addr_.size() == 0) {
        if (parent_dfo->has_child()) {
          DfoInfo *child_dfo = nullptr;
          if (OB_FAIL(parent_dfo->get_child(0, child_dfo))) {
            LOG_WARN("get child dfo failed", K(ret));
          } else {
            for (ObHashSet<ObAddr>::const_iterator it = child_dfo->location_addr_.begin();
                OB_SUCC(ret) && it != child_dfo->location_addr_.end(); ++it) {
              if (OB_FAIL(parent_dfo->location_addr_.set_refactored(it->first))){
                LOG_WARN("set refactored failed", K(ret), K(it->first));
              }
            }
          }
        } else if (OB_FAIL(parent_dfo->location_addr_.set_refactored(GCTX.self_addr()))){
          LOG_WARN("set refactored failed", K(ret), K(GCTX.self_addr()));
        }
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::create_dfo(DfoInfo *&dfo, int64_t dop)
{
  int ret = OB_SUCCESS;
  void *mem_ptr = dfo_allocator_.alloc(sizeof(DfoInfo));
  if (OB_ISNULL(mem_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail allocate memory", K(ret));
  } else if (nullptr == (dfo = new(mem_ptr) DfoInfo())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null ptr unexpected", KP(mem_ptr), K(ret));
  } else {
    dfo->set_dop(dop);
  }
  return ret;
}

int ObPxResourceAnalyzer::get_dfo_addr_set(const ObLogicalOperator &root_op, ObHashSet<ObAddr> &addr_set)
{
  int ret = OB_SUCCESS;
  if ((root_op.is_table_scan() && !root_op.get_contains_fake_cte()) ||
      (root_op.is_dml_operator() && (static_cast<const ObLogDelUpd&>(root_op)).is_pdml())) {
    const ObTablePartitionInfo *tbl_part_info = nullptr;
    if (root_op.is_table_scan()) {
      const ObLogTableScan &tsc = static_cast<const ObLogTableScan&>(root_op);
      tbl_part_info = tsc.get_table_partition_info();
    } else {
      const ObLogDelUpd &dml_op = static_cast<const ObLogDelUpd&>(root_op);
      tbl_part_info = dml_op.get_table_partition_info();
    }
    if (OB_ISNULL(tbl_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get table partition info failed", K(ret),
                                                  K(root_op.get_type()),
                                                  K(root_op.get_operator_id()));
    } else {
      const ObCandiTableLoc &phy_tbl_loc_info = tbl_part_info->get_phy_tbl_location_info();
      const ObCandiTabletLocIArray &phy_part_loc_info_arr = phy_tbl_loc_info.get_phy_part_loc_info_list();
      for (int64_t i = 0; i < phy_part_loc_info_arr.count(); ++i) {
        share::ObLSReplicaLocation replica_loc;
        if (OB_FAIL(phy_part_loc_info_arr.at(i).get_selected_replica(replica_loc))) {
          LOG_WARN("get selected replica failed", K(ret));
        } else if (OB_FAIL(addr_set.set_refactored(replica_loc.get_server(), 1))) {
          LOG_WARN("addr set refactored failed");
        } else {
          LOG_DEBUG("resource analyzer", K(root_op.get_type()),
                                         K(root_op.get_operator_id()),
                                         K(replica_loc.get_server()));
        }
      }
    }
  } else {
    int64_t num = root_op.get_num_of_child();
    for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < num; ++child_idx) {
      ObLogicalOperator *child_op = nullptr;
      if (OB_ISNULL(child_op = root_op.get_child(child_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(child_idx), K(num), K(ret));
      } else if (log_op_def::LOG_EXCHANGE == child_op->get_type() &&
                 static_cast<const ObLogExchange*>(child_op)->is_px_consumer()) {
        // do nothing
      } else if (OB_FAIL(SMART_CALL(get_dfo_addr_set(*child_op, addr_set)))) {
        LOG_WARN("get addr_set failed", K(ret));
      }
    }
  }

  return ret;
}

int ObPxResourceAnalyzer::walk_through_px_trees(
    ObIArray<PxInfo> &px_trees,
    int64_t &max_parallel_thread_count,
    int64_t &max_parallel_group_count,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  int64_t bucket_size = cal_next_prime(10);
  max_parallel_thread_count = 0;
  max_parallel_group_count = 0;
  ObHashMap<ObAddr, int64_t> thread_map;
  ObHashMap<ObAddr, int64_t> group_map;
  if (max_parallel_thread_map.created()) {
    max_parallel_thread_map.clear();
    max_parallel_group_map.clear();
  } else if (OB_FAIL(max_parallel_thread_map.create(bucket_size,
                                                    ObModIds::OB_SQL_PX,
                                                    ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(max_parallel_group_map.create(bucket_size,
                                                   ObModIds::OB_SQL_PX,
                                                   ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(thread_map.create(bucket_size, ObModIds::OB_SQL_PX, ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(group_map.create(bucket_size, ObModIds::OB_SQL_PX, ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < px_trees.count(); ++i) {
    PxInfo &px_info = px_trees.at(i);
    int64_t thread_count = 0;
    int64_t group_count = 0;
    thread_map.clear();
    group_map.clear();
    if (OB_FAIL(walk_through_dfo_tree(px_info, thread_count, group_count, thread_map, group_map))) {
      LOG_WARN("fail calc px thread group count", K(i), "total", px_trees.count(), K(ret));
    } else if (OB_ISNULL(px_info.root_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("QC op not set in px_info struct", K(ret));
    } else {
      px_info.threads_ = thread_count;
      // 将当前 px 的 expected 线程数设置到 QC 算子中
      px_info.root_op_->set_expected_worker_count(thread_count);

      max_parallel_thread_count += thread_count;
      max_parallel_group_count += group_count;
      if (OB_FAIL(px_tree_append(max_parallel_thread_map, thread_map))) {
        LOG_WARN("px tree dop append failed", K(ret));
      } else if (OB_FAIL(px_tree_append(max_parallel_group_map, group_map))) {
        LOG_WARN("px tree dop append failed", K(ret));
      }
    }
  }
  thread_map.destroy();
  group_map.destroy();
  return ret;
}

/* A dfo tree is scheduled in post order generally.
 * First, we traversal the tree in post order and generate edges.
 * Then for each edge:
 * 1. Schedule this edge if not scheduled.
 * 2. Schedule parent of this edge if not scheduled.
 * 3. Schedule depend siblings of this edge one by one. One by one means finish (i-1)th sibling before schedule i-th sibling.
 * 4. Finish this edge if it is a leaf node or all children are finished.
*/

/* This function also generate a ObHashMap<ObAddr, int64_t> max_parallel_thread_map.
 * Key is observer. Value is max sum of dops of dfos that are scheduled at same time on this observer.
 * Value means expected thread number on this server.
 * Once a dfo is scheduled or finished, we update(increase or decrease) the current_thread_map.
 * Then compare current thead count with max thread count, and update max_parallel_thread_map if necessary.
*/
int ObPxResourceAnalyzer::walk_through_dfo_tree(
    PxInfo &px_root,
    int64_t &max_parallel_thread_count,
    int64_t &max_parallel_group_count,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  // 模拟调度过程
  ObArray<DfoInfo *> edges;
  SchedOrderGenerator sched_order_gen;
  int64_t bucket_size = cal_next_prime(10);
  ObHashMap<ObAddr, int64_t> current_thread_map;
  ObHashMap<ObAddr, int64_t> current_group_map;

  if (OB_ISNULL(px_root.root_dfo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(DfoTreeNormalizer<DfoInfo>::normalize(*px_root.root_dfo_))) {
    LOG_WARN("fail normalize px tree", K(ret));
  } else if (OB_FAIL(sched_order_gen.generate(*px_root.root_dfo_, edges))) {
    LOG_WARN("fail generate sched order", K(ret));
  } else if (OB_FAIL(current_thread_map.create(bucket_size,
                                               ObModIds::OB_SQL_PX,
                                               ObModIds::OB_SQL_PX))){
    LOG_WARN("create hash map failed", K(ret));
  } else if (OB_FAIL(current_group_map.create(bucket_size,
                                              ObModIds::OB_SQL_PX,
                                              ObModIds::OB_SQL_PX))){
  }
#ifndef NDEBUG
  for (int x = 0; x < edges.count(); ++x) {
    LOG_DEBUG("dump dfo", K(x), K(*edges.at(x)));
  }
#endif

  int64_t threads = 0;
  int64_t groups = 0;
  int64_t max_threads = 0;
  int64_t max_groups = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < edges.count(); ++i) {
    DfoInfo &child = *edges.at(i);
    // schedule child if not scheduled.
    if (OB_FAIL(schedule_dfo(child, threads, groups, current_thread_map, current_group_map))) {
      LOG_WARN("schedule dfo failed", K(ret));
    } else if (child.has_parent() && OB_FAIL(schedule_dfo(*child.parent_, threads, groups,
                                                          current_thread_map, current_group_map))) {
      LOG_WARN("schedule parent dfo failed", K(ret));
    } else if (child.has_sibling() && child.depend_sibling_->not_scheduled()) {
      DfoInfo *sibling = child.depend_sibling_;
      while (NULL != sibling && OB_SUCC(ret)) {
        if (OB_UNLIKELY(!sibling->is_leaf_node())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sibling must be leaf node", K(ret));
        } else if (OB_FAIL(schedule_dfo(*sibling, threads, groups, current_thread_map,
                                        current_group_map))) {
          LOG_WARN("schedule sibling failed", K(ret));
        } else if (OB_FAIL(update_max_thead_group_info(threads, groups,
                    current_thread_map, current_group_map,
                    max_threads, max_groups,
                    max_parallel_thread_map, max_parallel_group_map))) {
          LOG_WARN("update max_thead group info failed", K(ret));
        } else if (OB_FAIL(finish_dfo(*sibling, threads, groups, current_thread_map,
                                        current_group_map))) {
          LOG_WARN("finish sibling failed", K(ret));
        } else {
          sibling = sibling->depend_sibling_;
        }
      }
    } else {
      if (OB_FAIL(update_max_thead_group_info(threads, groups,
                    current_thread_map, current_group_map,
                    max_threads, max_groups,
                    max_parallel_thread_map, max_parallel_group_map))) {
        LOG_WARN("update max_thead group info failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(finish_dfo(child, threads, groups, current_thread_map,
                                        current_group_map))) {
        LOG_WARN("finish sibling failed", K(ret));
      }
    }

#ifndef NDEBUG
      for (int x = 0; x < edges.count(); ++x) {
        LOG_DEBUG("dump dfo step.finish",
                  K(i), K(x), K(*edges.at(x)), K(threads), K(max_threads), K(groups), K(max_groups));
      }
#endif
  }
  max_parallel_thread_count = max_threads;
  max_parallel_group_count = max_groups;
  LOG_TRACE("end walk_through_dfo_tree", K(max_parallel_thread_count), K(max_parallel_group_count));
  return ret;
}

int ObPxResourceAnalyzer::px_tree_append(ObHashMap<ObAddr, int64_t> &max_parallel_count,
                                         ObHashMap<ObAddr, int64_t> &parallel_count)
{
  int ret = OB_SUCCESS;
  for (ObHashMap<ObAddr, int64_t>::const_iterator it = parallel_count.begin();
      OB_SUCC(ret) && it != parallel_count.end(); ++it) {
    bool is_exist = true;
    int64_t dop = 0;
    if (OB_FAIL(max_parallel_count.get_refactored(it->first, dop))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("get refactored failed", K(ret), K(it->first));
      } else {
        is_exist = false;
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      dop += it->second;
      if (OB_FAIL(max_parallel_count.set_refactored(it->first, dop, is_exist))){
        LOG_WARN("set refactored failed", K(ret), K(it->first), K(dop), K(is_exist));
      }
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::schedule_dfo(
    DfoInfo &dfo,
    int64_t &threads,
    int64_t &groups,
    ObHashMap<ObAddr, int64_t> &current_thread_map,
    ObHashMap<ObAddr, int64_t> &current_group_map)
{
  int ret = OB_SUCCESS;
  if (dfo.not_scheduled()) {
    dfo.set_scheduled();
    threads += dfo.get_dop();
    const int64_t group = 1;
    groups += group;
    ObHashSet<ObAddr> &addr_set = dfo.location_addr_;
    // we assume that should allocate same thread count for each sqc in the dfo.
    // this may not true. but we can't decide the real count for each sqc. just let it be for now
    const int64_t dop_per_addr = 0 == addr_set.size() ? dfo.get_dop() : (dfo.get_dop() + addr_set.size() - 1) / addr_set.size();
    if (OB_FAIL(update_parallel_map(current_thread_map, addr_set, dop_per_addr))) {
      LOG_WARN("increase current thread map failed", K(ret));
    } else if (OB_FAIL(update_parallel_map(current_group_map, addr_set, group))) {
      LOG_WARN("increase current group map failed", K(ret));
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::finish_dfo(
    DfoInfo &dfo,
    int64_t &threads,
    int64_t &groups,
    ObHashMap<ObAddr, int64_t> &current_thread_map,
    ObHashMap<ObAddr, int64_t> &current_group_map)
{
  int ret = OB_SUCCESS;
  if (dfo.is_scheduling() && (dfo.is_leaf_node() || dfo.is_all_child_finish())) {
    dfo.set_finished();
    threads -= dfo.get_dop();
    const int64_t group = 1;
    groups -= group;
    ObHashSet<ObAddr> &addr_set = dfo.location_addr_;
    const int64_t dop_per_addr = 0 == addr_set.size() ? dfo.get_dop() : (dfo.get_dop() + addr_set.size() - 1) / addr_set.size();
    if (OB_FAIL(update_parallel_map(current_thread_map, addr_set, -dop_per_addr))) {
      LOG_WARN("decrease current thread map failed", K(ret));
    } else if (OB_FAIL(update_parallel_map(current_group_map, addr_set, -group))) {
      LOG_WARN("decrease current group map failed", K(ret));
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::update_parallel_map(
    ObHashMap<ObAddr, int64_t> &parallel_map,
    const ObHashSet<ObAddr> &addr_set,
    int64_t count)
{
  int ret = OB_SUCCESS;
  for (hash::ObHashSet<ObAddr>::const_iterator it = addr_set.begin();
        OB_SUCC(ret) && it != addr_set.end(); it++) {
    if (OB_FAIL(update_parallel_map_one_addr(parallel_map, it->first, count, true))) {
      LOG_WARN("update parallel map one addr failed", K(ret));
    }
  }
  return ret;
}

// Update current_parallel_map or max_parallel_map.
// When update current_parallel_map, append is true, because we are increasing or decreasing count.
// When update max_parallel_map, append is false.
int ObPxResourceAnalyzer::update_parallel_map_one_addr(
    ObHashMap<ObAddr, int64_t> &parallel_map,
    const ObAddr &addr,
    int64_t count,
    bool append)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  int64_t origin_count = 0;
  if (OB_FAIL(parallel_map.get_refactored(addr, origin_count))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("get refactored failed", K(ret), K(addr));
    } else {
      is_exist = false;
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (append) {
      origin_count += count;
    } else {
      origin_count = max(origin_count, count);
    }
    if (OB_FAIL(parallel_map.set_refactored(addr, origin_count, is_exist))){
      LOG_WARN("set refactored failed", K(ret), K(addr), K(origin_count), K(is_exist));
    }
  }
  return ret;
}

int ObPxResourceAnalyzer::update_max_thead_group_info(
    const int64_t threads,
    const int64_t groups,
    const ObHashMap<ObAddr, int64_t> &current_thread_map,
    const ObHashMap<ObAddr, int64_t> &current_group_map,
    int64_t &max_threads,
    int64_t &max_groups,
    ObHashMap<ObAddr, int64_t> &max_parallel_thread_map,
    ObHashMap<ObAddr, int64_t> &max_parallel_group_map)
{
  int ret = OB_SUCCESS;
  max_threads = max(threads, max_threads);
  max_groups = max(groups, max_groups);
  for (ObHashMap<ObAddr, int64_t>::const_iterator it = current_thread_map.begin();
      OB_SUCC(ret) && it != current_thread_map.end(); ++it) {
    if (OB_FAIL(update_parallel_map_one_addr(max_parallel_thread_map, it->first, it->second, false))) {
      LOG_WARN("update parallel map one addr failed", K(ret));
    }
  }
  for (ObHashMap<ObAddr, int64_t>::const_iterator it = current_group_map.begin();
      OB_SUCC(ret) && it != current_group_map.end(); ++it) {
    if (OB_FAIL(update_parallel_map_one_addr(max_parallel_group_map, it->first, it->second, false))) {
      LOG_WARN("update parallel map one addr failed", K(ret));
    }
  }
  return ret;
}
