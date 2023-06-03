// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "share/table/ob_table_load_define.h"
#include "sql/engine/px/ob_sub_trans_ctrl.h"

namespace oceanbase
{
namespace storage
{
class ObSSTableInsertSliceWriter;

struct ObDirectLoadInsertTableParam
{
public:
  ObDirectLoadInsertTableParam();
  ~ObDirectLoadInsertTableParam();
  int assign(const ObDirectLoadInsertTableParam &other);
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(schema_version), K_(snapshot_version), K_(ls_partition_ids), K_(execution_id), K_(ddl_task_id));
public:
  uint64_t table_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t execution_id_;
  int64_t ddl_task_id_;
  int64_t data_version_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> ls_partition_ids_;
};

class ObDirectLoadInsertTableContext
{
public:
  ObDirectLoadInsertTableContext();
  ~ObDirectLoadInsertTableContext();
  void reset();
  int init(const ObDirectLoadInsertTableParam &param);
  int add_sstable_slice(const common::ObTabletID &tablet_id,
                        const blocksstable::ObMacroDataSeq &start_seq,
                        common::ObNewRowIterator &iter,
                        int64_t &affected_rows);
  int construct_sstable_slice_writer(const common::ObTabletID &tablet_id,
                                     const blocksstable::ObMacroDataSeq &start_seq,
                                     ObSSTableInsertSliceWriter *&slice_writer,
                                     common::ObIAllocator &allocator);
  int notify_tablet_finish(const common::ObTabletID &tablet_id);
  int commit();
  TO_STRING_KV(K_(param), K_(ddl_ctrl));
private:
  ObDirectLoadInsertTableParam param_;
  sql::ObDDLCtrl ddl_ctrl_;
  int64_t tablet_finish_count_ CACHE_ALIGNED;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
