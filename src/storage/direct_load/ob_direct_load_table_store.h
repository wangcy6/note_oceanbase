// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableDataDesc;
class ObDirectLoadTmpFileManager;
class ObDirectLoadTableBuilderAllocator;
class ObDirectLoadInsertTableContext;
class ObDirectLoadFastHeapTableContext;
class ObDirectLoadDMLRowHandler;

struct ObDirectLoadTableStoreParam
{
public:
  ObDirectLoadTableStoreParam();
  ~ObDirectLoadTableStoreParam();
  bool is_valid() const;
  TO_STRING_KV(K_(snapshot_version), K_(table_data_desc), KP_(datum_utils), KP_(col_descs),
               KP_(file_mgr), K_(is_multiple_mode), K_(is_fast_heap_table), KP_(insert_table_ctx),
               KP_(fast_heap_table_ctx), KP_(dml_row_handler), KP_(extra_buf), K_(extra_buf_size));
public:
  int64_t snapshot_version_;
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  ObDirectLoadTmpFileManager *file_mgr_;
  bool is_multiple_mode_;
  bool is_fast_heap_table_;
  bool online_opt_stat_gather_;
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  ObDirectLoadFastHeapTableContext *fast_heap_table_ctx_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  char *extra_buf_;
  int64_t extra_buf_size_;
};

class ObDirectLoadTableStoreBucket
{
public:
  ObDirectLoadTableStoreBucket();
  ~ObDirectLoadTableStoreBucket();
  int init(const ObDirectLoadTableStoreParam &param, const common::ObTabletID &tablet_id);
  int append_row(const common::ObTabletID &tablet_id, const blocksstable::ObDatumRow &datum_row);
  int close();
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator);
  void clean_up();
  TO_STRING_KV(KP(param_));
private:
  const ObDirectLoadTableStoreParam *param_;
  ObDirectLoadTableBuilderAllocator *table_builder_allocator_;
  ObIDirectLoadPartitionTableBuilder *table_builder_;
  bool is_inited_;
};

class ObDirectLoadTableStore
{
public:
  const static constexpr int64_t MAX_BUCKET_CNT = 1024;
  ObDirectLoadTableStore() : allocator_("TLD_TSBucket"), is_inited_(false) {}
  ~ObDirectLoadTableStore();
  int init(const ObDirectLoadTableStoreParam &param);
  int append_row(const common::ObTabletID &tablet_id, const blocksstable::ObDatumRow &datum_row);
  int close();
  void clean_up();
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator);
private:
  int new_bucket(ObDirectLoadTableStoreBucket *&bucket);
  int get_bucket(const common::ObTabletID &tablet_id, ObDirectLoadTableStoreBucket *&bucket);
private:
  ObDirectLoadTableStoreParam param_;
  common::ObArenaAllocator allocator_;
  common::ObArray<ObDirectLoadTableStoreBucket *> bucket_ptr_array_;
  common::hash::ObHashMap<common::ObTabletID, ObDirectLoadTableStoreBucket *> tablet_index_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
