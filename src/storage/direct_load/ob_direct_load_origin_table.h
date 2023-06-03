// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yiren.ly <>

#pragma once

#include "share/schema/ob_table_dml_param.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/direct_load/ob_direct_load_io_callback.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadOriginTableCreateParam
{
public:
  ObDirectLoadOriginTableCreateParam();
  ~ObDirectLoadOriginTableCreateParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(tablet_id), K_(ls_id));
public:
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
};

struct ObDirectLoadOriginTableMeta
{
public:
  ObDirectLoadOriginTableMeta();
  ~ObDirectLoadOriginTableMeta();
  TO_STRING_KV( K_(table_id), K_(tablet_id), K_(ls_id));
public:
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
};

class ObDirectLoadOriginTable
{
public:
  ObDirectLoadOriginTable();
  virtual ~ObDirectLoadOriginTable();
  int init(const ObDirectLoadOriginTableCreateParam &param);
  int scan(const blocksstable::ObDatumRange &key_range, common::ObIAllocator &allocator, ObIStoreRowIterator *&row_iter);
  bool is_valid() const { return is_inited_; }
  const ObDirectLoadOriginTableMeta &get_meta() const {return meta_; }
  const ObTabletHandle &get_tablet_handle() const { return tablet_handle_; }
  const ObTableStoreIterator &get_table_iter() const { return table_iter_; }
  blocksstable::ObSSTable *get_major_sstable() const { return major_sstable_; }
  TO_STRING_KV(K_(meta), K_(tablet_handle), K_(table_iter), KP_(major_sstable));
private:
  int prepare_tables();
private:
  ObDirectLoadOriginTableMeta meta_;
  ObTabletHandle tablet_handle_;
  ObTableStoreIterator table_iter_;
  blocksstable::ObSSTable *major_sstable_;
  bool is_inited_;
};

class ObDirectLoadOriginTableScanner : public ObIStoreRowIterator
{
public:
  ObDirectLoadOriginTableScanner();
  virtual ~ObDirectLoadOriginTableScanner();
  int init(ObDirectLoadOriginTable *table,
           const blocksstable::ObDatumRange &query_range);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row) override;
private:
  int init_table_access_param();
  int init_table_access_ctx();
  int init_get_table_param();
private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadOriginTable *origin_table_;
  ObArray<int32_t> col_ids_;
  share::schema::ObTableSchemaParam schema_param_;
  ObTableAccessParam table_access_param_;
  ObDirectLoadIOCallback io_callback_;
  ObStoreCtx store_ctx_;
  ObTableAccessContext table_access_ctx_;
  ObGetTableParam get_table_param_;
  ObMultipleScanMerge scan_merge_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase