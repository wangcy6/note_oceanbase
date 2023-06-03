// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "common/ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_osg_column_stat.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadFastHeapTableCreateParam
{
public:
  ObDirectLoadFastHeapTableCreateParam();
  ~ObDirectLoadFastHeapTableCreateParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(row_count));
public:
  common::ObTabletID tablet_id_;
  int64_t row_count_;
  common::ObArray<common::ObOptOSGColumnStat*> *column_stat_array_;
};

struct ObDirectLoadFastHeapTableMeta
{
  common::ObTabletID tablet_id_;
  int64_t row_count_;
  TO_STRING_KV(K_(tablet_id), K_(row_count));
};

class ObDirectLoadFastHeapTable : public ObIDirectLoadPartitionTable
{
public:
  ObDirectLoadFastHeapTable();
  virtual ~ObDirectLoadFastHeapTable();
  int init(const ObDirectLoadFastHeapTableCreateParam &param);
  const common::ObIArray<ObOptOSGColumnStat*> &get_column_stat_array() const
  {
    return column_stat_array_;
  }
  const common::ObTabletID &get_tablet_id() const override { return meta_.tablet_id_; }
  int64_t get_row_count() const override { return meta_.row_count_; }
  bool is_valid() const override { return is_inited_; }
  const ObDirectLoadFastHeapTableMeta &get_meta() const { return meta_; }
  TO_STRING_KV(K_(meta));
private:
  int copy_col_stat(const ObDirectLoadFastHeapTableCreateParam &param);
private:
  ObDirectLoadFastHeapTableMeta meta_;
  common::ObArenaAllocator allocator_;
  common::ObArray<common::ObOptOSGColumnStat*> column_stat_array_;
  bool is_inited_;
  DISABLE_COPY_ASSIGN(ObDirectLoadFastHeapTable);
};

} // namespace storage
} // namespace oceanbase
