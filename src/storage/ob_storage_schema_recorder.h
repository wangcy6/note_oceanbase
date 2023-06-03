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

#ifndef OCEANBASE_STORAGE_STORAGE_SCHEMA_RECORDER_
#define OCEANBASE_STORAGE_STORAGE_SCHEMA_RECORDER_

#include "lib/ob_define.h"
#include "storage/ob_storage_clog_recorder.h"
#include "storage/ob_storage_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/scn.h"

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObMultiVersionSchemaService;
} // namespace share
} // namespace schema

namespace storage
{
class ObTablet;
class ObIMemtableMgr;
class ObTabletHandle;

class ObStorageSchemaRecorder : public ObIStorageClogRecorder
{

public:
  ObStorageSchemaRecorder();
  ~ObStorageSchemaRecorder();

  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t saved_schema_version,
      const lib::Worker::CompatMode compat_mode,
      logservice::ObLogHandler *log_handler);
  void destroy();
  void reset();
  bool is_inited() const { return is_inited_; }
  bool is_valid() const
  {
    return is_inited_
        && ls_id_.is_valid()
        && tablet_id_.is_valid()
        && nullptr != log_handler_
        && max_saved_version_ >= 0;
  }

  // follower
  int replay_schema_log(const share::SCN &scn, const char *buf, const int64_t size, int64_t &pos);
  // leader
  int try_update_storage_schema(
      const int64_t table_id,
      const int64_t table_version,
      ObIAllocator &allocator,
      const int64_t timeout);

  ObStorageSchemaRecorder(const ObStorageSchemaRecorder&) = delete;
  ObStorageSchemaRecorder& operator=(const ObStorageSchemaRecorder&) = delete;
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(tablet_id));

private:
  virtual int inner_replay_clog(
      const int64_t update_version,
      const share::SCN &scn,
      const char *buf,
      const int64_t size,
      int64_t &pos) override;
  virtual int sync_clog_succ_for_leader(const int64_t update_version) override;
  virtual void sync_clog_failed_for_leader() override;

  int get_schema(int64_t &table_version);

  virtual int prepare_struct_in_lock(
      int64_t &update_version,
      ObIAllocator *allocator,
      char *&clog_buf,
      int64_t &clog_len) override;
  virtual int submit_log(
      const int64_t update_version,
      const char *clog_buf,
      const int64_t clog_len) override;
  virtual void free_struct_in_lock() override
  {
    free_allocated_info();
  }
  int generate_clog(
      char *&clog_buf,
      int64_t &clog_len);
  int64_t calc_schema_log_size() const;
  void free_allocated_info();
  int try_update_with_lock(const int64_t table_id, const int64_t table_version, const int64_t expire_ts);
<<<<<<< HEAD
  int get_tablet_handle(ObTabletHandle &tablet_handle);
  int replay_get_tablet_handle(const share::SCN &scn, ObTabletHandle &tablet_handle);
  // clog callback
  void update_table_schema_fail();
  void update_table_schema_succ(const int64_t table_version, bool &finish_flag);
=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  OB_INLINE int dec_ref_on_memtable(const bool sync_finish);

  bool is_inited_;
  bool ignore_storage_schema_;
  lib::Worker::CompatMode compat_mode_;
  char *clog_buf_;
<<<<<<< HEAD
  int64_t clog_len_;
  share::SCN clog_scn_;
=======
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

  ObTabletHandle *tablet_handle_ptr_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  ObStorageSchema *storage_schema_;
  ObIAllocator *allocator_;

  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  int64_t table_id_;

};

} // storage
} // oceanbase
#endif /* OCEANBASE_STORAGE_STORAGE_SCHEMA_RECORDER_ */
