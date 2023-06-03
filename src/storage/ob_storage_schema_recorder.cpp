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
#include "ob_storage_schema_recorder.h"

#include "lib/utility/ob_tracepoint.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_handler.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/scn.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace clog;
using namespace share::schema;

namespace storage
{

ObStorageSchemaRecorder::ObStorageSchemaRecorder()
  : ObIStorageClogRecorder(),
    is_inited_(false),
    ignore_storage_schema_(false),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    clog_buf_(nullptr),
<<<<<<< HEAD
    clog_len_(0),
    clog_scn_(),
=======
    tablet_handle_ptr_(nullptr),
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    schema_guard_(nullptr),
    storage_schema_(nullptr),
    allocator_(nullptr),
    ls_id_(),
    tablet_id_(),
    table_id_(0)
{
#if defined(__x86_64__)
  STATIC_ASSERT(sizeof(ObStorageSchemaRecorder) <= 120, "size of schema recorder is oversize");
#endif
}

ObStorageSchemaRecorder::~ObStorageSchemaRecorder()
{
  destroy();
}

void ObStorageSchemaRecorder::destroy()
{
  is_inited_ = false;
  ignore_storage_schema_ = false;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  ObIStorageClogRecorder::destroy();
  free_allocated_info();
  log_handler_ = NULL;
  ls_id_.reset();
  tablet_id_.reset();
<<<<<<< HEAD
  tablet_handle_.reset();
  clog_scn_.reset();
  clog_len_ = 0;
=======
  table_id_ = 0;
}

void ObStorageSchemaRecorder::reset()
{
  if (is_inited_) {
    ObIStorageClogRecorder::reset();
  }
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
}

int ObStorageSchemaRecorder::init(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t saved_schema_version,
    const lib::Worker::CompatMode compat_mode,
    logservice::ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(saved_schema_version < 0 || nullptr == log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(saved_schema_version), KP(log_handler));
  } else if (OB_FAIL(ObIStorageClogRecorder::init(saved_schema_version, log_handler))) {
    LOG_WARN("failed to init ObIStorageClogRecorder", K(ret), K(saved_schema_version), K(log_handler));
  } else {
    ignore_storage_schema_ = tablet_id.is_special_merge_tablet();
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    compat_mode_ = compat_mode;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

// schema log is barrier, there is no concurrency problem, no need to lock
int ObStorageSchemaRecorder::replay_schema_log(
    const SCN &scn,
    const char *buf,
    const int64_t size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t update_version = OB_INVALID_VERSION;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
<<<<<<< HEAD
    LOG_WARN("schema recorder not inited", K(ret));
  } else {
    int64_t table_version = OB_INVALID_VERSION;
    ObArenaAllocator tmp_allocator;
    ObStorageSchema replay_storage_schema;
    if (tablet_id_.is_special_merge_tablet()) {
      // do nothing
    } else if (OB_FAIL(serialization::decode_i64(buf, size, pos, &table_version))) {
      // table_version
      LOG_WARN("fail to deserialize table_version", K(ret), K_(tablet_id));
    } else if (table_version <= ATOMIC_LOAD(&max_saved_table_version_)) {
      LOG_INFO("skip schema log with smaller table version", K_(tablet_id), K(table_version),
          K(max_saved_table_version_));
    } else if (OB_FAIL(replay_get_tablet_handle(scn, tablet_handle_))) {
      LOG_WARN("failed to get tablet handle", K(ret), K_(tablet_id), K(scn));
    } else if (OB_FAIL(replay_storage_schema.deserialize(tmp_allocator, buf, size, pos))) {
      LOG_WARN("fail to deserialize storage schema", K(ret), K_(tablet_id));
    } else if (FALSE_IT(replay_storage_schema.set_sync_finish(true))) {
    } else if (OB_FAIL(tablet_handle_.get_obj()->save_multi_source_data_unit(&replay_storage_schema,
                                                                             scn,
                                                                             true/*for_replay*/,
                                                                             memtable::MemtableRefOp::NONE))) {
      LOG_WARN("failed to save storage schema on memtable", K(ret), K_(tablet_id), K(replay_storage_schema));
    } else {
      ATOMIC_SET(&max_saved_table_version_, table_version);
      LOG_INFO("success to replay schema log", K(ret), K_(tablet_id), K(max_saved_table_version_));
      replay_storage_schema.reset();
    }
    tablet_handle_.reset();
=======
    LOG_WARN("schema recorder not inited", K(ret), K_(tablet_id));
  } else if (ignore_storage_schema_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to update storage schema", K(ret), K_(tablet_id));
  } else if (OB_FAIL(serialization::decode_i64(buf, size, pos, &update_version))) {
    LOG_WARN("fail to deserialize table_version", K(ret), K_(tablet_id));
  } else if (OB_FAIL(ObIStorageClogRecorder::replay_clog(update_version, scn, buf, size, pos))) {
    LOG_WARN("failed to replay clog", K(ret), K(scn), K_(tablet_id), K(update_version));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  }
  return ret;
}

// replay after get update_version
int ObStorageSchemaRecorder::inner_replay_clog(
    const int64_t update_version,
    const SCN &scn,
    const char *buf,
    const int64_t size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObStorageSchema replay_storage_schema;
  ObTabletHandle tmp_tablet_handle;

  if (OB_FAIL(replay_get_tablet_handle(ls_id_, tablet_id_, scn, tmp_tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K_(tablet_id), K(scn));
  } else if (OB_FAIL(replay_storage_schema.deserialize(tmp_allocator, buf, size, pos))) {
    LOG_WARN("fail to deserialize table schema", K(ret), K_(tablet_id));
  } else if (FALSE_IT(replay_storage_schema.set_sync_finish(true))) {
  } else if (OB_FAIL(tmp_tablet_handle.get_obj()->save_multi_source_data_unit(&replay_storage_schema, scn,
      true/*for_replay*/, memtable::MemtableRefOp::NONE))) {
    LOG_WARN("failed to save storage schema", K(ret), K_(tablet_id), K(replay_storage_schema));
  } else {
    LOG_INFO("success to replay schema clog", K(ret), K_(ls_id), K_(tablet_id),
      K(replay_storage_schema.get_schema_version()), K(replay_storage_schema.compat_mode_));
  }
  replay_storage_schema.reset();
  tmp_tablet_handle.reset();
  return ret;
}

int ObStorageSchemaRecorder::try_update_storage_schema(
    const int64_t table_id,
    const int64_t table_version,
    ObIAllocator &allocator,
    const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema recorder not inited", K(ret));
<<<<<<< HEAD
  } else if (table_version < 0) {
    LOG_WARN("input schema version is invalid", K(ret), K(table_id), K(table_version));
  } else if (tablet_id_.is_special_merge_tablet()) {
    // do nothing
  } else if (table_version > ATOMIC_LOAD(&max_saved_table_version_)) {

    wait_to_lock(table_version); // lock
    allocator_ = &allocator;
    if (table_version > ATOMIC_LOAD(&max_saved_table_version_)) {
      LOG_INFO("save table schema", K_(ls_id), K_(tablet_id), K(table_version), K(max_saved_table_version_));
      int64_t sync_table_version = table_version;
      if (OB_FAIL(get_tablet_handle(tablet_handle_))) {
        LOG_WARN("failed to get tablet handle", K(ret), K_(ls_id), K_(tablet_id));
      } else if (OB_FAIL(prepare_schema(table_id, sync_table_version))) {
        LOG_WARN("fail to save table schema", K(ret), K_(ls_id), K_(tablet_id), K(sync_table_version));
      } else if (OB_FAIL(try_update_with_lock(table_id, sync_table_version, timeout))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("try update failed", K(ret), K_(ls_id), K_(tablet_id), K(table_version));
        }
      } else { // sync schema clog success
        FLOG_INFO("finish save table schema", K_(ls_id), K_(tablet_id), K(sync_table_version),
            "schema_version", storage_schema_->get_schema_version(), K_(clog_scn), K(timeout));
      }
    }

    // clear state no matter success or failed

    ATOMIC_STORE(&logcb_finish_flag_, true);
    free_allocated_info();
    tablet_handle_.reset();
    WEAK_BARRIER();
    ATOMIC_STORE(&lock_, false); // unlock
=======
  } else if (OB_UNLIKELY(table_version < 0 || table_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input schema version is invalid", K(ret), K_(tablet_id), K(table_version));
  } else if (ignore_storage_schema_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported to update storage schema", K(ret), K_(tablet_id));
  } else if (FALSE_IT(table_id_ = table_id)) { // clear in free_allocated_info
  } else if (OB_FAIL(try_update_for_leader(table_version, &allocator, timeout_ts))) {
    LOG_WARN("failed to update for leader", K(ret), K(table_version));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  }

  if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_BLOCK_FROZEN == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObStorageSchemaRecorder::sync_clog_failed_for_leader()
{
  dec_ref_on_memtable(false/*sync_finish*/);
}

<<<<<<< HEAD
int ObStorageSchemaRecorder::replay_get_tablet_handle(const SCN &scn, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->replay_get_tablet(tablet_id_, scn, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K_(ls_id), K_(tablet_id), K(scn));
  }
  return ret;
}

void ObStorageSchemaRecorder::update_table_schema_fail()
{
  dec_ref_on_memtable(false);
  ATOMIC_STORE(&logcb_finish_flag_, true);
}

void ObStorageSchemaRecorder::update_table_schema_succ(
    const int64_t table_version,
    bool &finish_flag)
{
  int ret = OB_SUCCESS;
  finish_flag = false;
  if (table_version <= ATOMIC_LOAD(&max_saved_table_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("schema log with smaller table version", K(ret), K_(tablet_id),
        K(table_version), K(max_saved_table_version_));
  } else if (OB_UNLIKELY(!clog_scn_.is_valid() || clog_scn_.is_min() || nullptr == storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    // clog_scn_ may be invalid because of concurrency in rare situation
    LOG_WARN("clog ts or storage schema is invalid", K(ret), K_(ls_id), K_(tablet_id),
        K_(clog_scn), KP_(storage_schema));
  } else if (storage_schema_->get_schema_version() != table_version) {
=======
int ObStorageSchemaRecorder::sync_clog_succ_for_leader(const int64_t update_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is invalid", K(ret), K_(clog_scn), KP_(storage_schema));
  } else if (OB_UNLIKELY(storage_schema_->get_schema_version() != update_version)) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema version not match", K(storage_schema_), K(update_version));
  } else if (OB_FAIL(dec_ref_on_memtable(true/*sync_finish*/))) {
    LOG_WARN("failed to save storage schema", K_(tablet_id), K(storage_schema_));
  } else {
    LOG_INFO("success to update storage schema", K(ret), K_(ls_id), K_(tablet_id), K(storage_schema_),
        K(update_version), K_(clog_scn));
  }
<<<<<<< HEAD
  if (OB_SUCC(ret)) {
    finish_flag = true;
    if (OB_FAIL(dec_ref_on_memtable(true))) {
      LOG_WARN("failed to save storage schema", K_(tablet_id), K(storage_schema_));
    } else {
      FLOG_INFO("update table schema success", K(ret), K_(ls_id), K_(tablet_id), K(table_version),
          "schema_version", table_version);
      ATOMIC_SET(&max_saved_table_version_, table_version);
    }
  }
  ATOMIC_STORE(&logcb_finish_flag_, true);
=======
  return ret;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
}

int ObStorageSchemaRecorder::dec_ref_on_memtable(const bool sync_finish)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == storage_schema_
      || nullptr == tablet_handle_ptr_
      || !tablet_handle_ptr_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema or tablet handle is unexpected null", K(ret), K_(ls_id), K_(tablet_id),
        KP_(storage_schema), K_(tablet_handle_ptr));
  } else {
    storage_schema_->set_sync_finish(sync_finish);
<<<<<<< HEAD
    if (OB_FAIL(tablet_handle_.get_obj()->save_multi_source_data_unit(storage_schema_, clog_scn_,
=======
    if (OB_FAIL(tablet_handle_ptr_->get_obj()->save_multi_source_data_unit(storage_schema_, clog_scn_,
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
        false/*for_replay*/, memtable::MemtableRefOp::DEC_REF, true/*is_callback*/))) {
      LOG_WARN("failed to save storage schema", K(ret), K_(tablet_id), K(storage_schema_));
    }
  }
  return ret;
}

int ObStorageSchemaRecorder::prepare_struct_in_lock(
  int64_t &update_version,
  ObIAllocator *allocator,
  char *&clog_buf,
  int64_t &clog_len)
{
  int ret = OB_SUCCESS;
  const int64_t alloc_size = sizeof(ObStorageCLogCb) + sizeof(ObTabletHandle)
      + sizeof(ObSchemaGetterGuard) + sizeof(ObStorageSchema);
  int64_t alloc_buf_offset = 0;
  char *buf = nullptr;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), K(allocator));
  } else if (FALSE_IT(allocator_ = allocator)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate schema guard", K(ret), K_(tablet_id));
  } else {
    logcb_ptr_ = new(buf) ObStorageCLogCb(*this);
    alloc_buf_offset += sizeof(ObStorageCLogCb);
    tablet_handle_ptr_ = new (buf + alloc_buf_offset) ObTabletHandle();
    alloc_buf_offset += sizeof(ObTabletHandle);
    schema_guard_ = new (buf + alloc_buf_offset) ObSchemaGetterGuard(share::schema::ObSchemaMgrItem::MOD_SCHEMA_RECORDER);
    alloc_buf_offset += sizeof(ObSchemaGetterGuard);
    storage_schema_ = new (buf + alloc_buf_offset) ObStorageSchema();
  }
  if (FAILEDx(get_tablet_handle(ls_id_, tablet_id_, *tablet_handle_ptr_))) {
    LOG_WARN("failed to get tablet handle", K(ret), K_(ls_id), K_(tablet_id));
  } else if (OB_FAIL(get_schema(update_version))) {
    LOG_WARN("fail to get expected schema", K(ret), K_(tablet_id), K(update_version));
  } else if (OB_FAIL(generate_clog(clog_buf, clog_len))) {
    LOG_WARN("failed to generate clog", K(ret), K_(tablet_id));
  }
  return ret;
}

void ObStorageSchemaRecorder::free_allocated_info()
{
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(logcb_ptr_)) {
      tablet_handle_ptr_->reset();
      tablet_handle_ptr_->~ObTabletHandle();
      schema_guard_->~ObSchemaGetterGuard();
      storage_schema_->~ObStorageSchema();
      allocator_->free(logcb_ptr_);
      logcb_ptr_ = nullptr;
      tablet_handle_ptr_ = nullptr;
      schema_guard_ = nullptr;
      storage_schema_ = nullptr;
    }
    if (OB_NOT_NULL(clog_buf_)) {
      allocator_->free(clog_buf_);
      clog_buf_ = nullptr;
    }
    allocator_ = nullptr;
  }
  table_id_ = 0;
}

int ObStorageSchemaRecorder::get_schema(
  int64_t &table_version)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *t_schema = NULL;

  if (OB_UNLIKELY(table_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(tablet_id), K(table_version));
  } else if (OB_UNLIKELY(nullptr == schema_guard_ || nullptr == storage_schema_ || nullptr == allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard/schema/allocator is null", K(ret), K_(tablet_id), KP_(schema_guard),
        KP_(storage_schema), KP_(allocator));
  } else if (OB_FAIL(MTL(ObTenantSchemaService*)->get_schema_service()->get_tenant_schema_guard(MTL_ID(), *schema_guard_))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(table_id_));
  } else if (OB_FAIL(schema_guard_->get_table_schema(MTL_ID(), table_id_, t_schema))
             || NULL == t_schema
             || table_version > t_schema->get_schema_version()) {
    // The version is checked here, so there is no need to check whether it is full
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get schema",  K(ret), K_(tablet_id), K(table_version), KPC(t_schema));
    if (NULL != t_schema) {
      LOG_WARN("current schema version", K(t_schema->get_schema_version()));
    }
  } else {
    table_version = t_schema->get_schema_version();
    if (OB_FAIL(storage_schema_->init(*allocator_, *t_schema, compat_mode_))) {
      LOG_WARN("failed to init storage schema", K(ret), K(t_schema));
    }
  }

  return ret;
}

int64_t ObStorageSchemaRecorder::calc_schema_log_size() const
{
  const int64_t size = tablet_id_.get_serialize_size()
      + serialization::encoded_length_i64(storage_schema_->get_schema_version()) // tablet_id + schema_version
      + storage_schema_->get_serialize_size();
  return size;
}

int ObStorageSchemaRecorder::submit_log(
  const int64_t update_version,
  const char *clog_buf,
  const int64_t clog_len)
{
  int ret = OB_SUCCESS;
<<<<<<< HEAD
  const bool need_nonblock = false;
  palf::LSN lsn;
  clog_scn_.reset();

  if (OB_UNLIKELY(nullptr == log_handler_ || nullptr == storage_schema_
      || !tablet_handle_.is_valid()
      || nullptr == clog_buf_
      || clog_len_ <= 0
      || nullptr == allocator_)) {
=======
  if (OB_UNLIKELY(nullptr == storage_schema_
      || nullptr == tablet_handle_ptr_
      || !tablet_handle_ptr_->is_valid()
      || nullptr == clog_buf
      || nullptr == allocator_
      || clog_len <= 0)) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler or storage_schema is null", K(ret), KP(storage_schema_),
        KP(clog_buf), K(clog_len), K(tablet_handle_ptr_));
  } else if (FALSE_IT(storage_schema_->set_sync_finish(false))) {
  } else if (OB_FAIL(tablet_handle_ptr_->get_obj()->save_multi_source_data_unit(storage_schema_,
      SCN::max_scn(), false/*for_replay*/, memtable::MemtableRefOp::INC_REF))) {
    if (OB_BLOCK_FROZEN != ret) {
      LOG_WARN("failed to inc ref for storage schema", K(ret), K_(tablet_id), K(storage_schema_));
    }
<<<<<<< HEAD
  }
  if (OB_SUCC(ret)) {
    logcb_ptr_->set_table_version(storage_schema_->get_schema_version());
    ATOMIC_STORE(&logcb_finish_flag_, false);
    storage_schema_->set_sync_finish(false);
    if (OB_FAIL(tablet_handle_.get_obj()->save_multi_source_data_unit(storage_schema_,
        SCN::max_scn(), false/*for_replay*/, memtable::MemtableRefOp::INC_REF))) {
      if (OB_BLOCK_FROZEN != ret) {
        LOG_WARN("failed to inc ref for storage schema", K(ret), K_(tablet_id), K(storage_schema_));
      }
    } else if (OB_FAIL(log_handler_->append(clog_buf_, clog_len_, SCN::min_scn(), need_nonblock, logcb_ptr_, lsn, clog_scn_))) {
      LOG_WARN("fail to submit log", K(ret), K_(tablet_id));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(dec_ref_on_memtable(false))) {
        LOG_ERROR("failed to dec ref on memtable", K(tmp_ret), K_(ls_id), K_(tablet_id));
      }
    } else {
      LOG_INFO("submit schema log succeed", K(ret), K_(ls_id), K_(tablet_id), K_(clog_scn), K_(clog_len),
          "schema_version", storage_schema_->get_schema_version());
=======
  } else if (OB_FAIL(write_clog(clog_buf, clog_len))) {
    LOG_WARN("fail to submit log", K(ret), K_(tablet_id));
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dec_ref_on_memtable(false))) {
      LOG_ERROR("failed to dec ref on memtable", K(tmp_ret), K_(ls_id), K_(tablet_id));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    }
  } else {
    LOG_INFO("submit schema log succeed", K(ret), K_(ls_id), K_(tablet_id), K(clog_scn_),
        "schema_version", storage_schema_->get_schema_version());
  }

  return ret;
}

int ObStorageSchemaRecorder::generate_clog(
    char *&clog_buf,
    int64_t &clog_len)
{
  int ret = OB_SUCCESS;
  clog_buf = nullptr;
  clog_len = 0;
  // tablet_id, schema_version, storage_schema
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  const logservice::ObLogBaseHeader log_header(
      logservice::ObLogBaseType::STORAGE_SCHEMA_LOG_BASE_TYPE,
      logservice::ObReplayBarrierType::STRICT_BARRIER/*need_replay_barrier*/);

  // log_header + tablet_id + schema_version + storage_schema
  if (OB_UNLIKELY(nullptr == storage_schema_ || nullptr == allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_schema is null", K(ret), KP(storage_schema_), KP_(allocator));
  } else if (OB_UNLIKELY(!storage_schema_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data storage schema is invalid", K(ret), K_(tablet_id), K(storage_schema_));
  } else if (FALSE_IT(buf_len = log_header.get_serialize_size() + calc_schema_log_size())) {
  } else if (buf_len >= common::OB_MAX_LOG_ALLOWED_SIZE) { // need be separated into several clogs
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("schema log too long", K(buf_len), LITERAL_K(common::OB_MAX_LOG_ALLOWED_SIZE));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K_(tablet_id));
  } else if (OB_FAIL(log_header.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize log header", K(ret));
  } else if (OB_FAIL(tablet_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize tablet_id", K(ret), K_(tablet_id));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, storage_schema_->get_schema_version()))) {
    LOG_WARN("fail to serialize table_version", K(ret), K_(tablet_id));
  } else if (OB_FAIL(storage_schema_->serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize data_table_schema", K(ret), K_(tablet_id));
  }

  if (OB_SUCC(ret)) {
    clog_buf_ = buf; // record to free later
    clog_buf = buf;
    clog_len = pos;
  } else if (nullptr != buf && nullptr != allocator_) {
    allocator_->free(buf);
    buf = nullptr;
  }
  return ret;
}

} // storage
} // oceanbase
