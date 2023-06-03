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

<<<<<<< HEAD
#include "ob_ddl_struct.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "share/ob_force_print_log.h"
#include "share/schema/ob_multi_version_schema_service.h"
=======
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

ObDDLMacroHandle::ObDDLMacroHandle()
  : block_id_()
{

}

ObDDLMacroHandle::ObDDLMacroHandle(const ObDDLMacroHandle &other)
{
  *this = other;
}

ObDDLMacroHandle &ObDDLMacroHandle::operator=(const ObDDLMacroHandle &other)
{
  if (&other != this) {
    (void)set_block_id(other.get_block_id());
  }
  return *this;
}

ObDDLMacroHandle::~ObDDLMacroHandle()
{
  reset_macro_block_ref();
}

int ObDDLMacroHandle::set_block_id(const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(reset_macro_block_ref())) {
    LOG_WARN("reset macro block reference failed", K(ret), K(block_id_));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
    LOG_ERROR("failed to increase macro block ref cnt", K(ret), K(block_id));
  } else {
    block_id_ = block_id;
  }
  return ret;
}

int ObDDLMacroHandle::reset_macro_block_ref()
{
  int ret = OB_SUCCESS;
  if (block_id_.is_valid()) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(block_id_))) {
      LOG_ERROR("failed to dec macro block ref cnt", K(ret), K(block_id_));
    } else {
      block_id_.reset();
    }
  }
  return ret;
}

ObDDLMacroBlock::ObDDLMacroBlock()
  : block_handle_(), logic_id_(), block_type_(DDL_MB_INVALID_TYPE), ddl_start_scn_(SCN::min_scn()), scn_(SCN::min_scn()), buf_(nullptr), size_(0)
{
}

ObDDLMacroBlock::~ObDDLMacroBlock()
{
}

int ObDDLMacroBlock::deep_copy(ObDDLMacroBlock &dst_block, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  void *tmp_buf = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(*this));
  } else if (OB_ISNULL(tmp_buf = allocator.alloc(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for macro block buffer failed", K(ret));
  } else {
    memcpy(tmp_buf, buf_, size_);
    dst_block.buf_ = reinterpret_cast<const char*>(tmp_buf);
    dst_block.size_ = size_;
    dst_block.block_type_ = block_type_;
    dst_block.block_handle_ = block_handle_;
    dst_block.logic_id_ = logic_id_;
    dst_block.ddl_start_scn_ = ddl_start_scn_;
    dst_block.scn_ = scn_;
  }
  return ret;
}

bool ObDDLMacroBlock::is_valid() const
{
  return block_handle_.get_block_id().is_valid()
    && logic_id_.is_valid()
    && DDL_MB_INVALID_TYPE != block_type_
    && ddl_start_scn_.is_valid_and_not_min()
    && scn_.is_valid_and_not_min()
    && nullptr != buf_
    && size_ > 0;
}


<<<<<<< HEAD
ObDDLKV::ObDDLKV()
  : is_inited_(false), ls_id_(), tablet_id_(), ddl_start_scn_(SCN::min_scn()), snapshot_version_(0),
    lock_(), allocator_("DDL_KV"), is_freezed_(false), is_closed_(false), last_freezed_scn_(SCN::min_scn()),
    min_scn_(SCN::max_scn()), max_scn_(SCN::min_scn()), freeze_scn_(SCN::max_scn()), pending_cnt_(0), cluster_version_(0),
    ref_cnt_(0), sstable_index_builder_(nullptr), index_block_rebuilder_(nullptr), is_rebuilder_closed_(false)
{
}

ObDDLKV::~ObDDLKV()
{
  destroy();
}

int ObDDLKV::init(const share::ObLSID &ls_id,
                  const common::ObTabletID &tablet_id,
                  const SCN &ddl_start_scn,
                  const int64_t snapshot_version,
                  const SCN &last_freezed_scn,
                  const int64_t cluster_version)

{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLKV has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid()
        || !tablet_id.is_valid()
        || !ddl_start_scn.is_valid_and_not_min()
        || snapshot_version <= 0
        || !last_freezed_scn.is_valid_and_not_min()
        || cluster_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(ddl_start_scn), K(snapshot_version), K(last_freezed_scn), K(cluster_version));
  } else {
    ObTabletDDLParam ddl_param;
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id;
    ddl_param.table_key_.tablet_id_ = tablet_id;
    ddl_param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    ddl_param.table_key_.version_range_.base_version_ = 0;
    ddl_param.table_key_.version_range_.snapshot_version_ = snapshot_version;
    ddl_param.start_scn_ = ddl_start_scn;
    ddl_param.snapshot_version_ = snapshot_version;
    ddl_param.cluster_version_ = cluster_version;
    if (OB_FAIL(ObTabletDDLUtil::prepare_index_builder(ddl_param, allocator_, sstable_index_builder_, index_block_rebuilder_))) {
      LOG_WARN("prepare index builder failed", K(ret));
    } else {
      ls_id_ = ls_id;
      tablet_id_ = tablet_id;
      ddl_start_scn_ = ddl_start_scn;
      snapshot_version_ = snapshot_version;
      last_freezed_scn_ = last_freezed_scn;
      cluster_version_ = cluster_version;
      is_inited_ = true;
      LOG_INFO("ddl kv init success", K(ls_id_), K(tablet_id_), K(ddl_start_scn_), K(snapshot_version_), K(last_freezed_scn_), K(cluster_version_));
    }
  }
  return ret;
}

void ObDDLKV::destroy()
{
  if (nullptr != index_block_rebuilder_) {
    index_block_rebuilder_->~ObIndexBlockRebuilder();
    allocator_.free(index_block_rebuilder_);
    index_block_rebuilder_ = nullptr;
  }
  if (nullptr != sstable_index_builder_) {
    sstable_index_builder_->~ObSSTableIndexBuilder();
    allocator_.free(sstable_index_builder_);
    sstable_index_builder_ = nullptr;
  }
  allocator_.reset();
}

int ObDDLKV::set_macro_block(const ObDDLMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_DDL_BLOCK_COUNT = 10L * 1024L * 1024L * 1024L / OB_SERVER_BLOCK_MGR.get_macro_block_size();
  int64_t freeze_block_count = MAX_DDL_BLOCK_COUNT;
#ifdef ERRSIM
  if (0 != GCONF.errsim_max_ddl_block_count) {
    freeze_block_count = GCONF.errsim_max_ddl_block_count;
    LOG_INFO("ddl set macro block count", K(freeze_block_count));
  }
#endif
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else if (OB_UNLIKELY(!macro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_block));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObUnitInfoGetter::ObTenantConfig unit;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(GCTX.omt_->get_tenant_unit(tenant_id, unit))) {
      LOG_WARN("get tenant unit failed", K(tmp_ret), K(tenant_id));
    } else {
      const int64_t log_allowed_block_count = unit.config_.log_disk_size() * 0.2 / OB_SERVER_BLOCK_MGR.get_macro_block_size();
      if (log_allowed_block_count <= 0) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid macro block count by log disk size", K(tmp_ret), K(tenant_id), K(unit.config_));
      } else {
        freeze_block_count = min(freeze_block_count, log_allowed_block_count);
      }
    }
  }
  if (OB_SUCC(ret) && ddl_blocks_.count() >= freeze_block_count) {
    ObDDLTableMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.start_scn_ = ddl_start_scn_;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      LOG_WARN("try schedule ddl merge dag failed when ddl kv is full ",
          K(tmp_ret), K(ls_id_), K(tablet_id_), K(ddl_blocks_.count()));
    }
  }
  if (OB_SUCC(ret)) {
    ObDataMacroBlockMeta *data_macro_meta = nullptr;
    ObArenaAllocator meta_allocator;
    TCWLockGuard guard(lock_);
    if (macro_block.ddl_start_scn_ != ddl_start_scn_) {
      if (macro_block.ddl_start_scn_ > ddl_start_scn_) {
        ret = OB_EAGAIN;
        LOG_INFO("ddl start scn too large, retry", K(ret),
            K(ls_id_), K(tablet_id_), K(ddl_start_scn_), K(macro_block));
      } else {
        // filter out and do nothing
        LOG_INFO("ddl start scn too small, maybe from old build task, ignore", K(ret),
            K(ls_id_), K(tablet_id_), K(ddl_start_scn_), K(macro_block));
      }
    } else if (macro_block.scn_ > freeze_scn_) {
      ret = OB_EAGAIN;
      LOG_INFO("this ddl kv is freezed, retry other ddl kv", K(ret), K(ls_id_), K(tablet_id_), K(macro_block), K(freeze_scn_));
    } else if (OB_FAIL(index_block_rebuilder_->append_macro_row(
            macro_block.buf_, macro_block.size_, macro_block.get_block_id()))) {
      LOG_WARN("append macro meta failed", K(ret), K(macro_block));
    } else if (OB_FAIL(ddl_blocks_.push_back(macro_block.block_handle_))) {
      LOG_WARN("push back block handle failed", K(ret), K(macro_block.block_handle_));
    } else {
      min_scn_ = SCN::min(min_scn_, macro_block.scn_);
      max_scn_ = SCN::max(max_scn_, macro_block.scn_);
      LOG_INFO("succeed to set macro block into ddl kv", K(macro_block));
    }
  }
  return ret;
}

int ObDDLKV::freeze(const SCN &freeze_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else {
    TCWLockGuard guard(lock_);
    if (is_freezed_) {
      // do nothing
    } else {
      if (freeze_scn.is_valid_and_not_min()) {
        freeze_scn_ = freeze_scn;
      } else if (max_scn_.is_valid_and_not_min()) {
        freeze_scn_ = max_scn_;
      } else {
        ret = OB_EAGAIN;
        LOG_INFO("ddl kv not freezed, try again", K(ret), K(ls_id_), K(tablet_id_), K(ddl_blocks_.count()));
      }
      if (OB_SUCC(ret)) {
        ATOMIC_SET(&is_freezed_, true);
        LOG_INFO("ddl kv freezed", K(ret), K(ls_id_), K(tablet_id_), K(ddl_blocks_.count()));
      }
    }
  }
  return ret;
}

int ObDDLKV::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else if (is_closed_) {
    // do nothing
    LOG_INFO("ddl kv already closed", K(*this));
  } else if (OB_FAIL(wait_pending())) {
    LOG_WARN("wait pending failed", K(ret));
  } else if (!is_rebuilder_closed_) {
    if (OB_FAIL(index_block_rebuilder_->close())) {
      LOG_WARN("index block rebuilder close failed", K(ret));
    } else {
      is_rebuilder_closed_ = true;
    }
  }
  if (OB_SUCC(ret) && !is_closed_) {
    ObTableHandleV2 table_handle;
    ObTabletDDLParam ddl_param;
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id_;
    ddl_param.table_key_.tablet_id_ = tablet_id_;
    ddl_param.table_key_.table_type_ = ObITable::TableType::KV_DUMP_SSTABLE;
    ddl_param.table_key_.scn_range_.start_scn_ = last_freezed_scn_;
    ddl_param.table_key_.scn_range_.end_scn_ = freeze_scn_;
    ddl_param.start_scn_ = ddl_start_scn_;
    ddl_param.snapshot_version_ = snapshot_version_;
    ddl_param.cluster_version_ = cluster_version_;
    if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(sstable_index_builder_, ddl_param, table_handle))) {
      LOG_WARN("create ddl sstable failed", K(ret));
    } else {
      is_closed_ = true;
      LOG_INFO("ddl kv closed success", K(*this));
    }
  }
  return ret;
}

void ObDDLKV::inc_pending_cnt()
{
  ATOMIC_INC(&pending_cnt_);
}

void ObDDLKV::dec_pending_cnt()
{
  ATOMIC_DEC(&pending_cnt_);
}

int ObDDLKV::wait_pending()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_freezed())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl kv not freezed", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls handle failed", K(ret), K(ls_id_));
  } else {
    SCN max_decided_scn;
    bool wait_ls_ts = true;
    bool wait_ddl_redo = true;
    const int64_t abs_timeout_ts = ObTimeUtility::fast_current_time() + 1000L * 1000L * 10L;
    while (OB_SUCC(ret)) {
      if (wait_ls_ts) {
        if (OB_FAIL(ls_handle.get_ls()->get_max_decided_scn(max_decided_scn))) {
          LOG_WARN("get max decided log ts failed", K(ret), K(ls_id_));
        } else {
          // max_decided_scn is the left border scn - 1
          wait_ls_ts = SCN::plus(max_decided_scn, 1) < freeze_scn_;
        }
      }
      if (OB_SUCC(ret)) {
        if (!wait_ls_ts) {
          wait_ddl_redo = is_pending();
        }
        if (wait_ls_ts || wait_ddl_redo) {
          if (ObTimeUtility::fast_current_time() > abs_timeout_ts) {
            ret = OB_TIMEOUT;
            LOG_WARN("wait pending ddl kv timeout", K(ret), K(*this), K(max_decided_scn), K(wait_ls_ts), K(wait_ddl_redo));
          } else {
            ob_usleep(1L * 1000L); // 1 ms
          }
          if (REACH_TIME_INTERVAL(1000L * 1000L * 1L)) {
            LOG_INFO("wait pending ddl kv", K(ret), K(*this));
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

ObDDLKVHandle::ObDDLKVHandle()
  : kv_(nullptr)
{
}

ObDDLKVHandle::~ObDDLKVHandle()
{
  reset();
}

int ObDDLKVHandle::set_ddl_kv(ObDDLKV *kv)
{
  int ret = OB_SUCCESS;
  if (nullptr != kv_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLKVHandle cannot be inited twice", K(ret));
  } else if (OB_ISNULL(kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(kv));
  } else {
    kv->inc_ref();
    kv_ = kv;
  }
  return ret;
}

int ObDDLKVHandle::get_ddl_kv(ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  if (!is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    kv = kv_;
  }
  return ret;
}

void ObDDLKVHandle::reset()
{
  if (nullptr != kv_) {
    if (0 == kv_->dec_ref()) {
      op_free(kv_);
      STORAGE_LOG(INFO, "free a ddl kv", KP(kv_));
    }
    kv_ = nullptr;
  }
}

ObDDLKVsHandle::ObDDLKVsHandle()
  : kv_array_()
{
}

ObDDLKVsHandle::~ObDDLKVsHandle()
{
  reset();
}

void ObDDLKVsHandle::reset()
{
  for (int64_t i = 0; i < kv_array_.count(); ++i) {
    ObDDLKV *kv = kv_array_.at(i);
    if (nullptr != kv) {
      if (0 == kv->dec_ref()) {
        op_free(kv);
        STORAGE_LOG(INFO, "free a ddl kv");
      }
      kv_array_.at(i) = nullptr;
    }
  }
  kv_array_.reset();
}

int ObDDLKVsHandle::add_ddl_kv(ObDDLKV *kv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(kv));
  } else if (OB_FAIL(kv_array_.push_back(kv))) {
    LOG_WARN("fail to push back kv array", K(ret));
  } else {
    kv->inc_ref();
  }
  return ret;
}

int ObDDLKVsHandle::get_ddl_kv(const int64_t idx, ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  if (idx >= kv_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(idx), K(kv_array_.count()));
  } else {
    kv = kv_array_.at(idx);
  }
  return ret;
}

ObDDLKVPendingGuard::ObDDLKVPendingGuard(ObTablet *tablet, const SCN &scn)
=======
ObDDLKVPendingGuard::ObDDLKVPendingGuard(ObTablet *tablet, const SCN &start_scn, const SCN &scn)
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  : tablet_(tablet), scn_(scn), kv_handle_(), ret_(OB_SUCCESS)
{
  int ret = OB_SUCCESS;
  ObDDLKV *curr_kv = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_UNLIKELY(nullptr == tablet || !scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(scn));
  } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret));
<<<<<<< HEAD
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_ddl_kv(scn, kv_handle_))) {
=======
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_ddl_kv(start_scn, scn, kv_handle_))) {
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    LOG_WARN("acquire ddl kv failed", K(ret));
  } else if (OB_ISNULL(curr_kv = static_cast<ObDDLKV *>(kv_handle_.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, active ddl kv must not be nullptr", K(ret));
  } else {
    curr_kv->inc_pending_cnt();
  }
  if (OB_FAIL(ret)) {
    kv_handle_.reset();
    ret_ = ret;
  }
}

int ObDDLKVPendingGuard::get_ddl_kv(ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  if (OB_FAIL(ret_)) {
    // do nothing
  } else {
    kv = static_cast<ObDDLKV *>(kv_handle_.get_table());
  }
  return ret;
}

ObDDLKVPendingGuard::~ObDDLKVPendingGuard()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret_) {
    ObDDLKV *curr_kv = static_cast<ObDDLKV *>(kv_handle_.get_table());
    if (nullptr != curr_kv) {
      curr_kv->dec_pending_cnt();
    }
  }
  kv_handle_.reset();
}

int ObDDLKVPendingGuard::set_macro_block(ObTablet *tablet, const ObDDLMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RETRY_COUNT = 10;
  if (OB_UNLIKELY(nullptr == tablet || !macro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(macro_block));
  } else {
    int64_t try_count = 0;
    while ((OB_SUCCESS == ret || OB_EAGAIN == ret) && try_count < MAX_RETRY_COUNT) {
      ObDDLKV *ddl_kv = nullptr;
<<<<<<< HEAD
      ObDDLKVPendingGuard guard(tablet, macro_block.scn_);
=======
      ObDDLKVPendingGuard guard(tablet, macro_block.ddl_start_scn_, macro_block.scn_);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
      if (OB_FAIL(guard.get_ddl_kv(ddl_kv))) {
        LOG_WARN("get ddl kv failed", K(ret));
      } else if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), KP(ddl_kv), K(guard));
      } else if (OB_FAIL(ddl_kv->set_macro_block(macro_block))) {
        LOG_WARN("fail to set macro block info", K(ret));
      } else {
        break;
      }
      if (OB_EAGAIN == ret) {
        ++try_count;
        LOG_WARN("retry get ddl kv and set macro block", K(try_count));
      }
    }
  }
  return ret;
}
