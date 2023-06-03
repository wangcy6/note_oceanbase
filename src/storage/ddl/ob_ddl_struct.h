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

#ifndef OCEANBASE_STORAGE_OB_DDL_STRUCT_H_
#define OCEANBASE_STORAGE_OB_DDL_STRUCT_H_

<<<<<<< HEAD
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "share/scn.h"
#include "storage/ob_i_table.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
=======
#include "lib/container/ob_array.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

namespace oceanbase
{
namespace storage
{

static const int64_t DDL_FLUSH_MACRO_BLOCK_TIMEOUT = 5 * 1000 * 1000;

class ObDDLMacroHandle
{
public:
  ObDDLMacroHandle();
  ObDDLMacroHandle(const ObDDLMacroHandle &other);
  ObDDLMacroHandle &operator=(const ObDDLMacroHandle &other);
  ~ObDDLMacroHandle();
  bool is_valid() const { return block_id_.is_valid(); }
  int set_block_id(const blocksstable::MacroBlockId &block_id);
  int reset_macro_block_ref();
  const blocksstable::MacroBlockId &get_block_id() const { return block_id_; }
  TO_STRING_KV(K_(block_id));
private:
  blocksstable::MacroBlockId block_id_;
};

class ObDDLMacroBlock final
{
public:
  ObDDLMacroBlock();
  ~ObDDLMacroBlock();
  const blocksstable::MacroBlockId &get_block_id() const { return block_handle_.get_block_id(); }
  int deep_copy(ObDDLMacroBlock &dst_block, common::ObIAllocator &allocator) const;
  bool is_valid() const;
  TO_STRING_KV(K_(block_handle), K_(logic_id), K_(block_type), K_(ddl_start_scn), K_(scn), KP_(buf), K_(size));
public:
  ObDDLMacroHandle block_handle_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  blocksstable::ObDDLMacroBlockType block_type_;
  share::SCN ddl_start_scn_;
  share::SCN scn_;
  const char *buf_;
  int64_t size_;
};


<<<<<<< HEAD
class ObDDLKV
{
public:
  ObDDLKV();
  ~ObDDLKV();
  int init(const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &ddl_start_scn,
           const int64_t snapshot_version,
           const share::SCN &last_freezed_scn,
           const int64_t cluster_version);
  void destroy();
  int set_macro_block(const ObDDLMacroBlock &macro_block);

  int freeze(const share::SCN &freeze_scn);
  bool is_freezed() const { return ATOMIC_LOAD(&is_freezed_); }
  int close();
  bool is_closed() const { return is_closed_; }
  share::SCN get_min_scn() const { return min_scn_; }
  share::SCN get_freeze_scn() const { return freeze_scn_; }
  share::SCN get_ddl_start_scn() const { return ddl_start_scn_; }
  int64_t get_macro_block_cnt() const { return ddl_blocks_.count(); }
  void inc_pending_cnt(); // used by ddl kv pending guard
  void dec_pending_cnt();
  bool is_pending() const { return ATOMIC_LOAD(&pending_cnt_) > 0; }
  int wait_pending();
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */); }
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(tablet_id), K_(ddl_start_scn), K_(snapshot_version),
      K_(is_freezed), K_(is_closed),
      K_(last_freezed_scn), K_(min_scn), K_(max_scn), K_(freeze_scn),
      K_(pending_cnt), K_(cluster_version), K_(ref_cnt), K(ddl_blocks_.count()),
      KP_(sstable_index_builder), KP_(index_block_rebuilder), K_(is_rebuilder_closed));
private:
  static const int64_t TOTAL_LIMIT = 10 * 1024 * 1024 * 1024L;
  static const int64_t HOLD_LIMIT = 10 * 1024 * 1024 * 1024L;
  friend class ObDDLSSTableIterator;
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN ddl_start_scn_; // the log ts of ddl start log
  int64_t snapshot_version_; // the snapshot version for major sstable which is completed by ddl
  common::TCRWLock lock_; // lock for ddl_blocks_ and freeze_log_ts_
  common::ObArenaAllocator allocator_;
  bool is_freezed_;
  bool is_closed_;
  share::SCN last_freezed_scn_; // the freezed log ts of last ddl kv. the log ts range of this ddl kv is (last_freezed_log_ts_, freeze_log_ts_]
  share::SCN min_scn_; // the min log ts of macro blocks
  share::SCN max_scn_; // the max log ts of macro blocks
  share::SCN freeze_scn_; // ddl kv refuse data larger than freeze log ts, freeze_log_ts >= max_log_ts
  int64_t pending_cnt_; // the amount of kvs that are replaying
  int64_t cluster_version_;
  int64_t ref_cnt_;
  ObArray<ObDDLMacroHandle> ddl_blocks_;
  blocksstable::ObSSTableIndexBuilder *sstable_index_builder_;
  blocksstable::ObIndexBlockRebuilder *index_block_rebuilder_;
  bool is_rebuilder_closed_;
};

class ObDDLKVHandle final
{
public:
  ObDDLKVHandle();
  ~ObDDLKVHandle();
  int set_ddl_kv(ObDDLKV *kv);
  int get_ddl_kv(ObDDLKV *&kv);
  bool is_valid() const { return nullptr != kv_; }
  void reset();
private:
  ObDDLKV *kv_;
  DISALLOW_COPY_AND_ASSIGN(ObDDLKVHandle);
};

class ObDDLKVsHandle final
{
public:
  ObDDLKVsHandle();
  ~ObDDLKVsHandle();
  int add_ddl_kv(ObDDLKV *ddl_kv);
  void reset();
  int64_t get_count() const { return kv_array_.count(); }
  int get_ddl_kv(const int64_t idx, ObDDLKV *&kv);
private:
  ObArray<ObDDLKV *> kv_array_;
};
=======
class ObDDLKV;


class ObTablet;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

class ObDDLKVPendingGuard final
{
public:
  static int set_macro_block(ObTablet *tablet, const ObDDLMacroBlock &macro_block);
public:
<<<<<<< HEAD
  ObDDLKVPendingGuard(ObTablet *tablet, const share::SCN &scn);
=======
  ObDDLKVPendingGuard(ObTablet *tablet, const share::SCN &start_scn, const share::SCN &scn);
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  ~ObDDLKVPendingGuard();
  int get_ret() const { return ret_; }
  int get_ddl_kv(ObDDLKV *&kv);
  TO_STRING_KV(KP(tablet_), K(scn_), K(kv_handle_), K(ret_));
private:
  ObTablet *tablet_;
<<<<<<< HEAD
  share::SCN scn_;
  ObDDLKVHandle kv_handle_;
=======
  share::SCN start_scn_;
  share::SCN scn_;
  ObTableHandleV2 kv_handle_;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  int ret_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif
