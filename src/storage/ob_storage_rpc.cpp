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
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_task_define.h"
#include "share/ob_force_print_log.h"
#include "ob_storage_rpc.h"
#include "storage/high_availability/ob_storage_ha_reader.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_log_handler.h"
#include "storage/restore/ob_ls_restore_handler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_storage_ha_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;

namespace obrpc
{

static bool is_copy_ls_inner_tablet(const common::ObIArray<common::ObTabletID> &tablet_id_list)
{
  bool is_inner = false;
  for (int64_t i = 0; i < tablet_id_list.count(); ++i) {
    is_inner = tablet_id_list.at(i).is_ls_inner_tablet();
    if (is_inner) {
      break;
    }
  }
  return is_inner;
}

static int compare_ls_rebuild_seq(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t remote_rebuild_seq)
{
  int ret = OB_SUCCESS;
  int64_t local_rebuild_seq = 0;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObMigrationUtils::get_ls_rebuild_seq(tenant_id, ls_id, local_rebuild_seq))) {
    LOG_WARN("failed to get ls rebuild seq", K(ret), K(tenant_id), K(ls_id));
  } else {
    if (local_rebuild_seq != remote_rebuild_seq) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      LOG_WARN("rebuild seq has changed", K(ret), K(local_rebuild_seq), K(remote_rebuild_seq));
      SERVER_EVENT_ADD("storage_ha", "compare_ls_rebuild_seq",
                       "tenant_id", tenant_id,
                       "ls_id", ls_id.id(),
                       "local_rebuild_seq", local_rebuild_seq,
                       "remote_rebuild_seq", remote_rebuild_seq);
    }
  }
  return ret;
}

ObCopyMacroBlockArg::ObCopyMacroBlockArg()
  : logic_macro_block_id_()
{
}

void ObCopyMacroBlockArg::reset()
{
  logic_macro_block_id_.reset();
}

bool ObCopyMacroBlockArg::is_valid() const
{
  return logic_macro_block_id_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockArg,
    logic_macro_block_id_);


ObCopyMacroBlockListArg::ObCopyMacroBlockListArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    table_key_(),
    arg_list_()
{
}

void ObCopyMacroBlockListArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  table_key_.reset();
  arg_list_.reset();
}

bool ObCopyMacroBlockListArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && table_key_.is_valid()
      && arg_list_.count() > 0;
}

int ObCopyMacroBlockListArg::assign(const ObCopyMacroBlockListArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch macro block list get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(arg_list_.assign(arg.arg_list_))) {
    LOG_WARN("failed to assign arg list", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    table_key_ = arg.table_key_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockListArg, tenant_id_, ls_id_, table_key_, arg_list_);

ObCopyMacroBlockRangeArg::ObCopyMacroBlockRangeArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    table_key_(),
    data_version_(0),
    backfill_tx_scn_(SCN::min_scn()),
    copy_macro_range_info_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1)
{
}

void ObCopyMacroBlockRangeArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  table_key_.reset();
  data_version_ = 0;
  backfill_tx_scn_.set_min();
  copy_macro_range_info_.reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

bool ObCopyMacroBlockRangeArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && table_key_.is_valid()
      && data_version_ >= 0
      && backfill_tx_scn_ >= SCN::min_scn()
      && copy_macro_range_info_.is_valid()
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);
}

int ObCopyMacroBlockRangeArg::assign(const ObCopyMacroBlockRangeArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign copy macro block range arg get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(copy_macro_range_info_.assign(arg.copy_macro_range_info_))) {
    LOG_WARN("failed to assign copy macro range info", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    table_key_ = arg.table_key_;
    data_version_ = arg.data_version_;
    backfill_tx_scn_ = arg.backfill_tx_scn_;
    need_check_seq_ = arg.need_check_seq_;
    ls_rebuild_seq_ = arg.ls_rebuild_seq_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockRangeArg, tenant_id_, ls_id_, table_key_, data_version_,
    backfill_tx_scn_, copy_macro_range_info_, need_check_seq_, ls_rebuild_seq_);

ObCopyMacroBlockHeader::ObCopyMacroBlockHeader()
  : is_reuse_macro_block_(false),
    occupy_size_(0)
{
}

void ObCopyMacroBlockHeader::reset()
{
  is_reuse_macro_block_ = false;
  occupy_size_ = 0;
}

bool ObCopyMacroBlockHeader::is_valid() const
{
  return occupy_size_ > 0;
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockHeader, is_reuse_macro_block_, occupy_size_);

ObCopyTabletInfoArg::ObCopyTabletInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_list_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    is_only_copy_major_(false),
    version_(OB_INVALID_ID)
{
}

void ObCopyTabletInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_list_.reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  is_only_copy_major_ = false;
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletInfoArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && tablet_id_list_.count() > 0
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_)
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyTabletInfoArg,
    tenant_id_, ls_id_, tablet_id_list_, need_check_seq_, ls_rebuild_seq_, is_only_copy_major_, version_);

ObCopyTabletInfo::ObCopyTabletInfo()
  : tablet_id_(),
    status_(ObCopyTabletStatus::MAX_STATUS),
    param_(),
    data_size_(0),
    version_(OB_INVALID_ID)
{
}

void ObCopyTabletInfo::reset()
{
  tablet_id_.reset();
  status_ = ObCopyTabletStatus::MAX_STATUS;
  param_.reset();
  data_size_ = 0;
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletInfo::is_valid() const
{
  return tablet_id_.is_valid()
      && ObCopyTabletStatus::is_valid(status_)
      && ((ObCopyTabletStatus::TABLET_EXIST == status_ && param_.is_valid() && data_size_ >= 0)
        || ObCopyTabletStatus::TABLET_NOT_EXIST == status_)
      && version_ != OB_INVALID_ID;
}

int ObCopyTabletInfo::assign(const ObCopyTabletInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy tablet info is invalid", K(ret), K(info));
  } else if (OB_FAIL(param_.assign(info.param_))) {
    LOG_WARN("failed to assign copy info param", K(ret), K(info));
  } else {
    status_ = info.status_;
    data_size_ = info.data_size_;
    version_ = info.version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopyTabletInfo, tablet_id_, status_, param_, data_size_, version_);

/******************ObCopyTabletSSTableInfoArg*********************/
ObCopyTabletSSTableInfoArg::ObCopyTabletSSTableInfoArg()
  : tablet_id_(),
    max_major_sstable_snapshot_(0),
    minor_sstable_scn_range_(),
    ddl_sstable_scn_range_()
{
}

ObCopyTabletSSTableInfoArg::~ObCopyTabletSSTableInfoArg()
{
}

void ObCopyTabletSSTableInfoArg::reset()
{
  tablet_id_.reset();
  max_major_sstable_snapshot_ = 0;
  minor_sstable_scn_range_.reset();
  ddl_sstable_scn_range_.reset();
}

bool ObCopyTabletSSTableInfoArg::is_valid() const
{
  return tablet_id_.is_valid()
      && max_major_sstable_snapshot_ >= 0
      && minor_sstable_scn_range_.is_valid()
      && ddl_sstable_scn_range_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCopyTabletSSTableInfoArg,
    tablet_id_, max_major_sstable_snapshot_, minor_sstable_scn_range_, ddl_sstable_scn_range_);

ObCopyTabletsSSTableInfoArg::ObCopyTabletsSSTableInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    is_only_copy_major_(false),
    tablet_sstable_info_arg_list_(),
    version_(OB_INVALID_ID)
{
}

ObCopyTabletsSSTableInfoArg::~ObCopyTabletsSSTableInfoArg()
{
  reset();
}

void ObCopyTabletsSSTableInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  is_only_copy_major_ = false;
  tablet_sstable_info_arg_list_.reset();
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletsSSTableInfoArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_)
      && tablet_sstable_info_arg_list_.count() > 0
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyTabletsSSTableInfoArg,
    tenant_id_, ls_id_, need_check_seq_, ls_rebuild_seq_, is_only_copy_major_, tablet_sstable_info_arg_list_, version_);


ObCopyTabletSSTableInfo::ObCopyTabletSSTableInfo()
  : tablet_id_(),
    table_key_(),
    param_()
{
}

void ObCopyTabletSSTableInfo::reset()
{
  tablet_id_.reset();
  table_key_.reset();
  param_.reset();
}

bool ObCopyTabletSSTableInfo::is_valid() const
{
  return tablet_id_.is_valid()
      && table_key_.is_valid()
      && param_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCopyTabletSSTableInfo,
    tablet_id_, table_key_, param_);


ObCopyLSInfoArg::ObCopyLSInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    version_(OB_INVALID_ID)
{
}

void ObCopyLSInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  version_ = OB_INVALID_ID;
}

bool ObCopyLSInfoArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyLSInfoArg,
    tenant_id_, ls_id_, version_);


ObCopyLSInfo::ObCopyLSInfo()
  : ls_meta_package_(),
    tablet_id_array_(),
    is_log_sync_(false),
    version_(OB_INVALID_ID)
{
}

void ObCopyLSInfo::reset()
{
  ls_meta_package_.reset();
  tablet_id_array_.reset();
  is_log_sync_ = false;
  version_ = OB_INVALID_ID;
}

bool ObCopyLSInfo::is_valid() const
{
  return ls_meta_package_.is_valid() && tablet_id_array_.count() > 0 && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyLSInfo,
    ls_meta_package_, tablet_id_array_, is_log_sync_, version_);

ObFetchLSMetaInfoArg::ObFetchLSMetaInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    version_(OB_INVALID_ID)
{
}

void ObFetchLSMetaInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  version_ = OB_INVALID_ID;
}

bool ObFetchLSMetaInfoArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObFetchLSMetaInfoArg, tenant_id_, ls_id_, version_);


ObFetchLSMetaInfoResp::ObFetchLSMetaInfoResp()
  : ls_meta_package_(),
    version_(OB_INVALID_ID)
{
}

void ObFetchLSMetaInfoResp::reset()
{
  ls_meta_package_.reset();
  version_ = OB_INVALID_ID;
}

bool ObFetchLSMetaInfoResp::is_valid() const
{
  return ls_meta_package_.is_valid()
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObFetchLSMetaInfoResp, ls_meta_package_, version_);

ObFetchLSMemberListArg::ObFetchLSMemberListArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}

bool ObFetchLSMemberListArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && ls_id_.is_valid();
}

void ObFetchLSMemberListArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObFetchLSMemberListArg, tenant_id_, ls_id_);

ObFetchLSMemberListInfo::ObFetchLSMemberListInfo()
  : member_list_()
{
}

bool ObFetchLSMemberListInfo::is_valid() const
{
  return member_list_.is_valid();
}

void ObFetchLSMemberListInfo::reset()
{
  member_list_.reset();
}

OB_SERIALIZE_MEMBER(ObFetchLSMemberListInfo, member_list_);

ObCopySSTableMacroRangeInfoArg::ObCopySSTableMacroRangeInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    copy_table_key_array_(),
    macro_range_max_marco_count_(0),
    need_check_seq_(false),
    ls_rebuild_seq_(0)
{
}

ObCopySSTableMacroRangeInfoArg::~ObCopySSTableMacroRangeInfoArg()
{
}

void ObCopySSTableMacroRangeInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  copy_table_key_array_.reset();
  macro_range_max_marco_count_ = 0;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

bool ObCopySSTableMacroRangeInfoArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && tablet_id_.is_valid()
      && copy_table_key_array_.count() > 0
      && macro_range_max_marco_count_ > 0;
}

int ObCopySSTableMacroRangeInfoArg::assign(const ObCopySSTableMacroRangeInfoArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy sstable macro range info arg is invalid", K(ret), K(arg));
  } else if (OB_FAIL(copy_table_key_array_.assign(arg.copy_table_key_array_))) {
    LOG_WARN("failed to assgin src table array", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    tablet_id_ = arg.tablet_id_;
    macro_range_max_marco_count_ = arg.macro_range_max_marco_count_;
    need_check_seq_ = arg.need_check_seq_;
    ls_rebuild_seq_ = arg.ls_rebuild_seq_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopySSTableMacroRangeInfoArg, tenant_id_, ls_id_,
    tablet_id_, copy_table_key_array_, macro_range_max_marco_count_,
    need_check_seq_, ls_rebuild_seq_);

ObCopySSTableMacroRangeInfoHeader::ObCopySSTableMacroRangeInfoHeader()
  : copy_table_key_(),
    macro_range_count_(0)
{
}

ObCopySSTableMacroRangeInfoHeader::~ObCopySSTableMacroRangeInfoHeader()
{
}

void ObCopySSTableMacroRangeInfoHeader::reset()
{
  copy_table_key_.reset();
  macro_range_count_ = 0;
}

bool ObCopySSTableMacroRangeInfoHeader::is_valid() const
{
  return copy_table_key_.is_valid()
      && macro_range_count_ >= 0;
}

OB_SERIALIZE_MEMBER(ObCopySSTableMacroRangeInfoHeader,
    copy_table_key_, macro_range_count_);

ObCopyTabletSSTableHeader::ObCopyTabletSSTableHeader()
  : tablet_id_(),
    status_(ObCopyTabletStatus::MAX_STATUS),
    sstable_count_(0),
    tablet_meta_(),
    version_(OB_INVALID_ID)
{
}

void ObCopyTabletSSTableHeader::reset()
{
  tablet_id_.reset();
  status_ = ObCopyTabletStatus::MAX_STATUS;
  sstable_count_ = 0;
  tablet_meta_.reset();
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletSSTableHeader::is_valid() const
{
  return tablet_id_.is_valid()
      && ObCopyTabletStatus::is_valid(status_)
      && sstable_count_ >= 0
      && ((ObCopyTabletStatus::TABLET_EXIST == status_ && tablet_meta_.is_valid())
          || ObCopyTabletStatus::TABLET_NOT_EXIST == status_)
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyTabletSSTableHeader,
    tablet_id_, status_, sstable_count_, tablet_meta_, version_);

ObNotifyRestoreTabletsArg::ObNotifyRestoreTabletsArg()
  : tenant_id_(OB_INVALID_ID), ls_id_(), tablet_id_array_(), restore_status_(), leader_proposal_id_(0)
{
}

void ObNotifyRestoreTabletsArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_array_.reset();
  leader_proposal_id_ = 0;
}

bool ObNotifyRestoreTabletsArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
         && restore_status_.is_valid()
         && leader_proposal_id_ > 0;
}

OB_SERIALIZE_MEMBER(ObNotifyRestoreTabletsArg, tenant_id_, ls_id_, tablet_id_array_, restore_status_, leader_proposal_id_);


ObNotifyRestoreTabletsResp::ObNotifyRestoreTabletsResp()
  : tenant_id_(OB_INVALID_ID), ls_id_(), restore_status_()
{
}

void ObNotifyRestoreTabletsResp::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}

bool ObNotifyRestoreTabletsResp::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
         && restore_status_.is_valid();
}

OB_SERIALIZE_MEMBER(ObNotifyRestoreTabletsResp, tenant_id_, ls_id_, restore_status_);


ObInquireRestoreArg::ObInquireRestoreArg()
  : tenant_id_(OB_INVALID_ID), ls_id_(), restore_status_()
{
}

void ObInquireRestoreArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}

bool ObInquireRestoreArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
	       && restore_status_.is_valid();
}

OB_SERIALIZE_MEMBER(ObInquireRestoreArg, tenant_id_, ls_id_, restore_status_);

ObInquireRestoreResp::ObInquireRestoreResp()
  : tenant_id_(OB_INVALID_ID), ls_id_(), is_leader_(false), restore_status_()
{
}

void ObInquireRestoreResp::reset()
{
  tenant_id_ = OB_INVALID_ID;
  is_leader_ = false;
  ls_id_.reset();
}

bool ObInquireRestoreResp::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
         && restore_status_.is_valid();
}

OB_SERIALIZE_MEMBER(ObInquireRestoreResp, tenant_id_, ls_id_, is_leader_, restore_status_);


ObRestoreUpdateLSMetaArg::ObRestoreUpdateLSMetaArg()
  : tenant_id_(OB_INVALID_ID), ls_meta_package_()
{
}

void ObRestoreUpdateLSMetaArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_meta_package_.reset();
}

bool ObRestoreUpdateLSMetaArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_meta_package_.is_valid();
}

OB_SERIALIZE_MEMBER(ObRestoreUpdateLSMetaArg, tenant_id_, ls_meta_package_);


template <ObRpcPacketCode RPC_CODE>
ObStorageStreamRpcP<RPC_CODE>::ObStorageStreamRpcP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
  : bandwidth_throttle_(bandwidth_throttle),
    last_send_time_(0),
    allocator_("SSRpcP")
{
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcP<RPC_CODE>::fill_data(const Data &data)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (serialization::encoded_length(data) > this->result_.get_remain()) {
    LOG_INFO("flush", K(this->result_));
    if (OB_FAIL(flush_and_wait())) {
      STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode(this->result_.get_data(),
                                      this->result_.get_capacity(),
                                      this->result_.get_position(),
                                      data))) {
      STORAGE_LOG(WARN, "failed to encode", K(ret));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::fill_buffer(blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    while (OB_SUCC(ret) && data.remain() > 0) {
      if (0 == this->result_.get_remain()) {
        if (OB_FAIL(flush_and_wait())) {
          STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
        }
      } else {
        int64_t fill_length = std::min(this->result_.get_remain(), data.remain());
        if (fill_length <= 0) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "fill_length must larger than 0", K(ret), K(fill_length), K(this->result_), K(data));
        } else {
          MEMCPY(this->result_.get_cur_pos(), data.current(), fill_length);
          this->result_.get_position() += fill_length;
          if (OB_FAIL(data.advance(fill_length))) {
            STORAGE_LOG(WARN, "failed to advance fill length", K(ret), K(fill_length), K(data));
          }
        }
      }
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcP<RPC_CODE>::fill_data_list(ObIArray<Data> &data_list)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_list.count(); ++i) {
      Data &data = data_list.at(i);
      if (data.get_serialize_size() > this->result_.get_remain()) {
        if (OB_FAIL(flush_and_wait())) {
          STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(data.serialize(this->result_.get_data(),
                                   this->result_.get_capacity(),
                                   this->result_.get_position()))) {
          STORAGE_LOG(WARN, "failed to encode data", K(ret));
        } else {
          STORAGE_LOG(DEBUG, "fill data", K(data), K(this->result_));
        }
      }
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcP<RPC_CODE>::fill_data_immediate(const Data &data)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (serialization::encoded_length(data) > this->result_.get_remain()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "data length is larger than result get_remain size, can not send",
        K(ret), K(serialization::encoded_length(data)), K(serialization::encoded_length(data)));
  } else if (OB_FAIL(serialization::encode(this->result_.get_data(),
                                           this->result_.get_capacity(),
                                           this->result_.get_position(),
                                           data))) {
    STORAGE_LOG(WARN, "failed to encode", K(ret));

  } else if (OB_FAIL(flush_and_wait())) {
    STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
  } else {
    LOG_INFO("flush", K(this->result_));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::flush_and_wait()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  if (NULL == bandwidth_throttle_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "bandwidth_throttle_ must not null", K(ret));
  } else {
    if (OB_SUCCESS != (tmp_ret = bandwidth_throttle_->limit_out_and_sleep(
        this->result_.get_position(), last_send_time_, max_idle_time))) {
      STORAGE_LOG(WARN, "failed limit out band", K(tmp_ret));
    }

    if (OB_FAIL(this->flush(OB_DEFAULT_STREAM_WAIT_TIMEOUT))) {
      STORAGE_LOG(WARN, "failed to flush", K(ret));
    } else {
      this->result_.get_position() = 0;
      last_send_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::alloc_buffer()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!this->result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::is_follower_ls(logservice::ObLogService *log_srv, ObLS *ls, bool &is_ls_follower)
{
  int ret = OB_SUCCESS;
  logservice::ObLogHandler *log_handler = nullptr;
  int64_t proposal_id = 0;
  ObRole role;
  if (OB_ISNULL(log_srv) || OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler should not be NULL", K(ret), KP(log_srv), K(ls));
  } else if (OB_FAIL(log_srv->get_palf_role(ls->get_ls_id(), role, proposal_id))) {
    LOG_WARN("fail to get role", K(ret), "ls_id", ls->get_ls_id());
  } else if (!is_follower(role)) {
    is_ls_follower = false;
    STORAGE_LOG(WARN, "I am not follower", K(ret), K(role), K(proposal_id));
  } else {
    is_ls_follower = true;
  }
  return ret;
}

ObHAFetchMacroBlockP::ObHAFetchMacroBlockP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
  : ObStorageStreamRpcP(bandwidth_throttle)
  , total_macro_block_count_(0)
{
}

int ObHAFetchMacroBlockP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    blocksstable::ObBufferReader data;
    char *buf = NULL;
    last_send_time_ = ObTimeUtility::current_time();
    int64_t occupy_size = 0;
    ObCopyMacroBlockHeader copy_macro_block_header;

    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "bandwidth_throttle must not null", K(ret), KP_(bandwidth_throttle));
    } else {
#ifdef ERRSIM
          if (!is_meta_tenant(arg_.tenant_id_) && 1001 == arg_.ls_id_.id() && !arg_.table_key_.tablet_id_.is_ls_inner_tablet()) {
            FLOG_INFO("errsim storage ha fetch macro block", K_(arg));
            SERVER_EVENT_SYNC_ADD("errsim_storage_ha", "fetch_macro_block");
            DEBUG_SYNC(BEFORE_MIGRATE_FETCH_MACRO_BLOCK);
          }
#endif

<<<<<<< HEAD
      SMART_VAR(storage::ObCopyMacroBlockObProducer, producer) {
        if (OB_FAIL(producer.init(arg_.tenant_id_, arg_.ls_id_, arg_.table_key_, arg_.copy_macro_range_info_,
            arg_.data_version_, arg_.backfill_tx_scn_))) {
          LOG_WARN("failed to init macro block producer", K(ret), K(arg_));
=======
      SMART_VARS_2((storage::ObCopyMacroBlockObProducer, producer), (ObCopyMacroBlockRangeArg, arg)) {
        if (OB_FAIL(arg.assign(arg_))) {
          LOG_WARN("failed to assign copy macro block range arg", K(ret), K(arg_));
        } else if (OB_FAIL(producer.init(arg.tenant_id_, arg.ls_id_, arg.table_key_, arg.copy_macro_range_info_,
            arg.data_version_, arg.backfill_tx_scn_))) {
          LOG_WARN("failed to init macro block producer", K(ret), K(arg));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
        } else {
          while (OB_SUCC(ret)) {
            copy_macro_block_header.reset();
            if (OB_FAIL(producer.get_next_macro_block(data, copy_macro_block_header))) {
              if (OB_ITER_END != ret) {
                STORAGE_LOG(WARN, "failed to get next macro block", K(ret));
              } else {
                ret = OB_SUCCESS;
              }
              break;
            } else if (!copy_macro_block_header.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "copy_macro_block_header should not be invalid", K(ret), K(copy_macro_block_header));
            } else if (OB_FAIL(fill_data(copy_macro_block_header))) {
              STORAGE_LOG(WARN, "failed to fill data length", K(ret), K(data.pos()), K(copy_macro_block_header));
            } else if (OB_FAIL(fill_buffer(data))) {
              STORAGE_LOG(WARN, "failed to fill data", K(ret), K(copy_macro_block_header));
            } else {
              STORAGE_LOG(INFO, "succeed to fill macro block",
                  "idx", total_macro_block_count_);
              ++total_macro_block_count_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (arg.need_check_seq_) {
            if (OB_FAIL(compare_ls_rebuild_seq(arg.tenant_id_, arg.ls_id_, arg.ls_rebuild_seq_))) {
              LOG_WARN("failed to compare ls rebuild seq", K(ret), K(arg));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (total_macro_block_count_ != arg.copy_macro_range_info_.macro_block_count_) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(ERROR, "macro block count not match",
                K(ret), K(total_macro_block_count_), K(arg.copy_macro_range_info_));
          }
        }
      }
    }
  }
  return ret;
}

ObFetchTabletInfoP::ObFetchTabletInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObFetchTabletInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    char * buf = NULL;
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    ObCopyTabletInfoObProducer producer;
    ObCopyTabletInfo tablet_info;
    const int64_t MAX_TABLET_NUM = 100;
    int64_t tablet_count = 0;
    LOG_INFO("start to fetch tablet info", K(arg_));

    last_send_time_ = ObTimeUtility::current_time();

    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "bandwidth_throttle_ must not null", K(ret),
                  KP_(bandwidth_throttle));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
      LOG_WARN("failed to log stream get migration status", K(ret), K(migration_status));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      STORAGE_LOG(WARN, "src migrate status do not allow migrate out", K(ret), K(migration_status));
    } else if (OB_FAIL(producer.init(arg_.tenant_id_, arg_.ls_id_, arg_.tablet_id_list_))) {
      LOG_WARN("failed to init copy tablet info producer", K(ret), K(arg_));
    } else {
#ifdef ERRSIM
      if (!is_meta_tenant(arg_.tenant_id_) && 1001 == arg_.ls_id_.id() && !is_copy_ls_inner_tablet(arg_.tablet_id_list_)) {
        FLOG_INFO("errsim storage ha fetch tablet info", K_(arg));
        SERVER_EVENT_SYNC_ADD("errsim_storage_ha", "fetch_tablet_info");
        DEBUG_SYNC(BEFORE_MIGRATE_FETCH_TABLET_INFO);
      }
#endif

      while (OB_SUCC(ret)) {
        tablet_info.reset();
        if (OB_FAIL(producer.get_next_tablet_info(tablet_info))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            STORAGE_LOG(WARN, "failed to get next tablet meta info", K(ret));
          }
        } else if (tablet_count >= MAX_TABLET_NUM) {
          if (this->result_.get_position() > 0 && OB_FAIL(flush_and_wait())) {
            LOG_WARN("failed to flush and wait", K(ret), K(tablet_info));
          } else {
            tablet_count = 0;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(fill_data(tablet_info))) {
          STORAGE_LOG(WARN, "fill to fill tablet info", K(ret), K(tablet_info));
        } else {
          tablet_count++;
        }
      }
      if (OB_SUCC(ret)) {
        if (arg_.need_check_seq_) {
          if (OB_FAIL(compare_ls_rebuild_seq(arg_.tenant_id_, arg_.ls_id_, arg_.ls_rebuild_seq_))) {
            LOG_WARN("failed to compare ls rebuild seq", K(ret), K_(arg));
          }
        }
      }
    }
  }
  return ret;
}

ObFetchSSTableInfoP::ObFetchSSTableInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObFetchSSTableInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    char * buf = NULL;
    ObCopySSTableInfoObProducer producer;
    ObCopyTabletSSTableInfo sstable_info;
    ObMigrationStatus migration_status;
    ObLS *ls = nullptr;
    LOG_INFO("start to fetch tablet sstable info", K(arg_));

    last_send_time_ = ObTimeUtility::current_time();

    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "bandwidth_throttle_ must not null", K(ret),
                  KP_(bandwidth_throttle));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(arg_));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      STORAGE_LOG(WARN, "src migrate status do not allow migrate out", K(ret), K(migration_status));
    } else if (OB_FAIL(build_tablet_sstable_info_(ls))) {
      LOG_WARN("failed to build tablet sstable info", K(ret), K(arg_));
    } else {
      if (arg_.need_check_seq_) {
        if (OB_FAIL(compare_ls_rebuild_seq(arg_.tenant_id_, arg_.ls_id_, arg_.ls_rebuild_seq_))) {
          LOG_WARN("failed to compare ls rebuild seq", K(ret), K_(arg));
        }
      }
    }
  }
  return ret;
}

int ObFetchSSTableInfoP::build_tablet_sstable_info_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObCopyTabletsSSTableInfoObProducer producer;
  obrpc::ObCopyTabletSSTableInfoArg arg;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tablet sstable info get invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(producer.init(arg_.tenant_id_, arg_.ls_id_, arg_.tablet_sstable_info_arg_list_))) {
    LOG_WARN("failed to init copy tablets sstable info ob producer", K(ret), K(arg_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(producer.get_next_tablet_sstable_info(arg))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet sstable info", K(ret), K(arg_));
        }
      } else if (OB_FAIL(build_sstable_info_(arg, ls))) {
        LOG_WARN("failed to get next tablet sstable info", K(arg));
      }
    }
  }
  return ret;
}

int ObFetchSSTableInfoP::build_sstable_info_(
    const obrpc::ObCopyTabletSSTableInfoArg &arg,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObCopySSTableInfoObProducer producer;
  obrpc::ObCopyTabletSSTableInfo sstable_info;
  obrpc::ObCopyTabletSSTableHeader tablet_sstable_header;

  if (!arg.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next tablet sstable info get invalid argument", K(ret), K(arg), KP(ls));
  } else if (OB_FAIL(producer.init(arg, ls))) {
    LOG_WARN("failed to init copy sstable info ob producer", K(ret), K(arg));
  } else if (OB_FAIL(producer.get_copy_tablet_sstable_header(tablet_sstable_header))) {
    LOG_WARN("failed to get copy tablet sstable header", K(ret), K(arg));
  } else if (OB_FAIL(fill_data(tablet_sstable_header))) {
    LOG_WARN("failed to fill tablet sstable header", K(ret), K(arg));
  } else if (0 == tablet_sstable_header.sstable_count_) {
    //do nothing
  } else {
    while (OB_SUCC(ret)) {
      sstable_info.reset();
      if (OB_FAIL(producer.get_next_sstable_info(sstable_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next sstable info", K(ret), K(arg));
        }
      } else if (OB_FAIL(fill_data(sstable_info))) {
        STORAGE_LOG(WARN, "fill to fill tablet info", K(ret), K(sstable_info));
      }
    }
  }
  return ret;
}


ObFetchLSInfoP::ObFetchLSInfoP()
{
}


int ObFetchLSInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    logservice::ObLogHandler *log_handler = nullptr;
    ObRole role;
    int64_t proposal_id = 0;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    ObMigrationStatus migration_status;
    ObLSMetaPackage ls_meta_package;
    bool is_need_rebuild = false;
    bool is_log_sync = false;
    const bool check_archive = true;

    LOG_INFO("start to fetch log stream info", K(arg_.ls_id_), K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif

    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fetch ls info get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot be migrate src", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("faield to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret), KP(log_handler), K(arg_));
    } else if (OB_FAIL(ls->get_ls_meta_package_and_tablet_ids(check_archive,
            result_.ls_meta_package_, result_.tablet_id_array_))) {
      LOG_WARN("failed to get ls meta package and tablet ids", K(ret));
    } else if (OB_FAIL(result_.ls_meta_package_.ls_meta_.get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(result_));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      STORAGE_LOG(WARN, "src migration status do not allow to migrate out", K(ret), "src migration status",
          migration_status);
    } else if (OB_FAIL(ObStorageHAUtils::get_server_version(result_.version_))) {
      LOG_WARN("failed to get server version", K(ret), K_(arg));
    } else if (OB_FAIL(MTL(logservice::ObLogService*)->get_palf_role(ls->get_ls_id(), role, proposal_id))) {
      STORAGE_LOG(WARN, "failed to get palf role", K(ret), K(arg_), "meta package", result_.ls_meta_package_);
    } else if (is_strong_leader(role)) {
      result_.is_log_sync_ = true;
    } else if (OB_FAIL(log_handler->is_in_sync(is_log_sync, is_need_rebuild))) {
      LOG_WARN("failed to check is in sync", K(ret), K(arg_));
    } else if (!is_log_sync || is_need_rebuild) {
      result_.is_log_sync_ = false;
    } else {
      result_.is_log_sync_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "succ to get partition group info", K_(result), K(ret));
  }
  return ret;

}

ObFetchLSMetaInfoP::ObFetchLSMetaInfoP()
{
}


int ObFetchLSMetaInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    logservice::ObLogHandler *log_handler = nullptr;
    ObLSMetaPackage ls_meta_package;
    const bool check_archive = true;
    LOG_INFO("start to fetch log stream info", K(arg_.ls_id_), K(arg_));
    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fetch ls info get invalid argument", K(ret), K(arg_));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("faield to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret), KP(log_handler), K(arg_));
    } else if (OB_FAIL(ls->get_ls_meta_package(check_archive, result_.ls_meta_package_))) {
      LOG_WARN("failed to get ls meta package", K(ret), K(arg_));
    } else if (OB_FAIL(ObStorageHAUtils::get_server_version(result_.version_))) {
      LOG_WARN("failed to get server version", K(ret), K_(arg));
    }
  }
  return ret;

}

ObFetchLSMemberListP::ObFetchLSMemberListP()
{
}

int ObFetchLSMemberListP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;
  MTL_SWITCH(tenant_id) {
    ObLSService *ls_svr = NULL;
    ObLSHandle ls_handle;
    ObLS *ls = NULL;
    logservice::ObLogHandler *log_handler = NULL;
    common::ObMemberList member_list;
    int64_t paxos_replica_num = 0;
    if (tenant_id != MTL_ID()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc get member list tenant not match", K(ret), K(tenant_id));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be null", K(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret));
    } else if (OB_FAIL(log_handler->get_paxos_member_list(member_list, paxos_replica_num))) {
      LOG_WARN("failed to get paxos member list", K(ret));
    } else if (OB_FAIL(result_.member_list_.deep_copy(member_list))) {
      LOG_WARN("failed to assign", K(ret), K(member_list));
    }
  }
  return ret;
}

ObFetchSSTableMacroInfoP::ObFetchSSTableMacroInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObFetchSSTableMacroInfoP::process()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  char * buf = NULL;
  ObMigrationStatus migration_status;
  LOG_INFO("start to fetch sstable macro info", K(arg_));

  last_send_time_ = ObTimeUtility::current_time();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

  if (OB_FAIL(guard.switch_to(arg_.tenant_id_))) {
    LOG_ERROR("switch tenant fail", K(ret), K(arg_));
  } else if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  } else if (OB_ISNULL(bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "bandwidth_throttle_ must not null", K(ret),
                KP_(bandwidth_throttle));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(arg_));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(arg_));
  } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
    ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
    STORAGE_LOG(WARN, "src migrate status do not allow migrate out", K(ret), K(migration_status));
  } else if (OB_FAIL(fetch_sstable_macro_info_header_())) {
    LOG_WARN("failed to fetch sstable macro info header", K(ret), K(arg_));
  } else {
    if (arg_.need_check_seq_) {
      if (OB_FAIL(compare_ls_rebuild_seq(arg_.tenant_id_, arg_.ls_id_, arg_.ls_rebuild_seq_))) {
        LOG_WARN("failed to compare ls rebuild seq", K(ret), K_(arg));
      }
    }
  }
  return ret;
}

int ObFetchSSTableMacroInfoP::fetch_sstable_macro_info_header_()
{
  int ret = OB_SUCCESS;
  ObCopySSTableMacroObProducer producer;
  obrpc::ObCopySSTableMacroRangeInfoHeader header;
  if (OB_FAIL(producer.init(arg_.tenant_id_, arg_.ls_id_, arg_.tablet_id_,
      arg_.copy_table_key_array_, arg_.macro_range_max_marco_count_))) {
    LOG_WARN("failed to init copy tablet info producer", K(ret), K(arg_));
  } else {
#ifdef ERRSIM
    if (!is_meta_tenant(arg_.tenant_id_) && 1001 == arg_.ls_id_.id() && !arg_.tablet_id_.is_ls_inner_tablet()) {
      FLOG_INFO("errsim storage ha fetch sstable info", K_(arg));
      SERVER_EVENT_SYNC_ADD("errsim_storage_ha", "fetch_sstable_info");
      DEBUG_SYNC(BEFORE_MIGRATE_FETCH_SSTABLE_MACRO_INFO);
    }
#endif
    while (OB_SUCC(ret)) {
      header.reset();
      if (OB_FAIL(producer.get_next_sstable_macro_range_info(header))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "failed to get next sstable macro range info", K(ret));
        }
      } else if (OB_FAIL(fill_data(header))) {
        STORAGE_LOG(WARN, "fill to fill tablet info", K(ret), K(header));
      } else if (OB_FAIL(fetch_sstable_macro_range_info_(header))) {
        LOG_WARN("failed to fetch sstable macro range info", K(ret), K(header));
      }
    }
  }
  return ret;
}

int ObFetchSSTableMacroInfoP::fetch_sstable_macro_range_info_(const obrpc::ObCopySSTableMacroRangeInfoHeader &header)
{
  int ret = OB_SUCCESS;

  if (!header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch sstable macro range info get invalid argument", K(ret) ,K(header));
  } else {
    SMART_VARS_2((ObCopySSTableMacroRangeObProducer, macro_range_producer), (ObCopyMacroRangeInfo, macro_range_info)) {
      if (OB_FAIL(macro_range_producer.init(
          arg_.tenant_id_, arg_.ls_id_, arg_.tablet_id_, header, arg_.macro_range_max_marco_count_))) {
        LOG_WARN("failed to init macro range producer", K(ret), K(arg_), K(header));
      } else {
        while (OB_SUCC(ret)) {
          macro_range_info.reuse();
          if (OB_FAIL(macro_range_producer.get_next_macro_range_info(macro_range_info))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next macro range info", K(ret), K(header), K(arg_));
            }
          } else if (OB_FAIL(fill_data(macro_range_info))) {
            LOG_WARN("failed to fill macro range info", K(ret), K(macro_range_info), K(arg_));
          }
        }
      }
    }
  }
  return ret;
}


ObNotifyRestoreTabletsP::ObNotifyRestoreTabletsP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{

}

int ObNotifyRestoreTabletsP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    logservice::ObLogService *log_srv = nullptr;
    int64_t disk_abnormal_time = 0;
    bool is_follower = false;

    LOG_INFO("start to notify follower restore tablets", K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif

    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("notify follower restore tablets get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", K(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_srv = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log srv should not be null", K(ret), KP(log_srv));
    } else if (OB_FAIL(is_follower_ls(log_srv, ls, is_follower))) {
      LOG_WARN("failed to check is follower", K(ret), KP(ls), K(arg_));
    } else if (!is_follower) {
      ret = OB_NOT_FOLLOWER;
      STORAGE_LOG(WARN, "I am not follower", K(ret), K(arg_));
    } else {
      ObLSRestoreHandler *ls_restore_handler = ls->get_ls_restore_handler();
      if (OB_FAIL(ls_restore_handler->handle_pull_tablet(
          arg_.tablet_id_array_, arg_.restore_status_, arg_.leader_proposal_id_))) {
        LOG_WARN("fail to handle pull tablet", K(ret), K(arg_));
      } else if (OB_FAIL(ls->get_restore_status(result_.restore_status_))) {
        LOG_WARN("fail to get restore status", K(ret));
      } else {
        result_.tenant_id_ = arg_.tenant_id_;
        result_.ls_id_ = arg_.ls_id_;
      }
    }
  }
  return ret;
}


ObInquireRestoreP::ObInquireRestoreP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{

}


int ObInquireRestoreP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    logservice::ObLogService *log_srv = nullptr;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    bool is_follower = false;

    LOG_INFO("start to inquire restore status", K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif

    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("notify follower restore get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_srv = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log srv should not be null", K(ret), KP(log_srv));
    } else if (OB_FAIL(is_follower_ls(log_srv, ls, is_follower))) {
      LOG_WARN("failed to check is follower", K(ret), KP(ls), K(arg_));
    } else if (OB_FAIL(ls->get_restore_status(result_.restore_status_))) {
      LOG_WARN("fail to get restore status", K(ret));
    } else if (is_follower) {
      result_.tenant_id_ = arg_.tenant_id_;
      result_.ls_id_ = arg_.ls_id_;
      result_.is_leader_ = false;
      LOG_INFO("succ to inquire restore status from follower", K(result_));
    } else {
      result_.tenant_id_ = arg_.tenant_id_;
      result_.ls_id_ = arg_.ls_id_;
      result_.is_leader_ = true;
      LOG_INFO("succ to inquire restore status from leader", K(ret), K(arg_), K(result_));
    }
  }
  return ret;
}

ObUpdateLSMetaP::ObUpdateLSMetaP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{

}


int ObUpdateLSMetaP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    bool is_follower = false;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;

    LOG_INFO("start to update ls meta", K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif
    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("notify follower restore get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->restore_update_ls(arg_.ls_meta_package_))) {
      LOG_WARN("failed to get log stream", K(ret), K(arg_));
    } else {
      LOG_INFO("succ to update ls meta", K(ret), K(arg_));
    }
  }
  return ret;
}

ObLobQueryP::ObLobQueryP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
  : ObStorageStreamRpcP(bandwidth_throttle)
{
  // the streaming interface may return multi packet. The memory may be freed after the first packet has been sended.
  // the deserialization of arg_ is shallow copy, so we need deep copy data to processor
  set_preserve_recv_data();
}

int ObLobQueryP::process_read()
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObLobQueryBlock header;
  blocksstable::ObBufferReader data;
  char *out_buf = nullptr;
  int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN - sizeof(ObLobQueryBlock);
  if (OB_ISNULL(out_buf = reinterpret_cast<char*>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc out data buffer.", K(ret));
  } else {
    ObString out;
    ObLobAccessParam param;
    param.scan_backward_ = arg_.scan_backward_;
    param.from_rpc_ = true;
    ObLobQueryIter *iter = nullptr;
    int64_t timeout = rpc_pkt_->get_timeout() + get_send_timestamp();
    if (OB_FAIL(lob_mngr->build_lob_param(param, allocator_, arg_.cs_type_, arg_.offset_,
        arg_.len_, timeout, arg_.lob_locator_))) {
      LOG_WARN("failed to build lob param", K(ret));
    } else if (OB_FAIL(lob_mngr->query(param, iter))) {
      LOG_WARN("failed to query lob.", K(ret), K(param));
    } else {
      while (OB_SUCC(ret)) {
        out.assign_buffer(out_buf, buf_len);
        if (OB_FAIL(iter->get_next_row(out))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to get next buffer", K(ret));
          }
        } else {
          header.size_ = out.length();
          data.assign(out.ptr(), out.length());
          // only scan backward need header
          if (OB_FAIL(fill_data(header))) {
            STORAGE_LOG(WARN, "failed to fill header", K(ret), K(header));
          } else if (OB_FAIL(fill_buffer(data))) {
            STORAGE_LOG(WARN, "failed to fill buffer", K(ret), K(data));
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(iter)) {
      iter->reset();
      common::sop_return(ObLobQueryIter, iter);
    }
  }
  return ret;
}

int ObLobQueryP::process_getlength()
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObLobQueryBlock header;
  blocksstable::ObBufferReader data;
  ObLobAccessParam param;
  param.scan_backward_ = arg_.scan_backward_;
  param.from_rpc_ = true;
  header.reset();
  uint64_t len = 0;
  int64_t timeout = rpc_pkt_->get_timeout() + get_send_timestamp();
  if (OB_FAIL(lob_mngr->build_lob_param(param, allocator_, arg_.cs_type_, arg_.offset_,
      arg_.len_, timeout, arg_.lob_locator_))) {
    LOG_WARN("failed to build lob param", K(ret));
  } else if (OB_FAIL(lob_mngr->getlength(param, len))) { // reuse size_ for lob_len
    LOG_WARN("failed to getlength lob.", K(ret), K(param));
  } else if (FALSE_IT(header.size_ = static_cast<int64_t>(len))) {
  } else if (OB_FAIL(fill_data(header))) {
    STORAGE_LOG(WARN, "failed to fill header", K(ret), K(header));
  }
  return ret;
}

int ObLobQueryP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLobManager *lob_mngr = MTL(ObLobManager*);
    // init result_
    char *buf = nullptr;
    int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN;
    if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc result data buffer.", K(ret));
    } else if (!result_.set_data(buf, buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (!arg_.lob_locator_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "lob locator is invalid", K(ret));
    } else if (!arg_.lob_locator_.is_persist_lob()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupport remote query non-persist lob.", K(ret), K(arg_.lob_locator_));
    } else if (arg_.qtype_ == ObLobQueryArg::QueryType::READ) {
      if (OB_FAIL(process_read())) {
        LOG_WARN("fail to process read", K(ret), K(arg_));
      }
    } else if (arg_.qtype_ == ObLobQueryArg::QueryType::GET_LENGTH) {
      if (OB_FAIL(process_getlength())) {
        LOG_WARN("fail to process read", K(ret), K(arg_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arg qtype.", K(ret), K(arg_));
    }
  }
  return ret;
}


} //namespace obrpc

namespace storage
{

ObStorageRpc::ObStorageRpc()
    : is_inited_(false),
      rpc_proxy_(NULL),
      rs_rpc_proxy_(NULL)
{
}

ObStorageRpc::~ObStorageRpc()
{
  destroy();
}

int ObStorageRpc::init(
    obrpc::ObStorageRpcProxy *rpc_proxy,
    const common::ObAddr &self,
    obrpc::ObCommonRpcProxy *rs_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "storage rpc has inited", K(ret));
  } else if (OB_ISNULL(rpc_proxy) || !self.is_valid() || OB_ISNULL(rs_rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObStorageRpc init with invalid argument",
        KP(rpc_proxy), K(self), KP(rs_rpc_proxy));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_ = self;
    rs_rpc_proxy_ = rs_rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObStorageRpc::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    rpc_proxy_ = NULL;
    self_ = ObAddr();
    rs_rpc_proxy_ = NULL;
  }
}

int ObStorageRpc::post_ls_info_request(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    obrpc::ObCopyLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "post ls info request get invalid argument", K(ret), K(src_info), K(ls_id));
  } else {
    ObCopyLSInfoArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(ObStorageHAUtils::get_server_version(arg.version_))) {
      LOG_WARN("failed to get server version", K(ret), K(ls_id));
    } else if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_).fetch_ls_info(arg, ls_info))) {
      LOG_WARN("failed to fetch ls info", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("fetch ls info successfully", K(ls_info));
    }
  }
  return ret;
}

int ObStorageRpc::post_ls_meta_info_request(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    obrpc::ObFetchLSMetaInfoResp &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "post ls info request get invalid argument", K(ret), K(src_info), K(ls_id));
  } else {
    ObFetchLSMetaInfoArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(ObStorageHAUtils::get_server_version(arg.version_))) {
      LOG_WARN("failed to get server version", K(ret), K(arg));
    } else if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_).fetch_ls_meta_info(arg, ls_info))) {
      LOG_WARN("failed to fetch ls info", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("fetch ls meta info successfully", K(ls_info));
    }
  }
  return ret;
}

int ObStorageRpc::post_ls_member_list_request(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    obrpc::ObFetchLSMemberListInfo &member_info)
{
  int ret = OB_SUCCESS;
  member_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage rpc is not inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !src_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("post ls member list request get invalid argument", K(ret), K(tenant_id), K(ls_id), K(src_info));
  } else {
    obrpc::ObFetchLSMemberListArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_).fetch_ls_member_list(arg, member_info))) {
      LOG_WARN("failed to fetch ls info", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("fetch ls member list successfully", K(member_info));
    }
  }
  return ret;
}

int ObStorageRpc::post_ls_disaster_recovery_res(const common::ObAddr &server,
                         const obrpc::ObDRTaskReplyResult &res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else if (OB_FAIL(rs_rpc_proxy_->to(server).disaster_recovery_task_reply(res))) {
    STORAGE_LOG(WARN, "post ls migration result failed", K(ret), K(res), K(server));
  } else {
    STORAGE_LOG(TRACE, "post_ls_disaster_recovery_res successfully", K(res), K(server));
  }
  return ret;
}

int ObStorageRpc::notify_restore_tablets(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &follower_info,
      const share::ObLSID &ls_id,
      const int64_t &proposal_id,
      const common::ObIArray<common::ObTabletID>& tablet_id_array,
      const share::ObLSRestoreStatus &restore_status,
      obrpc::ObNotifyRestoreTabletsResp &restore_resp)
{
  int ret = OB_SUCCESS;
  ObNotifyRestoreTabletsArg arg;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!follower_info.is_valid() || !ls_id.is_valid()
      || (tablet_id_array.empty() && (restore_status.is_restore_tablets_meta() || restore_status.is_quick_restore() || restore_status.is_restore_major_data()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "notify follower restore get invalid argument", K(ret), K(follower_info), K(ls_id), K(tablet_id_array));
  } else if (OB_FAIL(arg.tablet_id_array_.assign(tablet_id_array))) {
    STORAGE_LOG(WARN, "failed to assign tablet id array", K(ret), K(follower_info), K(ls_id), K(tablet_id_array));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.restore_status_ = restore_status;
    arg.leader_proposal_id_ = proposal_id;
    if (OB_FAIL(rpc_proxy_->to(follower_info.src_addr_).dst_cluster_id(follower_info.cluster_id_).notify_restore_tablets(arg, restore_resp))) {
      LOG_WARN("failed to notify follower restore tablets", K(ret), K(arg), K(follower_info), K(ls_id), K(tablet_id_array));
    } else {
      FLOG_INFO("notify follower restore tablets successfully", K(arg), K(follower_info), K(ls_id), K(tablet_id_array));
    }
  }
  return ret;
}

int ObStorageRpc::inquire_restore(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    const share::ObLSRestoreStatus &restore_status,
    obrpc::ObInquireRestoreResp &restore_resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "inquire restore get invalid argument", K(ret), K(src_info), K(ls_id));
  } else {
    ObInquireRestoreArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.restore_status_ = restore_status;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_).inquire_restore(arg, restore_resp))) {
      LOG_WARN("failed to inquire restore", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("inquire restore status successfully", K(arg), K(src_info));
    }
  }
  return ret;
}

int ObStorageRpc::update_ls_meta(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &dest_info,
    const storage::ObLSMetaPackage &ls_meta)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!dest_info.is_valid() || !ls_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(dest_info), K(ls_meta));
  } else {
    ObRestoreUpdateLSMetaArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_meta_package_ = ls_meta;
    if (OB_FAIL(rpc_proxy_->to(dest_info.src_addr_).dst_cluster_id(dest_info.cluster_id_).update_ls_meta(arg))) {
      LOG_WARN("failed to update ls meta", K(ret), K(dest_info), K(ls_meta));
    } else {
      FLOG_INFO("update ls meta succ", K(dest_info), K(ls_meta));
    }
  }

  return ret;
}


} // storage
} // oceanbase
