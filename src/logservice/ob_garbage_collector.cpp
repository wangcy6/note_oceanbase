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

#include "ob_garbage_collector.h"
#include "palf_handle_guard.h"
#include "ob_log_handler.h"
#include "ob_log_service.h"
#include "ob_switch_leader_adapter.h"
#include "archiveservice/ob_archive_service.h"
#include "share/scn.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/ls/ob_ls.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ls/ob_ls_life_manager.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "rootserver/ob_tenant_recovery_reportor.h"      // ObTenantRecoveryReportor
#include "share/ob_occam_time_guard.h"

namespace oceanbase
{
using namespace archive;
using namespace common;
using namespace share;
using namespace storage;
using namespace palf;
namespace logservice
{
class ObGarbageCollector::InsertLSFunctor
{
public:
  explicit InsertLSFunctor(const ObLSID &id)
      : id_(id), ret_value_(OB_SUCCESS) {}
  ~InsertLSFunctor() {}
public:
  bool operator()(const ObAddr &leader, ObLSArray &ls_array)
  {
    if (OB_SUCCESS != (ret_value_ = ls_array.push_back(id_))) {
      CLOG_LOG_RET(WARN, ret_value_, "ls_array push_back failed", K(ret_value_), K(id_), K(leader));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const { return ret_value_; }
private:
  ObLSID id_;
  int ret_value_;
private:
  DISALLOW_COPY_AND_ASSIGN(InsertLSFunctor);
};

class ObGarbageCollector::QueryLSIsValidMemberFunctor
{
public:
  QueryLSIsValidMemberFunctor(obrpc::ObSrvRpcProxy *rpc_proxy,
                              ObLSService *ls_service,
                              const common::ObAddr &self_addr,
                              const int64_t gc_seq,
                              ObGCCandidateArray &gc_candidates)
      : rpc_proxy_(rpc_proxy), ls_service_(ls_service), self_addr_(self_addr),
        gc_seq_(gc_seq), gc_candidates_(gc_candidates), ret_value_(common::OB_SUCCESS) {}
  ~QueryLSIsValidMemberFunctor() {}
public:
  bool operator()(const common::ObAddr &leader, ObLSArray &ls_array)
  {
    if (OB_SUCCESS != (ret_value_ = handle_ls_array_(leader, ls_array))) {
      CLOG_LOG_RET(WARN, ret_value_, "handle_pg_array_ failed", K(ret_value_), K(ls_array), K(leader));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const { return ret_value_; }
private:
  int handle_ls_array_(const ObAddr &leader,
                       const ObLSArray &ls_array);
  int handle_rpc_response_(const ObAddr &leader,
                           const obrpc::ObQueryLSIsValidMemberResponse &response);
  int try_renew_location_(const ObLSArray &ls_array);
private:
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObLSService *ls_service_;
  common::ObAddr self_addr_;
  int64_t gc_seq_;
  ObGCCandidateArray &gc_candidates_;
  int ret_value_;
private:
  DISALLOW_COPY_AND_ASSIGN(QueryLSIsValidMemberFunctor);
};

int ObGarbageCollector::QueryLSIsValidMemberFunctor::handle_ls_array_(const ObAddr &leader,
                                                                      const ObLSArray &ls_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t TIMEOUT = 10 * 1000 * 1000;
  const int64_t MAX_LS_CNT = 100;
  obrpc::ObQueryLSIsValidMemberRequest request;
  obrpc::ObQueryLSIsValidMemberResponse response;
  request.tenant_id_ = MTL_ID();

  for (int64_t index = 0; OB_SUCC(ret) && index < ls_array.count(); index++) {
    request.self_addr_ = self_addr_;
    if (OB_FAIL(request.ls_array_.push_back(ls_array[index]))) {
      CLOG_LOG(WARN, "request ls_array push_back failed", K(ret), K(leader));
    } else if ((index + 1) % MAX_LS_CNT == 0 || index == ls_array.count() - 1) {
      if (OB_SUCCESS != (tmp_ret = rpc_proxy_->to(leader)
                                             .by(MTL_ID())
                                             .timeout(TIMEOUT)
                                             .query_ls_is_valid_member(request, response))
          || (OB_SUCCESS != (tmp_ret = response.ret_value_))) {
        CLOG_LOG(WARN, "query_is_valid_member failed", K(tmp_ret), K(leader), K(request));
        if (OB_SUCCESS != (tmp_ret = try_renew_location_(ls_array))) {
          CLOG_LOG(WARN, "try_renew_location_ failed", K(tmp_ret), K(ls_array));
        }
      } else if (OB_SUCCESS != (tmp_ret = handle_rpc_response_(leader, response))) {
        CLOG_LOG(WARN, "handle_rpc_response failed", K(ret), K(leader));
      } else {
        request.reset();
        response.reset();
      }
    }
  }
  return ret;
}

int ObGarbageCollector::QueryLSIsValidMemberFunctor::try_renew_location_(const ObLSArray &ls_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < ls_array.count(); index++) {
    const ObLSID &id = ls_array[index];
    if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_renew(
        obrpc::ObRpcNetHandler::CLUSTER_ID, MTL_ID(), id))) {
      CLOG_LOG(WARN, "nonblock_renew location cache failed", K(ret), K(id));
    } else { /* do nothing */ }
  }
  return ret;
}

int ObGarbageCollector::QueryLSIsValidMemberFunctor::handle_rpc_response_(const ObAddr &leader,
                                                                          const obrpc::ObQueryLSIsValidMemberResponse &response)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSArray &ls_array = response.ls_array_;
  const common::ObSEArray<bool, 16> &candidates_status = response.candidates_status_;
  const common::ObSEArray<int, 16> &ret_array = response.ret_array_;

  if (ls_array.count() != candidates_status.count()
      || ls_array.count() != ret_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "response count not match, unexpected", K(ret), K(leader));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < ls_array.count(); index++) {
      ObLSHandle handle;
      ObLS *ls;
      ObGCHandler *gc_handler = NULL;
      const ObLSID &id = ls_array[index];
      const bool is_valid_member = candidates_status[index];
      bool need_gc = false;
      if (OB_SUCCESS != ret_array[index]) {
        CLOG_LOG(INFO, "remote_ret_code is not success, need renew location", K(id), K(leader),
            "remote_ret_code", ret_array[index]);
        if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_renew(
                           obrpc::ObRpcNetHandler::CLUSTER_ID, MTL_ID(), id))) {
          CLOG_LOG(WARN, "nonblock_renew location cache failed", K(id), K(tmp_ret));
        } else {}
      } else if (OB_ISNULL(ls_service_)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "log stream service is NULL", K(ret));
      } else if (OB_SUCCESS != (tmp_ret = ls_service_->get_ls(id, handle, ObLSGetMod::OBSERVER_MOD))) {
        CLOG_LOG(WARN, "get log stream failed", K(id), K(tmp_ret));
      } else if (OB_ISNULL(ls = handle.get_ls())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, " log stream not exist", K(id), K(tmp_ret));
      } else if (OB_ISNULL(gc_handler = ls->get_gc_handler())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "gc_handler is NULL", K(tmp_ret), K(id));
      } else if (!is_valid_member) {
        if (OB_SUCCESS != (tmp_ret = gc_handler->gc_check_invalid_member_seq(gc_seq_, need_gc))) {
          CLOG_LOG(WARN, "gc_check_invalid_member_seq failed", K(tmp_ret), K(id), K(leader), K(gc_seq_), K(need_gc));
        } else if (need_gc) {
          GCCandidate candidate;
          candidate.ls_id_ = id;
          candidate.ls_status_ = LSStatus::LS_NEED_GC;
          candidate.gc_reason_ = NOT_IN_LEADER_MEMBER_LIST;

          if (OB_FAIL(gc_candidates_.push_back(candidate))) {
            CLOG_LOG(WARN, "gc_candidates push_back failed", K(ret), K(id), K(leader));
          } else {
            CLOG_LOG(INFO, "gc_candidates push_back success", K(ret), K(candidate), K(leader));
          }
        } else {
          CLOG_LOG(INFO, "gc_check_invalid_member_seq set seq", K(tmp_ret), K(id), K(leader), K(gc_seq_), K(need_gc));
        }
      } else {
        CLOG_LOG(INFO, "GC check ls in member list, skip it", K(id), K(leader));
      }
    }
  }
  return ret;
}

//---------------ObGCLSLog---------------//
ObGCLSLog::ObGCLSLog()
{
  reset();
}

//GC类型日志回放要求同类型内按序, 考虑到回放的性能, 使用前向barrier
ObGCLSLog::ObGCLSLog(const int16_t log_type)
    : header_(ObLogBaseType::GC_LS_LOG_BASE_TYPE, ObReplayBarrierType::PRE_BARRIER),
      version_(GC_LOG_VERSION),
      log_type_(log_type)
{
}

ObGCLSLog::~ObGCLSLog()
{
  reset();
}

void ObGCLSLog::reset()
{
  header_.reset();
  version_ = 0;
  log_type_ = ObGCLSLOGType::UNKNOWN_TYPE;
}

int16_t ObGCLSLog::get_log_type()
{
  return log_type_;
}

DEFINE_SERIALIZE(ObGCLSLog)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, pos))) {
    CLOG_LOG(WARN, "serialize header_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    CLOG_LOG(WARN, "serialize version_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, log_type_))) {
    CLOG_LOG(WARN, "serialize log_type_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObGCLSLog)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, pos))) {
    CLOG_LOG(WARN, "deserialize header_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    CLOG_LOG(WARN, "deserialize version_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &log_type_))) {
    CLOG_LOG(WARN, "deserialize log_type_ failed", K(ret), KP(buf), K(data_len), K(pos));
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObGCLSLog)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i16(log_type_);
  return size;
}

//---------------ObGCHandler---------------//
ObGCHandler::ObGCHandler() : is_inited_(false),
                             rwlock_(common::ObLatchIds::GC_HANDLER_LOCK),
                             ls_(NULL),
                             gc_seq_invalid_member_(-1),
                             gc_start_ts_(OB_INVALID_TIMESTAMP)
{
}

ObGCHandler::~ObGCHandler()
{
  reset();
}

void ObGCHandler::reset()
{
  WLockGuard wlock_guard(rwlock_);
  gc_seq_invalid_member_ = -1;
  ls_ = NULL;
  gc_start_ts_ = OB_INVALID_TIMESTAMP;
  is_inited_ = false;
}

int ObGCHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "GC handler has already been inited", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ls), K(ret));
  } else {
    ls_ = ls;
    is_inited_ = true;
    CLOG_LOG(INFO, "GC handler init success", K(ret), K(ls->get_ls_id()));
  }
  return ret;
}

void ObGCHandler::execute_pre_gc_process(ObGarbageCollector::LSStatus &ls_status)
{
  switch (ls_status)
  {
  case ObGarbageCollector::LSStatus::LS_DROPPING :
  case ObGarbageCollector::LSStatus::LS_TENANT_DROPPING :
    handle_gc_ls_dropping_(ls_status);
    break;
  case ObGarbageCollector::LSStatus::LS_WAIT_OFFLINE :
    handle_gc_ls_offline_(ls_status);
    break;
  default:
    // do nothing
    CLOG_LOG(INFO, "current ls_status no need execute_pre_gc_process", K(ls_status));
    break;
  }
}

int ObGCHandler::check_ls_can_offline(const share::ObLSStatus &ls_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "GC handler not init");
  } else {
    RLockGuard rlock_guard(rwlock_);
    ObLSID ls_id = ls_->get_ls_id();
    LSGCState gc_state = INVALID_LS_GC_STATE;
    if (OB_FAIL(ls_->get_gc_state(gc_state))) {
      CLOG_LOG(WARN, "get_gc_state failed", K(ls_id), K(gc_state));
    } else if (!is_valid_ls_gc_state(gc_state)) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "ls check gc state invalid", K(ls_id), K(gc_state));
    } else if (is_ls_offline_finished_(gc_state)) {
      CLOG_LOG(INFO, "ls check_ls_can_offline success", K(ls_id), K(gc_state));
    } else if (is_ls_blocked_state_(gc_state)) {
      share::ObLSStatus current_ls_status = ls_status;
      ObGarbageCollector::LSStatus ls_gc_status = ObGarbageCollector::LSStatus::LS_INVALID_STATUS;
      if (ls_is_empty_status(ls_status)) {
        // rpc from old version, need get real ls status from table
        ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
        const ObLSID &id = ls_->get_ls_id();
        if (NULL == gc_service) {
          CLOG_LOG(WARN, "gc_service is null", K(ret));
        } else if (OB_FAIL(gc_service->get_ls_status_from_table(id, current_ls_status))) {
          CLOG_LOG(WARN, "failed to get ls status from table", K(ret), K(id));
        }
      }
      if (ls_is_dropping_status(current_ls_status)) {
        ls_gc_status = ObGarbageCollector::LSStatus::LS_DROPPING;
      } else if (ls_is_tenant_dropping_status(current_ls_status)) {
        ls_gc_status = ObGarbageCollector::LSStatus::LS_TENANT_DROPPING;
      }
      // invalid ls_gc_status will return false, ret will be rewritten as OB_EAGAIN
      if (is_tablet_clear_(ls_gc_status)) {
        CLOG_LOG(INFO, "ls check_ls_can_offline success", K(ls_id), K(gc_state));
      } else {
        ret = OB_EAGAIN;
        CLOG_LOG(WARN, "ls check_ls_can_offline need retry", K(ls_id), K(gc_state));
      }
    } else {
      // block log not flushed
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "ls check_ls_can_offline need retry", K(ls_id), K(gc_state));
    }
  }
  return ret;
}

int ObGCHandler::gc_check_invalid_member_seq(const int64_t gc_seq,
                                             bool &need_gc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartition is not inited", K(ret));
  } else if (gc_seq <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(gc_seq));
  } else {
    WLockGuard wlock_guard(rwlock_);
    CLOG_LOG(INFO, "gc_check_invalid_member_seq", K(gc_seq), K(gc_seq_invalid_member_));
    if (gc_seq == gc_seq_invalid_member_ + 1) {
      //连续两轮都不在leader成员列表中
      need_gc = true;
    }
    gc_seq_invalid_member_ = gc_seq;
  }
  return ret;
}

int ObGCHandler::replay(const void *buffer,
                        const int64_t nbytes,
                        const palf::LSN &lsn,
                        const SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObGCLSLog gc_log;
  const char *log_buf = static_cast<const char *>(buffer);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "gc_handler not inited", K(ret));
  } else if (OB_ISNULL(log_buf)
      || OB_UNLIKELY(nbytes <= 0)
      || OB_UNLIKELY(!lsn.is_valid())
      || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(buffer), K(nbytes), K(pos),
             K(lsn), K(scn), K(ret));
  } else if (OB_FAIL(gc_log.deserialize(log_buf, nbytes, pos))) {
    CLOG_LOG(WARN, "gc_log deserialize error", K(ret));
  } else {
    WLockGuard wlock_guard(rwlock_);
    ObGCLSLOGType log_type = static_cast<ObGCLSLOGType>(gc_log.get_log_type());
    (void)update_ls_gc_state_after_submit_log_(log_type, scn);
    CLOG_LOG(INFO, "replay gc log", K(log_type));
  }
  return ret;
}

SCN ObGCHandler::get_rec_scn()
{
  return SCN::max_scn();
}

int ObGCHandler::flush(SCN &scn)
{
  // do nothing
  return OB_SUCCESS;
}

void ObGCHandler::switch_to_follower_forcedly()
{
  RLockGuard Rlock_guard(rwlock_);
  CLOG_LOG(INFO, "gc_handler switch_to_follower_forcedly");
}

int ObGCHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  RLockGuard Rlock_guard(rwlock_);
  CLOG_LOG(INFO, "gc_handler switch_to_leader");
  return ret;
}

int ObGCHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  RLockGuard Rlock_guard(rwlock_);
  CLOG_LOG(INFO, "gc_handler switch_to_follower_gracefully");
  return ret;
}

int ObGCHandler::resume_leader()
{
  int ret = OB_SUCCESS;
  RLockGuard Rlock_guard(rwlock_);
  CLOG_LOG(INFO, "gc_handler resume_leader");
  return ret;
}

bool ObGCHandler::is_valid_ls_gc_state(const LSGCState &state)
{
  return LSGCState::MAX_LS_GC_STATE > state
         && LSGCState::INVALID_LS_GC_STATE < state;
}

bool ObGCHandler::is_ls_blocked_state_(const LSGCState &state)
{
  return LSGCState::LS_BLOCKED == state;
}

bool ObGCHandler::is_ls_offline_state_(const LSGCState &state)
{
  return LSGCState::LS_OFFLINE == state;
}

bool ObGCHandler::is_ls_wait_gc_state_(const LSGCState &state)
{
  return LSGCState::WAIT_GC == state;
}

bool ObGCHandler::is_ls_blocked_finished_(const LSGCState &state)
{
  return LSGCState::LS_BLOCKED <= state;
}

bool ObGCHandler::is_ls_offline_finished_(const LSGCState &state)
{
  return LSGCState::LS_OFFLINE <= state;
}

int ObGCHandler::get_gts_(const int64_t timeout_us, SCN &gts_scn)
{
  int ret = OB_SUCCESS;
  const transaction::MonotonicTs stc_ahead = transaction::MonotonicTs::current_time() ;
  transaction::MonotonicTs tmp_receive_gts_ts(0);
  const int64_t expire_time_us = common::ObTimeUtility::current_time() + timeout_us;
  do {
    ret = OB_TS_MGR.get_gts(MTL_ID(), stc_ahead, NULL, gts_scn, tmp_receive_gts_ts);
    if (ret == OB_EAGAIN) {
      if (common::ObTimeUtility::current_time() > expire_time_us) {
        ret = OB_TIMEOUT;
      } else {
        ob_usleep(10);
      }
    } else if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "get gts fail", KR(ret));
    } else if (!gts_scn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "get gts fail", K(gts_scn), K(ret));
    } else {
      CLOG_LOG(TRACE, "get gts", K(gts_scn));
    }
  } while (ret == OB_EAGAIN);
  return ret;
}

bool ObGCHandler::is_tablet_clear_(const ObGarbageCollector::LSStatus &ls_status)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  ObLSID ls_id = ls_->get_ls_id();
  if (ObGarbageCollector::is_ls_dropping_ls_status(ls_status)) {
    //TODO: transfer完成前先统一检查事务结束
    if (OB_FAIL(ls_->check_all_tx_clean_up())) {
      if (OB_EAGAIN == ret) {
        CLOG_LOG(INFO, "check_all_tx_clean_up need retry", K(ls_id), K(ret));
        if (OB_FAIL(ls_->kill_all_tx(true))) { //gracefully kill
          CLOG_LOG(WARN, "gracefully kill_all_tx failed", K(ret), K(ls_id));
        }
      } else {
        CLOG_LOG(WARN, "check_all_tx_clean_up failed", K(ls_id), K(ret));
      }
    } else {
      bool_ret = true;
      CLOG_LOG(INFO, "tablet is clear", K(ls_id), K(ls_status), K(bool_ret));
    }
  } else if (ObGarbageCollector::is_tenant_dropping_ls_status(ls_status)) {
    // tenant dropping only check transaction clean
    if (OB_FAIL(ls_->check_all_tx_clean_up())) {
      if (OB_EAGAIN == ret) {
        CLOG_LOG(INFO, "check_all_tx_clean_up need retry", K(ls_id), K(ret));
        if (OB_FAIL(ls_->kill_all_tx(true))) { //gracefully kill
          CLOG_LOG(WARN, "gracefully kill_all_tx failed", K(ret), K(ls_id));
        }
      } else {
        CLOG_LOG(WARN, "check_all_tx_clean_up failed", K(ls_id), K(ret));
      }
    } else {
      bool_ret = true;
      CLOG_LOG(INFO, "tablet is clear", K(ls_id), K(ls_status));
    }
  } else {
    CLOG_LOG(WARN, "invalid ls status", K(ls_id), K(ls_status));
  }
  return bool_ret;
}

void ObGCHandler::try_check_and_set_wait_gc_(ObGarbageCollector::LSStatus &ls_status)
{
  int ret = OB_SUCCESS;
  ObArchiveService *archive_service = MTL(ObArchiveService*);
  bool force_wait = true;
  bool ignore = true;
  SCN scn = SCN::min_scn();
  LSN lsn;
<<<<<<< HEAD
  SCN current_gts_scn = SCN::min_scn();
  LSGCState gc_state = INVALID_LS_GC_STATE;
  ObLSID ls_id = ls_->get_ls_id();
  SCN offline_scn;
=======
  SCN readable_scn = SCN::min_scn();
  LSGCState gc_state = INVALID_LS_GC_STATE;
  ObLSID ls_id = ls_->get_ls_id();
  SCN offline_scn;
  bool tenant_in_archive = false;
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  if (OB_FAIL(ls_->get_gc_state(gc_state))) {
    CLOG_LOG(WARN, "get_gc_state failed", K(ls_id), K(ret));
  } else if (OB_FAIL(ls_->get_offline_scn(offline_scn))) {
    CLOG_LOG(WARN, "get_gc_state failed", K(ls_id), K(gc_state), K(ret));
<<<<<<< HEAD
  } else if (OB_FAIL(get_gts_(GET_GTS_TIMEOUT_US, current_gts_scn))) {
    CLOG_LOG(WARN, "get_gts_ failed", K(ls_id), K(gc_state), K(current_gts_scn), K(ret));
  } else if (current_gts_scn.convert_to_ts() < offline_scn.convert_to_ts() + LS_CLOG_ALIVE_TIMEOUT_US) {
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ wait clog remove", K(ls_id), K(gc_state), K(current_gts_scn), K(offline_scn));
  // TODO: 归档暂不支持时间戳进度
  } else if (OB_FAIL(archive_service->get_ls_archive_progress(ls_id, lsn, force_wait, ignore))){
=======
  } else if (OB_FAIL(get_tenant_readable_scn_(readable_scn))) {
    CLOG_LOG(WARN, "get_tenant_readable_scn_ failed", K(ret), K(ls_id));
  } else if (readable_scn < offline_scn) {
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ wait readable_scn", K(ret), K(ls_id), K(gc_state), K(offline_scn), K(readable_scn));
  } else if (OB_FAIL(check_if_tenant_in_archive_(tenant_in_archive))) {
    CLOG_LOG(WARN, "check_if_tenant_in_archive_ failed", K(ret), K(ls_id), K(gc_state));
  } else if (! tenant_in_archive) {
    if (OB_FAIL(ls_->set_gc_state(LSGCState::WAIT_GC))) {
      CLOG_LOG(WARN, "set_gc_state failed", K(ls_id), K(gc_state), K(ret));
    }
    ls_status = ObGarbageCollector::LSStatus::LS_NEED_DELETE_ENTRY;
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ success", K(ls_id), K(gc_state), K(offline_scn), K(scn));
  } else if (OB_FAIL(archive_service->get_ls_archive_progress(ls_id, lsn, scn, force_wait, ignore))){
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
    CLOG_LOG(WARN, "get_ls_archive_progress failed", K(ls_id), K(gc_state), K(offline_scn), K(ret));
  } else if (ignore) {
    if (OB_FAIL(ls_->set_gc_state(LSGCState::WAIT_GC))) {
      CLOG_LOG(WARN, "set_gc_state failed", K(ls_id), K(gc_state), K(ret));
    }
    ls_status = ObGarbageCollector::LSStatus::LS_NEED_DELETE_ENTRY;
<<<<<<< HEAD
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ success", K(ls_id), K(gc_state), K(current_gts_scn), K(offline_scn), K(scn));
=======
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ success", K(ls_id), K(gc_state), K(offline_scn), K(scn));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  } else if (scn == offline_scn) {
    if (OB_FAIL(ls_->set_gc_state(LSGCState::WAIT_GC))) {
      CLOG_LOG(WARN, "set_gc_state failed", K(ls_id), K(gc_state), K(ret));
    }
    ls_status = ObGarbageCollector::LSStatus::LS_NEED_DELETE_ENTRY;
<<<<<<< HEAD
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ success", K(ls_id), K(gc_state), K(current_gts_scn), K(offline_scn), K(scn));
  } else if (scn > offline_scn) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls_archive_progress larger than offline scn", K(ls_id), K(gc_state), K(current_gts_scn), K(offline_scn), K(scn));
  } else {
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ wait archive", K(ls_id), K(gc_state), K(current_gts_scn), K(offline_scn), K(scn));
=======
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ success", K(ls_id), K(gc_state), K(offline_scn), K(scn));
  } else if (scn > offline_scn) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls_archive_progress larger than offline scn", K(ls_id), K(gc_state), K(offline_scn), K(scn));
  } else {
    CLOG_LOG(INFO, "try_check_and_set_wait_gc_ wait archive", K(ls_id), K(gc_state), K(offline_scn), K(scn));
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe
  }
}

int ObGCHandler::get_tenant_readable_scn_(SCN &readable_scn)
{
  return MTL(rootserver::ObTenantRecoveryReportor *)->get_tenant_readable_scn(readable_scn);
}

// 由于日志流GC导致的归档日志不完整是无法被归档检查出来的异常, 因此需要保证GC与归档状态互斥;
// 其他如clog回收/迁移复制等场景, 仅需考虑本地归档进度即可
int ObGCHandler::check_if_tenant_in_archive_(bool &in_archive)
{
  return MTL(ObArchiveService*)->check_tenant_in_archive(in_archive);
}

void ObGCHandler::submit_log_(const ObGCLSLOGType log_type)
{
  int ret = OB_SUCCESS;
  ObGCLSLog gc_log(log_type);
  char *buffer = nullptr;
  int64_t pos = 0;
  int64_t buffer_size = gc_log.get_serialize_size();
  ObGCLSLogCb cb;
  const bool need_nonblock = false;
  SCN ref_scn;
  palf::LSN lsn;
  SCN scn;
  ObMemAttr attr(MTL_ID(), ObModIds::OB_GC_LOG_BUFF);
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ls is NULL");
  } else if (OB_ISNULL(buffer = static_cast<char *>(mtl_malloc(buffer_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc buffer", K(ret), K(log_type));
  } else if (OB_FAIL(gc_log.serialize(buffer, buffer_size, pos))) {
    CLOG_LOG(WARN, "failed to serialize log header", K(ret), K(buffer_size), K(pos));
  } else if (OB_FAIL(get_gts_(GET_GTS_TIMEOUT_US, ref_scn))) {
    CLOG_LOG(WARN, "failed to get gts", K(ret), K(ref_scn));
  } else if (OB_FAIL(ls_->append(buffer,
                                 buffer_size,
                                 ref_scn,
                                 need_nonblock,
                                 &cb,
                                 lsn,
                                 scn))) {
    CLOG_LOG(WARN, "failed to submit log", K(ret), K(buffer_size), K(pos));
  } else {
    // 此处需要考虑能否做成异步
    int64_t retry_cnt = 0;
    const int64_t WAIT_TIME = 100; // us
    bool is_finished = false;
    while (!is_finished) {
      if (cb.is_succeed()) {
        (void)update_ls_gc_state_after_submit_log_(log_type, scn);
        is_finished = true;
        CLOG_LOG(INFO, "write GC ls log success", K(ret), K(log_type));
      } else if (cb.is_failed()) {
        is_finished = true;
        CLOG_LOG(WARN, "write GC ls log failed", K(ret), K(log_type));
      } else {
        ob_usleep(WAIT_TIME);
        retry_cnt++;
        if (retry_cnt % 1000 == 0) {
          CLOG_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "GC ls log wait cb too much time", K(retry_cnt), K(log_type));
        }
      }
    }
  }
  if (nullptr != buffer) {
    mtl_free(buffer);
    buffer = nullptr;
  }
}

void ObGCHandler::update_ls_gc_state_after_submit_log_(const ObGCLSLOGType log_type,
                                                       const SCN &scn)
{
  switch(log_type) {
  case ObGCLSLOGType::BLOCK_TABLET_TRANSFER_IN:
    (void)block_ls_transfer_in_(scn);
    break;
  case ObGCLSLOGType::OFFLINE_LS:
    (void)offline_ls_(scn);
    break;
  default:
    //do nothing
    CLOG_LOG(INFO, "invalid log_type", K(log_type));
    break;
  }
}

void ObGCHandler::block_ls_transfer_in_(const SCN &block_scn)
{
  int ret = OB_SUCCESS;
  LSGCState gc_state = INVALID_LS_GC_STATE;
  ObLSID ls_id = ls_->get_ls_id();
  //过滤重复回放场景
  if (OB_FAIL(ls_->get_gc_state(gc_state))) {
    CLOG_LOG(WARN, "get_gc_state failed", K(ls_id), K(gc_state));
  } else if (!is_valid_ls_gc_state(gc_state)) {
    CLOG_LOG(WARN, "ls check gc state invalid", K(ls_id), K(gc_state));
  } else if (is_ls_blocked_finished_(gc_state)) {
    CLOG_LOG(INFO, "ls already blocked, ignore", K(ls_id), K(gc_state), K(block_scn));
  //TODO: @keqing.llt transfer功能完成之前,先用杀事务代替transfer out
  } else if (OB_FAIL(ls_->block_tx_start())) {
    CLOG_LOG(WARN, "block_tx_start failed", K(ls_id), K(ret));
  } else if (OB_FAIL(ls_->set_gc_state(LSGCState::LS_BLOCKED))) {
    CLOG_LOG(WARN, "set_gc_state block failed", K(ls_id), K(ret));
  } else {
    CLOG_LOG(INFO, "block_ls_transfer_in_ success", K(ls_id), K(block_scn));
  }
}

void ObGCHandler::offline_ls_(const SCN &offline_scn)
{
  int ret = OB_SUCCESS;
  LSGCState gc_state = INVALID_LS_GC_STATE;
  ObLSID ls_id = ls_->get_ls_id();
  //过滤重复回放场景
  if (OB_FAIL(ls_->get_gc_state(gc_state))) {
    CLOG_LOG(WARN, "get_gc_state failed", K(ls_id), K(gc_state));
  } else if (!is_valid_ls_gc_state(gc_state)) {
    CLOG_LOG(WARN, "ls check gc state invalid", K(ls_id), K(gc_state));
  } else if (is_ls_offline_finished_(gc_state)) {
    SCN pre_offline_scn;
    if (OB_FAIL(ls_->get_offline_scn(pre_offline_scn))) {
      CLOG_LOG(WARN, "get_offline_scn failed", K(ls_id), K(gc_state));
    } else {
      CLOG_LOG(INFO, "ls already offline, ignore", K(ls_id), K(offline_scn), K(gc_state), K(pre_offline_scn));
    }
  } else if (OB_FAIL(ls_->set_gc_state(LSGCState::LS_OFFLINE))) {
    //TODO: @yanyuan 需要调用ObLS的offline_ls_接口
    //offline_scn依赖gc_state写slog, 顺序必须为先设置offline_scn再设置gc状态
    CLOG_LOG(WARN, "set_gc_state failed", K(ls_->get_ls_id()), K(ret));
  } else if (OB_FAIL(ls_->set_offline_scn(offline_scn))) {
    CLOG_LOG(WARN, "set_gc_state failed", K(ls_->get_ls_id()), K(ret));
  } else {
    CLOG_LOG(INFO, "offline_ls success",  K(ls_->get_ls_id()), K(offline_scn)); }
}

int ObGCHandler::get_palf_role_(common::ObRole &role)
{
  int ret = OB_SUCCESS;
  int64_t proposal_id = 0;
  bool is_pending_state = false;
  palf::PalfHandleGuard palf_handle;
  ObLogService *log_service = MTL(ObLogService*);
  if (OB_FAIL(log_service->open_palf(ls_->get_ls_id(), palf_handle))) {
    CLOG_LOG(WARN, "open_palf failed", K(ls_));
  } else if (OB_FAIL(palf_handle.get_role(role, proposal_id))) {
    CLOG_LOG(WARN, "palf_handle get_role failed", K(ls_));
  } else {
    // do nothing
  }
  return ret;
}

void ObGCHandler::handle_gc_ls_dropping_(const ObGarbageCollector::LSStatus &ls_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "GC handler not init");
  } else {
    WLockGuard wlock_guard(rwlock_);
    ObRole role;
    ObLSID ls_id = ls_->get_ls_id();
    LSGCState gc_state = INVALID_LS_GC_STATE;
    gc_start_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(get_palf_role_(role))) {
      CLOG_LOG(WARN, "get_palf_role_ failed", K(ls_id));
    } else if (ObRole::LEADER != role) {
      // follower do nothing
    } else if (OB_FAIL(ls_->get_gc_state(gc_state))) {
      CLOG_LOG(WARN, "get_gc_state failed", K(ls_id), K(gc_state));
    } else if (!is_valid_ls_gc_state(gc_state)) {
      CLOG_LOG(WARN, "ls check gc state invalid", K(ls_id), K(gc_state));
    } else if (is_ls_offline_finished_(gc_state)) {
      CLOG_LOG(INFO, "handle_gc_ls_dropping already finished", K(ls_id), K(gc_state));
    } else if (is_ls_blocked_state_(gc_state)) {
      // trigger kill all tx
      (void)is_tablet_clear_(ls_status);
    } else {
      (void)submit_log_(ObGCLSLOGType::BLOCK_TABLET_TRANSFER_IN);
      (void)is_tablet_clear_(ls_status);
    }
    CLOG_LOG(INFO, "ls handle_gc_ls_dropping_ finished", K(ls_id), K(role), K(gc_state));
  }
}

void ObGCHandler::handle_gc_ls_offline_(ObGarbageCollector::LSStatus &ls_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "GC handler not init");
  } else {
    WLockGuard wlock_guard(rwlock_);
    ObRole role;
    ObLSID ls_id = ls_->get_ls_id();
    LSGCState gc_state = INVALID_LS_GC_STATE;
    if (OB_FAIL(get_palf_role_(role))) {
      CLOG_LOG(WARN, "get_palf_role_ failed", K(ls_id));
    } else if (ObRole::LEADER != role) {
      // follower do nothing
    } else if (OB_FAIL(ls_->get_gc_state(gc_state))) {
      CLOG_LOG(WARN, "get_gc_state failed", K(ls_id), K(gc_state));
    } else if (!is_valid_ls_gc_state(gc_state)) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "ls check gc state invalid", K(ls_id), K(gc_state));
    } else if (!is_ls_blocked_finished_(gc_state)) {
      CLOG_LOG(WARN, "ls not ready for offline", K(ls_id), K(gc_state));
    } else if (is_ls_wait_gc_state_(gc_state)) {
      ls_status = ObGarbageCollector::LSStatus::LS_NEED_DELETE_ENTRY;
      CLOG_LOG(INFO, "handle_gc_ls_offline need delete entry", K(ls_id), K(gc_state));
    } else if (is_ls_offline_state_(gc_state)) {
      (void)try_check_and_set_wait_gc_(ls_status);
    } else {
      (void)submit_log_(ObGCLSLOGType::OFFLINE_LS);
      (void)try_check_and_set_wait_gc_(ls_status);
    }
    CLOG_LOG(INFO, "ls handle_gc_ls_offline finished", K(ls_id), K(role), K(gc_state));
  }
}

int ObGCHandler::diagnose(GCDiagnoseInfo &diagnose_info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "GC handler not init");
  } else {
    RLockGuard wlock_guard(rwlock_);
    if (OB_FAIL(ls_->get_gc_state(diagnose_info.gc_state_))) {
      CLOG_LOG(WARN, "get_gc_state failed", K(ls_id));
    } else {
      diagnose_info.gc_start_ts_ = gc_start_ts_;
    }
  }
  return ret;
}

//---------------ObGarbageCollector---------------//
void ObGarbageCollector::GCCandidate::set_ls_status(const share::ObLSStatus &ls_status)
{
  if (ls_is_normal_status(ls_status)) {
    ls_status_ = LSStatus::LS_NORMAL;
  } else if (ls_is_dropping_status(ls_status)) {
    ls_status_ = LSStatus::LS_DROPPING;
  } else if (ls_is_tenant_dropping_status(ls_status)) {
    ls_status_ = LSStatus::LS_TENANT_DROPPING;
  } else if (ls_is_wait_offline_status(ls_status)) {
    ls_status_ = LSStatus::LS_WAIT_OFFLINE;
  } else {
    ls_status_ = LSStatus::LS_INVALID_STATUS;
  }
}

ObGarbageCollector::ObGarbageCollector() : is_inited_(false),
                                           ls_service_(NULL),
                                           rpc_proxy_(NULL),
                                           sql_proxy_(NULL),
                                           self_addr_(),
                                           seq_(1)
{
}

ObGarbageCollector::~ObGarbageCollector()
{
  destroy();
}

int ObGarbageCollector::mtl_init(ObGarbageCollector* &gc_service)
{
  ObLSService *ls_service = MTL(ObLSService*);
  obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  const common::ObAddr self_addr = GCTX.self_addr();
  return gc_service->init(ls_service, rpc_proxy, sql_proxy, self_addr);
}

int ObGarbageCollector::init(ObLSService *ls_service,
                             obrpc::ObSrvRpcProxy *rpc_proxy,
                             common::ObMySQLProxy *sql_proxy,
                             const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObGarbageCollector is inited twice");
  } else if (OB_ISNULL(ls_service) || OB_ISNULL(rpc_proxy)
             || OB_ISNULL(sql_proxy) || !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(ls_service), KP(rpc_proxy), KP(sql_proxy), K(self_addr));
  } else {
    ls_service_ = ls_service;
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    self_addr_ = self_addr;
    seq_ = 1;
    is_inited_ = true;
  }
  CLOG_LOG(INFO, "ObGarbageCollector is inited", K(ret), K(self_addr_));
  return ret;
}

int ObGarbageCollector::start()
{
  int ret = OB_SUCCESS;
  ThreadPool::set_run_wrapper(MTL_CTX());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObGarbageCollector is not inited", K(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    CLOG_LOG(ERROR, "ObGarbageCollector thread failed to start", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

void ObGarbageCollector::stop()
{
  ObThreadPool::stop();
  CLOG_LOG(INFO, "ObGarbageCollector stop");
}

void ObGarbageCollector::wait()
{
  ObThreadPool::wait();
  CLOG_LOG(INFO, "ObGarbageCollector wait");
}

void ObGarbageCollector::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  ls_service_ = NULL;
  rpc_proxy_ = NULL;
  sql_proxy_ = NULL;
  self_addr_.reset();
}

void ObGarbageCollector::run1()
{
  CLOG_LOG(INFO, "Garbage Collector start to run");
  lib::set_thread_name("GCCollector");

  while (!has_set_stop()) {
    ObGCCandidateArray gc_candidates;
    int64_t gc_interval = GC_INTERVAL;
    CLOG_LOG(INFO, "Garbage Collector is running", K(seq_), K(gc_interval));
    gc_candidates.reset();
    (void)gc_check_member_list_(gc_candidates);
    (void)execute_gc_(gc_candidates);
    gc_candidates.reset();
    (void)gc_check_ls_status_(gc_candidates);
    (void)execute_gc_(gc_candidates);
    ob_usleep(gc_interval);
    seq_++;
  }
}

int ObGarbageCollector::get_ls_status_from_table(const ObLSID &ls_id,
                                                 share::ObLSStatus &ls_status)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = MTL_ID();
  ObLSStatusOperator ls_op;
  ObLSStatusInfo status_info;
  // sys tenant should always return LS_NORMAL
  if (OB_SYS_TENANT_ID == tenant_id) {
    ls_status = OB_LS_NORMAL;
  } else if (OB_FAIL(ls_op.get_ls_status_info(tenant_id, ls_id, status_info, *sql_proxy_))) {
    CLOG_LOG(INFO, "failed to get ls status info from table", K(ret), K(tenant_id), K(ls_id));
  } else {
    ls_status = status_info.status_;
  }
  return ret;
}

bool ObGarbageCollector::is_ls_dropping_ls_status(const LSStatus &status)
{
  return LSStatus::LS_DROPPING == status;
}

bool ObGarbageCollector::is_tenant_dropping_ls_status(const LSStatus &status)
{
  return LSStatus::LS_TENANT_DROPPING == status;
}

bool ObGarbageCollector::is_valid_ls_status_(const LSStatus &status)
{
  return LSStatus::LS_INVALID_STATUS < status && LSStatus::LS_MAX_STATUS > status;
}

bool ObGarbageCollector::is_need_gc_ls_status_(const LSStatus &status)
{
  return LSStatus::LS_NEED_GC == status;
}

bool ObGarbageCollector::is_normal_ls_status_(const LSStatus &status)
{
  return LSStatus::LS_NORMAL == status;
}

bool ObGarbageCollector::is_need_delete_entry_ls_status_(const LSStatus &status)
{
  return LSStatus::LS_NEED_DELETE_ENTRY == status;
}

int ObGarbageCollector::gc_check_member_list_(ObGCCandidateArray &gc_candidates)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time_us = ObTimeUtility::current_time();
  ServerLSMap server_ls_map;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObGarbageCollector is not inited", K(ret));
  } else if (OB_FAIL(server_ls_map.init("SvrLSMap", MTL_ID()))) {
    CLOG_LOG(WARN, "server_ls_map init failed", K(ret));
  } else if (OB_FAIL(construct_server_ls_map_for_member_list_(server_ls_map))) {
    CLOG_LOG(WARN, "construct_server_ls_map_for_member_list_ failed", K(ret));
  } else if (OB_FAIL(handle_each_ls_for_member_list_(server_ls_map, gc_candidates))) {
    CLOG_LOG(WARN, "handle_each_ls_for_member_list_ failed", K(ret));
  }
  CLOG_LOG(INFO, "gc_check_member_list_ cost time", K(ret), "time_us", ObTimeUtility::current_time() - begin_time_us);
  return ret;
}

int ObGarbageCollector::construct_server_ls_map_for_member_list_(ServerLSMap &server_ls_map) const
{
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  uint64_t tenant_id = MTL_ID();
  ObMigrationStatus migration_status;
  if (OB_FAIL(ls_service_->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD))) {
    CLOG_LOG(WARN, "get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "iter is NULL", K(ret), K(iter));
  } else {
    ObLS *ls = NULL;
    int tmp_ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      common::ObAddr leader;
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(WARN, "get next log stream failed", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "log stream is NULL", K(tmp_ret), K(ls));
      } else if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_get_leader(
                                cluster_id, tenant_id, ls->get_ls_id(), leader))) {
        if (is_location_service_renew_error(tmp_ret)) {
          if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_renew(
                      cluster_id, tenant_id, ls->get_ls_id()))) {
            CLOG_LOG(WARN, "nonblock_renew location cache failed", K(ls->get_ls_id()), K(tmp_ret));
          } else { /* do nothing */ }
        } else {
          CLOG_LOG(WARN, "fail to get leader", K(ls->get_ls_id()), K(leader), K(tmp_ret));
        }
      } else if (leader == self_addr_) {
        CLOG_LOG(INFO, "self is leader, skip it", K(ls->get_ls_id()));
      } else if (!leader.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "get invalid leader from location service", K(tmp_ret), K(ls->get_ls_id()));
      } else if (OB_SUCCESS != (tmp_ret = ls->get_migration_status(migration_status))) {
        CLOG_LOG(WARN, "get_migration_status failed", K(tmp_ret), K(ls->get_ls_id()));
      } else if (!ObMigrationStatusHelper::check_allow_gc(migration_status)) {
        CLOG_LOG(INFO, "current ls not allowed to gc", K(ls->get_ls_id()));
      } else if (OB_SUCCESS != (tmp_ret = construct_server_ls_map_(server_ls_map, leader, ls->get_ls_id()))) {
        CLOG_LOG(WARN, "construct_server_ls_map_ failed", K(tmp_ret), K(ls->get_ls_id()), K(leader));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObGarbageCollector::construct_server_ls_map_(ServerLSMap &server_ls_map,
                                                 const common::ObAddr &server,
                                                 const ObLSID &id) const
{
  int ret = OB_SUCCESS;
  InsertLSFunctor functor(id);
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(id), K(server));
  } else if (OB_SUCCESS == (ret = server_ls_map.operate(server, functor))) {
    // do nothing
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ObLSArray tmp_ls_array;
    if (OB_FAIL(server_ls_map.insert(server, tmp_ls_array))) {
      CLOG_LOG(WARN, "server_ls_map insert failed", K(ret), K(id), K(server));
    } else if (OB_SUCCESS != server_ls_map.operate(server, functor)) {
      ret = functor.get_ret_value();
      CLOG_LOG(WARN, "insert ls functor operate failed", K(ret), K(id), K(server));
    }
  } else {
    ret = functor.get_ret_value();
    CLOG_LOG(WARN, "insert ls functor operate failed", K(ret), K(id), K(server));
  }
  return ret;
}

int ObGarbageCollector::handle_each_ls_for_member_list_(ServerLSMap &server_ls_map,
                                                        ObGCCandidateArray &gc_candidates)
{
  int ret = OB_SUCCESS;
  QueryLSIsValidMemberFunctor functor(rpc_proxy_, ls_service_, self_addr_, seq_, gc_candidates);
  if (OB_SUCCESS != server_ls_map.for_each(functor)) {
    ret = functor.get_ret_value();
    CLOG_LOG(WARN, "handle_each_ls_for_member_list_ failed", K(ret));
  }
  return ret;
}

void ObGarbageCollector::gc_check_ls_status_(ObGCCandidateArray &gc_candidates)
{
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (OB_FAIL(ls_service_->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD))) {
    CLOG_LOG(WARN, "get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "iter is NULL", K(ret), K(iter));
  } else {
    ObLS *ls = NULL;
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(WARN, "get next log stream failed", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        CLOG_LOG(ERROR, "log stream is NULL", K(ls));
      } else if (OB_SUCCESS != (tmp_ret = gc_check_ls_status_(ls->get_ls_id(), gc_candidates))) {
        CLOG_LOG(WARN, "get_ls_status_ failed", K(tmp_ret), K(ls->get_ls_id()));
      } else {}
    }
  }
}

int ObGarbageCollector::check_if_tenant_has_been_dropped_(const uint64_t tenant_id,
                                                          bool &has_dropped)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  has_dropped = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    CLOG_LOG(WARN, "fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, has_dropped))) {
    CLOG_LOG(WARN, "fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObGarbageCollector::gc_check_ls_status_(const ObLSID &id,
                                            ObGCCandidateArray &gc_candidates)
{
  int ret = OB_SUCCESS;
  ObString status_str;
  const int64_t tenant_id = MTL_ID();
  ObLSStatusOperator ls_op;
  share::ObLSStatus ls_status = OB_LS_NORMAL;
  GCCandidate candidate;
  candidate.ls_id_ = id;
  candidate.ls_status_ = LSStatus::LS_NORMAL;
  candidate.gc_reason_ = GCReason::INVALID_GC_REASON;
  if (OB_FAIL(get_ls_status_from_table(id, ls_status))) {
    // 对应行被删除则说明可以gc
    if (OB_ENTRY_NOT_EXIST == ret) {
      candidate.ls_status_ = LSStatus::LS_NEED_GC;
      candidate.gc_reason_ = GCReason::LS_STATUS_ENTRY_NOT_EXIST;
      ret = OB_SUCCESS;
    // 少数派可能在查表时租户已经被删除,需要double check schema明确是否租户真的被删除
    } else {
      int tmp_ret = OB_SUCCESS;
      bool is_tenant_dropped = false;
      if (OB_SUCCESS != (tmp_ret = check_if_tenant_has_been_dropped_(tenant_id, is_tenant_dropped))) {
        CLOG_LOG(WARN, "check_if_tenant_has_been_dropped_ failed", K(tmp_ret), K(tenant_id), K(id));
      } else if (is_tenant_dropped) {
        candidate.ls_status_ = LSStatus::LS_NEED_GC;
        candidate.gc_reason_ = GCReason::LS_STATUS_ENTRY_NOT_EXIST;
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(WARN, "failed to get ls status from table", K(ret), K(tenant_id), K(id));
      }
    }
  } else {
    candidate.set_ls_status(ls_status);
    candidate.gc_reason_ = GCReason::INVALID_GC_REASON;
  }
  if (OB_SUCCESS == ret) {
    if (OB_FAIL(gc_candidates.push_back(candidate))) {
      CLOG_LOG(WARN, "gc_candidates push_back failed", K(ret), K(candidate));
    } else {
      CLOG_LOG(INFO, "gc_candidates push_back success", K(ret), K(candidate));
    }
  }
  return ret;
}

void ObGarbageCollector::execute_gc_(ObGCCandidateArray &gc_candidates)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t begin_time_us = ObTimeUtility::current_time();
  for (int64_t index = 0; OB_SUCC(ret) && index < gc_candidates.count(); index++) {
    const ObLSID id = gc_candidates[index].ls_id_;
    LSStatus &ls_status = gc_candidates[index].ls_status_;
    GCReason &gc_reason = gc_candidates[index].gc_reason_;
    ObLS *ls = NULL;
    ObLSHandle ls_handle;
    ObGCHandler *gc_handler = NULL;
    TIMEGUARD_INIT(CLOG, 30_s, 60_s);
    if (!is_valid_ls_status_(ls_status)) {
      CLOG_LOG(WARN, "invalid gc_state when execute_gc_", K(id), K(gc_candidates));
    } else if (is_normal_ls_status_(ls_status)) {
      CLOG_LOG(INFO, "ls status is normal, skip", K(id), K(gc_candidates));
    } else if (OB_ISNULL(ls_service_)) {
      ret = OB_NOT_INIT;
      CLOG_LOG(ERROR, "ls service is NULL", K(ret));
    } else if (OB_SUCCESS != (tmp_ret = ls_service_->get_ls(id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      CLOG_LOG(ERROR, "get ls failed", K(tmp_ret), K(id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ls not exist", K(tmp_ret), K(id));
    } else if (is_need_gc_ls_status_(ls_status)) {
      ObSwitchLeaderAdapter switch_leader_adapter;
      if (OB_FAIL(switch_leader_adapter.remove_from_election_blacklist(id.id(), self_addr_))) {
        CLOG_LOG(WARN, "remove_from_election_blacklist failed", K(ret), K(id), K_(self_addr));
      } else if (OB_SUCCESS != (tmp_ret = ls_service_->remove_ls(id, false))) {
        CLOG_LOG(WARN, "remove_ls failed", K(tmp_ret), K(id));
      } else {
        CLOG_LOG(INFO, "remove_ls success", K(id), K(gc_reason));
      }
    } else if (OB_ISNULL(gc_handler = ls->get_gc_handler())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "gc_handler is NULL", K(tmp_ret), K(id));
    } else {
      CLOG_LOG(INFO, "begin execute_pre_gc_process", K(id), K(ls_status));
      (void)gc_handler->execute_pre_gc_process(ls_status);
      if (is_need_delete_entry_ls_status_(ls_status)) {
        if (OB_SUCCESS != (tmp_ret = delete_ls_status_(id))) {
          CLOG_LOG(WARN, "delete_ls_status_ failed", K(tmp_ret), K(id));
        } else {
          CLOG_LOG(INFO, "delete_ls_status_ success", K(id));
        }
      }
    }
  }
  CLOG_LOG(INFO, "execute_gc cost time", K(ret), "time_us", ObTimeUtility::current_time() - begin_time_us);
}

int ObGarbageCollector::delete_ls_status_(const ObLSID &id)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = MTL_ID();
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "sql proxy is null", KR(ret));
  } else {
    ObLSLifeAgentManager ls_agent(*sql_proxy_);
    if (OB_FAIL(ls_agent.drop_ls(tenant_id, id, share::NORMAL_SWITCHOVER_STATUS))) {
      CLOG_LOG(WARN, "ls status table delete_ls failed", K(ret), K(id), K(tenant_id));
    } else {
      // do nothing
    }
  }
  CLOG_LOG(INFO, "delete_ls_status_", K(ret), K(id), K(tenant_id));
  return ret;
}

} // namespace logservice
} // namespace oceanbase
