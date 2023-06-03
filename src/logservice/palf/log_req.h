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

#ifndef OCEANBASE_LOGSERVICE_LOG_REQ_
#define OCEANBASE_LOGSERVICE_LOG_REQ_

#include "lib/utility/ob_unify_serialize.h"                    // OB_UNIS_VERSION
#include "lib/utility/ob_print_utils.h"                        // TO_STRING_KV
#include "log_meta_info.h"
#include "log_learner.h"                             // LogLearner, LogLearnerList
#include "logservice/palf/lsn.h"                                     // LSN
#include "log_writer_utils.h"                               // LogWriteBuf

namespace oceanbase
{
namespace palf
{
class LogPrepareMeta;

enum PushLogType
{
  PUSH_LOG = 0,
  FETCH_LOG_RESP = 1,
};

struct LogPushReq {
  OB_UNIS_VERSION(1);
public:
  LogPushReq();
  LogPushReq(const PushLogType push_log_type,
             const int64_t &msg_proposal_id,
             const int64_t &prev_log_proposal_id,
             const LSN &prev_lsn,
             const LSN &curr_lsn,
             const LogWriteBuf &write_buf);
  ~LogPushReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(push_log_type), K_(msg_proposal_id), K_(prev_log_proposal_id),
               K_(prev_lsn), K_(curr_lsn), K_(write_buf));
  int16_t push_log_type_;
  int64_t msg_proposal_id_;
  int64_t prev_log_proposal_id_;
  LSN prev_lsn_;
  // NB: no need record the proposal_id of curr_lsn, we will deserlize
  // to LogGroupEntry.
  LSN curr_lsn_;
  LogWriteBuf write_buf_;
};

struct LogPushResp {
  OB_UNIS_VERSION(1);
public:
  LogPushResp();
  LogPushResp(const int64_t &msg_proposal_id,
                const LSN &lsn);
  ~LogPushResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id), K_(lsn));
  int64_t msg_proposal_id_;
  LSN lsn_;
};

enum FetchLogType
{
  FETCH_LOG_FOLLOWER = 0,
  FETCH_LOG_LEADER_RECONFIRM = 1,
  FETCH_MODE_META = 2,
};

struct LogFetchReq {
  OB_UNIS_VERSION(1);
public:
  LogFetchReq();
  // NB: 'prev_lsn' is used to forward check on leader
  //     because we don't record these fields in LogGroupEntryHeader
  LogFetchReq(const FetchLogType fetch_type,
              const int64_t msg_proposal_id,
              const LSN &prev_lsn,
              const LSN &lsn,
              const int64_t fetch_log_size,
              const int64_t fetch_log_count,
              const int64_t accepted_mode_pid);
  ~LogFetchReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id), K_(fetch_type), K_(prev_lsn), K_(lsn), K_(fetch_log_size),
      K_(fetch_log_count), K_(accepted_mode_pid));
  int16_t fetch_type_;
  int64_t msg_proposal_id_;
  LSN prev_lsn_;
  LSN lsn_;
  int64_t fetch_log_size_;
  int64_t fetch_log_count_;
  int64_t accepted_mode_pid_;
};

struct NotifyRebuildReq {
  OB_UNIS_VERSION(1);
public:
  NotifyRebuildReq();
  NotifyRebuildReq(const LSN &base_lsn, const LogInfo &base_prev_log_info);
  ~NotifyRebuildReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(base_lsn), K_(base_prev_log_info));
  LSN base_lsn_;
  LogInfo base_prev_log_info_;
};

struct NotifyFetchLogReq {
  OB_UNIS_VERSION(1);
public:
  NotifyFetchLogReq();
  ~NotifyFetchLogReq();
  bool is_valid() const;
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    return pos;
  }
};

struct LogPrepareReq {
  OB_UNIS_VERSION(1);
public:
  LogPrepareReq();
  LogPrepareReq(const int64_t &log_proposal_id);
  LogPrepareReq(const LogPrepareReq &req);
  ~LogPrepareReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(log_proposal_id));
  int64_t log_proposal_id_;
};

struct LogPrepareResp {
  OB_UNIS_VERSION(1);
public:
  LogPrepareResp();
  LogPrepareResp(const int64_t &msg_proposal_id,
                 const bool vote_granted,
                 const int64_t &log_proposal_id,
                 const LSN &max_flushed_lsn,
                 const LogModeMeta &mode_meta,
                 const LSN &committed_end_lsn);
  ~LogPrepareResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id), K_(vote_granted), K_(log_proposal_id), K_(max_flushed_lsn),
      K_(log_mode_meta), K_(committed_end_lsn));
  int64_t msg_proposal_id_;
  bool vote_granted_;
  int64_t log_proposal_id_;
  LSN max_flushed_lsn_;
  LogModeMeta log_mode_meta_;
  LSN committed_end_lsn_;
};

struct LogChangeConfigMetaReq {
  OB_UNIS_VERSION(1);
public:
  LogChangeConfigMetaReq();
  LogChangeConfigMetaReq(const int64_t &msg_proposal_id,
                         const int64_t &prev_log_proposal_id,
                         const LSN &prev_lsn,
                         const int64_t &prev_mode_pid,
                         const LogConfigMeta &meta);
  ~LogChangeConfigMetaReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id), K_(prev_log_proposal_id), K_(prev_lsn),
      K_(prev_mode_pid), K_(meta));
  int64_t msg_proposal_id_;
  // Forward check, used to truncate the ghost logs of prev leader.
  int64_t prev_log_proposal_id_;
  LSN prev_lsn_;
  int64_t prev_mode_pid_;
  LogConfigMeta meta_;
};

struct LogChangeConfigMetaResp {
  OB_UNIS_VERSION(1);
public:
  LogChangeConfigMetaResp();
  LogChangeConfigMetaResp(const int64_t proposal_id, const LogConfigVersion &config_version);
  ~LogChangeConfigMetaResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(proposal_id), K_(config_version));
  int64_t proposal_id_;
  LogConfigVersion config_version_;
};

struct LogChangeModeMetaReq {
  OB_UNIS_VERSION(1);
public:
  LogChangeModeMetaReq();
  LogChangeModeMetaReq(const int64_t &msg_proposal_id,
                       const LogModeMeta &meta,
                       const bool is_applied_mode_meta);
  ~LogChangeModeMetaReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id), K_(meta), K_(is_applied_mode_meta));
  int64_t msg_proposal_id_;
  LogModeMeta meta_;
  bool is_applied_mode_meta_;
};

struct LogChangeModeMetaResp {
  OB_UNIS_VERSION(1);
public:
  LogChangeModeMetaResp();
  LogChangeModeMetaResp(const int64_t proposal_id);
  ~LogChangeModeMetaResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id));
  int64_t msg_proposal_id_;
};

struct LogGetMCStReq {
  OB_UNIS_VERSION(1);
public:
  LogGetMCStReq();
  LogGetMCStReq(const LogConfigVersion &config_version,
                const bool need_purge_throttling);
  ~LogGetMCStReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(config_version), K_(need_purge_throttling));
  LogConfigVersion config_version_;
  bool need_purge_throttling_;
};

struct LogGetMCStResp {
  OB_UNIS_VERSION(1);
public:
  LogGetMCStResp();
  LogGetMCStResp(const int64_t &msg_proposal_id,
                 const LSN &max_flushed_end_lsn,
                 const bool is_normal_replica,
                 const bool need_update_config_meta,
                 const int64_t last_slide_log_id);
  ~LogGetMCStResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id), K_(max_flushed_end_lsn), K_(is_normal_replica),
      K_(need_update_config_meta), K_(last_slide_log_id));
  int64_t msg_proposal_id_;
  LSN max_flushed_end_lsn_;
  bool is_normal_replica_;
  bool need_update_config_meta_;
  int64_t last_slide_log_id_;
};

enum LogLearnerReqType
{
  INVALID_LEARNER_REQ_TYPE = 0,
  RETIRE_PARENT = 1,
  RETIRE_CHILD = 2,
  KEEPALIVE_REQ = 3,
  KEEPALIVE_RESP = 4,
};

struct LogLearnerReq
{
  OB_UNIS_VERSION(1);
public:
  LogLearnerReq();
  LogLearnerReq(const LogLearner &sender,
              const LogLearnerReqType req_type);
  ~LogLearnerReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(sender), K_(req_type));

  // RETIRE_CHILD: old parent of child that send this msg (self)
  // KEEPALIVE_REQ: parent that sends this msg (self)
  // RETIRE_PARENT: child that send this msg (self)
  // KEEPALIVE_RESP: child that sends this msg (self)
  LogLearner sender_;
  // ReqType
  LogLearnerReqType req_type_;
};

struct LogRegisterParentReq
{
  OB_UNIS_VERSION(1);
public:
  LogRegisterParentReq();
  LogRegisterParentReq(const LogLearner &child, const bool is_to_leader);
  ~LogRegisterParentReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(child), K_(is_to_leader));

  // child that is registering parent
  LogLearner child_;
  // if this register req is sended to leader, and msg receiver has not been leader,
  // it should return REGISTER_NOT_MASTER
  bool is_to_leader_;
};

enum RegisterReturn
{
  INVALID_REG_RET = 0,
  REGISTER_DONE = 1,
  REGISTER_CONTINUE = 2,
  REGISTER_NOT_MASTER = 3,
  REGISTER_DIFF_REGION = 4,
};

struct LogRegisterParentResp
{
  OB_UNIS_VERSION(1);
public:
  LogRegisterParentResp();
  LogRegisterParentResp(const LogLearner &parent,
                        const LogCandidateList &candidate_list,
                        const RegisterReturn reg_ret);
  ~LogRegisterParentResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(parent), K_(candidate_list), K_(reg_ret));

  // parent itself, for msg pair varification
  LogLearner parent_;
  // candidate members that can be parent of this replica
  LogCandidateList candidate_list_;
  // REGISTER_DONE: parent_ has been registered as parent
  // REGISTER_CONTINUE: continue sending register_req to candidate_list_
  // REGISTER_NOT_MASTER: this replica has not been leader anymore
  RegisterReturn reg_ret_;
};

struct CommittedInfo
{
  OB_UNIS_VERSION(1);
public:
  CommittedInfo();
  CommittedInfo(const int64_t &msg_proposal_id,
                const int64_t prev_log_id,
                const int64_t &prev_log_proposal_id,
                const LSN &committed_end_lsn);
  ~CommittedInfo();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(msg_proposal_id), K_(prev_log_id),
               K_(prev_log_proposal_id), K_(committed_end_lsn));
  int64_t msg_proposal_id_;
  int64_t prev_log_id_;
  int64_t prev_log_proposal_id_;
  LSN committed_end_lsn_;
};

enum LogGetStatType
{
  INVALID_SYNC_GET_TYPE = 0,
  GET_LEADER_MAX_SCN = 1,
};

struct LogGetStatReq {
  OB_UNIS_VERSION(1);
public:
  LogGetStatReq();
  LogGetStatReq(const LogGetStatType get_type);
  ~LogGetStatReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(get_type));
  int16_t get_type_;
};

struct LogGetStatResp {
  OB_UNIS_VERSION(1);
public:
  LogGetStatResp();
  LogGetStatResp(const share::SCN &max_scn, const LSN &end_lsn);
  ~LogGetStatResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(max_scn), K_(end_lsn));
  share::SCN max_scn_;
  LSN end_lsn_;
};


} // end namespace palf
} // end namespace oceanbase

#endif
