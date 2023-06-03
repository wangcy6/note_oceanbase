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

#include "election_message.h"
#include <cstring>
#include "common/ob_clock_generator.h"
#include "common/ob_role.h"
#include "lib/ob_errno.h"
#include "lib/utility/serialization.h"
#include "observer/ob_server_struct.h"
#include "share/ob_occam_time_guard.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/election/algorithm/election_proposer.h"
#include "logservice/palf/election/utils/election_args_checker.h"
#include "logservice/palf/election/utils/election_common_define.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

using namespace common;
using namespace share;

// this is important debug info when meet Lease Expired ERROR! which is a high frequecy error in election
void print_debug_ts_if_reach_warn_threshold(const ElectionMsgBase &msg, const int64_t warn_threshold)
{
  #define PRINT_WRAPPER K(msg), K(req_debug_ts), K(diff), K(max_diff), K(warn_threshold), K(recaculated_warn_threshold)
  int64_t diff = 0;
  ElectionMsgDebugTs req_debug_ts;
  ElectionMsgDebugTs res_debug_ts;
  int64_t recaculated_warn_threshold = warn_threshold;
  // this must be true, or some terrible things happend, and can't be handle, abort is the best way
  OB_ASSERT(msg.get_msg_type() >= ElectionMsgType::PREPARE_REQUEST && msg.get_msg_type() < ElectionMsgType::INVALID_TYPE);
  if (msg.get_msg_type() == ElectionMsgType::PREPARE_RESPONSE) {
    recaculated_warn_threshold += CALCULATE_TIME_WINDOW_SPAN_TS();
    req_debug_ts = static_cast<const ElectionPrepareResponseMsg &>(msg).get_request_debug_ts();
    res_debug_ts = msg.get_debug_ts();
  } else if (msg.get_msg_type() == ElectionMsgType::ACCEPT_RESPONSE) {
    req_debug_ts = static_cast<const ElectionAcceptResponseMsg &>(msg).get_request_debug_ts();
    res_debug_ts = msg.get_debug_ts();
  } else {
    req_debug_ts = msg.get_debug_ts();
  }
  int64_t max_diff = std::max({req_debug_ts.src_construct_ts_, req_debug_ts.src_serialize_ts_,
                              req_debug_ts.dest_deserialize_ts_, req_debug_ts.dest_process_ts_,
                              res_debug_ts.src_construct_ts_, res_debug_ts.src_serialize_ts_,
                              res_debug_ts.dest_deserialize_ts_, res_debug_ts.dest_process_ts_}) - req_debug_ts.src_construct_ts_;
  if (req_debug_ts.src_serialize_ts_ != 0 && (diff = std::abs(req_debug_ts.src_serialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "request serialize in src too delay");
  } else if (req_debug_ts.dest_deserialize_ts_ != 0 && (diff = std::abs(req_debug_ts.dest_deserialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "request deserialize in dest too delay");
  } else if (req_debug_ts.dest_process_ts_ != 0 && (diff = std::abs(req_debug_ts.dest_process_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "request process in dest too delay");
  } else if (res_debug_ts.src_construct_ts_ != 0 && (diff = std::abs(res_debug_ts.src_construct_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response construct in src too delay");
  } else if (res_debug_ts.src_serialize_ts_ != 0 && (diff = std::abs(res_debug_ts.src_serialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response serialize in src too delay");
  } else if (res_debug_ts.dest_deserialize_ts_ != 0 && (diff = std::abs(res_debug_ts.dest_deserialize_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response deserialize in dest too delay");
  } else if (res_debug_ts.dest_process_ts_ != 0 && (diff = std::abs(res_debug_ts.dest_process_ts_ - req_debug_ts.src_construct_ts_)) > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "response process in dest too delay");
  } else if (max_diff > recaculated_warn_threshold) {
    LOG_NONE_RET(WARN, OB_ERR_UNEXPECTED, "max_diff too delay");
  }
  return;
  #undef PRINT_WRAPPER
}

ElectionMsgBase::ElectionMsgBase() :
id_(INVALID_VALUE),
restart_counter_(INVALID_VALUE),
ballot_number_(INVALID_VALUE),
msg_type_(static_cast<int64_t>(ElectionMsgType::INVALID_TYPE)) {}

ElectionMsgBase::ElectionMsgBase(const int64_t id,
                                 const common::ObAddr &self_addr,
                                 const int64_t restart_counter,
                                 const int64_t ballot_number,
                                 const ElectionMsgType msg_type) :
id_(id),
sender_(self_addr),
restart_counter_(restart_counter),
ballot_number_(ballot_number),
msg_type_(static_cast<int64_t>(msg_type)) {
  debug_ts_.src_construct_ts_ = ObClockGenerator::getRealClock();
}

void ElectionMsgBase::reset()
{
  sender_.reset();
  receiver_.reset();
  restart_counter_ = INVALID_VALUE;
  ballot_number_ = INVALID_VALUE;
  msg_type_ = static_cast<int64_t>(ElectionMsgType::INVALID_TYPE);
}

void ElectionMsgBase::set_receiver(const common::ObAddr &addr) { receiver_ = addr; }

int64_t ElectionMsgBase::get_restart_counter() const { return restart_counter_; }

int64_t ElectionMsgBase::get_ballot_number() const { return ballot_number_; }

bool ElectionMsgBase::is_valid() const
{
  return sender_.is_valid() && receiver_.is_valid() && restart_counter_ != INVALID_VALUE && ballot_number_ != INVALID_VALUE &&
         msg_type_ >= static_cast<int64_t>(ElectionMsgType::PREPARE_REQUEST) &&
         msg_type_ <= static_cast<int64_t>(ElectionMsgType::CHANGE_LEADER);
}

const common::ObAddr &ElectionMsgBase::get_sender() const { return sender_; }

const common::ObAddr &ElectionMsgBase::get_receiver() const { return receiver_; }

ElectionMsgType ElectionMsgBase::get_msg_type() const
{
  return static_cast<ElectionMsgType>(msg_type_);
}

ElectionMsgDebugTs ElectionMsgBase::get_debug_ts() const { return debug_ts_; }

void ElectionMsgBase::set_process_ts()
{
  debug_ts_.dest_process_ts_ = ObClockGenerator::getRealClock();
  print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
}

int64_t ElectionMsgBase::get_id() const { return id_; }

ElectionPrepareRequestMsgMiddle::ElectionPrepareRequestMsgMiddle(const int64_t id,
                                                                 const common::ObAddr &self_addr,
                                                                 const int64_t restart_counter,
                                                                 const int64_t ballot_number,
                                                                 const uint64_t inner_priority_seed,
                                                                 const LogConfigVersion membership_version) :
ElectionMsgBase(id,
                self_addr,
                restart_counter,
                ballot_number,
                ElectionMsgType::PREPARE_REQUEST),
role_(ObRole::INVALID_ROLE),
is_buffer_valid_(false),
inner_priority_seed_(inner_priority_seed),
membership_version_(membership_version)
{ memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE); }

ElectionPrepareRequestMsgMiddle::ElectionPrepareRequestMsgMiddle() :
ElectionMsgBase(),
role_(ObRole::INVALID_ROLE),
is_buffer_valid_(false),
inner_priority_seed_(static_cast<uint64_t>(PRIORITY_SEED_BIT::DEFAULT_SEED))
{
  memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE);
}

int ElectionPrepareRequestMsgMiddle::set(const ElectionPriority *priority,
                                   const common::ObRole role) {
  ELECT_TIME_GUARD(500_ms);
  int ret = common::OB_SUCCESS;
  role_ = static_cast<int64_t>(role);
  // create_buffer_and_serialize_priority(priority_buffer_, buffer_length_, priority);
  if (OB_NOT_NULL(priority)) {
    int64_t pos = 0;
    if (CLICK_FAIL(priority->serialize((char*)priority_buffer_, PRIORITY_BUFFER_SIZE, pos))) {
      ELECT_LOG(ERROR, "fail to serialize priority");
    } else {
      is_buffer_valid_ = true;
    }
  }
  return ret;
}

bool ElectionPrepareRequestMsgMiddle::is_buffer_valid() const { return is_buffer_valid_; }

const char *ElectionPrepareRequestMsgMiddle::get_priority_buffer() const { return (char*)priority_buffer_; }
  
common::ObRole ElectionPrepareRequestMsgMiddle::get_role() const { return static_cast<common::ObRole>(role_); }

LogConfigVersion ElectionPrepareRequestMsgMiddle::get_membership_version() const { return membership_version_; }

uint64_t ElectionPrepareRequestMsgMiddle::get_inner_priority_seed() const { return inner_priority_seed_; }

ElectionPrepareResponseMsgMiddle::ElectionPrepareResponseMsgMiddle() :
ElectionMsgBase(),
accepted_(false) {}

ElectionPrepareResponseMsgMiddle::
ElectionPrepareResponseMsgMiddle(const ObAddr &self_addr,
                                 const ElectionPrepareRequestMsgMiddle &request) :
ElectionMsgBase(request.get_id(),
                self_addr,
                request.get_restart_counter(),
                INVALID_VALUE,
                ElectionMsgType::PREPARE_RESPONSE),
accepted_(false) {
  set_receiver(request.get_sender());
  request_debug_ts_ = request.get_debug_ts();
  print_debug_ts_if_reach_warn_threshold(*this, MSG_DELAY_WARN_THRESHOLD);
}

void ElectionPrepareResponseMsgMiddle::set_accepted(const int64_t ballot_number, const Lease lease) {
  ballot_number_ = ballot_number;
  lease_ = lease;
  accepted_ = true;
}

bool ElectionPrepareResponseMsgMiddle::is_accepted() const { return accepted_; }

void ElectionPrepareResponseMsgMiddle::set_rejected(const int64_t ballot_number) {
  ballot_number_ = ballot_number;
  accepted_ = false;
}

const Lease &ElectionPrepareResponseMsgMiddle::get_lease() const { return lease_; }

ElectionAcceptRequestMsgMiddle::ElectionAcceptRequestMsgMiddle() :
ElectionMsgBase(),
lease_start_ts_on_proposer_(0),
lease_interval_(0) {}

ElectionMsgDebugTs ElectionPrepareResponseMsgMiddle::get_request_debug_ts() const { return request_debug_ts_; }

ElectionAcceptRequestMsgMiddle::ElectionAcceptRequestMsgMiddle(const int64_t id,
                                                               const ObAddr &self_addr,
                                                               const int64_t restart_counter,
                                                               const int64_t ballot_number,
                                                               const int64_t lease_start_ts_on_proposer,
                                                               const int64_t lease_interval,
                                                               const LogConfigVersion membership_version) :
ElectionMsgBase(id,
                self_addr,
                restart_counter,
                ballot_number,
                ElectionMsgType::ACCEPT_REQUEST),
lease_start_ts_on_proposer_(lease_start_ts_on_proposer),
lease_interval_(lease_interval),
membership_version_(membership_version) {}

int64_t ElectionAcceptRequestMsgMiddle::get_lease_start_ts_on_proposer() const
{
  return lease_start_ts_on_proposer_;
}

int64_t ElectionAcceptRequestMsgMiddle::get_lease_interval() const { return lease_interval_; }

ElectionAcceptResponseMsgMiddle::ElectionAcceptResponseMsgMiddle() :
ElectionMsgBase(),
lease_started_ts_on_proposer_(0),
lease_interval_(0),
accepted_(false),
is_buffer_valid_(false),
inner_priority_seed_(static_cast<uint64_t>(PRIORITY_SEED_BIT::DEFAULT_SEED))
{
  memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE);
}

LogConfigVersion ElectionAcceptRequestMsgMiddle::get_membership_version() const { return membership_version_; }

ElectionAcceptResponseMsgMiddle::
ElectionAcceptResponseMsgMiddle(const ObAddr &self_addr,
                                const uint64_t inner_priority_seed,
                                const LogConfigVersion &membership_version,
                                const ElectionAcceptRequestMsgMiddle &request) :
ElectionMsgBase(request.get_id(),
                self_addr,
                request.get_restart_counter(),
                INVALID_VALUE,
                ElectionMsgType::ACCEPT_RESPONSE),
lease_started_ts_on_proposer_(request.get_lease_start_ts_on_proposer()),
lease_interval_(request.get_lease_interval()),
accepted_(false),
is_buffer_valid_(false),
inner_priority_seed_(inner_priority_seed),
responsed_membership_version_(request.get_membership_version()),
membership_version_(membership_version)
{
  set_receiver(request.get_sender());
  memset(priority_buffer_, 0, PRIORITY_BUFFER_SIZE);
  request_debug_ts_ = request.get_debug_ts();
}

int ElectionAcceptResponseMsgMiddle::set_accepted(const int64_t ballot_number,
                                                  const ElectionPriority *priority) {
  ELECT_TIME_GUARD(500_ms);
  int ret = common::OB_SUCCESS;
  ballot_number_ = ballot_number;
  accepted_ = true;
  int64_t pos = 0;
  if (OB_NOT_NULL(priority)) {
    if (CLICK_FAIL(priority->serialize((char*)priority_buffer_, PRIORITY_BUFFER_SIZE, pos))) {
      ELECT_LOG(ERROR, "fail to serialize priority");
    } else {
      is_buffer_valid_ = true;
    }
  }
  return ret;
}

void ElectionAcceptResponseMsgMiddle::set_rejected(const int64_t ballot_number) {
  ballot_number_ = ballot_number;
  accepted_ = false;
}

bool ElectionAcceptResponseMsgMiddle::is_accepted() const { return accepted_; }

int64_t ElectionAcceptResponseMsgMiddle::get_lease_started_ts_on_proposer() const
{
  return lease_started_ts_on_proposer_;
}

int64_t ElectionAcceptResponseMsgMiddle::get_lease_interval() const { return lease_interval_; }

bool ElectionAcceptResponseMsgMiddle::is_buffer_valid() const { return is_buffer_valid_; }

const char *ElectionAcceptResponseMsgMiddle::get_priority_buffer() const { return (char*)priority_buffer_; }

LogConfigVersion ElectionAcceptResponseMsgMiddle::get_responsed_membership_version() const { return responsed_membership_version_; }

LogConfigVersion ElectionAcceptResponseMsgMiddle::get_membership_version() const { return membership_version_; }

ElectionMsgDebugTs ElectionAcceptResponseMsgMiddle::get_request_debug_ts() const { return request_debug_ts_; }

uint64_t ElectionAcceptResponseMsgMiddle::get_inner_priority_seed() const { return inner_priority_seed_; }

ElectionChangeLeaderMsgMiddle::ElectionChangeLeaderMsgMiddle() :
ElectionMsgBase(),
switch_source_leader_ballot_(INVALID_VALUE) {}

ElectionChangeLeaderMsgMiddle::ElectionChangeLeaderMsgMiddle(const int64_t id,
                                                             const ObAddr &self_addr,
                                                             const int64_t restart_counter,
                                                             const int64_t ballot_number,
                                                             int64_t switch_source_leader_ballot,
                                                             const LogConfigVersion membership_version) :
ElectionMsgBase(id,
                self_addr,
                restart_counter,
                ballot_number,
                ElectionMsgType::CHANGE_LEADER),
switch_source_leader_ballot_(switch_source_leader_ballot),
membership_version_(membership_version) {}

LogConfigVersion ElectionChangeLeaderMsgMiddle::get_membership_version() const { return membership_version_; }

int64_t ElectionChangeLeaderMsgMiddle::get_old_ballot_number() const { return switch_source_leader_ballot_; }

}// namespace election
}// namespace palf
}// namesapce oceanbase
