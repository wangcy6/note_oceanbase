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

#include "share/ob_define.h"
#include "common/ob_tablet_id.h"
#include "common/storage/ob_freeze_define.h"

#ifndef OCEABASE_STORAGE_TENANT_FREEZER_COMMON_
#define OCEABASE_STORAGE_TENANT_FREEZER_COMMON_
namespace oceanbase
{
namespace common
{
class ObMemstoreAllocatorMgr;
}
namespace storage
{
struct ObTenantFreezeArg
{
  storage::ObFreezeType freeze_type_; // minor/major
  int64_t try_frozen_scn_;            // frozen scn.

  DECLARE_TO_STRING;
  OB_UNIS_VERSION(1);
};

struct ObRetryMajorInfo
{
  uint64_t tenant_id_;
  int64_t frozen_scn_;

  ObRetryMajorInfo()
    : tenant_id_(UINT64_MAX),
      frozen_scn_(0)
  {}
  bool is_valid() const {
    return UINT64_MAX != tenant_id_;
  }
  void reset() {
    tenant_id_ = UINT64_MAX;
    frozen_scn_ = 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(frozen_scn));
};

// store a snapshot of the tenant info to make sure
// the tenant_info will be a atomic value
struct ObTenantFreezeCtx final
{
public:
  ObTenantFreezeCtx();
  ~ObTenantFreezeCtx() { reset(); }
  void reset();
public:
  // snapshot of tenant_info
  int64_t mem_lower_limit_;
  int64_t mem_upper_limit_;
  int64_t mem_memstore_limit_;

  // running data
  int64_t memstore_freeze_trigger_;
  int64_t max_mem_memstore_can_get_now_;
  int64_t kvcache_mem_;

  int64_t active_memstore_used_;
  int64_t total_memstore_used_;
  int64_t total_memstore_hold_;
private:
  DISABLE_COPY_ASSIGN(ObTenantFreezeCtx);
};

// store the tenant info, such as memory limit, memstore limit,
// slow freeze flag, freezing flag and so on.
class ObTenantInfo : public ObDLinkBase<ObTenantInfo>
{
public:
  ObTenantInfo();
  virtual ~ObTenantInfo() { reset(); }
  void reset();
  int update_frozen_scn(int64_t frozen_scn);
  int64_t mem_memstore_left() const;

  void update_mem_limit(const int64_t lower_limit, const int64_t upper_limit);
  void get_mem_limit(int64_t &lower_limit, int64_t &upper_limit) const;
  void update_memstore_limit(const int64_t memstore_limit_percentage);
  int64_t get_memstore_limit() const;
  void get_freeze_ctx(ObTenantFreezeCtx &ctx) const;
public:
  uint64_t tenant_id_;
  bool is_loaded_;             // whether the memory limit set or not.
  bool is_freezing_;           // is the tenant freezing now.
  int64_t last_freeze_clock_;
  int64_t frozen_scn_;         // used by major, the timestamp of frozen.
  int64_t freeze_cnt_;         // minor freeze times.
  int64_t last_halt_ts_;       // Avoid frequent execution of abort preheating
  bool slow_freeze_;           // Avoid frequent freezing when abnormal
  int64_t slow_freeze_timestamp_; // the last slow freeze time timestamp
  int64_t slow_freeze_min_protect_clock_;
  common::ObTabletID slow_tablet_;
private:
  // protect mem_lower_limit_/mem_upper_limit_/mem_memstore_limit_
  // to make sure it is consistency
  SpinRWLock lock_;
  int64_t mem_lower_limit_;    // the min memory limit
  int64_t mem_upper_limit_;    // the max memory limit
  // mem_memstore_limit will be checked when **leader** partitions
  // perform writing operation (select for update is included)
  int64_t mem_memstore_limit_; // the max memstore limit
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantInfo);
};

class ObTenantFreezeGuard
{
public:
  ObTenantFreezeGuard(common::ObMemstoreAllocatorMgr *allocator_mgr,
                      int &ret,
                      const int64_t warn_threshold = 60 * 1000 * 1000 /* 1 min */);
  ~ObTenantFreezeGuard();
private:
  common::ObMemstoreAllocatorMgr *allocator_mgr_;
  int64_t pre_retire_pos_;
  int &error_code_;
  ObTimeGuard time_guard_;
};

} // storage
} // oceanbase
#endif
