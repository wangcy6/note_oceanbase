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

#ifndef OB_TENANT_BASE_H_
#define OB_TENANT_BASE_H_

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/worker.h"
#include "lib/hash/ob_hashset.h"
#include "lib/thread/threads.h"
#include "lib/thread/thread_mgr.h"
#include "lib/allocator/ob_malloc.h"
#include "share/ob_tenant_role.h"//ObTenantRole
#include "lib/mysqlclient/ob_tenant_oci_envs.h"
namespace oceanbase
{
namespace common {
  class ObLDHandle;
  class ObTenantIOManager;
  template<typename T> class ObServerObjectPool;
  class ObDetectManager;
}
namespace omt {
 class ObPxPools;
 class ObTenant;
}
namespace obmysql {
  class ObMySQLRequestManager;
  class ObSqlNioServer;
}
namespace sql {
  namespace dtl { class ObTenantDfc; }
  class ObTenantSqlMemoryManager;
  class ObPlanMonitorNodeList;
  class ObPlanBaselineMgr;
  class ObDataAccessService;
  class ObDASIDService;
  class ObFLTSpanMgr;
  class ObUDRMgr;
  class ObPlanCache;
  class ObPsCache;
}
namespace blocksstable {
  class ObSharedMacroBlockMgr;
}
namespace storage {
  struct ObTenantStorageInfo;
  class ObLSService;
  class ObAccessService;
  class ObTenantFreezer;
  class ObTenantMetaMemMgr;
  class ObStorageLogger;
  class ObTenantTabletScheduler;
  class ObTenantCheckpointSlogHandler;
  class ObTenantFreezeInfoMgr;
  class ObStorageHAService;
  class ObStorageHAHandlerService;
  class ObLSRestoreService;
  class ObTenantSSTableMergeInfoMgr;
  class ObTenantTabletStatMgr;
  namespace checkpoint {
    class ObCheckPointService;
    class ObTabletGCService;
  }
  class ObLobManager;
}
namespace transaction {
  class ObTenantWeakReadService; // 租户弱一致性读服务
  class ObTransService;          // 事务服务
  class ObXAService;
  class ObTimestampService;
  class ObStandbyTimestampService;
  class ObTimestampAccess;
  class ObTransIDService;
  class ObTxLoopWorker;
  class ObPartTransCtx;
  namespace tablelock {
    class ObTableLockService;
  }
}
namespace concurrency_control {
  class ObMultiVersionGarbageCollector; // MVCC GC
}

namespace logservice
{
  class ObLogService;
  class ObGarbageCollector;
namespace coordinator
{
  class ObFailureDetector;
  class ObLeaderCoordinator;
}
}
namespace datadict
{
  class ObDataDictService;
}
namespace archive
{
  class ObArchiveService;
}
namespace compaction
{
  class ObTenantCompactionProgressMgr;
  class ObServerCompactionEventHistory;
}
namespace memtable
{
  class ObLockWaitMgr;
}
namespace rootserver
{
  class ObPrimaryMajorFreezeService;
  class ObRestoreMajorFreezeService;
  class ObTenantRecoveryReportor;
  class ObTenantInfoLoader;
  class ObCreateStandbyFromNetActor;
  class ObPrimaryLSService;
  class ObRestoreService;
  class ObRecoveryLSService;
  class ObArbitrationService;
  class ObHeartbeatService;
  class ObStandbySchemaRefreshTrigger;
}
namespace observer
{
  class ObTenantMetaChecker;
  class QueueThread;
  class ObTableLoadService;
}

// for ObTenantSwitchGuard 临时使用>>>>>>>>
namespace observer
{
  class ObAllVirtualTabletInfo;
  class ObAllVirtualTransCheckpointInfo;
  class ObAllVirtualTabletEncryptInfo;
  class ObAllVirtualTabletSSTableMacroInfo;
  class ObAllVirtualObjLock;
  class ObAllVirtualMemstoreInfo;
}
namespace storage {
  class MockTenantModuleEnv;
}

namespace share
{
class ObCgroupCtrl;
class ObTestModule;
class ObTenantDagScheduler;
class ObTenantModuleInitCtx;
class ObGlobalAutoIncService;
namespace schema
{
  class ObTenantSchemaService;
}
namespace detector
{
  class ObDeadLockDetectorMgr;
}

#define ArbMTLMember

// 在这里列举需要添加的租户局部变量的类型，租户会为每种类型创建一个实例。
// 实例的初始化和销毁逻辑由MTL_BIND接口指定。
// 使用MTL接口可以获取实例。
using ObPartTransCtxObjPool = common::ObServerObjectPool<transaction::ObPartTransCtx>;
#define MTL_MEMBERS                                  \
  MTL_LIST(                                          \
      ObPartTransCtxObjPool*,                        \
      common::ObTenantIOManager*,                    \
      storage::ObStorageLogger*,                     \
      blocksstable::ObSharedMacroBlockMgr*,          \
      storage::ObTenantMetaMemMgr*,                  \
      transaction::ObTransService*,                  \
      logservice::coordinator::ObLeaderCoordinator*, \
      logservice::coordinator::ObFailureDetector*,   \
      logservice::ObLogService*,                     \
      storage::ObLSService*,                         \
      storage::ObTenantCheckpointSlogHandler*,       \
      compaction::ObTenantCompactionProgressMgr*,    \
      compaction::ObServerCompactionEventHistory*,   \
      storage::ObTenantTabletStatMgr*,               \
      memtable::ObLockWaitMgr*,                      \
      logservice::ObGarbageCollector*,               \
      transaction::tablelock::ObTableLockService*,   \
      rootserver::ObPrimaryMajorFreezeService*,      \
      rootserver::ObRestoreMajorFreezeService*,      \
      observer::ObTenantMetaChecker*,                \
      observer::QueueThread *,                       \
      storage::ObStorageHAHandlerService*,           \
      rootserver::ObTenantRecoveryReportor*,         \
      rootserver::ObTenantInfoLoader*,         \
      rootserver::ObCreateStandbyFromNetActor*,         \
      rootserver::ObStandbySchemaRefreshTrigger*,    \
      rootserver::ObPrimaryLSService*,               \
      rootserver::ObRecoveryLSService*,              \
      rootserver::ObRestoreService*,                 \
      storage::ObLSRestoreService*,                  \
      storage::ObTenantSSTableMergeInfoMgr*,         \
      storage::ObLobManager*,                        \
      share::ObGlobalAutoIncService*,                \
      share::detector::ObDeadLockDetectorMgr*,       \
      transaction::ObXAService*,                     \
      transaction::ObTimestampService*,              \
      transaction::ObStandbyTimestampService*,       \
      transaction::ObTimestampAccess*,               \
      transaction::ObTransIDService*,                \
      sql::ObPlanBaselineMgr*,                       \
      sql::ObPsCache*,                               \
      sql::ObPlanCache*,                             \
      sql::dtl::ObTenantDfc*,                        \
      omt::ObPxPools*,                               \
      lib::Worker::CompatMode,                       \
      obmysql::ObMySQLRequestManager*,               \
      transaction::ObTenantWeakReadService*,         \
      storage::ObTenantStorageInfo*,                 \
      sql::ObTenantSqlMemoryManager*,                \
      sql::ObPlanMonitorNodeList*,                   \
      sql::ObDataAccessService*,                     \
      sql::ObDASIDService*,                          \
      share::schema::ObTenantSchemaService*,         \
      storage::ObTenantFreezer*,                     \
      storage::checkpoint::ObCheckPointService *,    \
      storage::checkpoint::ObTabletGCService *,      \
      archive::ObArchiveService*,                    \
      storage::ObTenantTabletScheduler*,             \
      share::ObTenantDagScheduler*,                  \
      storage::ObStorageHAService*,                  \
      storage::ObTenantFreezeInfoMgr*,               \
      transaction::ObTxLoopWorker *,                 \
      storage::ObAccessService*,                     \
      datadict::ObDataDictService*,                  \
      ArbMTLMember                                   \
      observer::ObTableLoadService*,                 \
      concurrency_control::ObMultiVersionGarbageCollector*, \
      sql::ObUDRMgr*,                        \
      sql::ObFLTSpanMgr*,                            \
      ObTestModule*,                                 \
      oceanbase::common::sqlclient::ObTenantOciEnvs*, \
      rootserver::ObHeartbeatService*,              \
      oceanbase::common::ObDetectManager*            \
  )


// 获取租户ID
#define MTL_ID() share::ObTenantEnv::get_tenant_local()->id()
// 获取是否为主租户
#define MTL_IS_PRIMARY_TENANT() share::ObTenantEnv::get_tenant()->is_primary_tenant()
// 更新租户role
#define MTL_SET_TENANT_ROLE(tenant_role) share::ObTenantEnv::get_tenant()->set_tenant_role(tenant_role)
// 获取租户role
#define MTL_GET_TENANT_ROLE() share::ObTenantEnv::get_tenant()->get_tenant_role()
// 获取租户模块
#define MTL_CTX() (share::ObTenantEnv::get_tenant())
// 获取租户初始化参数,仅在初始化时使用
#define MTL_INIT_CTX() (share::ObTenantEnv::get_tenant_local()->get_mtl_init_ctx())
// 获取租户模块检查租户ID
#define MTL_WITH_CHECK_TENANT(TYPE, tenant_id) share::ObTenantEnv::mtl<TYPE>(tenant_id)
// 注册线程池动态变更
#define MTL_REGISTER_THREAD_DYNAMIC(factor, th) \
  share::ObTenantEnv::get_tenant() == nullptr ? OB_ERR_UNEXPECTED : share::ObTenantEnv::get_tenant()->register_module_thread_dynamic(factor, th)
// 取消线程池动态变更
#define MTL_UNREGISTER_THREAD_DYNAMIC(th) \
  share::ObTenantEnv::get_tenant() == nullptr ? OB_ERR_UNEXPECTED : share::ObTenantEnv::get_tenant()->unregister_module_thread_dynamic(th)
#define MTL_IS_MINI_MODE() share::ObTenantEnv::get_tenant()->is_mini_mode()

// 注意MTL_BIND调用需要在租户创建之前，否则会导致租户创建时无法调用到绑定的函数。
#define MTL_BIND(INIT, DESTROY) \
  share::ObTenantBase::mtl_bind_func(nullptr, INIT, nullptr, nullptr, nullptr, DESTROY);
#define MTL_BIND2(NEW, INIT, START, STOP, WAIT, DESTROY) \
  share::ObTenantBase::mtl_bind_func(NEW, INIT, START, STOP, WAIT, DESTROY);

// 获取租户局部的实例
//
// 需要和租户上下文配合使用，获取指定类型的租户局部实例。
// 比如MTL(ObPxPools*)就可以获取当前租户的PX池子。
#define MTL(TYPE) ::oceanbase::share::ObTenantEnv::mtl<TYPE>()

// 辅助函数
#define MTL_LIST(...) __VA_ARGS__

// thread dynamic impl interface
class ThreadDynamicImpl
{
public:
  virtual int set_thread_cnt(int cnt) = 0;
};

// thread dynamic resource node
// support TG/Threads/DynamicImpl
class ThreadDynamicNode
{
public:
  enum DynamicType {
    INVALID = 0,
    TG = 1,
    USER_THREAD = 2,
    DYNAMIC_IMPL = 3,
  };
  ThreadDynamicNode() :type_(INVALID), tg_id_(0),user_thread_(nullptr), dynamic_impl_(nullptr) {}
  ThreadDynamicNode(int64_t tg_id) :type_(TG), tg_id_(tg_id),user_thread_(nullptr), dynamic_impl_(nullptr) {}
  ThreadDynamicNode(lib::Threads *th) :type_(USER_THREAD), tg_id_(0),user_thread_(th), dynamic_impl_(nullptr) {}
  ThreadDynamicNode(ThreadDynamicImpl *dynamic_impl) :type_(DYNAMIC_IMPL), tg_id_(0),user_thread_(nullptr), dynamic_impl_(dynamic_impl) {}
  bool operator == (const ThreadDynamicNode &other) const {
    if (type_ == other.type_) {
      if (type_ == TG) {
        return tg_id_ == other.tg_id_;
      } else if (type_ == USER_THREAD) {
        return user_thread_ == other.user_thread_;
      } else if (type_ == DYNAMIC_IMPL) {
        return dynamic_impl_ == other.dynamic_impl_;
      } else {
        return false;
      }
    }
    return false;
  }
  uint64_t hash() const {
    int64_t hash_value = 0;
    if (tg_id_ != 0) {
      hash_value = tg_id_;
    } else if (user_thread_ != nullptr) {
      hash_value = common::murmurhash(&user_thread_, sizeof(user_thread_), hash_value);
    } else if (dynamic_impl_ != nullptr) {
      hash_value = common::murmurhash(&dynamic_impl_, sizeof(dynamic_impl_), hash_value);
    }
    return hash_value;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  DynamicType get_type() { return type_; }
  int64_t get_tg_id() { return tg_id_; }
  lib::Threads *get_user_thread() { return user_thread_; }
  ThreadDynamicImpl *get_dynamic_impl() { return dynamic_impl_; }

  TO_STRING_KV(K_(type), K_(tg_id), KP_(user_thread), KP_(dynamic_impl));
private:
  DynamicType type_;
  int64_t tg_id_;
  lib::Threads *user_thread_;
  ThreadDynamicImpl *dynamic_impl_;
};

//======================================================================//
// 暴露给各个模块的Tenant类, 需要暴露的接口放在这里(租户级的service，mgr等)
class ObTenantBase : public lib::TGHelper
{
// get_tenant时omt内部会给tenant加读锁,
// ObTenantSpaceFetcher析构需要解锁，
// 因此将unlock接口暴露给ObTenantSpaceFetcher
friend class ObTenantSpaceFetcher;
friend class omt::ObTenant;
friend class ObTenantEnv;

template<class T> struct Identity {};

public:
  // TGHelper need
  virtual int pre_run(lib::Threads*) override;
  virtual int end_run(lib::Threads*) override;
  virtual void tg_create_cb(int tg_id) override;
  virtual void tg_destroy_cb(int tg_id) override;

  inline common::hash::ObHashSet<int64_t> &get_tg_set()
  {
    return tg_set_;
  }

  int update_thread_cnt(double tenant_unit_cpu);
  int64_t update_memory_size(int64_t memory_size)
  {
    int64_t orig_size = memory_size_;
    memory_size_ = memory_size;
    return orig_size;
  }
  int64_t get_memory_size() { return memory_size_; }
  bool update_mini_mode(bool mini_mode)
  {
    bool orig_mode = mini_mode_;
    mini_mode_ = mini_mode;
    return orig_mode;
  }
  bool is_mini_mode() const { return mini_mode_; }
  int64_t get_max_session_num(const int64_t rl_max_session_num);
  int register_module_thread_dynamic(double dynamic_factor, int tg_id);
  int unregister_module_thread_dynamic(int tg_id);

  int register_module_thread_dynamic(double dynamic_factor, lib::Threads *th);
  int unregister_module_thread_dynamic(lib::Threads *th);

  int register_module_thread_dynamic(double dynamic_factor, ThreadDynamicImpl *impl);
  int unregister_module_thread_dynamic(ThreadDynamicImpl *impl);
public:
  ObTenantBase(const uint64_t id, bool enable_tenant_ctx_check = false);
  ObTenantBase &operator=(const ObTenantBase &ctx);
  int init(ObCgroupCtrl *cgroup = nullptr);
  void destroy();
  virtual inline uint64_t id() const override { return id_; }
  ObCgroupCtrl *get_cgroup(lib::ThreadCGroup cgroup);

  const ObTenantModuleInitCtx *get_mtl_init_ctx() const { return mtl_init_ctx_; }

  void set_tenant_role(const share::ObTenantRole::Role tenant_role_value)
  {
    (void)ATOMIC_STORE(&tenant_role_value_, tenant_role_value);
    return ;
  }

  share::ObTenantRole::Role get_tenant_role() const
  {
    return ATOMIC_LOAD(&tenant_role_value_);
  }

 /**
  * @description:
  *    Only when it is clear that it is a standby/restore tenant, it returns not primary tenant.
  *    The correct value can be obtained after the tenant role loaded in subsequent retry.
  * @return whether allow strong consistency read write
  */
  bool is_primary_tenant()
  {
    return share::is_primary_tenant(ATOMIC_LOAD(&tenant_role_value_));
  }

  template<class T>
  T get() { return inner_get(Identity<T>()); }

  template<class T>
  void set(T v) { return inner_set(v); }


private:
  int create_mtl_module();
  int init_mtl_module();
  int start_mtl_module();
  void stop_mtl_module();
  void wait_mtl_module();
  void destroy_mtl_module();

#define MEMBER(TYPE, IDX)                                       \
public:                                                         \
  typedef int (*new_m##IDX##_func_name)(TYPE &);                \
  typedef int (*init_m##IDX##_func_name)(TYPE &);               \
  typedef int (*start_m##IDX##_func_name)(TYPE &);              \
  typedef void (*stop_m##IDX##_func_name)(TYPE &);              \
  typedef void (*wait_m##IDX##_func_name)(TYPE &);              \
  typedef void (*destroy_m##IDX##_func_name)(TYPE &);           \
  static void mtl_bind_func(                                    \
      new_m##IDX##_func_name new_func,                          \
      init_m##IDX##_func_name init_func,                        \
      start_m##IDX##_func_name start_func,                      \
      stop_m##IDX##_func_name stop_func,                        \
      wait_m##IDX##_func_name wait_func,                        \
      destroy_m##IDX##_func_name destroy_func)                  \
  {                                                             \
    new_m##IDX##_func = new_func;                               \
    init_m##IDX##_func = init_func;                             \
    start_m##IDX##_func = start_func;                           \
    stop_m##IDX##_func = stop_func;                             \
    wait_m##IDX##_func = wait_func;                             \
    destroy_m##IDX##_func = destroy_func;                       \
  }                                                             \
private:                                                        \
TYPE inner_get(Identity<TYPE>)                                  \
  {                                                             \
    return m##IDX##_;                                           \
  }                                                             \
void inner_set(TYPE v)                                          \
  {                                                             \
    m##IDX##_ = v;                                              \
  }                                                             \
TYPE m##IDX##_;                                                 \
static new_m##IDX##_func_name new_m##IDX##_func;                \
static init_m##IDX##_func_name init_m##IDX##_func;              \
static start_m##IDX##_func_name start_m##IDX##_func;            \
static stop_m##IDX##_func_name stop_m##IDX##_func;              \
static wait_m##IDX##_func_name wait_m##IDX##_func;              \
static destroy_m##IDX##_func_name destroy_m##IDX##_func;

  LST_DO2(MEMBER, (), MTL_MEMBERS);

protected:
  virtual int unlock(common::ObLDHandle &handle)
  {
    UNUSED(handle);
    return OB_SUCCESS;
  }

protected:
  // tenant id
  uint64_t id_;
  bool inited_;
  bool created_;
  share::ObTenantModuleInitCtx *mtl_init_ctx_;
  share::ObTenantRole::Role tenant_role_value_;

private:
  common::hash::ObHashSet<int64_t> tg_set_;
  // tenant thread dynamic follow unit config
  typedef common::hash::ObHashMap<ThreadDynamicNode, double> ThreadDynamicFactorMap;
  ThreadDynamicFactorMap thread_dynamic_factor_map_;

  ObCgroupCtrl *cgroups_;
  bool enable_tenant_ctx_check_;
  int64_t thread_count_;
  int64_t memory_size_;
  bool mini_mode_;
};

using ReleaseCbFunc = std::function<int (common::ObLDHandle&)>;
extern int get_tenant_base_with_lock(uint64_t tenant_id, ObLDHandle &handle, ObTenantBase *&ctx, ReleaseCbFunc &release_cb);

class ObTenantEnv
{
public:
  static void set_tenant(ObTenantBase *ctx);
  static inline ObTenantBase *&get_tenant()
  {
#ifdef ENABLE_INITIAL_EXEC_TLS_MODEL
    static thread_local ObTenantBase* __attribute__((tls_model("initial-exec"))) ctx = nullptr;
#else
    static thread_local ObTenantBase* __attribute__((tls_model("local-dynamic"))) ctx = nullptr;
#endif
    return ctx;
  }
  static inline ObTenantBase *get_tenant_local()
  {
#ifdef ENABLE_INITIAL_EXEC_TLS_MODEL
    static thread_local ObTenantBase __attribute__((tls_model("initial-exec"))) ctx(OB_INVALID_TENANT_ID);
#else
    static thread_local ObTenantBase __attribute__((tls_model("local-dynamic"))) ctx(OB_INVALID_TENANT_ID);
#endif
    return &ctx;
  }
  template<class T>
  static inline T mtl()
  {
    return get_tenant_local()->get<T>();
  }
  template<class T>
  static inline T mtl(uint64_t tenant_id)
  {
    T obj = T();
    if (tenant_id == MTL_ID()) {
      obj = get_tenant_local()->get<T>();
    }
    return obj;
  }
};

class ObTenantSwitchGuard
{
friend class omt::ObTenant;
friend class storage::MockTenantModuleEnv;

friend ObTenantSwitchGuard _make_tenant_switch_guard();
private:
  ObTenantSwitchGuard() { reset(); }
public:
  ObTenantSwitchGuard(ObTenantBase *ctx);
  // just for make guard
  ObTenantSwitchGuard(const ObTenantSwitchGuard &other) {
    UNUSED(other);
    reset();
  }
  ~ObTenantSwitchGuard()
  {
    release();
  }
  int switch_to(uint64_t tenant_id, bool need_check_allow = true);
  int switch_to(ObTenantBase *ctx);
  void release();
  void reset()
  {
    loop_num_ = 0;
    on_switch_ = false;
    stash_tenant_ = nullptr;
    release_cb_ = nullptr;
  }
  // for MTL_SWITCH
  int loop_num_;
private:
  bool on_switch_;
  ObTenantBase *stash_tenant_;
  common::ObLDHandle lock_handle_;
  ReleaseCbFunc release_cb_;
  lib::ObTLTaGuard ta_guard_;
};

inline ObTenantSwitchGuard _make_tenant_switch_guard()
{
  static ObTenantSwitchGuard _guard;
  return _guard;
}

#define MAKE_TENANT_SWITCH_SCOPE_GUARD(guard) \
  share::ObTenantSwitchGuard guard = share::_make_tenant_switch_guard()

#define MTL_SWITCH(tenant_id) \
  for (share::ObTenantSwitchGuard g = share::_make_tenant_switch_guard(); g.loop_num_ == 0; g.loop_num_++) \
    if (OB_SUCC(g.switch_to(tenant_id)))

  inline void *mtl_malloc(int64_t nbyte, const common::ObMemAttr &attr)
  {
    common::ObMemAttr inner_attr = attr;
    if (OB_SERVER_TENANT_ID == inner_attr.tenant_id_ &&
        nullptr != MTL_CTX()) {
      inner_attr.tenant_id_ = MTL_ID();
    }
    return ob_malloc(nbyte, inner_attr);
  }

  inline void *mtl_malloc(int64_t nbyte, const lib::ObLabel &label)
  {
    common::ObMemAttr attr;
    attr.label_ = label;
    return mtl_malloc(nbyte, attr);
  }

  inline void mtl_free(void *ptr)
  {
    return ob_free(ptr);
  }

  inline void *mtl_malloc_align(int64_t alignment, int64_t nbyte, const common::ObMemAttr &attr)
  {
    common::ObMemAttr inner_attr = attr;
    if (OB_SERVER_TENANT_ID == inner_attr.tenant_id_ &&
        nullptr != MTL_CTX()) {
      inner_attr.tenant_id_ = MTL_ID();
    }
    return ob_malloc_align(alignment, nbyte, inner_attr);
  }

  inline void *mtl_malloc_align(int64_t alignment , int64_t byte, const lib::ObLabel &label)
  {
    common::ObMemAttr attr;
    attr.label_ = label;
    return mtl_malloc_align(alignment, byte, attr);
  }

  inline void mtl_free_align(void *ptr)
  {
    return ob_free_align(ptr);
  }

  #define MTL_NEW(T, label, ...)                                \
  ({                                                            \
    T* ret = NULL;                                              \
    void *buf = oceanbase::share::mtl_malloc(sizeof(T), label); \
    if (OB_NOT_NULL(buf))                                       \
    {                                                           \
      ret = new(buf) T(__VA_ARGS__);                            \
    }                                                           \
    ret;                                                        \
  })

  #define MTL_DELETE(T, label, ptr)               \
    do{                                           \
      if (NULL != ptr)                            \
      {                                           \
        ptr->~T();                                \
        oceanbase::share::mtl_free(ptr);          \
        ptr = NULL;                               \
      }                                           \
    } while(0)


#define mtl_sop_borrow(type) MTL(common::ObServerObjectPool<type>*)->borrow_object()
#define mtl_sop_return(type, ptr) MTL(common::ObServerObjectPool<type>*)->return_object(ptr)

} // end of namespace share

} // end of namespace oceanbase


#endif // OB_TENANT_BASE_H_
