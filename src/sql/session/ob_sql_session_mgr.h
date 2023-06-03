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

#ifndef _OCEABASE_SQL_SESSION_OB_SQL_SESSION_MGR_H_
#define _OCEABASE_SQL_SESSION_OB_SQL_SESSION_MGR_H_

#include "lib/hash/ob_concurrent_hash_map.h"
#include "lib/container/ob_concurrent_bitset.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_end_trans_callback.h"
namespace oceanbase
{
namespace observer
{
struct ObSMConnection;
}
namespace sql
{

class ObFreeSessionCtx
{
public:
  ObFreeSessionCtx() :
    has_inc_active_num_(false),
    tenant_id_(common::OB_INVALID_ID),
    sessid_(0),
    proxy_sessid_(0)
  {
  }
  ~ObFreeSessionCtx() {}
  VIRTUAL_TO_STRING_KV(K_(has_inc_active_num), K_(tenant_id), K_(sessid), K_(proxy_sessid));
  bool has_inc_active_num_;
  uint64_t tenant_id_;
  uint32_t sessid_;
  uint64_t proxy_sessid_;
};

class ObSQLSessionMgr : public common::ObTimerTask
{
public:
  static const int64_t SCHEDULE_PERIOD = 1000*1000*5; //5s
  static const uint32_t NON_DURABLE_VALUE = 0;
  static const uint32_t MAX_VERSION = UINT8_MAX;//255
  static const uint32_t SERVER_SESSID_TAG = 1ULL << 31;
  static const uint32_t LOCAL_SESSID_TAG = 0; // used for sessions overflow
  static const int64_t BUCKET_COUNT = 1024;
  typedef SessionInfoKey Key;
  typedef common::hash::ObHashMap<uint64_t, ObSQLSessionInfo*> SessionMap;
  explicit ObSQLSessionMgr():
      //null_callback_(),
      sessinfo_map_(),
      sessid_sequence_(),
      first_seq_(NON_DURABLE_VALUE),
      increment_sessid_(NON_DURABLE_VALUE)
  {
  }
  virtual ~ObSQLSessionMgr(){}

  int init();

  /**
   * @brief create a new ObSQLSessionInfo, and its session id is sessid
   * @param conn : connection information
   * @param sess_info : point to the ObSQLSessionInfo that the function created; used as value
   */
  void destroy();
  int create_session(observer::ObSMConnection *conn, ObSQLSessionInfo *&sess_info);
  // create session by session id and proxy session id.
  // need call revert_session if return success.
  int create_session(const uint64_t tenant_id, const uint32_t sessid, const uint64_t proxy_sessid,
      const int64_t create_time, ObSQLSessionInfo *&session_info);

  /**
   * @brief get the ObSQLSessioninfo
   * @param sessid : session id; used as key
   * @param sess_info : point to the ObSQLSessionInfo that the function get; used as value
   */
  int get_session(uint32_t sessid, ObSQLSessionInfo *&sess_info);
  int inc_session_ref(const ObSQLSessionInfo *my_session);
  int free_session(const ObFreeSessionCtx &ctx);

  int get_session_count(int64_t &sess_cnt);

  /**
   * @brief if you create or get session successfully, you must call
   *        this function after using session
   *
   * @param sess_info : the session that you want to revert
   */
  void revert_session(ObSQLSessionInfo *sess_info);

  /**
   * @brief use the function to traverse all session
   * @param fn : it can be a pointer of the function or function object
   */
  template <typename Function>
  int for_each_session(Function &fn);

  template <typename Function>
  int for_each_hold_session(Function &fn);

  int kill_query(ObSQLSessionInfo &session);
  int set_query_deadlocked(ObSQLSessionInfo &session);
  static int kill_query(ObSQLSessionInfo &session,
      const ObSQLSessionState status);
  int kill_idle_timeout_tx(ObSQLSessionInfo *session);
  int kill_deadlock_tx(ObSQLSessionInfo *session);
  int kill_session(ObSQLSessionInfo &session);
  int disconnect_session(ObSQLSessionInfo &session);

  // kill all sessions from this tenant.
  int kill_tenant(const uint64_t tenant_id);

  /**
   * @brief timing clean time out session
   */
  virtual void runTimerTask();
  void try_check_session();

  // get min active snapshot version for all session
  int get_min_active_snapshot_version(share::SCN &snapshot_version);

  //used for guarantee the unique sessid when observer generates sessid
  static uint64_t extract_server_id(uint32_t sessid);
  static bool is_server_sessid(uint32_t sessid) {return SERVER_SESSID_TAG & sessid;}
  static int is_need_clear_sessid(const observer::ObSMConnection *conn, bool &is_need);
  int fetch_first_sessid();
  //  in_mgr = false means fail back to local session allocating, avoid remote/distribute executing fail
  int create_sessid(uint32_t &sessid, bool in_mgr = true);
  int mark_sessid_used(uint32_t sess_id);
  int mark_sessid_unused(uint32_t sess_id);
  //inline ObNullEndTransCallback &get_null_callback() { return null_callback_; }
  SessionMap &get_sess_hold_map() { return sess_hold_map_; }
private:
  int check_session_leak();
  int get_avaiable_local_seq(uint32_t &local_seq);
  int set_first_seq(int64_t first_seq);

  class SessionPool
  {
  public:
    SessionPool();
  public:
    int init();
    int pop_session(uint64_t tenant_id, ObSQLSessionInfo *&session);
    int push_session(uint64_t tenant_id, ObSQLSessionInfo *&session);
    int64_t count() const;
    TO_STRING_KV(K(session_pool_.capacity()),
                 K(session_pool_.get_total()),
                 K(session_pool_.get_free()));
  private:
    static const int64_t POOL_CAPACIPY = 512;
    static const uint64_t POOL_INITED = 0;
    common::ObFixedQueue<ObSQLSessionInfo> session_pool_;
    ObSQLSessionInfo *session_array[POOL_CAPACIPY];
  };

  class SessionPoolMap
  {
  public:
    SessionPoolMap(ObIAllocator &alloc)
      : pool_blocks_(ObWrapperAllocator(alloc)),
        alloc_(alloc),
        lock_(common::ObLatchIds::SESSION_POOL_MAP_LOCK)
    {}
    int get_session_pool(uint64_t tenant_id, SessionPool *&session_pool);
  private:
    static const uint64_t BLOCK_ID_SHIFT = 10;
    static const uint64_t SLOT_ID_MASK = 0x3FF;
    static const uint64_t SLOT_PER_BLOCK = 1ULL << BLOCK_ID_SHIFT;
    typedef SessionPool *SessionPoolBlock[SLOT_PER_BLOCK];
    typedef Ob2DArray<SessionPoolBlock *, 1024, ObWrapperAllocator> SessionPoolBlocks;
  private:
    int create_pool_block(uint64_t block_id, SessionPoolBlock *&pool_block);
    int create_session_pool(SessionPoolBlock &pool_block, uint64_t slot_id, SessionPool *&session_pool);
    uint64_t get_block_id(uint64_t tenant_id) const;
    uint64_t get_slot_id(uint64_t tenant_id) const;
  private:
    SessionPoolBlocks pool_blocks_;
    ObIAllocator &alloc_;
    ObSpinLock lock_;
  };

  class ValueAlloc
  {
  public:
    ValueAlloc()
      : allocator_(SET_USE_500("SessMap")),
        session_pool_map_(allocator_),
        alloc_total_count_(0),
        alloc_from_pool_count_(0),
        free_total_count_(0),
        free_to_pool_count_(0)
    {}
    ~ValueAlloc() {}
    int clean_tenant(uint64_t tenant_id);
    ObSQLSessionInfo* alloc_value(uint64_t tenant_id);
    void free_value(ObSQLSessionInfo *sess);
    SessionInfoHashNode* alloc_node(ObSQLSessionInfo* value)
    {
      UNUSED(value);
      return op_alloc(SessionInfoHashNode);
    }
    void free_node(SessionInfoHashNode* node)
    {
      if (NULL != node) {
        op_free(node);
        node = NULL;
      }
    }
  private:
    bool is_valid_tenant_id(uint64_t tenant_id) const;
  private:
    ObArenaAllocator allocator_;
    SessionPoolMap session_pool_map_;
    volatile int64_t alloc_total_count_;
    volatile int64_t alloc_from_pool_count_;
    volatile int64_t free_total_count_;
    volatile int64_t free_to_pool_count_;
    static const int64_t MAX_REUSE_COUNT = 10000;
    static const int64_t MAX_SYS_VAR_MEM = 256 * 1024;
  };

#ifdef OB_USE_ASAN
  typedef common::ObTenantLinkHashMap<Key, ObSQLSessionInfo, ValueAlloc, ZeroRefHandle> HashMap;
#else
  typedef common::ObTenantLinkHashMap<Key, ObSQLSessionInfo, ValueAlloc> HashMap;
#endif

  struct DumpHoldSession
  {
    DumpHoldSession() {}
    int operator()(common::hash::HashMapPair<uint64_t, ObSQLSessionInfo *> &entry);
  };

  class CheckSessionFunctor
  {
  public:
    CheckSessionFunctor() : sess_mgr_(NULL) {}
    explicit CheckSessionFunctor(ObSQLSessionMgr *sess_mgr): sess_mgr_(sess_mgr) {}
    virtual ~CheckSessionFunctor(){}
    bool operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info);
  private:
    ObSQLSessionMgr *sess_mgr_;
  };

  class KillTenant
  {
  public:
    KillTenant(ObSQLSessionMgr *mgr, const uint64_t tenant_id)
        : mgr_(mgr), tenant_id_(tenant_id)
    {}
    bool operator() (sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo* sess_info);
  private:
    ObSQLSessionMgr *mgr_;
    const uint64_t tenant_id_;
  };

private:
  //注:必须在session_map_之前定义，依赖于析构的顺序。
  //ObNullEndTransCallback null_callback_;
  // used for manage ObSQLSessionInfo
  HashMap sessinfo_map_;
  // design doc:
  // |<---------------------------------32bit---------------------------->|
  // 31b 30b   29b                18b  16b                              0b
  // +----+------------------------------+--------------------------------+
  // |Mask|resvd|    Server Id     |    Local Seq = 16 + 2                |
  // +----+------------------------------+--------------------------------+
  static const uint16_t LOCAL_SEQ_LEN = 18;
  static const uint16_t RESERVED_SERVER_ID_LEN = 1;
  static const uint16_t SERVER_ID_LEN = 13 - RESERVED_SERVER_ID_LEN;
  static const uint32_t MAX_LOCAL_SEQ = (1ULL << LOCAL_SEQ_LEN) - 1;
  static const uint64_t MAX_SERVER_ID = (1ULL << SERVER_ID_LEN) - 1;
  common::ObFixedQueue<void> sessid_sequence_;
  uint32_t first_seq_;
  uint32_t increment_sessid_;
  SessionMap sess_hold_map_;
  DISALLOW_COPY_AND_ASSIGN(ObSQLSessionMgr);
}; // end of class ObSQLSessionMgr

template <typename Function>
int ObSQLSessionMgr::for_each_session(Function &fn)
{
  return sessinfo_map_.for_each(fn);
}

template <typename Function>
int ObSQLSessionMgr::for_each_hold_session(Function &fn)
{
  return get_sess_hold_map().foreach_refactored(fn);
}

inline int ObSQLSessionMgr::get_session(uint32_t sessid, ObSQLSessionInfo *&sess_info)
{
  int ret = sessinfo_map_.get(Key(sessid), sess_info);
  const bool v = GCONF._enable_trace_session_leak;
  if (OB_UNLIKELY(v)) {
    if (OB_LIKELY(sess_info)) {
      sess_info->on_get_session();
    }
  }
  return ret;
}

inline void ObSQLSessionMgr::revert_session(ObSQLSessionInfo *sess_info)
{
  const bool v = GCONF._enable_trace_session_leak;
  if (OB_UNLIKELY(v)) {
    if (OB_LIKELY(nullptr != sess_info)) {
      sess_info->on_revert_session();
    }
  }
  sessinfo_map_.revert(sess_info);
}

inline int ObSQLSessionMgr::get_session_count(int64_t &sess_cnt)
{
  sess_cnt = sessinfo_map_.count();
  return 0;
}


class ObSessionGetterGuard
{
public:
  explicit ObSessionGetterGuard(ObSQLSessionMgr &sess_mgr, uint32_t sessid);
  ~ObSessionGetterGuard();
  inline int get_session(ObSQLSessionInfo *&session)
  {
    session = session_;
    return ret_;
  }
private:
  int ret_;
  ObSQLSessionMgr &mgr_;
  ObSQLSessionInfo *session_;
};

} // end of namespace sql
} // end of namespace oceanbase

#endif /* _OCEABASE_SQL_SESSION_OB_SQL_SESSION_MGR_H_ */
