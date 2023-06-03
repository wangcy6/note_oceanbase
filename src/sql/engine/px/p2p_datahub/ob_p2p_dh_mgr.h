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
#ifndef __SQL_ENG_P2P_DH_MGR_H__
#define __SQL_ENG_P2P_DH_MGR_H__
#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_share_info.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"

namespace oceanbase
{
namespace sql
{

class ObPxSQCProxy;

class ObP2PDatahubManager
{
public:
  struct P2PMsgMergeCall
  {
    P2PMsgMergeCall(ObP2PDatahubMsgBase &db_msg) : dh_msg_(db_msg), need_free_(false) {};
    ~P2PMsgMergeCall() = default;
    int operator() (common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    int ret_;
    ObP2PDatahubMsgBase &dh_msg_;
    bool need_free_;
  };
  struct P2PRegenerateCall
  {
    P2PRegenerateCall(ObP2PDatahubMsgBase &db_msg) : ret_(OB_SUCCESS), dh_msg_(db_msg) {};
    ~P2PRegenerateCall() = default;
    int operator() (common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    int ret_;
    ObP2PDatahubMsgBase &dh_msg_;
  };
  struct P2PMsgGetCall
  {
    P2PMsgGetCall(ObP2PDatahubMsgBase *&db_msg) : dh_msg_(db_msg), ret_(OB_SUCCESS) {};
    ~P2PMsgGetCall() = default;
    void operator() (common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    ObP2PDatahubMsgBase *&dh_msg_;
    int ret_;
  };

  struct P2PMsgEraseIfCall
  {
    P2PMsgEraseIfCall() : ret_(OB_SUCCESS) {};
    ~P2PMsgEraseIfCall() = default;
    bool operator() (common::hash::HashMapPair<ObP2PDhKey, ObP2PDatahubMsgBase *> &entry);
    int ret_;
  };

public:
  ObP2PDatahubManager() : map_(), is_inited_(false),
      p2p_dh_proxy_(), p2p_dh_id_(0)
  {}
  ~ObP2PDatahubManager() { destroy(); }
  static ObP2PDatahubManager &instance();
  typedef common::hash::ObHashMap<ObP2PDhKey, ObP2PDatahubMsgBase *> MsgMap;
  int init();
  void destroy();
  int process_msg(ObP2PDatahubMsgBase &msg);
  int send_p2p_msg(
      ObP2PDatahubMsgBase &msg,
      ObPxSQCProxy &sqc_proxy);
  int send_local_p2p_msg(ObP2PDatahubMsgBase &msg);
  template<typename T>
  int alloc_msg(int64_t tenant_id, T *&msg_ptr);

  int alloc_msg(common::ObIAllocator &allocator,
                ObP2PDatahubMsgBase::ObP2PDatahubMsgType type,
                ObP2PDatahubMsgBase *&msg_ptr);
  int send_local_msg(ObP2PDatahubMsgBase *msg);
  int atomic_get_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg);
  int set_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg);
  int erase_msg(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg);
  int erase_msg_if(ObP2PDhKey &dh_key, ObP2PDatahubMsgBase *&msg, bool& is_erased, bool need_unreg_dm=true);
  MsgMap &get_map() { return map_; }
  int deep_copy_msg(ObP2PDatahubMsgBase &msg, ObP2PDatahubMsgBase *&new_msg);
  void free_msg(ObP2PDatahubMsgBase *&msg);
  obrpc::ObP2PDhRpcProxy &get_proxy() { return p2p_dh_proxy_; }
  int generate_p2p_dh_id(int64_t &p2p_dh_id);
private:
  template<typename T>
  int alloc_msg(common::ObIAllocator &allocator,
                T *&msg_ptr);
private:
  static const int64_t BUCKET_NUM = 131072; //2^17
private:
  MsgMap map_;
  bool is_inited_;
  obrpc::ObP2PDhRpcProxy p2p_dh_proxy_;
  int64_t p2p_dh_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObP2PDatahubManager);
};

#define PX_P2P_DH (::oceanbase::sql::ObP2PDatahubManager::instance())

} //end sql;
} //end oceanbase


#endif
