#ifndef OCEANBASE_SQL_ENGINE_LINK_OP_H_
#define OCEANBASE_SQL_ENGINE_LINK_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/ob_sql_utils.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/dblink/ob_dblink_utils.h"
#include "lib/string/ob_hex_utils_base.h"

namespace oceanbase
{
namespace sql
{

class ObLinkSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObLinkSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);


  int set_param_infos(const common::ObIArray<ObParamPosIdx> &param_infos);
  int set_stmt_fmt(const char *stmt_fmt_buf, int64_t stmt_fmt_len);

  common::ObIAllocator &allocator_;
  common::ObFixedArray<ObParamPosIdx, common::ObIAllocator> param_infos_;
  common::ObString stmt_fmt_;
  char *stmt_fmt_buf_;
  int64_t stmt_fmt_len_;
  uint64_t dblink_id_;
  bool is_reverse_link_;
};

class ObLinkOp : public ObOperator
{
public:
  static constexpr int64_t CHECK_STATUS_ROWS_INTERVAL =  1 << 10;
  typedef common::ParamStore ObParamStore;
  explicit ObLinkOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObLinkOp() { destroy(); }
  virtual void destroy() { reset(); }

  inline void set_link_driver_proto(common::sqlclient::DblinkDriverProto type) { link_type_ = type; }
  virtual void reset() = 0;
  int init_dblink(uint64_t dblink_id, common::ObDbLinkProxy *dblink_proxy, bool in_xa_trascaction = false);
  int execute_link_stmt(const common::ObString &link_stmt_fmt,
            const common::ObIArray<ObParamPosIdx> &param_infos,
            const ObParamStore &param_store,
            ObReverseLink *reverse_link = NULL);
  virtual int inner_execute_link_stmt(const char *link_stmt) = 0;
  static int init_dblink_param_ctx(ObExecContext &exec_ctx, common::sqlclient::dblink_param_ctx &param_ctx);
  static int get_charset_id(ObExecContext &exec_ctx, uint16_t &charset_id, uint16_t &ncharset_id);
protected:
  int combine_link_stmt(const common::ObString &link_stmt_fmt,
                        const common::ObIArray<ObParamPosIdx> &param_infos,
                        const ObParamStore &param_store,
                        ObReverseLink *reverse_link = NULL);
  int extend_stmt_buf(int64_t need_size = 0);
  virtual void reset_dblink() = 0;
  void reset_link_sql();
  int set_next_sql_req_level();
protected:
  uint64_t tenant_id_;
  uint64_t dblink_id_;
  uint32_t sessid_;
  const ObDbLinkSchema *dblink_schema_;
  common::ObDbLinkProxy *dblink_proxy_;
  common::sqlclient::ObISQLConnection *dblink_conn_;
  common::ObIAllocator &allocator_;
  char *stmt_buf_;
  int64_t stmt_buf_len_;
  int64_t next_sql_req_level_;
  static const int64_t STMT_BUF_BLOCK;
  common::sqlclient::DblinkDriverProto link_type_;
  bool in_xa_trascaction_; // is dblink write/read remote database in xa trasaction
  static const char * head_comment_fmt_;
  static const int64_t head_comment_length_;
  static const char *proxy_route_info_fmt_;
  static const int64_t proxy_route_info_fmt_length_;
  static const int64_t proxy_route_ip_port_size_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_LINK_OP_ */
