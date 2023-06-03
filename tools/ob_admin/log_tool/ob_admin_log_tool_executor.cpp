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

#define USING_LOG_PREFIX CLOG
#include <getopt.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_admin_log_tool_executor.h"
#include "dump/ob_admin_dump_block.h"
#include "cmd_args_parser.h"


namespace oceanbase
{
using namespace share;
namespace tools
{

ObAdminLogExecutor::~ObAdminLogExecutor()
{
  if (NULL != mutator_str_buf_) {
    ob_free(mutator_str_buf_);
    mutator_str_buf_ = NULL;
    mutator_buf_size_ = 0;
  }
}

int ObAdminLogExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 4) || OB_ISNULL(argv)) {
    print_usage();
    ret = OB_INVALID_ARGUMENT;
  } else {
    int new_argc = argc - 1;
    char **new_argv = argv + 1;
    if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_log) : OB_NEED_RETRY)) {
      LOG_INFO("finish dump_log", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_meta) : OB_NEED_RETRY)) {
      LOG_INFO("finsh dump_meta", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_tx_format) : OB_NEED_RETRY)) {
      LOG_INFO("finsh dump_tx_format ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_filter) : OB_NEED_RETRY)) {
      LOG_INFO("finsh dump_filter", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, stat) : OB_NEED_RETRY)) {
      LOG_INFO("finsh stat", K(ret));
    } else {
      fprintf(stderr, "failed %d", ret);
      print_usage();
    }
  }
  return ret;
}

void ObAdminLogExecutor::print_usage()
{
  fprintf(stdout,
          "Usages:\n"
          "$ob_admin log_tool dump_log log_files ## ./ob_admin log_tool dump_log 1 2 3 ##将log文件中的内容全部打印出来\n"
          "$ob_admin log_tool dump_tx_format log_files ## ./ob_admin log_tool dump_tx_format 1 2 3 ##将log文件中的事务相关内容以json格式打印\n"
          "$ob_admin log_tool dump_filter 'filter_conditions' log_files ## ./ob_admin log_tool dump_filter 'tx_id=xxxx;tablet_id=xxx' 1 2 3"
          "##按照过滤条件将log文件中的事务相关内容打印,目前支持按照事务id(tx_id=xxxx),tablet_id(tablet_id=xxxx)进行过滤，多个条件之间以;隔开\n"
          "$ob_admin log_tool stat log_files ## ./ob_admin log_tool stat 1\n"
          "如何通过LSN快速定位日志:\n"
          "1. 获取文件ID: BLOCK_ID=LSN/(64MB-4KB)\n"
          "2. 根据LSN去输出文件中执行grep操作"
         );
}

int ObAdminLogExecutor::dump_log(int argc, char **argv)
{
  return dump_all_blocks_(argc, argv, LogFormatFlag::NO_FORMAT);
}

int ObAdminLogExecutor::dump_meta(int argc, char **argv)
{
  return dump_all_blocks_(argc, argv, share::LogFormatFlag::META_FORMAT);
}

int ObAdminLogExecutor::dump_tx_format(int argc,char ** argv)
{
  int ret = OB_SUCCESS;
  ret = dump_all_blocks_(argc, argv, LogFormatFlag::TX_FORMAT);
  return ret;
}

int ObAdminLogExecutor::dump_filter(int argc,char ** argv)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(filter_.parse(argv[0]))) {
    LOG_WARN("parse filter failed", K(ret), K(argv[0]));
  } else {
    LOG_INFO("dump with filter", K_(filter), K(argv[0]));
    if (OB_FAIL(dump_all_blocks_(argc - 1 , argv + 1, LogFormatFlag::FILTER_FORMAT))) {
      LOG_WARN("failed to dump filter", K(ret), K(argv[0]));
    }
  }

  return ret;
}

int ObAdminLogExecutor::stat(int argc,char ** argv)
{
  int ret = OB_SUCCESS;
  ret = dump_all_blocks_(argc, argv, LogFormatFlag::STAT_FORMAT);
  return ret;
}

int ObAdminLogExecutor::dump_all_blocks_(int argc, char **argv, LogFormatFlag flag)
{
  int ret = OB_SUCCESS;
  ObAdminMutatorStringArg str_arg;
  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argvments", K(argc), K(ret));
  } else if (OB_FAIL(alloc_mutator_string_buf_())) {
    LOG_WARN("alloc mutator string buf failed", K(ret));
  } else {
    str_arg.flag_ = flag;
    str_arg.buf_ = mutator_str_buf_;
    str_arg.buf_len_ = mutator_buf_size_;
    str_arg.pos_ = 0;
    str_arg.filter_ = filter_;
    for (int i = 0; i < argc && OB_SUCC(ret); i++) {
      if (OB_FAIL(dump_single_block_(argv[i], str_arg))) {
        LOG_WARN("failed to dump block", K(argv[i]), K(ret));
      } else {
        LOG_INFO("dump_single_block_ success", K(argv[i]));
      }
    }
  }
  return ret;
}

int ObAdminLogExecutor::dump_single_block_(const char *block_path,
                                           ObAdminMutatorStringArg &str_arg)
{
  int ret = OB_SUCCESS;
  ObLogStat log_stat;
  str_arg.log_stat_ = &log_stat;
  if (share::LogFormatFlag::META_FORMAT == str_arg.flag_) {
    ret = dump_single_meta_block_(block_path, str_arg);
  } else {
    ret = dump_single_log_block_(block_path, str_arg);
  }
  return ret;
}

int ObAdminLogExecutor::dump_single_log_block_(const char *block_path,
                                               ObAdminMutatorStringArg &str_arg)
{
  int ret = OB_SUCCESS;
  ObAdminDumpBlock dump_block(block_path, str_arg);
  if (OB_FAIL(dump_block.dump())) {
    LOG_WARN("ObAdminDumpBlock dump failed", K(ret), K(block_path));
  } else if (LogFormatFlag::STAT_FORMAT == str_arg.flag_) {
    fprintf(stdout, "LOG_STAT : %s, total_size:%ld\n", to_cstring(*(str_arg.log_stat_)), str_arg.log_stat_->total_size());
  }
  return ret;
}

int ObAdminLogExecutor::dump_single_meta_block_(const char *block_path,
                                                ObAdminMutatorStringArg &str_arg)
{
  int ret = OB_SUCCESS;
  ObAdminDumpMetaBlock dump_block(block_path, str_arg);
  if (OB_FAIL(dump_block.dump())) {
    LOG_WARN("ObAdminDumpBlock dump failed", K(ret), K(block_path));
  }
  return ret;
}

int ObAdminLogExecutor::alloc_mutator_string_buf_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mutator_str_buf_)) {
    mutator_str_buf_ = static_cast<char *>(ob_malloc(MAX_TX_LOG_STRING_SIZE, "AdminDumpLog"));
    mutator_buf_size_ = MAX_TX_LOG_STRING_SIZE;
  }

  if (OB_ISNULL(mutator_str_buf_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    mutator_buf_size_ = 0;
  }

  return ret;
}

}
}
