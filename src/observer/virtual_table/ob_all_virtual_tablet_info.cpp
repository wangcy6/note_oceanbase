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

#include "observer/ob_server.h"
#include "observer/virtual_table/ob_all_virtual_tablet_info.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"
<<<<<<< HEAD
=======
#include "storage/tablet/ob_tablet.h"
>>>>>>> 529367cd9b5b9b1ee0672ddeef2a9930fe7b95fe

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace observer
{

ObAllVirtualTabletInfo::ObAllVirtualTabletInfo()
    : ObVirtualTableScannerIterator(),
      addr_(),
      ls_id_(share::ObLSID::INVALID_LS_ID),
      ls_iter_guard_(),
      ls_tablet_iter_()
{
}

ObAllVirtualTabletInfo::~ObAllVirtualTabletInfo()
{
  reset();
}

void ObAllVirtualTabletInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_tablet_iter_.reset();
  ls_iter_guard_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualTabletInfo::release_last_tenant()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  ls_iter_guard_.reset();
  ls_tablet_iter_.reset();
}

int ObAllVirtualTabletInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualTabletInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

int ObAllVirtualTabletInfo::get_next_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (!ls_iter_guard_.get_ptr()
        || OB_FAIL(ls_iter_guard_->get_next(ls))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to switch tenant", K(ret));
      }
      // switch to next tenant
      ret = OB_ITER_END;
    } else if (OB_ISNULL(ls)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "ls is null", K(ret));
    } else {
      ls_id_ = ls->get_ls_id().id();
      break;
    }
  }

  return ret;
}

int ObAllVirtualTabletInfo::get_next_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_tablet_iter_.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next tablet", K(ret));
      }
      ret = OB_SUCCESS; // continue to next ls
      ObLS *ls = nullptr;
      if (OB_FAIL(get_next_ls(ls))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next ls", K(ret));
        }
      } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(ls_tablet_iter_))) {
        SERVER_LOG(WARN, "fail to get tablet iter", K(ret));
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObAllVirtualTabletInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (ls_iter_guard_.get_ptr() == nullptr && OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter_guard_, ObLSGetMod::OBSERVER_MOD))) {
    SERVER_LOG(WARN, "get_ls_iter fail", K(ret));
  } else if (OB_FAIL(get_next_tablet(tablet_handle))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get_next_tablet failed", K(ret));
    }
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tablet should not null", K(ret), K(tablet_handle));
  } else {
    const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // ls_id
          cur_row_.cells_[i].set_int(ls_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // tablet_id
          cur_row_.cells_[i].set_int(tablet_meta.tablet_id_.id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // data_tablet_id
          cur_row_.cells_[i].set_int(tablet_meta.data_tablet_id_.id());
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // ref_tablet_id
          cur_row_.cells_[i].set_int(0);
          break;
          //TODO:SCN
        case OB_APP_MIN_COLUMN_ID + 7:
          // checkpoint_ts
          cur_row_.cells_[i].set_uint64(tablet_meta.clog_checkpoint_scn_.get_val_for_inner_table_field());
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // snapshot_version
          cur_row_.cells_[i].set_uint64(tablet_meta.snapshot_version_);
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // multi_version_start
          cur_row_.cells_[i].set_uint64(tablet_meta.multi_version_start_);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
