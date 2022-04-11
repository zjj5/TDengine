/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "vnodeInt.h"

const SVnodeCfg vnodeCfgDefault = {
    .vgId = -1,
    .flag = 0,
    // vnd
    .isHeap = 0,
    .szBuf = 96 * 1024 * 1024,  // 96M
    // wal
    .walCfg = {.level = TAOS_WAL_WRITE},
    // meta
    .szPage = 4096,
    .szCache = 256,
    // tsdb
    .precision = TSDB_TIME_PRECISION_MILLI,
    .compress = TWO_STAGE_COMP,
    .minutes = 60 * 24 * 10,  // 10 days
    .minRows = 100,
    .maxRows = 4096,
    .keep0 = 60 * 24 * 3650,  // 10 years
    .keep1 = 60 * 24 * 3650,  // 10 years
    .keep2 = 60 * 24 * 3650   // 10 years
};

void vnodeGetDefaultCfg(SVnodeCfg *pCfg) { memcpy(pCfg, &vnodeCfgDefault, sizeof(*pCfg)); }

int vnodeCheckCfg(SVnodeCfg *pCfg) {
  // vgId
  if (pCfg->vgId <= 0) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    return -1;
  }

  // flags
  if (pCfg->flag != 0 && pCfg->flag != VND_FLG_STREM_MODE) {
    terrno = TSDB_CODE_VND_INVLID_FLAG;
    return -1;
  }

  // TODO

  return 0;
}

int vnodeSaveCfg(const char *path, SVnodeCfg *pCfg) {
  // TODO
  return 0;
}

int vnodeLoadCfg(const char *path, SVnodeCfg *pCfg) {
  // TODO
  return 0;
}