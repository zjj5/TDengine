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

#define VND_CFG_FILENAME     "config.json"
#define VND_CFG_FILENAME_TMP "config.json_tmp"

static char *vnodeCfgToStr(SVnodeCfg *pCfg);
static int   vnodeStrToCfg(char *pStr, SVnodeCfg *pCfg);

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
  TdFilePtr pFile;
  char      cname[TSDB_FILENAME_LEN];
  char      tname[TSDB_FILENAME_LEN];
  char     *str;

  snprintf(cname, TSDB_FILENAME_LEN, "%s%s%s", path, TD_DIRSEP, VND_CFG_FILENAME);
  snprintf(tname, TSDB_FILENAME_LEN, "%s%s%s", path, TD_DIRSEP, VND_CFG_FILENAME_TMP);

  str = vnodeCfgToStr(pCfg);

  pFile = taosOpenFile(tname, TD_FILE_CTEATE | TD_FILE_WRITE);

  taosWriteFile(pFile, str, strlen(str));

  taosCloseFile(&pFile);
  taosRenameFile(tname, cname);

  taosMemoryFree(str);

  return 0;
}

int vnodeLoadCfg(const char *path, SVnodeCfg *pCfg) {
  TdFilePtr pFile;
  char      cname[TSDB_FILENAME_LEN];
  char      str[2048];

  snprintf(cname, TSDB_FILENAME_LEN, "%s%s%s", path, TD_DIRSEP, VND_CFG_FILENAME);

  pFile = taosOpenFile(cname, TD_FILE_READ);
  if (pFile == NULL) {
    // TODO
    ASSERT(0);
    return -1;
  }

  taosReadFile(pFile, str, 2048);
  vnodeStrToCfg(str, pCfg);

  taosCloseFile(&pFile);

  return 0;
}

static char *vnodeCfgToStr(SVnodeCfg *pCfg) {
  SJson *pJson;
  char  *pStr;

  pJson = tjsonCreateObject();

  tjsonAddIntegerToObject(pJson, "vgId", pCfg->vgId);
  tjsonAddIntegerToObject(pJson, "flag", pCfg->flag);
  // vnd
  tjsonAddIntegerToObject(pJson, "isHeap", pCfg->isHeap);
  tjsonAddIntegerToObject(pJson, "szBuf", pCfg->szBuf);
  // wal
  tjsonAddIntegerToObject(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod);
  tjsonAddIntegerToObject(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod);
  tjsonAddIntegerToObject(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod);
  tjsonAddIntegerToObject(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize);
  tjsonAddIntegerToObject(pJson, "wal.segSize", pCfg->walCfg.segSize);
  tjsonAddIntegerToObject(pJson, "wal.level", pCfg->walCfg.level);
  // meta
  tjsonAddIntegerToObject(pJson, "szPage", pCfg->szPage);
  tjsonAddIntegerToObject(pJson, "szCache", pCfg->szCache);

  // tq

  // tsdb
  tjsonAddIntegerToObject(pJson, "precision", pCfg->precision);
  tjsonAddIntegerToObject(pJson, "compress", pCfg->compress);
  tjsonAddIntegerToObject(pJson, "minutesPerFile", pCfg->minutes);
  tjsonAddIntegerToObject(pJson, "minRows", pCfg->minRows);
  tjsonAddIntegerToObject(pJson, "maxRows", pCfg->maxRows);
  tjsonAddIntegerToObject(pJson, "keep0", pCfg->keep0);
  tjsonAddIntegerToObject(pJson, "keep1", pCfg->keep1);
  tjsonAddIntegerToObject(pJson, "keep2", pCfg->keep2);

  pStr = tjsonToString(pJson);

  tjsonDelete(pJson);

  return pStr;
}

static int vnodeStrToCfg(char *pStr, SVnodeCfg *pCfg) {
  SJson *pJson;

  pJson = tjsonParse(pStr);

  tjsonGetNumberValue(pJson, "vgId", pCfg->vgId);
  tjsonGetNumberValue(pJson, "vgId", pCfg->vgId);
  tjsonGetNumberValue(pJson, "flag", pCfg->flag);
  // vnd
  tjsonGetNumberValue(pJson, "isHeap", pCfg->isHeap);
  tjsonGetNumberValue(pJson, "szBuf", pCfg->szBuf);
  // wal
  tjsonGetNumberValue(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod);
  tjsonGetNumberValue(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod);
  tjsonGetNumberValue(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod);
  tjsonGetNumberValue(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize);
  tjsonGetNumberValue(pJson, "wal.segSize", pCfg->walCfg.segSize);
  tjsonGetNumberValue(pJson, "wal.level", pCfg->walCfg.level);
  // meta
  tjsonGetNumberValue(pJson, "szPage", pCfg->szPage);
  tjsonGetNumberValue(pJson, "szCache", pCfg->szCache);

  // tq

  // tsdb
  tjsonGetNumberValue(pJson, "precision", pCfg->precision);
  tjsonGetNumberValue(pJson, "compress", pCfg->compress);
  tjsonGetNumberValue(pJson, "minutesPerFile", pCfg->minutes);
  tjsonGetNumberValue(pJson, "minRows", pCfg->minRows);
  tjsonGetNumberValue(pJson, "maxRows", pCfg->maxRows);
  tjsonGetNumberValue(pJson, "keep0", pCfg->keep0);
  tjsonGetNumberValue(pJson, "keep1", pCfg->keep1);
  tjsonGetNumberValue(pJson, "keep2", pCfg->keep2);

  tjsonDelete(pJson);

  pCfg->walCfg.vgId = pCfg->vgId;

  return 0;
}