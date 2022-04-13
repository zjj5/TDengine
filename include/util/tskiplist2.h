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

#ifndef _TD_UTIL_SKILIST2_H
#define _TD_UTIL_SKILIST2_H

#include "os.h"
#include "taos.h"
#include "tarray.h"
#include "tfunctional.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSL_MAX_LEVEL 15

typedef struct SSkipList2 SSkipList2;
typedef struct SSLCursor  SSLCursor;
typedef struct SSLCfg     SSLCfg;

int tslCreate(SSLCfg *pCfg, SSkipList2 **ppSl);
int tslDestroy(SSkipList2 *pSl);
int tslPut(SSkipList2 *pSl, void *pKey, int kLen, void *pVal, int vLen);
int tslGet(SSkipList2 *pSl, void *pKey, int kLen);

struct SSLCfg {
  int8_t maxLevel;
  int    kLen;
  int    vLen;
  void *(*xMalloc)(void *, int);
  void (*xFree)(void *, void *);
  void *pPool;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SKILIST2_H*/