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

typedef int (*tslComparFn)(const void *pKey1, int kLen1, const void *pKey2, int kLen2);

typedef struct SSkipList2 SSkipList2;
typedef struct SSLNode    SSLNode;
typedef struct SSLCursor  SSLCursor;
typedef struct SSLCfg     SSLCfg;
typedef struct SSLItem    SSLItem;

// SSkipList2
int tslCreate(const SSLCfg *pCfg, SSkipList2 **ppSl);
int tslDestroy(SSkipList2 *pSl);
int tslPut(SSkipList2 *pSl, const SSLItem *pItem);
int tslGet(SSkipList2 *pSl, const void *pKey, int kLen);
int tslDel(SSkipList2 *pSl, const void *pKey, int kLen);

// SSLCursor
#define TSLC_FLG_BWD 0x1
int tslCursorOpen(SSLCursor *pSlc, SSkipList2 *pSl, int flag);
int tslCursorClose(SSLCursor *pSlc);

int tslCursorSeek(SSLCursor *pSlc, const void *pKey, int kLen, int flag);
int tslCursorPut(SSLCursor *pSlc, const SSLItem *pItem);

struct SSLItem {
  int         kLen;
  int         vLen;
  const void *pKey;
  const void *pVal;
};

struct SSLCfg {
  int8_t      maxLevel;
  int         kLen;
  int         vLen;
  tslComparFn xComparFn;
  void *(*xMalloc)(void *, int);
  void (*xFree)(void *, void *);
  void *pPool;
};

struct SSLCursor {
  int         flag;
  SSkipList2 *pSl;
  SSLItem     item;
  SSLNode    *curs[TSL_MAX_LEVEL];
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_SKILIST2_H*/