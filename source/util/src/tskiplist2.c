/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "tskiplist2.h"

typedef struct SSLNode SSLNode;
struct __attribute__((__packed__)) SSLNode {
  int8_t   level;
  SSLNode *forward[];
};

#define SL_NODE_FORWARD(n, l)  (n)->forward[(l)]
#define SL_NODE_BACKWARD(n, l) (n)->forward[(n)->level + (l)]
#define SL_NODE_DATA(n)        &(n)->forward[(n)->level * 2]

struct SSkipList2 {
  uint32_t seed;
  SSLCfg  *pCfg;
};

struct SSLCursor {
  // data
};

static SSLNode *tslNodeNew(SSkipList2 *pSl, int8_t level, int size);
static void     tslNodeFree(SSkipList2 *pSl, SSLNode *pNode);
static int      tslPutVarInt(uint8_t *p, int v);
static int      tslGetVarInt(const uint8_t *p, int *v);

int tslCreate(SSLCfg *pCfg, SSkipList2 **ppSl) {
  SSkipList2 *pSl = NULL;

  *ppSl = NULL;
  if (pCfg == NULL) {
    // TODO
  }

  pSl = (SSkipList2 *)pCfg->xMalloc(pCfg->pPool, sizeof(*pSl));
  if (pSl == NULL) {
    return -1;
  }

  pSl->seed = taosRand();
  pSl->pCfg = pCfg;

  *ppSl = pSl;
  return 0;
}

int tslDestroy(SSkipList2 *pSl) {
  if (pSl && pSl->pCfg->xFree) {
    pSl->pCfg->xFree(pSl->pCfg->pPool, pSl);
  }

  return 0;
}

int tslPut(SSkipList2 *pSl, void *pKey, int kLen, void *pVal, int vLen) {
  // TODO
  return 0;
}

int tslGet(SSkipList2 *pSl, void *pKey, int kLen) {
  // TODO
  return 0;
}

static SSLNode *tslNodeNew(SSkipList2 *pSl, int8_t level, int size) {
  SSLNode *pNode;
  SSLCfg  *pCfg;
  int      tsize;

  pNode = NULL;
  pCfg = pSl->pCfg;
  tsize = sizeof(*pNode) + sizeof(SSLNode *) * level * 2 + size;

  pNode = pCfg->xMalloc(pCfg->pPool, tsize);
  if (pNode) {
    pNode->level = level;
  }

  return pNode;
}

static void tslNodeFree(SSkipList2 *pSl, SSLNode *pNode) {
  if (pSl->pCfg->xFree && pNode) {
    pSl->pCfg->xFree(pSl->pCfg->pPool, pNode);
  }
}

static int tslEncode(SSkipList2 *pSl, void *pKey, int kLen, void *pVal, int vLen, uint8_t *p) {
  int     n = 0;
  SSLCfg *pCfg = pSl->pCfg;

  ASSERT(kLen != 0);
  ASSERT(pCfg->kLen < 0 || pCfg->kLen == vLen);
  ASSERT(pCfg->vLen < 0 || pCfg->vLen == vLen);

  if (pCfg->kLen < 0) {
    n += tslPutVarInt(p, kLen);
  }

  if (pCfg->vLen < 0) {
    n += tslPutVarInt(p ? p + n : p, vLen);
  }

  if (p) {
    memcpy(p + n, pKey, kLen);
  }
  n += kLen;

  if (p) {
    memcpy(p + n, pVal, vLen);
  }
  n += vLen;

  return n;
}

static inline int tslPutVarInt(uint8_t *p, int v) {
  int n = 0;

  ASSERT(v > 0);

  for (;;) {
    if (v <= 0x7f) {
      if (p) p[n] = v;
      n++;
      break;
    }

    if (p) p[n] = (v & 0x7f) | 0x80;
    n++;
    v >>= 7;
  }

  ASSERT(n < 6);

  return n;
}

static inline int tslGetVarInt(const uint8_t *p, int *v) {
  int n = 0;
  int tv = 0;
  int t;

  for (;;) {
    if (p[n] <= 0x7f) {
      t = p[n];
      tv |= (t << (7 * n));
      n++;
      break;
    }

    t = p[n] & 0x7f;
    tv |= (t << (7 * n));
    n++;
  }

  ASSERT(n < 6);
  ASSERT(tv > 0);

  *v = tv;
  return n;
}
