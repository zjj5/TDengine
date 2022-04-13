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

#define TSL_RAND_FACTOR 4

struct __attribute__((__packed__)) SSLNode {
  int8_t   level;
  SSLNode *forward[];
};

#define TSL_NODE_FORWARD(n, l)  (n)->forward[(l)]
#define TSL_NODE_BACKWARD(n, l) (n)->forward[(n)->level + (l)]
#define TSL_NODE_DATA(n)        (&(n)->forward[(n)->level * 2])
#define TSL_NODE_SIZE(NL)       (sizeof(SSLNode) + sizeof(SSLNode *) * (NL)*2)
#define TSL_NODE_HALF_SIZE(NL)  (sizeof(SSLNode) + sizeof(SSLNode *) * (NL))

struct SSkipList2 {
  int8_t        level;
  uint32_t      seed;
  const SSLCfg *pCfg;
};

#define TSL_HEAD_NODE(sl) ((SSLNode *)&(sl)[1])
#define TSL_TAIL_NODE(sl) ((SSLNode *)POINTER_SHIFT(TSL_HEAD_NODE(sl), TSL_NODE_HALF_SIZE((sl)->pCfg->maxLevel)))

static SSLNode *tslNodeNew(SSkipList2 *pSl, int8_t level, int psize);
static void     tslNodeFree(SSkipList2 *pSl, SSLNode *pNode);
static int      tslPutVarInt(uint8_t *p, int v);
static int      tslGetVarInt(const uint8_t *p, int *v);
static void    *tslDefaultMalloc(void *pPool, int size);
static void     tslDefaultFree(void *pPool, void *p);
static int      tslDefaultComparFn(const void *pKey1, int kLen1, const void *pKey2, int kLen2);
static int8_t   tslRandLevel(SSkipList2 *pSl);
static int      tslCheckCfg(const SSLCfg *pCfg);
static int      tslEncode(SSkipList2 *pSl, const SSLItem *pItem, uint8_t *p);

static const SSLCfg defaultCfg = {
    .maxLevel = TSL_MAX_LEVEL,        // maxLevel
    .kLen = -1,                       // kLen
    .vLen = -1,                       // vLen
    .xComparFn = tslDefaultComparFn,  // xComparFn
    .xMalloc = tslDefaultMalloc,      // xMalloc
    .xFree = tslDefaultFree,          // xFree
    .pPool = NULL                     // pPool
};

int tslCreate(const SSLCfg *pCfg, SSkipList2 **ppSl) {
  SSkipList2 *pSl = NULL;
  SSLNode    *pHead;
  SSLNode    *pTail;
  int         tsize;

  *ppSl = NULL;
  if (pCfg == NULL) {
    pCfg = &defaultCfg;
  } else {
    if (tslCheckCfg(pCfg) < 0) {
      return -1;
    }
  }

  tsize = sizeof(*pSl) + 2 * TSL_NODE_HALF_SIZE(pCfg->maxLevel);
  pSl = (SSkipList2 *)pCfg->xMalloc(pCfg->pPool, tsize);
  if (pSl == NULL) {
    return -1;
  }

  pSl->level = 0;
  pSl->seed = taosRand();
  pSl->pCfg = pCfg;

  pHead = TSL_HEAD_NODE(pSl);
  pTail = TSL_TAIL_NODE(pSl);

  pHead->level = pCfg->maxLevel;
  pTail->level = pCfg->maxLevel;

  for (int i = 0; i < pCfg->maxLevel; i++) {
    TSL_NODE_FORWARD(pHead, i) = pTail;
    TSL_NODE_BACKWARD(pTail, i) = pHead;
  }

  *ppSl = pSl;
  return 0;
}

int tslDestroy(SSkipList2 *pSl) {
  if (pSl && pSl->pCfg->xFree) {
    pSl->pCfg->xFree(pSl->pCfg->pPool, pSl);
  }

  return 0;
}

int tslPut(SSkipList2 *pSl, const SSLItem *pItem) {
  int      psize;
  int8_t   level;
  SSLNode *pNode;
  SSLNode *pHead;
  SSLNode *pTail;

  psize = tslEncode(pSl, pItem, NULL);
  level = tslRandLevel(pSl);
  pNode = tslNodeNew(pSl, level, psize);
  if (pNode == NULL) {
    return -1;
  }

  tslEncode(pSl, pItem, (uint8_t *)TSL_NODE_DATA(pNode));

  return 0;
}

int tslPutBatch(SSkipList2 *pSl, void *iter) {
  // TODO
  return 0;
}

int tslGet(SSkipList2 *pSl, void *pKey, int kLen) {
  // TODO
  return 0;
}

int tslCursorOpen(SSLCursor *pSlc, SSkipList2 *pSl, int flags) {
  memset(pSlc, 0, sizeof(*pSlc));

  pSlc->flags = flags;
  pSlc->item.kLen = -1;
  pSlc->item.vLen = -1;

  return 0;
}

int tslCursorClose(SSLCursor *pSlc) { return 0; }

static SSLNode *tslNodeNew(SSkipList2 *pSl, int8_t level, int psize) {
  SSLNode      *pNode;
  int           tsize;
  const SSLCfg *pCfg;

  pNode = NULL;
  pCfg = pSl->pCfg;
  tsize = TSL_NODE_SIZE(level) + psize;

  pNode = (SSLNode *)pCfg->xMalloc(pCfg->pPool, tsize);
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

static int tslEncode(SSkipList2 *pSl, const SSLItem *pItem, uint8_t *p) {
  int           n = 0;
  const SSLCfg *pCfg = pSl->pCfg;

  ASSERT(pItem->kLen != 0);
  ASSERT(pCfg->kLen < 0 || pCfg->kLen == pItem->kLen);
  ASSERT(pCfg->vLen < 0 || pCfg->vLen == pItem->vLen);

  if (pCfg->kLen < 0) {
    n += tslPutVarInt(p, pItem->kLen);
  }

  if (pCfg->vLen < 0) {
    n += tslPutVarInt(p ? p + n : p, pItem->vLen);
  }

  if (p) {
    memcpy(p + n, pItem->pKey, pItem->kLen);
  }
  n += pItem->kLen;

  if (p) {
    memcpy(p + n, pItem->pVal, pItem->vLen);
  }
  n += pItem->vLen;

  return n;
}

static int tslDecode(SSkipList2 *pSl, uint8_t *p, int psize, SSLItem *pItem) {
  int           n = 0;
  const SSLCfg *pCfg = pSl->pCfg;

  pItem->kLen = -1;
  pItem->vLen = -1;
  pItem->pKey = NULL;
  pItem->pVal = NULL;

  if (pCfg->kLen < 0) {
    n += tslGetVarInt(p + n, &pItem->kLen);
  } else {
    pItem->kLen = pCfg->kLen;
  }

  ASSERT(n <= psize);

  if (pCfg->vLen < 0) {
    n += tslGetVarInt(p + n, &pItem->vLen);
  } else {
    pItem->vLen = pCfg->vLen;
  }

  ASSERT(n <= psize);

  pItem->pKey = (void *)(p + n);
  n += pItem->kLen;
  pItem->pVal = (void *)(p + n);
  n += pItem->vLen;

  ASSERT(n == psize);

  return 0;
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

static inline void *tslDefaultMalloc(void *pPool, int size) {
  void *p = NULL;

  p = taosMemoryMalloc(size);

  return p;
}

static inline void tslDefaultFree(void *pPool, void *p) { taosMemoryFree(p); }

static inline int tslDefaultComparFn(const void *pKey1, int kLen1, const void *pKey2, int kLen2) {
  int mlen;
  int c;

  mlen = kLen1 < kLen2 ? kLen1 : kLen2;
  c = memcmp(pKey1, pKey2, mlen);
  if (c == 0) {
    if (kLen1 < kLen2) {
      c = -1;
    } else if (kLen1 > kLen2) {
      c = 1;
    } else {
      c = 0;
    }
  }
  return c;
}

static int8_t tslRandLevel(SSkipList2 *pSl) {
  int8_t level = 1;

  if (pSl->level > 0) {
    while ((taosRandR(&(pSl->seed)) % TSL_RAND_FACTOR) == 0 && level <= pSl->pCfg->maxLevel) {
      level++;
    }

    if (level > pSl->level) {
      if (pSl->level < pSl->pCfg->maxLevel) {
        level = pSl->level + 1;
      } else {
        level = pSl->level;
      }
    }
  }

  return level;
}

static int tslCheckCfg(const SSLCfg *pCfg) {
  if (pCfg->maxLevel < 1 || pCfg->maxLevel > TSL_MAX_LEVEL) {
    return -1;
  }

  if (pCfg->kLen == 0) {
    return -1;
  }

  if (pCfg->xComparFn == NULL) {
    return -1;
  }

  if (pCfg->xMalloc == NULL) {
    return -1;
  }

  return 0;
}