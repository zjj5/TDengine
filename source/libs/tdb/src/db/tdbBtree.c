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

#include "tdbInt.h"

struct SBTree {
  SPgno          root;
  int            keyLen;
  int            valLen;
  SPager *       pPager;
  FKeyComparator kcmpr;
  u8             fanout;
  int            pageSize;
  int            maxLocal;
  int            minLocal;
  int            maxLeaf;
  int            minLeaf;
};

typedef struct SFreeCell {
  u16 size;
  u16 next;
} SFreeCell;

static int tdbBtCursorMoveTo(SBtCursor *pCur, const void *pKey, int kLen);
static int tdbEncodeLength(u8 *pBuf, uint32_t len);
static int tdbBtCursorMoveToRoot(SBtCursor *pCur);
static int tdbInitBtPage(SPage *pPage, SBtPage **ppBtPage);
static int tdbCompareKeyAndCell(const void *pKey, int kLen, const void *pCell);
static int tdbDefaultKeyCmprFn(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2);

int tdbBtreeOpen(SPgno rtPgno, int keyLen, int valLen, SPager *pPager, FKeyComparator kcmpr, SBTree **ppBt) {
  SBTree *pBt;

  *ppBt = NULL;

  pBt = (SBTree *)calloc(1, sizeof(*pBt));
  if (pBt == NULL) {
    return -1;
  }

  // pBt->root
  pBt->root = rtPgno;
  // pBt->keyLen
  pBt->keyLen = keyLen;
  // pBt->valLen
  pBt->valLen = valLen;
  // pBt->pPager
  pBt->pPager = pPager;
  // pBt->kcmpr
  pBt->kcmpr = kcmpr ? kcmpr : tdbDefaultKeyCmprFn;
  // pBt->fanout
  if (keyLen == TDB_VARIANT_LEN) {
    pBt->fanout = TDB_DEFAULT_FANOUT;
  } else {
    ASSERT(0);
    // TODO: pBt->fanout = 0;
  }
  // pBt->pageSize
  pBt->pageSize = tdbPagerGetPageSize(pPager);
  // pBt->maxLocal
  pBt->maxLocal = (pBt->pageSize - sizeof(SPageHdr)) / pBt->fanout;
  // pBt->minLocal
  pBt->minLocal = (pBt->pageSize - sizeof(SPageHdr)) / pBt->fanout / 2;
  // pBt->maxLeaf
  pBt->maxLeaf = pBt->pageSize - sizeof(SPageHdr);
  // pBt->minLeaf
  pBt->minLeaf = pBt->minLocal;

  *ppBt = pBt;
  return 0;
}

int tdbBtreeClose(SBTree *pBt) {
  // TODO
  return 0;
}

int tdbBtreeCursor(SBtCursor *pCur, SBTree *pBt) {
  pCur->pBt = pBt;
  pCur->iPage = -1;
  pCur->pPage = NULL;
  pCur->idx = 0;

  return 0;
}

int tdbBtCursorInsert(SBtCursor *pCur, const void *pKey, int kLen, const void *pVal, int vLen) {
  int      ret;
  SPager * pPager;
  SBtPage *pPage;

  ret = tdbBtCursorMoveTo(pCur, pKey, kLen);
  if (ret < 0) {
    // TODO: handle error
    return -1;
  }

  pPage = pCur->pPage;

  return 0;
}

static int tdbBtCursorMoveTo(SBtCursor *pCur, const void *pKey, int kLen) {
  int      ret;
  SBtPage *pBtPage;
  void *   pCell;

  ret = tdbBtCursorMoveToRoot(pCur);
  if (ret < 0) {
    return -1;
  }

  // if (pCur->pPage->pHdr->nCells == 0) {
  //   // Tree is empty
  // } else {
  //   for (;;) {
  //     int lidx, ridx, midx, c;

  //     pBtPage = pCur->pPage;
  //     lidx = 0;
  //     ridx = pBtPage->pHdr->nCells - 1;
  //     while (lidx <= ridx) {
  //       midx = (lidx + ridx) >> 1;
  //       pCell = (void *)(pBtPage->aData + pBtPage->aCellIdx[midx]);

  //       c = tdbCompareKeyAndCell(pKey, kLen, pCell);
  //       if (c == 0) {
  //         break;
  //       } else if (c < 0) {
  //         lidx = lidx + 1;
  //       } else {
  //         ridx = ridx - 1;
  //       }
  //     }
  //   }

  //   /* code */
  // }

  return 0;
}

static int tdbEncodeKeyValue(const void *pKey, int kLen, int kLenG, const void *pVal, int vLen, int vLenG, void *pBuf,
                             int *bLen) {
  u8 *pPtr;

  ASSERT(kLen > 0 && vLen > 0);
  ASSERT(kLenG == TDB_VARIANT_LEN || kLenG == kLen);
  ASSERT(vLenG == TDB_VARIANT_LEN || vLenG == vLen);

  pPtr = (u8 *)pBuf;
  if (kLenG == TDB_VARIANT_LEN) {
    pPtr += tdbEncodeLength(pPtr, kLen);
  }

  if (vLenG == TDB_VARIANT_LEN) {
    pPtr += tdbEncodeLength(pPtr, vLen);
  }

  memcpy(pPtr, pKey, kLen);
  pPtr += kLen;

  memcpy(pPtr, pVal, vLen);
  pPtr += vLen;

  *bLen = pPtr - (u8 *)pBuf;
  return 0;
}

static int tdbDecodeKeyValue(const void *pBuf, void *pKey, int *kLen, void *pVal, int *vLen) {
  if (*kLen == TDB_VARIANT_LEN) {
    // Decode the key length
  }

  if (*vLen == TDB_VARIANT_LEN) {
    // Decode the value length
  }

  // TODO: decode the key and value

  return 0;
}

static int tdbEncodeLength(u8 *pBuf, uint32_t len) {
  int iCount = 0;

  while (len > 127) {
    pBuf[iCount++] = (u8)((len & 0xff) | 128);
    len >>= 7;
  }

  pBuf[iCount++] = (u8)len;

  return iCount;
}

static int tdbBtCursorMoveToRoot(SBtCursor *pCur) {
  SBTree * pBt;
  SPager * pPager;
  SPage *  pPage;
  SBtPage *pBtPage;
  int      ret;

  pBt = pCur->pBt;
  pPager = pBt->pPager;

  pPage = tdbPagerGet(pPager, pBt->root, true);
  if (pPage == NULL) {
    // TODO: handle error
  }

  ret = tdbInitBtPage(pPage, &pBtPage);
  if (ret < 0) {
    // TODO
    return 0;
  }

  pCur->pPage = pBtPage;
  pCur->iPage = 0;

  return 0;
}

static int tdbInitBtPage(SPage *pPage, SBtPage **ppBtPage) {
  SBtPage *pBtPage;

  // pBtPage = (SBtPage *)pPage->pExtra;
  // pBtPage->pHdr = (SPgHdr *)pPage->pData;
  // pBtPage->aCellIdx = (u16 *)(&((pBtPage->pHdr)[1]));
  // pBtPage->aData = (u8 *)pPage->pData;

  *ppBtPage = pBtPage;
  return 0;
}
static int tdbCompareKeyAndCell(const void *pKey, int kLen, const void *pCell) {
  /* TODO */
  return 0;
}

static int tdbDefaultKeyCmprFn(const void *pKey1, int keyLen1, const void *pKey2, int keyLen2) {
  int mlen;
  int cret;

  ASSERT(keyLen1 > 0 && keyLen2 > 0 && pKey1 != NULL && pKey2 != NULL);

  mlen = keyLen1 < keyLen2 ? keyLen1 : keyLen2;
  cret = memcmp(pKey1, pKey2, mlen);
  if (cret == 0) {
    if (keyLen1 < keyLen2) {
      cret = -1;
    } else if (keyLen1 > keyLen2) {
      cret = 1;
    } else {
      cret = 0;
    }
  }
  return cret;
}