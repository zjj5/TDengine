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

static int metaTbDbKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2);
static int metaSkmDbKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2);
static int metaUidCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2);
static int metaCtbIdxKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2);
static int metaCtimeIdxKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2);

int metaOpen(SVnode* pVnode, SMeta** ppMeta) {
  int    slen;
  int    ret;
  SMeta* pMeta;

  *ppMeta = NULL;
  slen = strlen(pVnode->path) + strlen(tfsGetPrimaryPath(pVnode->pTfs)) + strlen(VND_META_DIR) + 3;

  // allocate handle
  pMeta = (SMeta*)taosMemoryCalloc(1, sizeof(*pMeta) + slen);
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMeta->path = (char*)&pMeta[1];
  sprintf(pMeta->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP, VND_META_DIR);
  pMeta->pVnode = pVnode;

  // open env
  ret = tdbEnvOpen(pMeta->path, pVnode->config.szPage, pVnode->config.szCache, &pMeta->pEnv);
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open table DB
  ret = tdbDbOpen("table.db", sizeof(STbDbKey), TDB_VARIANT_LEN, metaTbDbKeyCmpr, pMeta->pEnv, &(pMeta->pTbDB));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open schema DB
  ret = tdbDbOpen("schema.db", sizeof(SSkmDbKey), TDB_VARIANT_LEN, metaSkmDbKeyCmpr, pMeta->pEnv, &(pMeta->pSkmDB));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open name index
  ret = tdbDbOpen("name.idx", TDB_VARIANT_LEN, sizeof(tb_uid_t), NULL, pMeta->pEnv, &(pMeta->pNameIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open super table index
  ret = tdbDbOpen("stb.idx", sizeof(tb_uid_t), 0, metaUidCmpr, pMeta->pEnv, &(pMeta->pStbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open normal table index
  ret = tdbDbOpen("ntb.idx", sizeof(tb_uid_t), 0, metaUidCmpr, pMeta->pEnv, &(pMeta->pNtbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open child table index
  ret = tdbDbOpen("ctb.idx", sizeof(SCtbIdxKey), 0, metaCtbIdxKeyCmpr, pMeta->pEnv, &(pMeta->pCtbIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  // open created time index
  ret = tdbDbOpen("ctime.idx", sizeof(SCtimeIdxKey), 0, metaCtimeIdxKeyCmpr, pMeta->pEnv, &(pMeta->pCtimeIdx));
  if (ret < 0) {
    // TODO
    ASSERT(0);
    return -1;
  }

  *ppMeta = pMeta;
  return 0;
}

int metaClose(SMeta* pMeta) {
  // TODO
  return 0;
}

static int metaUidCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  tb_uid_t uid1, uid2;

  ASSERT(kLen1 == kLen2);
  ASSERT(kLen1 == sizeof(tb_uid_t));

  uid1 = ((tb_uid_t*)pKey1)[0];
  uid2 = ((tb_uid_t*)pKey2)[0];

  if (uid1 < uid2) {
    return -1;
  } else if (uid1 > uid2) {
    return 1;
  }

  return 0;
}

static int metaTbDbKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  STbDbKey* pk1 = (STbDbKey*)pKey1;
  STbDbKey* pk2 = (STbDbKey*)pKey2;

  ASSERT(kLen1 == kLen2);
  ASSERT(kLen1 == sizeof(STbDbKey));

  if (pk1->uid > pk2->uid) {
    return 1;
  } else if (pk1->uid < pk2->uid) {
    return -1;
  }

  if (pk1->ver > pk2->ver) {
    return 1;
  } else if (pk1->ver < pk2->ver) {
    return -1;
  }

  return 0;
}

static int metaSkmDbKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  SSkmDbKey* pk1 = (SSkmDbKey*)pKey1;
  SSkmDbKey* pk2 = (SSkmDbKey*)pKey2;

  ASSERT(kLen1 == kLen2);
  ASSERT(kLen1 == sizeof(SSkmDbKey));

  if (pk1->uid > pk2->uid) {
    return 1;
  } else if (pk1->uid < pk2->uid) {
    return -1;
  }

  if (pk1->sver > pk2->sver) {
    return 1;
  } else if (pk1->sver < pk2->sver) {
    return -1;
  }

  return 0;
}

static int metaCtbIdxKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  SCtbIdxKey* pk1 = (SCtbIdxKey*)pKey1;
  SCtbIdxKey* pk2 = (SCtbIdxKey*)pKey2;

  ASSERT(kLen1 == kLen2);
  ASSERT(kLen1 == sizeof(SCtbIdxKey));

  if (pk1->suid > pk2->suid) {
    return 1;
  } else if (pk1->suid < pk2->suid) {
    return -1;
  }

  if (pk1->uid > pk2->uid) {
    return 1;
  } else if (pk1->uid < pk2->uid) {
    return -1;
  }

  return 0;
}

static int metaCtimeIdxKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  SCtimeIdxKey* pk1 = (SCtimeIdxKey*)pKey1;
  SCtimeIdxKey* pk2 = (SCtimeIdxKey*)pKey2;

  ASSERT(kLen1 == kLen2);
  ASSERT(kLen1 == sizeof(SCtimeIdxKey));

  if (pk1->ctime > pk2->ctime) {
    return 1;
  } else if (pk1->ctime < pk2->ctime) {
    return -1;
  }

  if (pk1->uid > pk2->uid) {
    return 1;
  } else if (pk1->uid < pk2->uid) {
    return -1;
  }

  return 0;
}
