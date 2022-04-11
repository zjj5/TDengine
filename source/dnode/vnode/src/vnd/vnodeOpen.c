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

static int vnodeOpenMeta(SVnode *pVnode);
static int vnodeOpenTsdb(SVnode *pVnode);
static int vnodeOpenWal(SVnode *pVnode);
static int vnodeOpenTq(SVnode *pVnode);
static int vnodeCloseMeta(SVnode *pVnode);
static int vnodeCloseTsdb(SVnode *pVnode);
static int vnodeCloseWal(SVnode *pVnode);
static int vnodeCloseTq(SVnode *pVnode);

int vnodeCreate(const char *path, SVnodeCfg *pCfg, STfs *pTfs) {
  char dir[TSDB_FILENAME_LEN];
  // TODO: check if directory exists

  // check config
  if (vnodeCheckCfg(pCfg) < 0) {
    vError("vgId: %d failed to create vnode since: %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  // create vnode env
  tfsMkdir(pTfs, path);
  snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  if (vnodeSaveCfg(dir, pCfg) < 0) {
    vError("vgId: %d failed to save vnode config since %s", pCfg->vgId, tstrerror(terrno));
    return -1;
  }

  return 0;
}

void vnodeDestroy(const char *path, STfs *pTfs) { tfsRmdir(pTfs, path); }

int vnodeOpen(const char *path, SVnode **ppVnode, STfs *pTfs) {
  SVnode   *pVnode;
  char      dir[TSDB_FILENAME_LEN];
  int       ret;
  int       slen;
  SVnodeCfg cfg;

  *ppVnode = NULL;

  // load config
  snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, path);
  ret = vnodeLoadCfg(dir, &cfg);
  if (ret < 0) {
    vError("failed to open vnode %s since %s", path, tstrerror(terrno));
    return -1;
  }

  // check config
  if (vnodeCheckCfg(&cfg) < 0) {
    vError("failed to open vnode %s since %s", path, tstrerror(terrno));
    return -1;
  }

  // open the vnode from the environment
  slen = strlen(path);
  pVnode = taosMemoryCalloc(1, sizeof(*pVnode) + slen + 1);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  memcpy(&pVnode->config, &cfg, sizeof(cfg));
  pVnode->path = (char *)&pVnode[1];
  memcpy(pVnode->path, path, slen);
  pVnode->path[slen] = '\0';
  pVnode->pTfs = pTfs;

  // open buffer pool sub-system
  ret = vnodeOpenBufPool(pVnode, pVnode->config.isHeap ? 0 : pVnode->config.szBuf * 1024 * 1024 / 3);
  if (ret < 0) {
    return -1;
  }

  // open meta sub-system
  ret = vnodeOpenMeta(pVnode);
  if (ret < 0) {
    return -1;
  }

  // open meta tsdb-system
  ret = vnodeOpenTsdb(pVnode);
  if (ret < 0) {
    return -1;
  }

  // open meta wal-system
  ret = vnodeOpenWal(pVnode);
  if (ret < 0) {
    return -1;
  }

  // open meta tq-system
  ret = vnodeOpenTq(pVnode);
  if (ret < 0) {
    return -1;
  }

#if 0
  // open meta query-system
  ret = vnodeQueryOpen(pVnode);
  if (ret < 0) {
    return -1;
  }

  // make vnode start to work
  ret = vnodeBegin(pVnode);
  if (ret < 0) {
    return -1;
  }
#endif

  *ppVnode = pVnode;
  return 0;
}

void vnodeClose(SVnode *pVnode) {
  if (pVnode) {
    // vnodeSyncCommit(pVnode);
    // vnodeQueryClose(pVnode);
    // vnodeCloseTq(pVnode);
    // vnodeCloseWal(pVnode);
    // vnodeCloseTsdb(pVnode);
    // vnodeCloseMeta(pVnode);
    // taosMemoryFree(pVnode);
  }
}

// static methods ----------
static int vnodeOpenMeta(SVnode *pVnode) {
  int ret;

  ret = metaOpen(pVnode, &pVnode->pMeta);
  if (ret < 0) {
    vError("vgId: %d failed to open vnode meta since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    return -1;
  }

  vDebug("vgId: %d vnode meta is opened", TD_VNODE_ID(pVnode));

  return 0;
}

static int vnodeOpenTsdb(SVnode *pVnode) {
  int ret;

  ret = tsdbOpen(pVnode, &pVnode->pTsdb);
  if (ret < 0) {
    vError("vgId: %d failed to open vnode tsdb since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    return -1;
  }

  vDebug("vgId: %d vnode tsdb is opened", TD_VNODE_ID(pVnode));

  return 0;
}

static int vnodeOpenWal(SVnode *pVnode) {
  char path[TSDB_FILENAME_LEN];

  snprintf(path, TSDB_FILENAME_LEN, "%s/%s", pVnode->path, VND_WAL_DIR);

  pVnode->pWal = walOpen(path, &pVnode->config.walCfg);
  if (pVnode->pWal == NULL) {
    vError("vgId: %d failed to open vnode wal since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    return -1;
  }

  vDebug("vgId: %d vnode wal is opened", TD_VNODE_ID(pVnode));

  return 0;
}

static int vnodeOpenTq(SVnode *pVnode) {
#if 0
  char path[TSDB_FILENAME_LEN];

  snprintf(path, TSDB_FILENAME_LEN, "%s/%s", pVnode->path, VND_TQ_DIR);

  // TODO: refact here
  pVnode->pTq = tqOpen(path, pVnode, pVnode->pWal, pVnode->pMeta, &pVnode->config.tqCfg, NULL);
  if (pVnode->pTq == NULL) {
    vError("vgId: %d failed to open vnode tq since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    return -1;
  }

  vDebug("vgId: %d vnode tq is opened", TD_VNODE_ID(pVnode));
#endif

  return 0;
}

static int vnodeCloseMeta(SVnode *pVnode) {
  int ret = 0;

#if 0
  if (pVnode->pMeta) {
    ret = metaClose(pVnode->pMeta);
    pVnode->pMeta = NULL;
  }
#endif

  return ret;
}

static int vnodeCloseTsdb(SVnode *pVnode) {
  int ret = 0;
#if 0

  if (pVnode->pTsdb) {
    ret = tsdbClose(pVnode->pTsdb);
    pVnode->pTsdb = NULL;
  }
#endif

  return ret;
}

static int vnodeCloseWal(SVnode *pVnode) {
#if 0
  if (pVnode->pWal) {
    walClose(pVnode->pWal);
    pVnode->pWal = NULL;
  }
#endif

  return 0;
}

static int vnodeCloseTq(SVnode *pVnode) {
#if 0
  if (pVnode->pTq) {
    tqClose(pVnode->pTq);
    pVnode->pTq = NULL;
  }
#endif

  return 0;
}