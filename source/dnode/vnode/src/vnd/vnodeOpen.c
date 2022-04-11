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

int vnodeCreate(const char *path, SVnodeCfg *pCfg) {
  // TODO
  return 0;
}

void vnodeDestroy(const char *path) {
  // TODO
  taosRemoveDir(path);
}

int vnodeOpen(const char *path, const SVnodeCfg *pVnodeCfg, SVnode **ppVnode) {
#if 0
  SVnode *pVnode;
  int     ret;

  *ppVnode = NULL;

  if (1) {
    // create a new vnode
    // 1. validate the config parameter
    // 2. create the vnode on disk (create directory and files)
  } else {
    // open an existing vnode
    // 1. load the config
    // 2. check the config
  }

  // open the vnode from the environment
  pVnode = taosMemoryCalloc(1, sizeof(*pVnode));
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  // open buffer pool sub-system
  uint8_t heap = 0;
  ret = vnodeOpenBufPool(pVnode, heap ? 0 : pVnode->config.wsize / 3);
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

  *ppVnode = pVnode;
#endif
  return 0;
}

void vnodeClose(SVnode *pVnode) {
#if 0
  if (pVnode) {
    vnodeSyncCommit(pVnode);
    vnodeQueryClose(pVnode);
    vnodeCloseTq(pVnode);
    vnodeCloseWal(pVnode);
    vnodeCloseTsdb(pVnode);
    vnodeCloseMeta(pVnode);
    taosMemoryFree(pVnode);
  }
#endif
}

// static methods ----------
static int vnodeOpenMeta(SVnode *pVnode) {
#if 0
  int ret;

  ret = metaOpen(pVnode, &pVnode->pMeta);
  if (ret < 0) {
    vError("vgId: %d failed to open vnode meta since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    return -1;
  }

  vDebug("vgId: %d vnode meta is opened", TD_VNODE_ID(pVnode));
#endif

  return 0;
}

static int vnodeOpenTsdb(SVnode *pVnode) {
#if 0
  int ret;

  ret = tsdbOpen(pVnode, &pVnode->pTsdb);
  if (ret < 0) {
    vError("vgId: %d failed to open vnode tsdb since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    return -1;
  }

  vDebug("vgId: %d vnode tsdb is opened", TD_VNODE_ID(pVnode));

#endif
  return 0;
}

static int vnodeOpenWal(SVnode *pVnode) {
#if 0
  char path[TSDB_FILENAME_LEN];

  snprintf(path, TSDB_FILENAME_LEN, "%s/%s", pVnode->path, VND_WAL_DIR);

  pVnode->pWal = walOpen(path, &pVnode->config.walCfg);
  if (pVnode->pWal == NULL) {
    vError("vgId: %d failed to open vnode wal since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    return -1;
  }

  vDebug("vgId: %d vnode wal is opened", TD_VNODE_ID(pVnode));

#endif
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