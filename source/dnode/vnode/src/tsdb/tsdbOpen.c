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

static int tsdbOpenImpl(STsdb *pTsdb);

int tsdbOpen(SVnode *pVnode, STsdb **ppTsdb) {
  STsdb *pTsdb;
  int    slen;

  *ppTsdb = NULL;
  slen = strlen(pVnode->path) + strlen(VND_TSDB_DIR) + 2;

  pTsdb = taosMemoryCalloc(1, sizeof(*pTsdb));
  if (pTsdb == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pTsdb->path = (char *)&pTsdb[1];
  sprintf(pTsdb->path, "%s%s%s", pVnode->path, TD_DIRSEP, VND_TSDB_DIR);
  pTsdb->pVnode = pVnode;

  if (tsdbOpenImpl(pTsdb) < 0) {
    tsdbError("vgId:%d failed to open vnode tsdb since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
    // TODO
    return -1;
  }

  *ppTsdb = pTsdb;
  return 0;
}

int tsdbClose(STsdb *pTsdb) {
  // TODO
  return 0;
}

static int tsdbOpenImpl(STsdb *pTsdb) {
  STfs *pTfs = pTsdb->pVnode->pTfs;
  char  path[TSDB_FILENAME_LEN];

  snprintf(path, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pTfs), TD_DIRSEP, pTsdb->path);
  taosMkDir(path);

  // TODO

  return 0;
}