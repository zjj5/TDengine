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

int tqOpen(SVnode* pVnode, STQ** ppTq) {
  STQ* pTq;
  int  slen;

  *ppTq = NULL;
  slen = strlen(tfsGetPrimaryPath(pVnode->pTfs)) + strlen(pVnode->path) + strlen(VND_TQ_DIR) + 3;

  pTq = (STQ*)taosMemoryCalloc(1, sizeof(*pTq) + slen);
  if (pTq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pTq->path = (char*)&pTq[1];
  sprintf(pTq->path, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP, VND_TQ_DIR);
  pTq->pVnode = pVnode;

#if 0
  pTq->tqMeta = tqStoreOpen(pTq, path, (FTqSerialize)tqSerializeConsumer, (FTqDeserialize)tqDeserializeConsumer,
                            (FTqDelete)taosMemoryFree, 0);
  if (pTq->tqMeta == NULL) {
    taosMemoryFree(pTq);
    return NULL;
  }


  pTq->pStreamTasks = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
#endif

  *ppTq = pTq;
  return 0;
}

void tqClose(STQ* pTq) {
  if (pTq) {
    taosMemoryFree(pTq);
  }
}
