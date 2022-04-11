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

typedef struct SVnodeTask SVnodeTask;
struct SVnodeTask {
  SVnodeTask* next;
  SVnodeTask* prev;
  int (*execute)(void*);
  void* arg;
};

struct SVnodeGlobal {
  int8_t        init;
  int8_t        stop;
  int           nthreads;
  TdThread*     threads;
  TdThreadMutex mutex;
  TdThreadCond  hasTask;
  SVnodeTask*   head;
  SVnodeTask*   tail;
};

struct SVnodeGlobal vnodeGlobal;

static void* loop(void* arg);

int vnodeInit(int nthreads) {
  int8_t init;
  int    ret;

  init = atomic_val_compare_exchange_8(&(vnodeGlobal.init), 0, 1);
  if (init) {
    return 0;
  }

  vnodeGlobal.stop = 0;

  vnodeGlobal.nthreads = nthreads;
  vnodeGlobal.threads = taosMemoryCalloc(nthreads, sizeof(TdThread));
  if (vnodeGlobal.threads == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    vError("failed to init vnode module since: %s", tstrerror(terrno));
    return -1;
  }

  taosThreadMutexInit(&vnodeGlobal.mutex, NULL);
  taosThreadCondInit(&vnodeGlobal.hasTask, NULL);

  for (int i = 0; i < nthreads; i++) {
    taosThreadCreate(&(vnodeGlobal.threads[i]), NULL, loop, NULL);
  }

  if (walInit() < 0) {
    return -1;
  }

  return 0;
}

void vnodeCleanup() {
  int8_t init;

  init = atomic_val_compare_exchange_8(&(vnodeGlobal.init), 1, 0);
  if (init == 0) return;

  // set stop
  taosThreadMutexLock(&(vnodeGlobal.mutex));
  vnodeGlobal.stop = 1;
  taosThreadCondBroadcast(&(vnodeGlobal.hasTask));
  taosThreadMutexUnlock(&(vnodeGlobal.mutex));

  // wait for threads
  for (int i = 0; i < vnodeGlobal.nthreads; i++) {
    taosThreadJoin(vnodeGlobal.threads[i], NULL);
  }

  // clear source
  taosMemoryFreeClear(vnodeGlobal.threads);
  taosThreadCondDestroy(&(vnodeGlobal.hasTask));
  taosThreadMutexDestroy(&(vnodeGlobal.mutex));
}

int vnodeScheduleTask(int (*execute)(void*), void* arg) {
  SVnodeTask* pTask;

  ASSERT(!vnodeGlobal.stop);

  pTask = taosMemoryMalloc(sizeof(*pTask));
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pTask->execute = execute;
  pTask->arg = arg;

  taosThreadMutexLock(&(vnodeGlobal.mutex));
  // TODO: add to queue
  taosThreadCondSignal(&(vnodeGlobal.hasTask));
  taosThreadMutexUnlock(&(vnodeGlobal.mutex));

  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */
static void* loop(void* arg) {
  setThreadName("vnode-commit");

  SVnodeTask* pTask;
  for (;;) {
    taosThreadMutexLock(&(vnodeGlobal.mutex));
    for (;;) {
      // pTask = TD_DLIST_HEAD(&(vnodeGlobal.queue));
      // if (pTask == NULL) {
      //   if (vnodeGlobal.stop) {
      //     taosThreadMutexUnlock(&(vnodeGlobal.mutex));
      //     return NULL;
      //   } else {
      //     taosThreadCondWait(&(vnodeGlobal.hasTask), &(vnodeGlobal.mutex));
      //   }
      // } else {
      //   TD_DLIST_POP(&(vnodeGlobal.queue), pTask);
      //   break;
      // }
    }

    taosThreadMutexUnlock(&(vnodeGlobal.mutex));

    (*(pTask->execute))(pTask->arg);
    taosMemoryFree(pTask);
  }

  return NULL;
}