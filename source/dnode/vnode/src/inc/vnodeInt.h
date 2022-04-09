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

#ifndef _TD_VNODE_DEF_H_
#define _TD_VNODE_DEF_H_

#include "executor.h"
#include "os.h"
#include "qworker.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tfs.h"
#include "tglobal.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tmacro.h"
#include "tmallocator.h"
#include "tskiplist.h"
#include "ttime.h"
#include "ttimer.h"
#include "vnode.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMeta        SMeta;
typedef struct STsdb        STsdb;
typedef struct STQ          STQ;
typedef struct SVBufPool    SVBufPool;
typedef struct SSink        SSink;
typedef struct SQWorkerMgmt SQHandle;

typedef struct {
  int64_t processed;
  int64_t committed;
  int64_t applied;
} SVState;

struct SVnode {
  int32_t    vgId;
  char*      path;
  SVnodeCfg  config;
  SVState    state;
  SVBufPool* pPool;
  SVBufPool* inUse;
  SVBufPool* onCommit;
  SVBufPool* onRecycle;
  SMeta*     pMeta;
  STsdb*     pTsdb;
  SWal*      pWal;
  STQ*       pTq;
  SSink*     pSink;
  tsem_t     canCommit;
  SQHandle*  pQuery;
  SMsgCb     msgCb;
  STfs*      pTfs;
};

#define TD_VNODE_ID(PVNODE) (PVNODE)->vgId

// sma
void smaHandleRes(void* pVnode, int64_t smaId, const SArray* data);

#include "vnd.h"

#include "meta.h"

#include "tsdb.h"

#include "tq.h"

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_DEF_H_*/
