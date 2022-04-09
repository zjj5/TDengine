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

#ifndef _TD_VNODE_VND_H_
#define _TD_VNODE_VND_H_

#ifdef __cplusplus
extern "C" {
#endif

// vndDebug
// clang-format off
#define vFatal(...) do { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define vError(...) do { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define vWarn(...)  do { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define vInfo(...)  do { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define vDebug(...) do { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND ", DEBUG_DEBUG, vDebugFlag, __VA_ARGS__); }}    while(0)
#define vTrace(...) do { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND ", DEBUG_TRACE, vDebugFlag, __VA_ARGS__); }}    while(0)
// clang-format on

// vnodeBufferPool
typedef struct SVBufPoolNode SVBufPoolNode;
struct SVBufPoolNode {
  SVBufPoolNode* prev;
  int64_t        size;
  uint8_t        data[];
};

struct SVBufPool {
  SVBufPool*     next;
  int64_t        nRef;
  int64_t        size;
  uint8_t*       ptr;
  SVBufPoolNode* pTail;
  SVBufPoolNode  node;
};

int   vnodeOpenBufPool(SVnode* pVnode);
int   vnodeCloseBufPool(SVnode* pVnode);
void  vnodeBufPoolReset(SVBufPool* pPool);
void* vnodeBufPoolMalloc(SVBufPool* pPool, size_t size);
void  vnodeBufPoolFree(SVBufPool* pPool, void* ptr);

#if 1  // -----------------------------
typedef struct SVnodeTask {
  TD_DLIST_NODE(SVnodeTask);
  void* arg;
  int (*execute)(void*);
} SVnodeTask;

typedef struct SVnodeMgr {
  td_mode_flag_t vnodeInitFlag;
  // For commit
  bool          stop;
  uint16_t      nthreads;
  TdThread*     threads;
  TdThreadMutex mutex;
  TdThreadCond  hasTask;
  TD_DLIST(SVnodeTask) queue;
} SVnodeMgr;

typedef struct {
  int8_t  streamType;  // sma or other
  int8_t  dstType;
  int16_t padding;
  int32_t smaId;
  int64_t tbUid;
  int64_t lastReceivedVer;
  int64_t lastCommittedVer;
} SStreamSinkInfo;

struct SSink {
  SVnode*   pVnode;
  SHashObj* pHash;  // streamId -> SStreamSinkInfo
};

extern SVnodeMgr vnodeMgr;

// SVState
int  vnodeScheduleTask(SVnodeTask* task);
int  vnodeQueryOpen(SVnode* pVnode);
void vnodeQueryClose(SVnode* pVnode);

// vnodeCfg.h
extern const SVnodeCfg defaultVnodeOptions;

int  vnodeValidateOptions(const SVnodeCfg*);
void vnodeOptionsCopy(SVnodeCfg* pDest, const SVnodeCfg* pSrc);

// For commit
#define vnodeShouldCommit vnodeBufPoolIsFull
int vnodeSyncCommit(SVnode* pVnode);
int vnodeAsyncCommit(SVnode* pVnode);

int vnodeBegin(SVnode* pVnode);

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_VND_H_*/
