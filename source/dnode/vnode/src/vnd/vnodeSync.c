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

#include "vnodeSync.h"
#include "tmsgcb.h"
#include "vnd.h"

int32_t vnodeSyncOpen(SVnode *pVnode) {
  SSyncInfo syncInfo;

  syncInfo.vgId = pVnode->vgId;
  SSyncCfg *pCfg = &(syncInfo.syncCfg);
  pCfg->replicaNum = pVnode->config.syncCfg.replicaNum;
  pCfg->myIndex = pVnode->config.syncCfg.myIndex;
  memcpy(pCfg->nodeInfo, pVnode->config.syncCfg.nodeInfo, sizeof(pCfg->nodeInfo));

  snprintf(syncInfo.path, sizeof(syncInfo.path), "%s/sync", pVnode->path);
  syncInfo.pWal = pVnode->pWal;

  syncInfo.pFsm = syncVnodeMakeFsm(pVnode);
  syncInfo.rpcClient = NULL;
  syncInfo.FpSendMsg = vnodeSendMsg;
  syncInfo.queue = NULL;
  syncInfo.FpEqMsg = vnodeSyncEqMsg;

  pVnode->sync = syncOpen(&syncInfo);
  assert(pVnode->sync > 0);

  // for test
  setPingTimerMS(pVnode->sync, 1000);
  setElectTimerMS(pVnode->sync, 500);
  setHeartbeatTimerMS(pVnode->sync, 100);

  return 0;
}

int32_t vnodeSyncStart(SVnode *pVnode) {
  syncStart(pVnode->sync);
  return 0;
}

void vnodeSyncClose(SVnode *pVnode) {
  // stop by ref id
  syncStop(pVnode->sync);
}

void vnodeSyncSetQ(SVnode *pVnode, void *qHandle) { syncSetQ(pVnode->sync, (void *)(&(pVnode->msgCb))); }

void vnodeSyncSetRpc(SVnode *pVnode, void *rpcHandle) { syncSetRpc(pVnode->sync, (void *)(&(pVnode->msgCb))); }

/*
int32_t vnodeSyncEqMsg(void *queue, SRpcMsg *pMsg) {
  int32_t ret = 0;
  char    logBuf[128];

  SRpcMsg *pTemp;
  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  memcpy(pTemp, pMsg, sizeof(SRpcMsg));

  STaosQueue *pMsgQ = queue;
  taosWriteQitem(pMsgQ, pTemp);

  return ret;
}
*/

int32_t vnodeSyncEqMsg(void *qHandle, SRpcMsg *pMsg) {
  int32_t ret = 0;

  SMsgCb *pMsgCb = qHandle;
  if (pMsgCb->queueFps[SYNC_QUEUE] != NULL) {
    tmsgPutToQueue(qHandle, SYNC_QUEUE, pMsg);
  } else {
    vError("vnodeSyncEqMsg queue is NULL, SYNC_QUEUE:%d", SYNC_QUEUE);
  }

  return ret;
}

int32_t vnodeSendMsg(void *rpcHandle, const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t ret = 0;

  SMsgCb *pMsgCb = rpcHandle;
  if (pMsgCb->queueFps[SYNC_QUEUE] != NULL) {
    tmsgSendReq(rpcHandle, pEpSet, pMsg);
  } else {
    vError("vnodeSendMsg queue is NULL, SYNC_QUEUE:%d", SYNC_QUEUE);
  }

  return ret;
}

void CommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf), "==callback== ==CommitCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);

  SVnode *pVnode = (SVnode *)(pFsm->data);

  SRpcMsg msg;
  int32_t ret = syncGetAndDelRespRpc(pVnode->sync, cbMeta.seqNum, &msg);
  if (ret == 1) {
    ((SRpcMsg *)pMsg)->ahandle = msg.ahandle;
    ((SRpcMsg *)pMsg)->handle = msg.handle;
  }

  tmsgPutToQueue(&(pVnode->msgCb), APPLY_QUEUE, (SRpcMsg *)pMsg);
}

void PreCommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf),
           "==callback== ==PreCommitCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n", pFsm, cbMeta.index,
           cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

void RollBackCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta) {
  char logBuf[256];
  snprintf(logBuf, sizeof(logBuf), "==callback== ==RollBackCb== pFsm:%p, index:%ld, isWeak:%d, code:%d, state:%d %s \n",
           pFsm, cbMeta.index, cbMeta.isWeak, cbMeta.code, cbMeta.state, syncUtilState2String(cbMeta.state));
  syncRpcMsgLog2(logBuf, (SRpcMsg *)pMsg);
}

SSyncFSM *syncVnodeMakeFsm(SVnode *pVnode) {
  SSyncFSM *pFsm = (SSyncFSM *)taosMemoryMalloc(sizeof(SSyncFSM));
  pFsm->data = pVnode;
  pFsm->FpCommitCb = CommitCb;
  pFsm->FpPreCommitCb = PreCommitCb;
  pFsm->FpRollBackCb = RollBackCb;
  return pFsm;
}