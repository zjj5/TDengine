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

#define _DEFAULT_SOURCE
#include "sync.h"
#include "vnd.h"

int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg) { return 0; }

int32_t vnodeCompact(SVnode *pVnode) { return 0; }

int32_t vnodeSync(SVnode *pVnode) { return 0; }

int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad) {
  pLoad->vgId = pVnode->vgId;

  // pLoad->role = TAOS_SYNC_STATE_LEADER;
  pLoad->role = syncGetMyRole(pVnode->sync);

  pLoad->numOfTables = metaGetTbNum(pVnode->pMeta);
  pLoad->numOfTimeSeries = 400;
  pLoad->totalStorage = 300;
  pLoad->compStorage = 200;
  pLoad->pointsWritten = 100;
  pLoad->numOfSelectReqs = 1;
  pLoad->numOfInsertReqs = 3;
  pLoad->numOfInsertSuccessReqs = 2;
  pLoad->numOfBatchInsertReqs = 5;
  pLoad->numOfBatchInsertSuccessReqs = 4;
  return 0;
}

int vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
  assert(pSyncNode != NULL);

  ESyncState state = syncGetMyRole(pVnode->sync);
  SyncTerm   currentTerm = syncGetMyTerm(pVnode->sync);

  SMsgHead *pHead = pMsg->pCont;

  char  logBuf[512];
  char *syncNodeStr = sync2SimpleStr(pVnode->sync);
  snprintf(logBuf, sizeof(logBuf), "==vnodeProcessSyncReq== msgType:%d, syncNode: %s", pMsg->msgType, syncNodeStr);
  syncRpcMsgLog2(logBuf, pMsg);
  taosMemoryFree(syncNodeStr);

  SRpcMsg *pRpcMsg = pMsg;

  if (pRpcMsg->msgType == TDMT_VND_SYNC_TIMEOUT) {
    SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
    syncTimeoutDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_PING) {
    SyncPing *pSyncMsg = syncPingFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnPingCb(pSyncNode, pSyncMsg);
    syncPingDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_PING_REPLY) {
    SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
    syncPingReplyDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_CLIENT_REQUEST) {
    SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnClientRequestCb(pSyncNode, pSyncMsg);
    syncClientRequestDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_REQUEST_VOTE) {
    SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnRequestVoteCb(pSyncNode, pSyncMsg);
    syncRequestVoteDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_REQUEST_VOTE_REPLY) {
    SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnRequestVoteReplyCb(pSyncNode, pSyncMsg);
    syncRequestVoteReplyDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_APPEND_ENTRIES) {
    SyncAppendEntries *pSyncMsg = syncAppendEntriesFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnAppendEntriesCb(pSyncNode, pSyncMsg);
    syncAppendEntriesDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_APPEND_ENTRIES_REPLY) {
    SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    syncNodeOnAppendEntriesReplyCb(pSyncNode, pSyncMsg);
    syncAppendEntriesReplyDestroy(pSyncMsg);

  } else {
    vError("==vnodeProcessSyncReq== error msg type:%d", pRpcMsg->msgType);
  }

  syncNodeRelease(pSyncNode);

  return 0;
}
