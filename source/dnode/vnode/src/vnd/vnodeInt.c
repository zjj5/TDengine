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
  pLoad->role = TAOS_SYNC_STATE_LEADER;
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
  syncRpcMsgLog2((char *)"==vnodeProcessSyncReq==", pMsg);

  SRpcMsg *pRpcMsg = pMsg;

  if (pRpcMsg->msgType == TDMT_VND_SYNC_TIMEOUT) {
    SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncTimeoutDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_PING) {
    SyncPing *pSyncMsg = syncPingFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnPingCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncPingDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_PING_REPLY) {
    SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncPingReplyDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_CLIENT_REQUEST) {
    SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnClientRequestCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncClientRequestDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_REQUEST_VOTE) {
    SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnRequestVoteCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncRequestVoteDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_REQUEST_VOTE_REPLY) {
    SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnRequestVoteReplyCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncRequestVoteReplyDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_APPEND_ENTRIES) {
    SyncAppendEntries *pSyncMsg = syncAppendEntriesFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnAppendEntriesCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncAppendEntriesDestroy(pSyncMsg);

  } else if (pRpcMsg->msgType == TDMT_VND_SYNC_APPEND_ENTRIES_REPLY) {
    SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pRpcMsg);
    assert(pSyncMsg != NULL);

    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    syncNodeOnAppendEntriesReplyCb(pSyncNode, pSyncMsg);
    syncNodeRelease(pSyncNode);
    syncAppendEntriesReplyDestroy(pSyncMsg);

  } else {
    vError("==vnodeProcessSyncReq== error msg type:%d", pRpcMsg->msgType);
  }

  return 0;
}
