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

static int vnodeConvertAndCopyReq(SVnode *pVnode, SRpcMsg *pMsg, void **ppCont, int *contLen);
static int vnodeProcessCreateStbReq(SVnode *pVnode, void *pCont, int contLen, int64_t version);
static int vnodeProcessAlterStbReq(SVnode *pVnode, void *pCont, int contLen, int64_t version);
static int vnodeProcessDropStbReq(SVnode *pVnode, void *pCont, int contLen, int64_t version);
static int vnodeProcessCreateTableReq(SVnode *pVnode, void *pCont, int contLen, int64_t version);
static int vnodeProcessAlterTableReq(SVnode *pVnode, void *pCont, int contLen, int64_t version);
static int vnodeProcessDropTableReq(SVnode *pVnode, void *pCont, int contLen, int64_t version);

int vnodePreProcessWriteMsgs(SVnode *pVnode, SArray *pMsgs, int64_t *pVer) {
  SRpcMsg *pMsg;
  int64_t  version;

  version = ++pVnode->state.processed;

  for (int i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pMsg = &(*(SNodeMsg **)taosArrayGet(pMsgs, i))[0].rpcMsg;

    if (walWrite(pVnode->pWal, version, pMsg->msgType, pMsg->pCont, pMsg->contLen) < 0) {
      vError("vgId: %d failed to pre-process write message, version %" PRId64 " since: %s", TD_VNODE_ID(pVnode),
             version, tstrerror(terrno));
      return -1;
    }
  }

  walFsync(pVnode->pWal, false);

  *pVer = version;
  return 0;
}

int vnodeProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg **pRsp) {
  int   ret;
  void *pCont;
  int   contLen;

  ASSERT(pVnode->state.applied <= version);

  // check commit
  if (version > pVnode->state.applied && pVnode->pPool->size >= pVnode->config.szBuf / 3) {
    // async commit
    if (vnodeAsyncCommit(pVnode) < 0) {
      vError("vgId: %d failed to async commit", TD_VNODE_ID(pVnode));
      ASSERT(0);
    }

    // start a new write session
    if (vnodeBegin(pVnode) < 0) {
      vError("vgId: %d failed to begin vnode since %s", TD_VNODE_ID(pVnode), tstrerror(terrno));
      ASSERT(0);
    }
  }

  pVnode->state.applied = version;

  // convert and copy the request
  ret = vnodeConvertAndCopyReq(pVnode, pMsg, &pCont, &contLen);
  if (ret < 0) {
    vError("vgId: %d failed to convert and copy request of version %" PRId64, TD_VNODE_ID(pVnode), version);
    ASSERT(0);
  }

  switch (pMsg->msgType) {
    // meta =================
    case TDMT_VND_CREATE_STB:
      return vnodeProcessCreateStbReq(pVnode, pCont, contLen, version);
    case TDMT_VND_ALTER_STB:
      return vnodeProcessAlterStbReq(pVnode, pCont, contLen, version);
    case TDMT_VND_DROP_STB:
      return vnodeProcessDropStbReq(pVnode, pCont, contLen, version);
    case TDMT_VND_CREATE_TABLE:
      return vnodeProcessCreateTableReq(pVnode, pCont, contLen, version);
    case TDMT_VND_ALTER_TABLE:
      return vnodeProcessAlterTableReq(pVnode, pCont, contLen, version);
    case TDMT_VND_DROP_TABLE:
      return vnodeProcessDropTableReq(pVnode, pCont, contLen, version);
    case TDMT_VND_CREATE_SMA:
      // TODO
      break;
    // tsdb =================
    case TDMT_VND_SUBMIT:
      // TODO
      break;
    // tq =================
    case TDMT_VND_MQ_SET_CONN:
      // TODO
      break;
    case TDMT_VND_MQ_REB:
      // TODO
      break;
    case TDMT_VND_MQ_CANCEL_CONN:
      // TODO
      break;
    case TDMT_VND_TASK_DEPLOY:
      // TODO
      break;
    case TDMT_VND_TASK_WRITE_EXEC:
      // TODO
      break;
    default:
      ASSERT(0);
  }
  return 0;
}

int vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
#if 0
  vTrace("message in query queue is processing");
  SReadHandle handle = {.reader = pVnode->pTsdb, .meta = pVnode->pMeta, .config = &pVnode->config};

  switch (pMsg->msgType) {
    case TDMT_VND_QUERY:
      return qWorkerProcessQueryMsg(&handle, pVnode->pQuery, pMsg);
    case TDMT_VND_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in query queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
#endif
  return 0;
}

int vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
#if 0
  vTrace("message in fetch queue is processing");
  char   *msgstr = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);
  switch (pMsg->msgType) {
    case TDMT_VND_FETCH:
      return qWorkerProcessFetchMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_FETCH_RSP:
      return qWorkerProcessFetchRsp(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_RES_READY:
      return qWorkerProcessReadyMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_TASKS_STATUS:
      return qWorkerProcessStatusMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_CANCEL_TASK:
      return qWorkerProcessCancelMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_DROP_TASK:
      return qWorkerProcessDropMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_SHOW_TABLES:
      return qWorkerProcessShowMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_SHOW_TABLES_FETCH:
      return vnodeGetTableList(pVnode, pMsg);
      //      return qWorkerProcessShowFetchMsg(pVnode->pMeta, pVnode->pQuery, pMsg);
    case TDMT_VND_TABLE_META:
      return vnodeGetTableMeta(pVnode, pMsg);
    case TDMT_VND_CONSUME:
      return tqProcessPollReq(pVnode->pTq, pMsg, pInfo->workerId);
    case TDMT_VND_TASK_PIPE_EXEC:
    case TDMT_VND_TASK_MERGE_EXEC:
      return tqProcessTaskExec(pVnode->pTq, msgstr, msgLen, 0);
    case TDMT_VND_STREAM_TRIGGER:
      return tqProcessStreamTrigger(pVnode->pTq, pMsg->pCont, pMsg->contLen, 0);
    case TDMT_VND_QUERY_HEARTBEAT:
      return qWorkerProcessHbMsg(pVnode, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
#endif
  return 0;
}

int vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  /*vInfo("sync message is processed");*/
  return 0;
}

static int vnodeConvertAndCopyReq(SVnode *pVnode, SRpcMsg *pMsg, void **ppCont, int *contLen) {
  // TODO
  return 0;
}

static int vnodeProcessCreateStbReq(SVnode *pVnode, void *pCont, int contLen, int64_t version) {
  SVCreateTbReq req;
  // TODO
  return 0;
}

static int vnodeProcessAlterStbReq(SVnode *pVnode, void *pCont, int contLen, int64_t version) {
  // TODO
  return 0;
}

static int vnodeProcessDropStbReq(SVnode *pVnode, void *pCont, int contLen, int64_t version) {
  // TODO
  return 0;
}

static int vnodeProcessCreateTableReq(SVnode *pVnode, void *pCont, int contLen, int64_t version) {
  // TODO
  return 0;
}

static int vnodeProcessAlterTableReq(SVnode *pVnode, void *pCont, int contLen, int64_t version) {
  // TODO
  return 0;
}

static int vnodeProcessDropTableReq(SVnode *pVnode, void *pCont, int contLen, int64_t version) {
  // TODO
  return 0;
}
