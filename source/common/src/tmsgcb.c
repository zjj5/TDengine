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
#include "tmsgcb.h"

static SMsgCb tsDefaultMsgCb;

void tmsgSetDefaultMsgCb(const SMsgCb* pMsgCb) { tsDefaultMsgCb = *pMsgCb; }

int32_t tmsgPutToQueue(const SMsgCb* pMsgCb, EQueueType qtype, SRpcMsg* pReq) {
  return (*pMsgCb->queueFps[qtype])(pMsgCb->pWrapper, pReq);
}

int32_t tmsgGetQueueSize(const SMsgCb* pMsgCb, int32_t vgId, EQueueType qtype) {
  return (*pMsgCb->qsizeFp)(pMsgCb->pWrapper, vgId, qtype);
}

int32_t tmsgSendReq(const SMsgCb* pMsgCb, const SEpSet* epSet, SRpcMsg* pReq) {
  return (*pMsgCb->sendReqFp)(pMsgCb->pWrapper, epSet, pReq);
}

void tmsgSendRsp(const SRpcMsg* pRsp) { return (*tsDefaultMsgCb.sendRspFp)(tsDefaultMsgCb.pWrapper, pRsp); }

void tmsgRegisterBrokenLinkArg(const SMsgCb* pMsgCb, SRpcMsg* pMsg) {
  (*pMsgCb->registerBrokenLinkArgFp)(pMsgCb->pWrapper, pMsg);
}

void tmsgReleaseHandle(void* handle, int8_t type) {
  (*tsDefaultMsgCb.releaseHandleFp)(tsDefaultMsgCb.pWrapper, handle, type);
}