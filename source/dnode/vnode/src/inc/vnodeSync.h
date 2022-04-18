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

#ifndef _TD_VNODE_SYNC_H_
#define _TD_VNODE_SYNC_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "sync.h"
#include "vnode.h"

int32_t vnodeSyncOpen(SVnode *pVnode);
int32_t vnodeSyncStart(SVnode *pVnode);
void    vnodeSyncClose(SVnode *pVnode);

int32_t vnodeSyncEqMsg(void *qHandle, SRpcMsg *pMsg);
int32_t vnodeSendMsg(void *rpcHandle, const SEpSet *pEpSet, SRpcMsg *pMsg);

/*
void CommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SyncIndex index, bool isWeak, int32_t code, ESyncState state,
              uint64_t seqNum);
*/

void CommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta);
void PreCommitCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SFsmCbMeta cbMeta);

void      RollBackCb(struct SSyncFSM *pFsm, const SRpcMsg *pMsg, SyncIndex index, bool isWeak, int32_t code,
                     ESyncState state);
SSyncFSM *syncMakeFsm();

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_SYNC_H_*/
