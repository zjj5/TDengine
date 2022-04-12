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

#ifndef _TD_LIBS_SYNC_RAFT_ENTRY_H
#define _TD_LIBS_SYNC_RAFT_ENTRY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "syncMessage.h"
#include "taosdef.h"

typedef enum EntryType {
  SYNC_RAFT_ENTRY_NOOP = 0,
  SYNC_RAFT_ENTRY_DATA = 1,
  SYNC_RAFT_ENTRY_CONFIG = 2,
} EntryType;

typedef struct SSyncEntry {
  uint32_t  bytes;
  uint32_t  msgType;
  uint32_t  originalRpcType;
  uint64_t  seqNum;
  bool      isWeak;
  SyncTerm  term;
  SyncIndex index;
  EntryType entryType;
  uint32_t  dataLen;
  char      data[];
} SSyncEntry;

SSyncEntry* syncEntryBuild(uint32_t dataLen);
SSyncEntry* syncEntryBuild2(SyncClientRequestCopy* pMsg, SyncTerm term, SyncIndex index);  // step 4
SSyncEntry* syncEntryBuild3(SyncClientRequestCopy* pMsg, SyncTerm term, SyncIndex index, EntryType entryType);
SSyncEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index);
void        syncEntryDestory(SSyncEntry* pEntry);
char*       syncEntrySerialize(const SSyncEntry* pEntry, uint32_t* len);  // step 5
SSyncEntry* syncEntryDeserialize(const char* buf, uint32_t len);          // step 6
cJSON*      syncEntry2Json(const SSyncEntry* pEntry);
char*       syncEntry2Str(const SSyncEntry* pEntry);
void        syncEntry2OriginalRpc(const SSyncEntry* pEntry, SRpcMsg* pRpcMsg);  // step 7

// for debug ----------------------
void syncEntryPrint(const SSyncEntry* pObj);
void syncEntryPrint2(char* s, const SSyncEntry* pObj);
void syncEntryLog(const SSyncEntry* pObj);
void syncEntryLog2(char* s, const SSyncEntry* pObj);

// =================================================

typedef struct SSyncRaftEntry {
  uint32_t bytes;

  // RpcMsg
  uint32_t msgType;
  int32_t  vgId;

  // ClientRequest
  uint64_t seqNum;
  bool     isWeak;

  // log
  SyncTerm  term;
  SyncIndex index;

  // RpcMsg contLen, pCont
  uint32_t dataLen;
  char     data[];

} SSyncRaftEntry;

SSyncRaftEntry* syncRaftEntryBuild(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index);  // step 4
SSyncRaftEntry* syncRaftEntryBuildNoop(SyncTerm term, SyncIndex index);
void            syncRaftEntryDestory(SSyncRaftEntry* pEntry);
cJSON*          syncRaftEntry2Json(const SSyncRaftEntry* pEntry);
char*           syncRaftEntry2Str(const SSyncRaftEntry* pEntry);
void            syncRaftEntry2RpcMsg(const SSyncRaftEntry* pEntry, SRpcMsg* pRpcMsg);

// for debug ----------------------
void syncRaftEntryPrint(const SSyncRaftEntry* pObj);
void syncRaftEntryPrint2(char* s, const SSyncRaftEntry* pObj);
void syncRaftcEntryLog(const SSyncRaftEntry* pObj);
void syncRaftEntryLog2(char* s, const SSyncRaftEntry* pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_ENTRY_H*/
