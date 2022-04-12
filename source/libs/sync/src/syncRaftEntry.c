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

#include "syncRaftEntry.h"
#include "sync.h"
#include "syncUtil.h"
#include "tmsg.h"

SSyncEntry* syncEntryBuild(uint32_t dataLen) {
  uint32_t    bytes = sizeof(SSyncEntry) + dataLen;
  SSyncEntry* pEntry = taosMemoryMalloc(bytes);
  assert(pEntry != NULL);
  memset(pEntry, 0, bytes);
  pEntry->bytes = bytes;
  pEntry->dataLen = dataLen;
  return pEntry;
}

// step 4. SyncClientRequestCopy => SSyncEntry, add term, index
SSyncEntry* syncEntryBuild2(SyncClientRequestCopy* pMsg, SyncTerm term, SyncIndex index) {
  SSyncEntry* pEntry = syncEntryBuild3(pMsg, term, index, SYNC_RAFT_ENTRY_DATA);
  assert(pEntry != NULL);

  return pEntry;
}

SSyncEntry* syncEntryBuild3(SyncClientRequestCopy* pMsg, SyncTerm term, SyncIndex index, EntryType entryType) {
  SSyncEntry* pEntry = syncEntryBuild(pMsg->dataLen);
  assert(pEntry != NULL);

  pEntry->msgType = pMsg->msgType;
  pEntry->originalRpcType = pMsg->originalRpcType;
  pEntry->seqNum = pMsg->seqNum;
  pEntry->isWeak = pMsg->isWeak;
  pEntry->term = term;
  pEntry->index = index;
  pEntry->entryType = entryType;
  pEntry->dataLen = pMsg->dataLen;
  memcpy(pEntry->data, pMsg->data, pMsg->dataLen);

  return pEntry;
}

SSyncEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index) {
  SSyncEntry* pEntry = syncEntryBuild(0);
  assert(pEntry != NULL);
  pEntry->term = term;
  pEntry->index = index;
  pEntry->entryType = SYNC_RAFT_ENTRY_NOOP;

  return pEntry;
}

void syncEntryDestory(SSyncEntry* pEntry) {
  if (pEntry != NULL) {
    taosMemoryFree(pEntry);
  }
}

// step 5. SSyncEntry => bin, to raft log
char* syncEntrySerialize(const SSyncEntry* pEntry, uint32_t* len) {
  char* buf = taosMemoryMalloc(pEntry->bytes);
  assert(buf != NULL);
  memcpy(buf, pEntry, pEntry->bytes);
  if (len != NULL) {
    *len = pEntry->bytes;
  }
  return buf;
}

// step 6. bin => SSyncEntry, from raft log
SSyncEntry* syncEntryDeserialize(const char* buf, uint32_t len) {
  uint32_t    bytes = *((uint32_t*)buf);
  SSyncEntry* pEntry = taosMemoryMalloc(bytes);
  assert(pEntry != NULL);
  memcpy(pEntry, buf, len);
  assert(len == pEntry->bytes);
  return pEntry;
}

cJSON* syncEntry2Json(const SSyncEntry* pEntry) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pEntry != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pEntry->bytes);
    cJSON_AddNumberToObject(pRoot, "msgType", pEntry->msgType);
    cJSON_AddNumberToObject(pRoot, "originalRpcType", pEntry->originalRpcType);
    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->seqNum);
    cJSON_AddStringToObject(pRoot, "seqNum", u64buf);
    cJSON_AddNumberToObject(pRoot, "isWeak", pEntry->isWeak);
    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->index);
    cJSON_AddStringToObject(pRoot, "index", u64buf);
    cJSON_AddNumberToObject(pRoot, "entryType", pEntry->entryType);
    cJSON_AddNumberToObject(pRoot, "dataLen", pEntry->dataLen);

    char* s;
    s = syncUtilprintBin((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);

    s = syncUtilprintBin2((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncEntry", pRoot);
  return pJson;
}

char* syncEntry2Str(const SSyncEntry* pEntry) {
  cJSON* pJson = syncEntry2Json(pEntry);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// step 7. SSyncEntry => original SRpcMsg, commit to user, delete seqNum, isWeak, term, index
void syncEntry2OriginalRpc(const SSyncEntry* pEntry, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(*pRpcMsg));
  pRpcMsg->msgType = pEntry->originalRpcType;
  pRpcMsg->contLen = pEntry->dataLen;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  memcpy(pRpcMsg->pCont, pEntry->data, pRpcMsg->contLen);
}

// for debug ----------------------
void syncEntryPrint(const SSyncEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint | len:%zu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncEntryPrint2(char* s, const SSyncEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  printf("syncEntryPrint2 | len:%zu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncEntryLog(const SSyncEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog | len:%zu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncEntryLog2(char* s, const SSyncEntry* pObj) {
  char* serialized = syncEntry2Str(pObj);
  sTrace("syncEntryLog2 | len:%zu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}

// ======================================

SSyncRaftEntry* syncRaftEntryBuild(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  assert(pMsg->rpcMsg.contLen >= 0);

  uint32_t        bytes = sizeof(SSyncRaftEntry) + pMsg->rpcMsg.contLen;
  SSyncRaftEntry* pEntry = taosMemoryMalloc(bytes);
  assert(pEntry != NULL);
  memset(pEntry, 0, bytes);

  pEntry->bytes = bytes;
  pEntry->msgType = pMsg->rpcMsg.msgType;

  SMsgHead* pHead = pMsg->rpcMsg.pCont;
  pEntry->vgId = pHead->vgId;

  pEntry->term = term;
  pEntry->index = index;
  pEntry->dataLen = pMsg->rpcMsg.contLen;
  memcpy(pEntry->data, pMsg->rpcMsg.pCont, pMsg->rpcMsg.contLen);

  return pEntry;
}

SSyncRaftEntry* syncRaftEntryBuildNoop(SyncTerm term, SyncIndex index) {
  uint32_t        bytes = sizeof(SSyncRaftEntry);
  SSyncRaftEntry* pEntry = taosMemoryMalloc(bytes);
  assert(pEntry != NULL);
  memset(pEntry, 0, bytes);

  pEntry->bytes = bytes;
  pEntry->msgType = TDMT_VND_SYNC_NOOP_ENTRY;
  pEntry->vgId = -1;

  pEntry->term = term;
  pEntry->index = index;
  pEntry->dataLen = 0;

  return pEntry;
}

void syncRaftEntryDestory(SSyncRaftEntry* pEntry) {
  if (pEntry != NULL) {
    taosMemoryFree(pEntry);
  }
}

cJSON* syncRaftEntry2Json(const SSyncRaftEntry* pEntry) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pEntry != NULL) {
    cJSON_AddNumberToObject(pRoot, "bytes", pEntry->bytes);
    cJSON_AddNumberToObject(pRoot, "msgType", pEntry->msgType);
    cJSON_AddNumberToObject(pRoot, "vgId", pEntry->vgId);

    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->seqNum);
    cJSON_AddStringToObject(pRoot, "seqNum", u64buf);
    cJSON_AddNumberToObject(pRoot, "isWeak", pEntry->isWeak);

    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", pEntry->index);
    cJSON_AddStringToObject(pRoot, "index", u64buf);

    cJSON_AddNumberToObject(pRoot, "dataLen", pEntry->dataLen);

    char* s;
    s = syncUtilprintBin((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data", s);
    taosMemoryFree(s);

    s = syncUtilprintBin2((char*)(pEntry->data), pEntry->dataLen);
    cJSON_AddStringToObject(pRoot, "data2", s);
    taosMemoryFree(s);
  }

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncRaftEntry", pRoot);
  return pJson;
}

char* syncRaftEntry2Str(const SSyncRaftEntry* pEntry) {
  cJSON* pJson = syncRaftEntry2Json(pEntry);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

void syncRaftEntry2RpcMsg(const SSyncRaftEntry* pEntry, SRpcMsg* pRpcMsg) {
  memset(pRpcMsg, 0, sizeof(SRpcMsg));
  pRpcMsg->msgType = pEntry->msgType;
  pRpcMsg->contLen = pEntry->dataLen;
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  memcpy(pRpcMsg->pCont, pEntry->data, pRpcMsg->contLen);
}

// for debug ----------------------
void syncRaftEntryPrint(const SSyncRaftEntry* pObj) {
  char* serialized = syncRaftEntry2Str(pObj);
  printf("syncRaftEntryPrint | len:%zu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRaftEntryPrint2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncRaftEntry2Str(pObj);
  printf("syncRaftEntryPrint2 | len:%zu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncRaftcEntryLog(const SSyncRaftEntry* pObj) {
  char* serialized = syncRaftEntry2Str(pObj);
  sTrace("syncRaftcEntryLog | len:%zu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncRaftEntryLog2(char* s, const SSyncRaftEntry* pObj) {
  char* serialized = syncRaftEntry2Str(pObj);
  sTrace("syncRaftEntryLog2 | len:%zu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}
