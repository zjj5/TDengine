#include <gtest/gtest.h>
#include <stdio.h>
#include "syncEnv.h"
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SSyncNode* pSyncNode;
SWal *pWal;
SSyncRaftLog* pLog;
const char *pWalPath = "./syncRaftLogTest_wal";

void init() {
  walInit();
  taosRemoveDir(pWalPath);

  SWalCfg walCfg;
  memset(&walCfg, 0, sizeof(SWalCfg));
  walCfg.vgId = 1000;
  walCfg.fsyncPeriod = 1000;
  walCfg.retentionPeriod = 1000;
  walCfg.rollPeriod = 1000;
  walCfg.retentionSize = 1000;
  walCfg.segSize = 1000;
  walCfg.level = TAOS_WAL_FSYNC;
  pWal = walOpen(pWalPath, &walCfg);
  assert(pWal != NULL);

  pSyncNode = (SSyncNode*)taosMemoryMalloc(sizeof(SSyncNode));
  memset(pSyncNode, 0, sizeof(SSyncNode));
  pSyncNode->pWal = pWal;
}

void cleanup() {
  walClose(pWal);
  walCleanUp();
  taosMemoryFree(pSyncNode);
}

void initRpcMsg(SRpcMsg* pMsg, int i) {
  memset(pMsg, 0, sizeof(pMsg));
  pMsg->msgType = 9876;
  pMsg->contLen = 50;
  pMsg->code = 1234;
  pMsg->pCont = rpcMallocCont(pMsg->contLen);
  SMsgHead* pHead = (SMsgHead*)(pMsg->pCont);
  pHead->contLen = pMsg->contLen;
  pHead->vgId = 66;
  void* pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  snprintf((char*)pBuf, pMsg->contLen - sizeof(SMsgHead), "%s -%d-", "hello, rpc msg", i);
}

void initSyncClientRequest(SyncClientRequest* pMsg, int i) {
  memset(pMsg, 0, sizeof(SyncClientRequest));
  pMsg->seqNum = 10;
  pMsg->isWeak = 1;

  SRpcMsg rpcMsg;
  initRpcMsg(&rpcMsg, i);
  memcpy(&(pMsg->rpcMsg), &rpcMsg, sizeof(SRpcMsg));
}

void syncRaftLogTest() {
  pLog = syncRaftLogCreate(pSyncNode);
  assert(pLog);
  assert(pLog->getLastIndex(pLog) == SYNC_INDEX_INVALID);

  syncRaftLogPrint2((char*)"syncRaftLogTest", pLog);

  for (int i = 0; i < 5; ++i) {
    SyncClientRequest syncMsg;
    initSyncClientRequest(&syncMsg, i);
  
    SSyncRaftEntry entry;
    syncRaftEntryInit(&entry, &syncMsg, 100+i, i);

    syncRaftEntryPrint2((char*)"write entry:", &entry);
    pLog->appendEntry(pLog, &entry);

    if (i == 0) {
      assert(pLog->getLastIndex(pLog) == SYNC_INDEX_BEGIN);
    }
  }
  syncRaftLogPrint2((char*)"after appendEntry", pLog);

  pLog->truncate(pLog, 3);
  syncRaftLogPrint2((char*)"after truncate 3", pLog);
}

int main(int argc, char** argv) {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  init();
  syncRaftLogTest();
  cleanup();

  return 0;
}
