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

void initRpcMsg(SRpcMsg* pMsg) {
  memset(pMsg, 0, sizeof(pMsg));
  pMsg->msgType = 9876;
  pMsg->contLen = 50;
  pMsg->code = 1234;
  pMsg->pCont = rpcMallocCont(pMsg->contLen);
  printf("--------rpcMallocCont: %p \n", pMsg->pCont);
  SMsgHead* pHead = (SMsgHead*)(pMsg->pCont);
  pHead->contLen = pMsg->contLen;
  pHead->vgId = 66;
  void* pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  snprintf((char*)pBuf, pMsg->contLen - sizeof(SMsgHead), "%s", "hello, rpc msg");
}

void initSyncClientRequest(SyncClientRequest* pMsg) {
  memset(pMsg, 0, sizeof(SyncClientRequest));
  pMsg->seqNum = 10;
  pMsg->isWeak = 1;

  SRpcMsg rpcMsg;
  initRpcMsg(&rpcMsg);
  memcpy(&(pMsg->rpcMsg), &rpcMsg, sizeof(SRpcMsg));
}

void test1() {
  SRpcMsg rpcMsg;
  initRpcMsg(&rpcMsg);

  syncRpcMsgPrint2((char*)"==test1==", &rpcMsg);
  rpcFreeCont(rpcMsg.pCont);
  printf("--------rpcFreeCont: %p \n", rpcMsg.pCont);
}

void test2() {
  SyncClientRequest msg;
  initSyncClientRequest(&msg);

  rpcFreeCont(msg.rpcMsg.pCont);
  printf("--------rpcFreeCont: %p \n", msg.rpcMsg.pCont);
}

void test3() {
  SyncClientRequest msg;
  initSyncClientRequest(&msg);

  SSyncRaftEntry* pEntry = syncRaftEntryBuild(&msg, 100, 9527);
  syncRaftEntryPrint2((char*)"==test3 syncRaftEntryBuild==", pEntry);

  SRpcMsg rpcMsg;
  syncRaftEntry2RpcMsg(pEntry, &rpcMsg);
  syncRpcMsgPrint2((char*)"==test3 syncRaftEntry2RpcMsg==", &rpcMsg);

  rpcFreeCont(rpcMsg.pCont);
  printf("--------rpcFreeCont: %p \n", rpcMsg.pCont);

  syncRaftEntryDestory(pEntry);
}

void test4() {
  SSyncRaftEntry* pEntry = syncRaftEntryBuildNoop(100, 9527);
  syncRaftEntryPrint2((char*)"==test4 syncRaftEntryBuildNoop==", pEntry);
  syncRaftEntryDestory(pEntry);
}

int main(int argc, char** argv) {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

   test1();
   test2();
  test3();
   test4();

  return 0;
}
