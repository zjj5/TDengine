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
}

void test2() {
  SyncClientRequest msg;
  initSyncClientRequest(&msg);

  syncClientRequestPrint2((char*)"==test2==", &msg);
  rpcFreeCont(msg.rpcMsg.pCont);
}

void test3() {
  SyncClientRequest syncMsg;
  initSyncClientRequest(&syncMsg);

  SSyncRaftEntry entry;
  syncRaftEntryInit(&entry, &syncMsg, 100, 9527);

  syncRaftEntryPrint2((char*)"==test3 syncRaftEntryInit==", &entry);

  SRpcMsg rpcMsg;
  syncRaftEntry2RpcMsg(&entry, &rpcMsg);
  syncRpcMsgPrint2((char*)"==test3 syncRaftEntry2RpcMsg==", &rpcMsg);

  rpcFreeCont(rpcMsg.pCont);
}

void test4() {
  SSyncRaftEntry entry;
  syncRaftEntryInitNoop(&entry, 100, 9527, 1000);
  syncRaftEntryPrint2((char*)"==test4 syncRaftEntryInitNoop==", &entry);

  SRpcMsg rpcMsg;
  syncRaftEntry2RpcMsg(&entry, &rpcMsg);
  syncRpcMsgPrint2((char*)"==test4 syncRaftEntry2RpcMsg==", &rpcMsg);

  rpcFreeCont(entry.rpcMsg.pCont);
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
