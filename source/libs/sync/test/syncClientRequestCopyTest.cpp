#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SyncClientRequestCopy *createMsg() {
  SRpcMsg rpcMsg;
  memset(&rpcMsg, 0, sizeof(rpcMsg));
  rpcMsg.msgType = 12345;
  rpcMsg.contLen = 20;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  strcpy((char *)rpcMsg.pCont, "hello rpc");
  SyncClientRequestCopy *pMsg = syncClientRequestCopyBuild2(&rpcMsg, 123, true, 1000);
  rpcFreeCont(rpcMsg.pCont);
  return pMsg;
}

void test1() {
  SyncClientRequestCopy *pMsg = createMsg();
  syncClientRequestCopyPrint2((char *)"test1:", pMsg);
  syncClientRequestCopyDestroy(pMsg);
}

void test2() {
  SyncClientRequestCopy *pMsg = createMsg();
  uint32_t               len = pMsg->bytes;
  char *                 serialized = (char *)taosMemoryMalloc(len);
  syncClientRequestCopySerialize(pMsg, serialized, len);
  SyncClientRequestCopy *pMsg2 = syncClientRequestCopyBuild(pMsg->dataLen);
  syncClientRequestCopyDeserialize(serialized, len, pMsg2);
  syncClientRequestCopyPrint2((char *)"test2: syncClientRequestCopySerialize -> syncClientRequestCopyDeserialize ",
                              pMsg2);

  taosMemoryFree(serialized);
  syncClientRequestCopyDestroy(pMsg);
  syncClientRequestCopyDestroy(pMsg2);
}

void test3() {
  SyncClientRequestCopy *pMsg = createMsg();
  uint32_t               len;
  char *                 serialized = syncClientRequestCopySerialize2(pMsg, &len);
  SyncClientRequestCopy *pMsg2 = syncClientRequestCopyDeserialize2(serialized, len);
  syncClientRequestCopyPrint2((char *)"test3: syncClientRequestCopySerialize3 -> syncClientRequestCopyDeserialize2 ",
                              pMsg2);

  taosMemoryFree(serialized);
  syncClientRequestCopyDestroy(pMsg);
  syncClientRequestCopyDestroy(pMsg2);
}

void test4() {
  SyncClientRequestCopy *pMsg = createMsg();
  SRpcMsg                rpcMsg;
  syncClientRequestCopy2RpcMsg(pMsg, &rpcMsg);
  SyncClientRequestCopy *pMsg2 = (SyncClientRequestCopy *)taosMemoryMalloc(rpcMsg.contLen);
  syncClientRequestCopyFromRpcMsg(&rpcMsg, pMsg2);
  syncClientRequestCopyPrint2((char *)"test4: syncClientRequestCopy2RpcMsg -> syncClientRequestCopyFromRpcMsg ", pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncClientRequestCopyDestroy(pMsg);
  syncClientRequestCopyDestroy(pMsg2);
}

void test5() {
  SyncClientRequestCopy *pMsg = createMsg();
  SRpcMsg                rpcMsg;
  syncClientRequestCopy2RpcMsg(pMsg, &rpcMsg);
  SyncClientRequestCopy *pMsg2 = syncClientRequestCopyFromRpcMsg2(&rpcMsg);
  syncClientRequestCopyPrint2((char *)"test5: syncClientRequestCopy2RpcMsg -> syncClientRequestCopyFromRpcMsg2 ",
                              pMsg2);

  rpcFreeCont(rpcMsg.pCont);
  syncClientRequestCopyDestroy(pMsg);
  syncClientRequestCopyDestroy(pMsg2);
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  logTest();

  test1();
  test2();
  test3();
  test4();
  test5();

  return 0;
}
