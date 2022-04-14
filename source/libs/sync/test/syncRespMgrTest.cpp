#include "syncRespMgr.h"
//#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}
SSyncRespMgr *pMgr = NULL;

void syncRespMgrInsert(SyncIndex begin, SyncIndex end) {
  for (SyncIndex i = begin; i <= end; ++i) {
    SRespStub stub;
    memset(&stub, 0, sizeof(SRespStub));
    stub.createTime = taosGetTimestampMs();
    stub.rpcMsg.code = 100 + i;
    stub.rpcMsg.ahandle = (void *)(200 + i);
    stub.rpcMsg.handle = (void *)(300 + i);
    int32_t ret = syncRespMgrAdd(pMgr, i, &stub);
    assert(ret == 0);
  }
}

void syncRespMgrDel(SyncIndex begin, SyncIndex end) {
  for (SyncIndex i = begin; i <= end; ++i) {
    int32_t ret = syncRespMgrDel(pMgr, i);
    assert(ret == 0);
  }
}

void printStub(SRespStub *p) {
  printf("createTime:%ld, rpcMsg.code:%d rpcMsg.ahandle:%ld rpcMsg.handle:%ld \n", p->createTime, p->rpcMsg.code,
         (int64_t)(p->rpcMsg.ahandle), (int64_t)(p->rpcMsg.handle));
}
void syncRespMgrPrint() {
  printf("\n-----------------------------------\n");
  taosThreadMutexLock(&(pMgr->mutex));

  SRespStub *p = (SRespStub *)taosHashIterate(pMgr->pRespHash, NULL);
  while (p) {
    printStub(p);
    p = (SRespStub *)taosHashIterate(pMgr->pRespHash, p);
  }

  taosThreadMutexUnlock(&(pMgr->mutex));
}

void syncRespMgrGetTest(SyncIndex i) {
  printf("------syncRespMgrGetTest------- \n");
  SRespStub stub;
  int32_t   ret = syncRespMgrGet(pMgr, i, &stub);
  if (ret == 1) {
    printStub(&stub);
  } else if (ret == 0) {
    printf("%ld notFound \n", i);
  }
}

void test1() {
  pMgr = syncRespMgrCreate(NULL, 0);
  assert(pMgr != NULL);

  syncRespMgrInsert(1, 10);
  syncRespMgrPrint();

  syncRespMgrDel(5, 7);
  syncRespMgrPrint();

  for (SyncIndex i = 1; i <= 10; ++i) {
    syncRespMgrGetTest(i);
  }

  syncRespMgrDestroy(pMgr);
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  logTest();
  test1();

  return 0;
}
