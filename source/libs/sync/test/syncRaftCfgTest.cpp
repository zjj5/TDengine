#include "syncRaftStore.h"
//#include <gtest/gtest.h>
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftCfg.h"
#include "syncUtil.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

SSyncCfg* createSyncCfg() {
  SSyncCfg* pCfg = (SSyncCfg*)taosMemoryMalloc(sizeof(SSyncCfg));
  memset(pCfg, 0, sizeof(SSyncCfg));

  pCfg->replicaNum = 3;
  pCfg->myIndex = 1;
  for (int i = 0; i < pCfg->replicaNum; ++i) {
    ((pCfg->nodeInfo)[i]).nodePort = i * 100;
    snprintf(((pCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pCfg->nodeInfo)[i]).nodeFqdn), "100.200.300.%d", i);
  }

  return pCfg;
}

void test1() {
  SSyncCfg* pCfg = createSyncCfg();
  syncCfgPrint2((char*)__FUNCTION__, pCfg);
  taosMemoryFree(pCfg);
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();
  test1();

  return 0;
}