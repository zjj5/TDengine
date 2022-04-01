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

#include "syncRaftCfg.h"
#include "cJSON.h"
#include "syncEnv.h"
#include "syncUtil.h"

SRaftCfg *raftCfgOpen(const char *path) {}

int32_t raftCfgClose(SRaftCfg *pRaftCfg) {}

int32_t raftCfgPersist(SRaftCfg *pRaftCfg) {}

cJSON *syncCfg2Json(SSyncCfg *pSyncCfg) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pSyncCfg != NULL) {
    cJSON_AddNumberToObject(pRoot, "replicaNum", pSyncCfg->replicaNum);
    cJSON_AddNumberToObject(pRoot, "myIndex", pSyncCfg->myIndex);

    cJSON *pNodeInfoArr = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "nodeInfo", pNodeInfoArr);
    for (int i = 0; i < pSyncCfg->replicaNum; ++i) {
      cJSON *pNodeInfo = cJSON_CreateObject();
      cJSON_AddNumberToObject(pNodeInfo, "nodePort", ((pSyncCfg->nodeInfo)[i]).nodePort);
      cJSON_AddStringToObject(pNodeInfo, "nodeFqdn", ((pSyncCfg->nodeInfo)[i]).nodeFqdn);
      cJSON_AddItemToArray(pNodeInfoArr, pNodeInfo);
    }
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncCfg", pRoot);
  return pJson;
}

char *syncCfg2Str(SSyncCfg *pSyncCfg) {
  cJSON *pJson = syncCfg2Json(pSyncCfg);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

int32_t syncCfgFromJson(const cJSON *pRoot, SSyncCfg *pSyncCfg) {
  memset(pSyncCfg, 0, sizeof(SSyncCfg));
  cJSON *pJson = cJSON_GetObjectItem(pRoot, "SSyncCfg");

  cJSON *pReplicaNum = cJSON_GetObjectItem(pJson, "replicaNum");
  assert(cJSON_IsNumber(pReplicaNum));
  pSyncCfg->replicaNum = cJSON_GetNumberValue(pReplicaNum);

  cJSON *pMyIndex = cJSON_GetObjectItem(pJson, "myIndex");
  assert(cJSON_IsNumber(pMyIndex));
  pSyncCfg->myIndex = cJSON_GetNumberValue(pMyIndex);

  cJSON *pNodeInfoArr = cJSON_GetObjectItem(pJson, "nodeInfo");
  int    arraySize = cJSON_GetArraySize(pNodeInfoArr);
  assert(arraySize == pSyncCfg->replicaNum);

  for (int i = 0; i < arraySize; ++i) {
    cJSON *pNodeInfo = cJSON_GetArrayItem(pNodeInfoArr, i);
    assert(pNodeInfo != NULL);

    cJSON *pNodePort = cJSON_GetObjectItem(pNodeInfo, "nodePort");
    assert(cJSON_IsNumber(pNodePort));
    ((pSyncCfg->nodeInfo)[i]).nodePort = cJSON_GetNumberValue(pNodePort);

    cJSON *pNodeFqdn = cJSON_GetObjectItem(pNodeInfo, "nodeFqdn");
    assert(cJSON_IsString(pNodeFqdn));
    snprintf(((pSyncCfg->nodeInfo)[i]).nodeFqdn, sizeof(((pSyncCfg->nodeInfo)[i]).nodeFqdn), "%s",
             pNodeFqdn->valuestring);
  }

  return 0;
}

int32_t syncCfgFromStr(const char *s, SSyncCfg *pSyncCfg) {
  cJSON *pRoot = cJSON_Parse(s);
  assert(pRoot != NULL);

  int32_t ret = syncCfgFromJson(pRoot, pSyncCfg);
  assert(ret == 0);

  cJSON_Delete(pRoot);
  return 0;
}

cJSON *raftCfg2Json(SRaftCfg *pRaftCfg) {}

char *raftCfg2Str(SRaftCfg *pRaftCfg) {}

int32_t raftCfgSerialize(SRaftCfg *pRaftCfg, char *buf, size_t len) {}

int32_t raftCfgDeserialize(SRaftCfg *pRaftCfg, char *buf, size_t len) {}

int32_t syncCfgCreateFile(SSyncCfg *cfg, const char *path) {}

// for debug ----------------------
void syncCfgPrint(SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  printf("syncCfgPrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncCfgPrint2(char *s, SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  printf("syncCfgPrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  taosMemoryFree(serialized);
}

void syncCfgLog(SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  sTrace("syncCfgLog | len:%lu | %s", strlen(serialized), serialized);
  taosMemoryFree(serialized);
}

void syncCfgLog2(char *s, SSyncCfg *pCfg) {
  char *serialized = syncCfg2Str(pCfg);
  sTrace("syncCfgLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  taosMemoryFree(serialized);
}