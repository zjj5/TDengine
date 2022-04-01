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
  char * serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

int32_t syncCfgFromJson(const cJSON *pJson, SSyncCfg *pSyncCfg) {}

int32_t syncCfgFromStr(const char *s, SSyncCfg *pSyncCfg) {}

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