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

#ifndef _TD_VNODE_META_H_
#define _TD_VNODE_META_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SMetaCache SMetaCache;
typedef struct SMetaIdx   SMetaIdx;
typedef struct SMetaDB    SMetaDB;

// SMetaDB
int  metaOpenDB(SMeta* pMeta);
void metaCloseDB(SMeta* pMeta);
int  metaSaveTableToDB(SMeta* pMeta, STbCfg* pTbCfg);
int  metaRemoveTableFromDb(SMeta* pMeta, tb_uid_t uid);
int  metaSaveSmaToDB(SMeta* pMeta, STSma* pTbCfg);
int  metaRemoveSmaFromDb(SMeta* pMeta, int64_t indexUid);

// SMetaCache
int  metaOpenCache(SMeta* pMeta);
void metaCloseCache(SMeta* pMeta);

// SMetaCfg
extern const SMetaCfg defaultMetaOptions;
// int                   metaValidateOptions(const SMetaCfg*);
void metaOptionsCopy(SMetaCfg* pDest, const SMetaCfg* pSrc);

// SMetaIdx
int  metaOpenIdx(SMeta* pMeta);
void metaCloseIdx(SMeta* pMeta);
int  metaSaveTableToIdx(SMeta* pMeta, const STbCfg* pTbOptions);
int  metaRemoveTableFromIdx(SMeta* pMeta, tb_uid_t uid);

// STbUidGnrt
typedef struct STbUidGenerator {
  tb_uid_t nextUid;
} STbUidGenerator;

// STableUidGenerator
int  metaOpenUidGnrt(SMeta* pMeta);
void metaCloseUidGnrt(SMeta* pMeta);

// tb_uid_t
#define IVLD_TB_UID 0
tb_uid_t metaGenerateUid(SMeta* pMeta);

struct SMeta {
  char*                 path;
  SMetaCfg              options;
  SMetaDB*              pDB;
  SMetaIdx*             pIdx;
  SMetaCache*           pCache;
  STbUidGenerator       uidGnrt;
  SMemAllocatorFactory* pmaf;
};

SMeta* metaOpen(const char* path, const SMetaCfg* pMetaCfg, SMemAllocatorFactory* pMAF);
void   metaClose(SMeta* pMeta);

// clang-format off
#define metaFatal(...) do { if (metaDebugFlag & DEBUG_FATAL) { taosPrintLog("META FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define metaError(...) do { if (metaDebugFlag & DEBUG_ERROR) { taosPrintLog("META ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define metaWarn(...)  do { if (metaDebugFlag & DEBUG_WARN)  { taosPrintLog("META WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define metaInfo(...)  do { if (metaDebugFlag & DEBUG_INFO)  { taosPrintLog("META ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define metaDebug(...) do { if (metaDebugFlag & DEBUG_DEBUG) { taosPrintLog("META ", DEBUG_DEBUG, metaDebugFlag, __VA_ARGS__); }} while(0)
#define metaTrace(...) do { if (metaDebugFlag & DEBUG_TRACE) { taosPrintLog("META ", DEBUG_TRACE, metaDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_META_H_*/