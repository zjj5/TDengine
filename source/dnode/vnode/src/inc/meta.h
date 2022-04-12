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

typedef struct STbDbKey     STbDbKey;
typedef struct SSkmDbKey    SSkmDbKey;
typedef struct SCtbIdxKey   SCtbIdxKey;
typedef struct SCtimeIdxKey SCtimeIdxKey;

// metaDebug ==================
// clang-format off
#define metaFatal(...) do { if (metaDebugFlag & DEBUG_FATAL) { taosPrintLog("META FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define metaError(...) do { if (metaDebugFlag & DEBUG_ERROR) { taosPrintLog("META ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define metaWarn(...)  do { if (metaDebugFlag & DEBUG_WARN)  { taosPrintLog("META WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define metaInfo(...)  do { if (metaDebugFlag & DEBUG_INFO)  { taosPrintLog("META ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define metaDebug(...) do { if (metaDebugFlag & DEBUG_DEBUG) { taosPrintLog("META ", DEBUG_DEBUG, metaDebugFlag, __VA_ARGS__); }} while(0)
#define metaTrace(...) do { if (metaDebugFlag & DEBUG_TRACE) { taosPrintLog("META ", DEBUG_TRACE, metaDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

// metaOpen ==================
int metaOpen(SVnode* pVnode, SMeta** ppMeta);
int metaClose(SMeta* pMeta);

// metaExe ==================
int metaBegin(SMeta* pMeta);
int metaCommit(SMeta* pMeta);

struct SMeta {
  char*   path;
  SVnode* pVnode;
  TENV*   pEnv;
  TDB*    pTbDB;
  TDB*    pSkmDB;
  TDB*    pNameIdx;
  TDB*    pStbIdx;
  TDB*    pNtbIdx;
  TDB*    pCtbIdx;
  TDB*    pCtimeIdx;
  // TODO: hash for tags
};

// pTbDB
struct __attribute__((__packed__)) STbDbKey {
  tb_uid_t uid;
  int64_t  ver;
};

// pSkmDB
struct __attribute__((__packed__)) SSkmDbKey {
  tb_uid_t uid;
  int32_t  sver;
};

// pCtbIdx
struct __attribute__((__packed__)) SCtbIdxKey {
  tb_uid_t suid;
  tb_uid_t uid;
};

// pCtimeIdx
struct __attribute__((__packed__)) SCtimeIdxKey {
  TSKEY    ctime;
  tb_uid_t uid;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_META_H_*/