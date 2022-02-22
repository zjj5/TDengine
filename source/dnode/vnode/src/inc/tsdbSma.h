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

#ifndef _TD_TSDB_SMA_H_
#define _TD_TSDB_SMA_H_

// The function Ids available for SMA calculation should occupy the 2^n numbers
#define SMA_FUNC_AVG  0x01U
#define SMA_FUNC_SUM  0x02U
#define SMA_FUNC_MIN  0x04U
#define SMA_FUNC_MAX  0x08U
#define SMA_FUNC_LAST 0x10U
// TBD
#define SMA_FUNC_MAX_SIZE (1 << 63U)

typedef enum {
  TD_TIME_UNIT_YEAR = 0,
  TD_TIME_UNIT_MONTH = 1,     //
  TD_TIME_UNIT_DAY = 2,       //
  TD_TIME_UNIT_HOUR = 3,      //
  TD_TIME_UNIT_MINUTE = 4,    //
  TD_TIME_UNIT_SEC = 5,       //
  TD_TIME_UNIT_MILISEC = 6,   //
  TD_TIME_UNIT_MICROSEC = 7,  //
  TD_TIME_UNIT_NANOSEC = 8
} ETDTimeUnit;

typedef uint64_t sma_func_t;
typedef struct {
  uint64_t   tableUid;
  int64_t    interval;
  int64_t    sliding;
  sma_func_t smaFuncId;
  col_id_t   colId;
  uint8_t    intervalUnit;
  uint8_t    slidingUnit;
} STimeRangeSmaItem;

typedef struct {
  uint64_t tableUid;
  int64_t  interval;
  int64_t  sliding;
  // TODO: use definition from schema =>
  col_id_t *  colIds;     // sorted column ids
  sma_func_t *smaFuncId;  // sorted sma function ids
  col_id_t    numOfColId;
  uint16_t    numOfFuncId;
  uint8_t     intervalUnit;
  uint8_t     slidingUnit;
  // TODO: use definition from schema <=
} STimeRangeSma;
//  STimeRangeSma *param;

typedef struct {
  STimeWindow tsWindow;       // [skey, ekey]
  int32_t     numOfSmaBlock;  // total number of sma blocks. The sma blocks for each column is numOfSmaBlock/numOfColId.
  int32_t     dataLen;        // total data length
  col_id_t *  colIds;         // e.g. 2,4,9,10
  col_id_t    numOfColId;
  char        data[];
} STimeRangeData;

// TimeRangeSma的最大粒度为天。因为原始TS数据是以天粒度标识的，例如，days=7表示7天的数据保存在一个文件组中。

static FORCE_INLINE uint64_t tsdbEncodeFuncIds(sma_func_t *smaFuncId, int32_t nFuncIds) { return 0; }

int32_t tsdbInsertTSmaDataImpl(STsdb *pTsdb, STimeRangeSma *param, STimeRangeData *pData);
int32_t tsdbInsertRSmaData(STsdb *pTsdb);

#if 0

typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

typedef struct {
  uint64_t uid;
  int64_t  offset;
  int64_t  size;
} SKVRecord;

void tsdbGetRtnSnap(STsdb *pRepo, SRtn *pRtn);

static FORCE_INLINE int TSDB_KEY_FID(TSKEY key, int32_t days, int8_t precision) {
  if (key < 0) {
    return (int)((key + 1) / tsTickPerDay[precision] / days - 1);
  } else {
    return (int)((key / tsTickPerDay[precision] / days));
  }
}

static FORCE_INLINE int tsdbGetFidLevel(int fid, SRtn *pRtn) {
  if (fid >= pRtn->maxFid) {
    return 0;
  } else if (fid >= pRtn->midFid) {
    return 1;
  } else if (fid >= pRtn->minFid) {
    return 2;
  } else {
    return -1;
  }
}

#define TSDB_DEFAULT_BLOCK_ROWS(maxRows) ((maxRows)*4 / 5)

int   tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord);
void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord);
void *tsdbCommitData(STsdbRepo *pRepo);
int   tsdbApplyRtnOnFSet(STsdbRepo *pRepo, SDFileSet *pSet, SRtn *pRtn);
int tsdbWriteBlockInfoImpl(SDFile *pHeadf, STable *pTable, SArray *pSupA, SArray *pSubA, void **ppBuf, SBlockIdx *pIdx);
int tsdbWriteBlockIdx(SDFile *pHeadf, SArray *pIdxA, void **ppBuf);
int tsdbWriteBlockImpl(STsdbRepo *pRepo, STable *pTable, SDFile *pDFile, SDataCols *pDataCols, SBlock *pBlock,
                       bool isLast, bool isSuper, void **ppBuf, void **ppCBuf);
int   tsdbApplyRtn(STsdbRepo *pRepo);

#endif

#endif /* _TD_TSDB_SMA_H_ */