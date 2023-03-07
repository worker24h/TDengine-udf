#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <inttypes.h>
#include "taosudf.h"

#define precent_memcmp(s1, s2, size) \
        memcmp(s1, s2, size) == 0 ? 1 : 0 

typedef struct udf_perecent {
  int32_t count;
  int32_t totalCount;
} UdfPercent;


DLL_EXPORT int32_t udf_percent_init() {
  return 0;
}

DLL_EXPORT int32_t udf_percent_destroy() {
  return 0;
}

DLL_EXPORT int32_t udf_percent_start(SUdfInterBuf *buf) {
  UdfPercent* percent = (UdfPercent*)(buf->buf);
  percent->count = 0;
  percent->totalCount = 0;
  buf->bufLen = sizeof(UdfPercent);
  buf->numOfResult = 0;
  return 0;
}

DLL_EXPORT int32_t udf_percent(SUdfDataBlock* block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
  if (block->numOfCols != 2) {
    return TSDB_CODE_UDF_INVALID_INPUT;
  }
  int32_t numNotNull = 0;

  //save total count
  UdfPercent* percent = (UdfPercent*)(interBuf->buf);
  percent->totalCount += block->numOfRows;
  
  // acquire target value from index `1`
  int index = 1;
  SUdfColumn *targetCol = block->udfCols[index];
  char* targetValue = udfColDataGetData(targetCol, index);

  
  index = 0;
  SUdfColumn *col = block->udfCols[index];
  for (int32_t j = 0; j < block->numOfRows; j++) {
    if (udfColDataIsNull(col, j)) {//不考虑空值null
        continue;
    }
    char* cell = udfColDataGetData(col, j);
    switch (col->colMeta.type) {
      case TSDB_DATA_TYPE_BOOL: 
      case TSDB_DATA_TYPE_TINYINT: 
      case TSDB_DATA_TYPE_UTINYINT:
        percent->count += precent_memcmp(cell, targetValue, 1); // 1 byte
        break;
      
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT:
        percent->count += precent_memcmp(cell, targetValue, 2); // 2 byte
        break;
      
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_UINT:
        percent->count += precent_memcmp(cell, targetValue, 4); // 4 byte
        break;
      
      case TSDB_DATA_TYPE_BIGINT: 
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_UBIGINT:
        percent->count += precent_memcmp(cell, targetValue, 8); // 8 byte
        break;
      
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_NCHAR: {
        int length = strlen(cell);
        if (strlen(targetValue) == length) {
          percent->count += precent_memcmp(cell, targetValue, length);
        }
        break;
      }
      default: 
        break;
    }
    ++numNotNull;
  }
  UdfPercent* output = (UdfPercent*)(newInterBuf->buf);
  output->totalCount = percent->totalCount;
  output->count = percent->count;
  if (interBuf->numOfResult == 0 && numNotNull == 0) {
    newInterBuf->numOfResult = 0;
  } else {
    newInterBuf->numOfResult = 1;
  }
  return 0;
}

DLL_EXPORT int32_t udf_percent_finish(SUdfInterBuf* buf, SUdfInterBuf *resultData) {
  if (buf->numOfResult == 0) {
    resultData->numOfResult = 0;
    return 0;
  }
  UdfPercent* percent = (UdfPercent*)(buf->buf);
  if (percent->totalCount == 0) {
    resultData->numOfResult = 0;
    return 0;
  }

  *(double*)(resultData->buf) = (double)percent->count / (double)percent->totalCount;
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;
  
  return 0;
}



