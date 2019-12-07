#ifndef SORT_MERGE_JOIN__RELATION_DATA_H_
#define SORT_MERGE_JOIN__RELATION_DATA_H_

#include <cstdint>
#include <cstdio>
#include "array.h"
#include "common.h"

struct RelationData : public Array<Array<u64>> {
  RelationData(uint64_t row_n, uint64_t col_n);
  void print(FILE *fp = stdout, char delimiter = ' ');
};

#endif //SORT_MERGE_JOIN__RELATION_DATA_H_
