#ifndef SORT_MERGE_JOIN__RELATION_DATA_H_
#define SORT_MERGE_JOIN__RELATION_DATA_H_

#include <cstdint>
#include <cstdio>
#include "array.h"
#include "common.h"
#include "joinable.h"
#include "parse.h"

struct RelationData : public Array<Array<u64>> {
  RelationData(uint64_t row_n, uint64_t col_n);
  /**
   * Creates a joinable object from relation data.
   * The "key_index" specifies which column will be used as a join column.
   * The row_ids of the joinable are the index of each relation data tuple.
   *
   * @param key_index The index of the join column.
   * @return A Joinable object.
   */
  Joinable to_joinable(size_t key_index, StretchyBuf<Predicate> filter_predicates);
  void print(FILE *fp = stdout, char delimiter = ' ');

  static RelationData from_binary_file(const char *filename);
};

#endif //SORT_MERGE_JOIN__RELATION_DATA_H_
