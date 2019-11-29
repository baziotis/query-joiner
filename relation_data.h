#ifndef SORT_MERGE_JOIN__RELATIONDATA_H_
#define SORT_MERGE_JOIN__RELATIONDATA_H_

#include <cstdint>
#include <cstdio>
#include <zconf.h>
#include "array.h"

struct RelationData {
  explicit RelationData(u64 row_n, u64 col_n);

  Array<u64> &operator[](u64 index);

  u64 nrows() const;
  u64 ncolumns(u64 row_index) const;

  void print(int fd = STDOUT_FILENO, char delimiter = ' ');
  void print(FILE *fp = stdout, char delimiter = ' ');

  static RelationData from_binary_file(int fd);

 private:
  Array<Array<u64>> columns;
};

#endif //SORT_MERGE_JOIN__RELATIONDATA_H_
