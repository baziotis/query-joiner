#ifndef SORT_MERGE_JOIN__RELATION_DATA_H_
#define SORT_MERGE_JOIN__RELATION_DATA_H_

#include <cstdint>
#include <cstdio>

class RelationData {
 public:
  RelationData(uint64_t row_n, uint64_t col_n);
  ~RelationData();
  uint64_t *get_column(uint64_t index);
  uint64_t set_value(uint64_t row_index, uint64_t col_index, uint64_t value);
  uint64_t get_value(uint64_t row_index, uint64_t col_index);
  uint64_t row_count();
  uint64_t col_count();
  void print(FILE *fp = stdout, char delimiter = ' ');
 private:
  uint64_t **columns;
  uint64_t row_n;
  uint64_t col_n;
};

#endif //SORT_MERGE_JOIN__RELATION_DATA_H_
