#include "relation_data.h"

RelationData::RelationData(uint64_t row_n, uint64_t col_n) : Array(row_n) {
  for (size_t i = 0U; i != row_n; ++i) {
    this->push(Array<u64>(col_n));
  }
}

void RelationData::print(FILE *fp, char delimiter) {
  fprintf(fp, "%ld %ld\n", this->size, (*this)[0].size);
  for (Array<u64> cols : *this) {
    size_t i = 0U;
    for (u64 c : cols) {
      fprintf(fp, "%lu", c.v);
      if (i == cols.size - 1) {
        fprintf(fp, "\n");
      } else {
        fprintf(fp, "%c", delimiter);
      }
    }
  }
}
