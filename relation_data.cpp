#include "relation_data.h"
#include "joinable.h"

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

Joinable RelationData::to_joinable(size_t key_index) {
  assert(key_index < this->size);
  Joinable joinable(this->size);
  for (size_t i = 0; i < this->size; ++i) {
    auto entry = joinable[i];
    entry.first = this->operator[](key_index)[i];
    entry.second = i;
  }
  return joinable;
}
