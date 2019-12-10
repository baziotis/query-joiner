#include <fcntl.h>
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

RelationData RelationData::from_binary_file(const char *filename) {
  int fd = ::open(filename, O_RDONLY);
  uint64_t header[2] = {0};
  read(fd, header, sizeof(header));
  uint64_t nr_rows = header[0];
  uint64_t nr_cols = header[1];
  RelationData data(nr_rows, nr_cols);
  for (size_t i = 0U; i != nr_rows; ++i) {
    Array<u64> cols(nr_cols);
    read(fd, cols.data, nr_cols * sizeof(uint64_t));
    cols.size = nr_cols;
    data.push(cols);
  }
  close(fd);
  return data;
}
