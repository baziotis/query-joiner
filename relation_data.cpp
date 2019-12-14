#include <fcntl.h>
#include "relation_data.h"
#include "joinable.h"

RelationData::RelationData(uint64_t row_n, uint64_t col_n) : Array(col_n) {
  for (size_t i = 0U; i != col_n; ++i) {
    this->push(Array<u64>(row_n));
  }
}

void RelationData::free() {
  for (Array<u64> &cols : *this) {
    cols.clear_and_free();
  }
  clear_and_free();
}

void RelationData::print(FILE *fp, char delimiter) {
  size_t col_n = this->size;
  size_t row_n =  (*this)[0].size;
  fprintf(fp, "%ld %ld\n", col_n, row_n);

  for (size_t i = 0; i < row_n; i++) {
    for (size_t j = 0; j < col_n; j++) {
      fprintf(fp, "%lu", this->operator[](j).operator[](i).v);
      if (j == col_n - 1) {
        fprintf(fp, "\n");
      } else {
        fprintf(fp, "%c", delimiter);
      }
    }
  }
}

Joinable RelationData::to_joinable(size_t key_index, StretchyBuf<Predicate> filter_predicates) {
  assert(key_index < this->size);
  size_t row_n = this->operator[](0).size;
  StretchyBuf<JoinableEntry> list;
  for (size_t i = 0; i < row_n; ++i) {
    bool tuple_is_match = true;
    for (auto filter: filter_predicates) {
      auto compare_value = this->operator[](filter.lhs.second)[i];
      switch (filter.op) {
        case '>':
          tuple_is_match &= compare_value.v > filter.filter_val;
          break;
        case '<':
          tuple_is_match &= compare_value.v < filter.filter_val;
          break;
        case '=':
          tuple_is_match &= compare_value.v == filter.filter_val;
          break;
        default:
          assert(false); // Not so good.
          break;
      }
    }
    if (tuple_is_match) {
      JoinableEntry entry {this->operator[](key_index)[i], i};
      list.push(entry);
    }
  }
  // Convert list to array.
  if (list.len == 0)
    return Joinable::empty();
  Joinable joinable(list.len);
  for (auto e: list) {
    joinable.push(e);
  }
  list.free();
  return joinable;
}

RelationData RelationData::from_binary_file(const char *filename) {
  int fd = ::open(filename, O_RDONLY);
  uint64_t header[2] = {0};
  read(fd, header, sizeof(header));
  uint64_t nr_rows = header[0];
  uint64_t nr_cols = header[1];
  RelationData data(nr_rows, nr_cols);
  for (size_t i = 0U; i != nr_cols; ++i) {
    Array<u64> row(nr_rows);
    read(fd, row.data, nr_rows * sizeof(uint64_t));
    row.size = nr_rows;
    data[i] = row;
  }
  close(fd);
  return data;
}

