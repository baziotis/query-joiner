#include <fcntl.h>
#include "relation_data.h"
#include "report_utils.h"
#include "utils.h"
#include "pair.h"

RelationData::RelationData(u64 row_n, u64 col_n) : columns(row_n) {
  for (size_t i = 0U; i != (size_t)col_n; ++i) {
    columns.push(Array<u64>(col_n));
  }
}

u64 RelationData::nrows() const {
  return columns.size;
}

u64 RelationData::ncolumns(u64 row_index) const {
  return columns[row_index].size;
}

Array<u64> &RelationData::operator[](u64 index) {
  return columns[index];
}

void RelationData::print(int fd, char delimiter) {
  freport(fd, "%lu %lu\n", columns.size, columns[0].size);
  for (auto &column : columns) {
    size_t i = 0U;
    for (u64 column_value : column) {
      freport(fd, "%lu", column_value.v);
      if (i != column.size - 1U) {
        freport(fd, "%c", delimiter);
      } else {
        freport(fd);
      }
      ++i;
    }
  }
}

void RelationData::print(FILE *fp, char delimiter) {
  print(fp->_fileno, delimiter);
}

RelationData RelationData::from_binary_file(int fd) {
  Pair<size_t, size_t> header{};
  read(fd, &header, sizeof(header));
  RelationData data{header.first, header.second};

  for (Array<u64> &column : data.columns) {
    read(fd, &column.data, header.second * sizeof(u64));
    column.size = header.second;
  }

  close(fd);

  return data;
}

