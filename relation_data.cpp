#include "relation_data.h"

uint64_t RelationData::col_count() {
  return col_n;
}

uint64_t RelationData::row_count() {
  return row_n;
}

uint64_t RelationData::get_value(uint64_t row_index, uint64_t col_index) {
  return columns[col_index][row_index];
}

uint64_t RelationData::set_value(uint64_t row_index, uint64_t col_index, uint64_t value) {
  this->columns[col_index][row_index] = value;
}

uint64_t *RelationData::get_column(uint64_t index) {
  return columns[index];
}

RelationData::RelationData(uint64_t row_n, uint64_t col_n)
  : row_n(row_n), col_n(col_n) {
  this->columns = new uint64_t*[col_n];
  for (uint64_t i = 0; i < col_n; ++i) {
    this->columns[i] = new uint64_t[row_n];
  }
}
RelationData::~RelationData() {
  for (uint64_t i = 0; i < col_n; ++i) {
    delete columns[i];
  }
  delete columns;
}
void RelationData::print(FILE *fp, char delimiter) {
  fprintf(fp, "%ld %ld\n", this->row_n, this->col_n);
  for (size_t r = 0; r < this->row_n; ++r) {
    for (size_t c = 0; c < this->col_n; ++c) {
      fprintf(fp, "%ld", this->columns[c][r]);
      if (c == this->col_n - 1) {
        fprintf(fp, "\n");
      } else {
        fprintf(fp, "%c", delimiter);
      }
    }
  }
}
