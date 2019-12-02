#include <cassert>
#include "intermediate_result.h"

IntermediateResult::IntermediateResult(size_t max_column_n)
  : column_n(0), row_n(0), max_column_n(max_column_n) {
  this->columns = new uint64_t*[max_column_n];
  // Initialize to null means that these columns are not allocated yet.
  for (size_t i = 0; i < max_column_n; i++) {
    this->columns[i] = nullptr;
  }
}

IntermediateResult::~IntermediateResult() {
  for (size_t i = 0; i < max_column_n; i++)
    if (this->column_is_allocated(i))
      delete this->columns[i];
  delete this->columns;
}

uint64_t *IntermediateResult::get_column(size_t relation_index) {
  assert(relation_index < this->max_column_n);
  assert(this->column_is_allocated(relation_index));
  return this->columns[relation_index];
}

size_t IntermediateResult::column_count() {
  return this->column_n;
}

size_t IntermediateResult::row_count() {
  return this->row_n;
}

bool IntermediateResult::column_is_allocated(size_t relation_index) {
  assert(relation_index < this->max_column_n);
  return this->columns[relation_index] != nullptr;
}

void IntermediateResult::allocate_column(size_t relation_index, size_t rows) {
  assert(relation_index < this->max_column_n);
  assert(!this->column_is_allocated(relation_index));
  this->columns[relation_index] = new uint64_t[rows];
  this->column_n++;
  this->row_n = rows;
}

void IntermediateResult::deallocate_column(size_t relation_index) {
  assert(relation_index < this->max_column_n);
  assert(this->column_is_allocated(relation_index));
  this->columns[relation_index] = nullptr;
  this->column_n--;
}
bool IntermediateResult::is_empty() {
  return this->column_n == 0;
}
