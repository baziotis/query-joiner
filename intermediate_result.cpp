#include <cassert>
#include "intermediate_result.h"

IntermediateResult::IntermediateResult(size_t max_column_n)
  : column_n(0), row_n(0), max_column_n(max_column_n) {
  this->columns = new uint64_t*[max_column_n];
  this->relation_map = new int[max_column_n];
  // Initialize to null means that these columns are not allocated yet.
  for (size_t i = 0; i < max_column_n; i++) {
    this->columns[i] = nullptr;
    this->relation_map[i] = RelationNotPresentYet;
  }
}

IntermediateResult::~IntermediateResult() {
  for (size_t i = 0; i < max_column_n; i++)
    if (this->is_allocated_column(i))
      delete this->columns[i];
  delete this->columns;
  delete this->relation_map;
}

uint64_t *IntermediateResult::get_column(size_t relation_index) {
  assert(relation_index < this->max_column_n);
  int index = this->relation_map[relation_index];
  assert(index < this->max_column_n);
  assert(this->is_allocated_column(index));
  return this->columns[index];
}

size_t IntermediateResult::column_count() {
  return this->column_n;
}

size_t IntermediateResult::row_count() {
  return this->row_n;
}

bool IntermediateResult::is_allocated_column(size_t index) {
  assert(index < this->max_column_n);
  return this->columns[index] != nullptr;
}

void IntermediateResult::deallocate_column(size_t index) {
  assert(index < this->max_column_n);
  assert(this->is_allocated_column(index));
  delete this->columns[index];
  this->columns[index] = nullptr;
  this->column_n--;
}

void IntermediateResult::allocate_column(size_t index, size_t rows) {
  assert(index < this->max_column_n);
  assert(!this->is_allocated_column(index));
  this->columns[index] = new uint64_t[rows];
}

size_t IntermediateResult::get_unallocated_column_index() {
  for (size_t i = 0; i < max_column_n; i++)
    if (!this->is_allocated_column(i))
      return i;
  assert(false);
  return -1;
}

void IntermediateResult::assign_relation_to_column(size_t relation_index, size_t column_index) {
  this->relation_map[relation_index] = column_index;
}

void IntermediateResult::unassign_column(size_t column_index) {
  assert(this->relation_map[column_index] != RelationNotPresentYet);
  this->relation_map[column_index] = RelationNotPresentYet;
}

