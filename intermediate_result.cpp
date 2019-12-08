#include <cassert>
#include "intermediate_result.h"

IntermediateResult::IntermediateResult(RelationStorage &rs, size_t max_column_n)
  : relation_storage(rs), column_n(0), row_n(0), max_column_n(max_column_n) {
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

bool IntermediateResult::is_empty() {
  return this->column_n == 0;
}

Joinable IntermediateResult::to_joinable(size_t relation_index, size_t key_index) {
  assert(column_is_allocated(relation_index));
  // Oddly this is the number of columns of relation at "relation_index".
  assert(key_index < relation_storage[relation_index].size);
  Joinable joinable(this->row_n);
  RelationData target_relation = relation_storage[relation_index];
  for (size_t i = 0; i < this->row_n; ++i) {
    auto entry = joinable[i];
    entry.first = target_relation[key_index][i];
    entry.second = this->columns[relation_index][i];
  }
  return joinable;
}

void IntermediateResult::execute_join(size_t left_relation_index,
                                      size_t left_key_index,
                                      size_t right_relation_index,
                                      size_t right_key_index) {
  assert(left_relation_index < this->max_column_n);
  assert(right_relation_index < this->max_column_n);

  if (column_is_allocated(left_relation_index) && column_is_allocated(right_relation_index)) {
    // In this case both relations are already present in the intermediate result.
    join_existing_relations(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (!column_is_allocated(left_relation_index) && !column_is_allocated(right_relation_index)) {
    // This case should occur only once, when the intermediate result is empty.
    join_initial_relations(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (column_is_allocated(left_relation_index) && !column_is_allocated(right_relation_index)) {
    // This is the common case where one of the join relations exist in the ir
    // and the other one is new...
    join_common_relations(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (!column_is_allocated(left_relation_index) && column_is_allocated(right_relation_index)) {
    // This is the common case where one of the join relations exist in the ir
    // and the other one is new...
    join_common_relations(
        right_relation_index, right_key_index,
        left_relation_index, left_key_index);
  }
}
void IntermediateResult::join_initial_relations(size_t left_relation_index,
                                                size_t left_key_index,
                                                size_t right_relation_index,
                                                size_t right_key_index) {
  // Because this is the initial join, make sure the ir is empty.
  // Otherwise the state of the ir is not valid.
  assert(this->is_empty());
  // Get the two relations to join as joinables.
  Joinable r1 = relation_storage[left_relation_index].to_joinable(left_key_index);
  Joinable r2 = relation_storage[right_relation_index].to_joinable(right_key_index);
  Join join;
  auto result = join(r1, r2);
  auto *left_column = new uint64_t[result.len];
  auto *right_column = new uint64_t[result.len];
  for (size_t i = 0; i < result.len; ++i) {
    left_column[i] = result[i].first;
    right_column[i] = result[i].second;
  }
  columns[left_relation_index] = left_column;
  columns[right_relation_index] = right_column;
  this->row_n = result.len;
  this->column_n = 2;
}

void IntermediateResult::join_common_relations(size_t existing_relation_index,
                                               size_t existing_relation_key_index,
                                               size_t new_relation_index,
                                               size_t new_relation_key_index) {
  assert(column_is_allocated(existing_relation_index));
  assert(!column_is_allocated(new_relation_index));
  Joinable r_left = this->to_joinable(existing_relation_index, existing_relation_key_index);
  Joinable r_right = relation_storage[new_relation_index].to_joinable(new_relation_key_index);
  Join join;
  auto result = join(r_left, r_right);
  // Loop for the allocated existing coluns.
  for (size_t j = 0; j < this->max_column_n; ++j) {
    if (!column_is_allocated(j))
      continue;
    auto *existing_column = new uint64_t[result.len];
    for (size_t i = 0; i < result.len; ++i) {
      // Row id of the join result is index to the tuple of the ir.
      existing_column[i] = columns[j][(uint64_t)result[i].first];
    }
    delete columns[j];
    columns[j] = existing_column;
  }
  // Double check...
  assert(!column_is_allocated(new_relation_index));
  auto *new_column = new uint64_t[result.len];
  for (size_t i = 0; i < result.len; ++i) {
    new_column[i] = result[i].second;
  }
  this->columns[new_relation_index] = new_column;
  this->column_n++;
  this->row_n = result.len;
}

void IntermediateResult::join_existing_relations(size_t left_relation_index,
                                                 size_t left_key_index,
                                                 size_t right_relation_index,
                                                 size_t right_key_index) {
  assert(false);
  // TODO maybe this case can be handled as a WHERE clause... (possibly at parse time).
}


