#include <cassert>
#include "intermediate_result.h"

IntermediateResult::IntermediateResult(RelationStorage &rs, ParseQueryResult &pqr, size_t max_column_n)
  : relation_storage(rs), parse_query_result(pqr), column_n(0),
  row_n(0), max_column_n(max_column_n) {

}

IntermediateResult::~IntermediateResult() {
  this->clear_and_free();
}

StretchyBuf<u64> &IntermediateResult::get_column(size_t relation_index) {
  assert(relation_index < this->max_column_n);
  assert(this->column_is_allocated(relation_index));
  return this->operator[](relation_index);
}

size_t IntermediateResult::column_count() {
  return this->column_n;
}

size_t IntermediateResult::row_count() {
  return this->row_n;
}

bool IntermediateResult::column_is_allocated(size_t relation_index) {
  assert(relation_index < this->max_column_n);
  return this->get_column(relation_index).data != nullptr;
}

bool IntermediateResult::is_empty() {
  return this->column_n == 0;
}

Joinable IntermediateResult::to_joinable(size_t relation_index, size_t key_index) {
  assert(column_is_allocated(relation_index));
  // Oddly this is the number of columns of relation at "relation_index".
  assert(key_index < relation_storage[relation_index].size);
  auto filter_predicates = get_relation_filters(relation_index);
  Joinable joinable;
  RelationData target_relation = relation_storage[relation_index];
  for (size_t i = 0; i < this->row_n; ++i) {
    bool tuple_is_match = true;
    for (auto filter: filter_predicates) {
      auto compare_value = target_relation[filter.lhs.second][filter.lhs.first];
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
      JoinableEntry entry {
          target_relation[key_index][i],
          this->operator[](relation_index)[i]
      };
      joinable.push(entry);
    }
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
    execute_join_as_filter(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (!column_is_allocated(left_relation_index) && !column_is_allocated(right_relation_index)) {
    // This case should occur only once, when the intermediate result is empty.
    execute_initial_join(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (column_is_allocated(left_relation_index) && !column_is_allocated(right_relation_index)) {
    // This is the common case where one of the join relations exist in the ir
    // and the other one is new...
    execute_common_join(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (!column_is_allocated(left_relation_index) && column_is_allocated(right_relation_index)) {
    // This is the common case where one of the join relations exist in the ir
    // and the other one is new...
    execute_common_join(
        right_relation_index, right_key_index,
        left_relation_index, left_key_index);
  }
}
void IntermediateResult::execute_initial_join(size_t left_relation_index,
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
  auto join_result = join(r1, r2);
  StretchyBuf<u64> column1;
  StretchyBuf<u64> column2;
  for (auto join_row: join_result) {
    auto rowid_1 = join_row.first;
    for (auto rowid_2: join_row.second) {
      column1.push(rowid_1);
      column2.push(rowid_2);
    }
  }
  this->operator[](left_relation_index) = column1;
  this->operator[](right_relation_index) = column2;
  this->column_n = 2;
  this->row_n = column1.len;
}

void IntermediateResult::execute_common_join(size_t existing_relation_index,
                                             size_t existing_relation_key_index,
                                             size_t new_relation_index,
                                             size_t new_relation_key_index) {
  assert(column_is_allocated(existing_relation_index));
  assert(!column_is_allocated(new_relation_index));
  Joinable r_left = this->to_joinable(existing_relation_index, existing_relation_key_index);
  Joinable r_right = relation_storage[new_relation_index].to_joinable(new_relation_key_index);
  Join join;
  auto join_result = join(r_left, r_right);
  // Loop for the allocated existing columns.
  for (size_t j = 0; j < this->max_column_n; ++j) {
    if (!column_is_allocated(j))
      continue;
    StretchyBuf<u64> aux_column(join_result.len);
    auto current_column = this->get_column(j);
    for (auto join_row: join_result) {
      auto rowid_1 = join_row.first;
      size_t len = join_row.second.len;
      while (len--) { // For each item of the second list.
        aux_column.push(current_column[rowid_1]);
      }
    }
    current_column.free();
    this->operator[](j) = aux_column;
  }
  // Double check...
  assert(!column_is_allocated(new_relation_index));

  StretchyBuf<u64> aux_column(join_result.len);
  for (auto join_row: join_result) {
    for (auto rowid_2: join_row.second) { // For each item of the second list.
      aux_column.push(rowid_2);
    }
  }
  this->operator[](new_relation_index) = aux_column;
  this->column_n++;
  this->row_n = aux_column.len;
}

void IntermediateResult::execute_join_as_filter(size_t left_relation_index,
                                                size_t left_key_index,
                                                size_t right_relation_index,
                                                size_t right_key_index) {
  assert(column_is_allocated(left_relation_index));
  assert(column_is_allocated(right_relation_index));
  // Find the row_ids of the ir that match the filter.
  StretchyBuf<size_t> ir_rowids;
  for (size_t i = 0; i < this->row_n; i++) {
    auto left_rowid = this->operator[](left_relation_index)[i];
    auto left_value = relation_storage[left_relation_index][left_key_index][left_rowid];
    auto right_rowid = this->operator[](right_relation_index)[i];
    auto right_value = relation_storage[right_relation_index][right_key_index][right_rowid];
    if (left_value == right_value) {
      ir_rowids.push(i);
    }
  }
  // Loop for the allocated existing columns.
  for (size_t j = 0; j < this->max_column_n; ++j) {
    if (!column_is_allocated(j))
      continue;
    StretchyBuf<u64> aux_column(ir_rowids.len);
    auto current_column = this->get_column(j);
    for (auto ir_rowid: ir_rowids) {
      aux_column.push(current_column[ir_rowid]);
    }
    current_column.free();
    this->operator[](j) = aux_column;
  }
  this->row_n = ir_rowids.len;
}

StretchyBuf<uint64_t> IntermediateResult::execute_select(StretchyBuf<Pair<size_t, size_t>> relation_column_pairs) {
  StretchyBuf<uint64_t> result;
  for (auto pair: relation_column_pairs) {
    size_t relation_index = pair.first;
    size_t column_index = pair.second;
    // Assert that we are requesting columns that exist in the ir.
    assert(column_is_allocated(relation_index));
    // Use these rowids to index into the relation data.
    auto rowids = this->operator[](relation_index);
    u64 sum = 0;
    for (auto rowid: rowids) {
      // Accumulate the specified column value into a sum.
      sum = sum + this->relation_storage[relation_index][column_index][rowid];
    }
    // Push the sum of each selected column into a collection.
    // The order of the sums of each column is the same as the order in the parameter collection.
    result.push(sum);
  }
  return result;
}

Array<Predicate> IntermediateResult::get_relation_filters(size_t relation_index) {
  Array<Predicate> result;
  for (auto predicate: this->parse_query_result.predicates) {
    if (predicate.kind != PRED::FILTER)
      continue; // If the predicate is not a simple filter continue...
    if (predicate.lhs.first != relation_index)
      continue; // Continue if the relation of the predicate is not matching.
    result.push(predicate);
  }
  return result;
}
