#include <cassert>
#include "intermediate_result.h"

extern TaskScheduler scheduler;

IntermediateResult::IntermediateResult(RelationStorage &rs, const ParseQueryResult &pqr)
    : Array(rs.size), relation_storage(rs), parse_query_result(pqr), column_n(0),
      row_n(0), max_column_n(rs.size), aux{}, sort_context{} {
  this->size = rs.size;
  for (size_t i = 0; i < this->size; i++) {
    this->operator[](i).data = nullptr;
  }
  this->sorting.set_none();
}

size_t IntermediateResult::column_count() {
  return this->column_n;
}

size_t IntermediateResult::row_count() {
  return this->row_n;
}

bool IntermediateResult::column_is_allocated(size_t relation_index) {
  assert(relation_index < this->max_column_n);
  return this->operator[](relation_index).data != nullptr;
}

bool IntermediateResult::is_empty() {
  return this->column_n == 0;
}

Joinable IntermediateResult::to_joinable(size_t relation_index, size_t key_index) {
  assert(column_is_allocated(relation_index));
  // Oddly this is the number of columns of relation at "relation_index".
  assert(key_index < relation_storage[get_global_relation_index(relation_index)].size);
  assert(this->row_n != 0);
  RelationData target_relation = relation_storage[get_global_relation_index(relation_index)];
  Joinable joinable(this->row_n);
  for (size_t i = 0; i < this->row_n; ++i) {
    u64 rowid = this->operator[](relation_index)[i];
    JoinableEntry entry{target_relation[key_index][rowid], i};
    joinable.push(entry);
  }
  return joinable;
}

void IntermediateResult::execute_join(size_t left_relation_index,
                                      size_t left_key_index,
                                      size_t right_relation_index,
                                      size_t right_key_index) {
  assert(left_relation_index < this->max_column_n);
  assert(right_relation_index < this->max_column_n);

  bool left_allocated = column_is_allocated(left_relation_index);
  bool right_allocated = column_is_allocated(right_relation_index);

  if (left_allocated && right_allocated) {
    // In this case both relations are already present in the intermediate result.
    execute_join_as_filter(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (!left_allocated && !right_allocated) {
    // This case should occur only once, when the intermediate result is empty.
    execute_initial_join(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else if (left_allocated) {
    // This is the common case where one of the join_with_ir relations exist in the ir
    // and the other one is new...
    execute_common_join(
        left_relation_index, left_key_index,
        right_relation_index, right_key_index);
  } else {
    // This is the common case where one of the join_with_ir relations exist in the ir
    // and the other one is new...
    execute_common_join(
        right_relation_index, right_key_index,
        left_relation_index, left_key_index);
  }
}

static size_t sort_threshold = sysconf(_SC_LEVEL1_DCACHE_SIZE);

static inline void noop() {}

static inline void sort_wrapper(Joinable *joinable, size_t threshold) {
  Joinable aux{joinable->size};
  aux.size = joinable->size;
  StretchyBuf<Joinable::SortContext> context_stack{};
  joinable->sort({aux, context_stack}, threshold);
  aux.clear_and_free();
  context_stack.free();
}

void IntermediateResult::execute_initial_join(size_t left_relation_index,
                                              size_t left_key_index,
                                              size_t right_relation_index,
                                              size_t right_key_index) {
  // Because this is the initial join_with_ir, make sure the ir is empty.
  // Otherwise the state of the ir is not valid.
  assert(this->is_empty());
  // Get the two relations to join_with_ir as joinables.
  Joinable r_left = relation_storage[get_global_relation_index(left_relation_index)]
      .to_joinable(left_key_index, get_relation_filters(left_relation_index));
  Joinable r_right = relation_storage[get_global_relation_index(right_relation_index)]
      .to_joinable(right_key_index,
                   left_relation_index != right_relation_index ?
                   get_relation_filters(right_relation_index) : StretchyBuf<Predicate>());
  if (r_left.size == 0 || r_right.size == 0) {
    // Exit the query execution...
    this->row_n = 0;
    this->operator[](left_relation_index) = StretchyBuf<u64>(0);
    this->operator[](right_relation_index) = StretchyBuf<u64>(0);
    return;
  }
  Future<void> *left_future;
  if (!relation_is_sorted(left_relation_index, left_key_index)) {
    if (r_left.capacity > aux.capacity) {
      aux.clear_and_free();
      aux.reserve(r_left.capacity);
      aux.size = r_left.capacity;
    }
    sort_context.reset();
    left_future = &scheduler.add_task(sort_wrapper, &r_left, sort_threshold);
  } else {
    left_future = &scheduler.add_task(noop);
  }
  Future<void> *right_future;
  if (!relation_is_sorted(right_relation_index, right_key_index)) {
    if (r_right.capacity > aux.capacity) {
      aux.clear_and_free();
      aux.reserve(r_right.capacity);
      aux.size = r_right.capacity;
    }
    sort_context.reset();
    right_future = &scheduler.add_task(sort_wrapper, &r_right, sort_threshold);
  } else {
    right_future = &scheduler.add_task(noop);
  }
  left_future->wait();
  right_future->wait();
  left_future->free();
  right_future->free();

  Join join;
  auto join_result = join(r_left, r_right);
  r_left.clear_and_free();
  r_right.clear_and_free();
  StretchyBuf<u64> column1;
  StretchyBuf<u64> column2;
  for (auto join_row: join_result) {
    auto rowid_1 = join_row.first;
    for (auto rowid_2: join_row.second) {
      column1.push(rowid_1);
      column2.push(rowid_2);
    }
  }
  free_join_result(join_result);
  this->operator[](left_relation_index) = column1;
  this->operator[](right_relation_index) = column2;
  this->column_n = 2;
  this->row_n = column1.len;

  // Update information about the sorting state of the ir. Later used as optimization.
  this->sorting.sorted_relation_index_1 = left_relation_index;
  this->sorting.relation_1_sorting_key = left_key_index;
  this->sorting.sorted_relation_index_2 = right_relation_index;
  this->sorting.relation_2_sorting_key = right_key_index;
}

IntermediateResult IntermediateResult::join_with_ir(IntermediateResult &ir,
                                                    size_t this_relation_index,
                                                    size_t this_key_index,
                                                    size_t right_relation_index,
                                                    size_t right_key_index) {
  assert(column_is_allocated(this_relation_index));
  if (this->row_n == 0 || ir.row_n == 0) {
    for (size_t i = 0; i < ir.column_n; i++) {
      if (ir.column_is_allocated(i))
        this->operator[](i) = StretchyBuf<u64>(0);
    }
    ir.clear_and_free();
    this->column_n += ir.column_n;
    return *this;
  }
  Joinable r_this = this->to_joinable(this_relation_index, this_key_index);
  Joinable r_right = ir.to_joinable(right_relation_index, right_key_index);
  if (r_this.size == 0 || r_right.size == 0) {
    // Exit the query execution...
    this->row_n = 0;
    ir.clear_and_free();
    return *this;
  }
  Future<void> *left_future;
  if (!relation_is_sorted(this_relation_index, this_key_index)) {
    if (r_this.capacity > aux.capacity) {
      aux.clear_and_free();
      aux.reserve(r_this.capacity);
      aux.size = r_this.capacity;
    }
    sort_context.reset();
    left_future = &scheduler.add_task(sort_wrapper, &r_this, sort_threshold);
  } else {
    left_future = &scheduler.add_task(noop);
  }

  Future<void> *right_future;
  if (!ir.relation_is_sorted(right_relation_index, right_key_index)) {
    if (r_right.capacity > aux.capacity) {
      aux.clear_and_free();
      aux.reserve(r_right.capacity);
      aux.size = r_right.capacity;
    }
    sort_context.reset();
    right_future = &scheduler.add_task(sort_wrapper, &r_right, sort_threshold);
  } else {
    right_future = &scheduler.add_task(noop);
  }
  left_future->wait();
  right_future->wait();
  left_future->free();
  right_future->free();

  Join join;
  auto join_result = join(r_this, r_right);
  r_this.clear_and_free();
  r_right.clear_and_free();
  // Loop for the allocated existing columns.
  for (size_t j = 0; j < this->max_column_n; ++j) {
    if (!column_is_allocated(j))
      continue;
    StretchyBuf<u64> aux_column(join_result.len);
    auto current_column = this->operator[](j);
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
  // Loop for the new columns that will be added from param ir.
  for (size_t j = 0; j < this->max_column_n; ++j) {
    if (!ir.column_is_allocated(j))
      continue;
    StretchyBuf<u64> aux_column(join_result.len);
    auto current_column = ir[j];
    for (auto join_row: join_result) {
      auto rowid_1 = join_row.first;
      for (auto rowid_2: join_row.second) {
        aux_column.push(current_column[rowid_2]);
      }
    }
    this->operator[](j) = aux_column;
  }

  this->row_n = join_result.len;
  free_join_result(join_result);
  this->column_n += ir.column_n;

  // Dont forget to delete the param ir.
  ir.free();

  // Update information about the sorting state of the ir. Later used as optimization.
  this->sorting.sorted_relation_index_1 = this_relation_index;
  this->sorting.relation_1_sorting_key = this_key_index;
  this->sorting.sorted_relation_index_2 = right_relation_index;
  this->sorting.relation_2_sorting_key = right_key_index;
  return *this;
}

void IntermediateResult::execute_common_join(size_t existing_relation_index,
                                             size_t existing_relation_key_index,
                                             size_t new_relation_index,
                                             size_t new_relation_key_index) {
  assert(column_is_allocated(existing_relation_index));
  assert(!column_is_allocated(new_relation_index));
  if (this->row_n == 0) {
    column_n++;
    this->operator[](existing_relation_index) = StretchyBuf<u64>(0);
    this->operator[](new_relation_index) = StretchyBuf<u64>(0);
    return;
  }
  Joinable r_existing = this->to_joinable(existing_relation_index, existing_relation_key_index);
  Joinable r_new = relation_storage[get_global_relation_index(new_relation_index)]
      .to_joinable(new_relation_key_index, get_relation_filters(new_relation_index));
  if (r_existing.size == 0 || r_new.size == 0) {
    // Exit the query execution...
    this->row_n = 0;
    return;
  }
  Future<void> *left_future;
  if (!relation_is_sorted(existing_relation_index, existing_relation_key_index)) {
    if (r_existing.capacity > aux.capacity) {
      aux.clear_and_free();
      aux.reserve(r_existing.capacity);
      aux.size = r_existing.capacity;
    }
    sort_context.reset();
    left_future = &scheduler.add_task(sort_wrapper, &r_existing, sort_threshold);
  } else {
    left_future = &scheduler.add_task(noop);
  }

  Future<void> *right_future;
  if (!relation_is_sorted(new_relation_index, new_relation_key_index)) {
    if (r_new.capacity > aux.capacity) {
      aux.clear_and_free();
      aux.reserve(r_new.capacity);
      aux.size = r_new.capacity;
    }
    sort_context.reset();
    right_future = &scheduler.add_task(sort_wrapper, &r_new, sort_threshold);
  } else {
    right_future = &scheduler.add_task(noop);
  }

  left_future->wait();
  right_future->wait();
  left_future->free();
  right_future->free();

  Join join;
  auto join_result = join(r_existing, r_new);
  r_existing.clear_and_free();
  r_new.clear_and_free();
  // Loop for the allocated existing columns.
  for (size_t j = 0; j < this->max_column_n; ++j) {
    if (!column_is_allocated(j))
      continue;
    StretchyBuf<u64> aux_column(join_result.len);
    auto current_column = this->operator[](j);
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
  free_join_result(join_result);
  this->operator[](new_relation_index) = aux_column;
  this->column_n++;
  this->row_n = aux_column.len;

  // Update information about the sorting state of the ir. Later used as optimization.
  this->sorting.sorted_relation_index_1 = existing_relation_index;
  this->sorting.relation_1_sorting_key = existing_relation_key_index;
  this->sorting.sorted_relation_index_2 = new_relation_index;
  this->sorting.relation_2_sorting_key = new_relation_key_index;
}

void IntermediateResult::execute_join_as_filter(size_t left_relation_index,
                                                size_t left_key_index,
                                                size_t right_relation_index,
                                                size_t right_key_index) {
  assert(column_is_allocated(left_relation_index));
  assert(column_is_allocated(right_relation_index));
  if (this->row_n == 0)
    return;
  // Find the row_ids of the ir that match the filter.
  StretchyBuf<size_t> ir_rowids;
  for (size_t i = 0; i < this->row_n; i++) {
    auto left_rowid = this->operator[](left_relation_index)[i];
    auto left_value =
        relation_storage[get_global_relation_index(left_relation_index)][left_key_index][left_rowid];
    auto right_rowid = this->operator[](right_relation_index)[i];
    auto right_value =
        relation_storage[get_global_relation_index(right_relation_index)][right_key_index][right_rowid];
    if (left_value == right_value) {
      ir_rowids.push(i);
    }
  }
  // Loop for the allocated existing columns.
  for (size_t j = 0; j < this->max_column_n; ++j) {
    if (!column_is_allocated(j))
      continue;
    StretchyBuf<u64> aux_column(ir_rowids.len);
    auto current_column = this->operator[](j);
    for (auto ir_rowid: ir_rowids) {
      aux_column.push(current_column[ir_rowid]);
    }
    current_column.free();
    this->operator[](j) = aux_column;
  }
  this->row_n = ir_rowids.len;
}

StretchyBuf<uint64_t> IntermediateResult::execute_select(Array<Pair<int, int>> relation_column_pairs) {
  StretchyBuf<uint64_t> result;
  for (auto pair: relation_column_pairs) {
    size_t relation_index = pair.first;
    size_t column_index = pair.second;
    if (this->row_n == 0) {
      result.push(0);
      continue;
    }
    // Assert that we are requesting columns that exist in the ir.
    // assert(column_is_allocated(relation_index)); // Removed this due to empty ir's.
    // Use these rowids to index into the relation data.
    auto rowids = this->operator[](relation_index);
    uint64_t sum = 0;
    for (auto rowid: rowids) {
      // Accumulate the specified column value into a sum.
      sum = sum
          + this->relation_storage[get_global_relation_index(relation_index)][column_index][rowid].v;
    }
    // Push the sum of each selected column into a collection.
    // The order of the sums of each column is the same as the order in the parameter collection.
    result.push(sum);
  }
  return result;
}

StretchyBuf<Predicate> IntermediateResult::get_relation_filters(size_t relation_index) {
  StretchyBuf<Predicate> result;
  for (auto predicate: this->parse_query_result.predicates) {
    if (predicate.kind != PRED::FILTER)
      continue; // If the predicate is not a simple filter continue...
    if (predicate.lhs.first != relation_index)
      continue; // Continue if the relation of the predicate is not matching.
    result.push(predicate);
  }
  return result;
}

StretchyBuf<uint64_t> IntermediateResult::execute_query() {
  for (auto predicate: this->parse_query_result.predicates) {
    if (predicate.kind != PRED::JOIN)
      continue;
    execute_join(predicate);
    if (this->row_n == 0)
      break;
  }
  return this->execute_select(parse_query_result.sums);
}

size_t IntermediateResult::get_global_relation_index(size_t local_relation_index) {
  return parse_query_result.actual_relations[local_relation_index];
}

void IntermediateResult::free_join_result(StretchyBuf<Join::JoinRow> &join_result) {
  for (auto item: join_result)
    item.second.free();
  join_result.free();
}

bool IntermediateResult::relation_is_sorted(size_t relation_index, size_t key_index) {
  return (relation_index == sorting.sorted_relation_index_1 &&
      key_index == sorting.relation_1_sorting_key) ||
      (relation_index == sorting.sorted_relation_index_2 &&
          key_index == sorting.relation_2_sorting_key);
}

void IntermediateResult::free() {
  for (auto col: *this) {
    col.free();
  }
  this->clear_and_free();
}

void IntermediateResult::execute_join(const Predicate &predicate) {
//  if (column_n != 0)
//    previous_join.wait();
//  previous_join = scheduler.add_task(
//      execute_join_static, this, predicate
//      );
  execute_join_static(this, predicate);
}

void IntermediateResult::execute_join_static(IntermediateResult *ir, const Predicate &predicate) {
  ir->execute_join(predicate.lhs.first, predicate.lhs.second,
                   predicate.rhs.first, predicate.rhs.second);
}

void IntermediateResult::Sorting::set_none() {
  sorted_relation_index_1 = -1;
  sorted_relation_index_2 = -1;
}
