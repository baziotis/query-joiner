//
// Created by aris on 29/11/19.
//

#ifndef SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_
#define SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_

#include <zconf.h>
#include <cstdint>

/**
 * Represents the intermediate result of a query predicate execution.
 * The row-ids of each table in the intermediate result are stored column-wise.
 * Each column represents row-ids of a table.
 * i.e : If we join 3 tables, the intermediate result will store 3 columns
 * each holding the join result row-ids of each table respectively.
 * At initialization we specify the maximum number of columns that will be stored.
 * This information is made known at query-parse time.
 *(essentially the number of tables in the 'from' clause of the query).
 */
class IntermediateResult {
 public:
  explicit IntermediateResult(size_t max_column_n);
  ~IntermediateResult();
  uint64_t *get_column(size_t relation_index);
  size_t column_count();
  size_t row_count();
  bool is_allocated_column(size_t index);
  void deallocate_column(size_t index);
  void allocate_column(size_t index, size_t row_n);
  size_t get_unallocated_column_index();
  void assign_relation_to_column(size_t relation_index, size_t column_index);
  void unassign_column(size_t column_index);

 private:
  uint64_t **columns;
  int *relation_map; // Maps relation index to column index.
  size_t max_column_n;
  size_t column_n;
  size_t row_n;

  static const int RelationNotPresentYet = -1;
};

#endif //SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_