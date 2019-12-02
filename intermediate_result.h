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
  /**
   * Constructs an empty intermediate result that can hold up to
   * <max_column_n> columns corresponding to relations in the from clause
   * of a query.
   * @param max_column_n The maximum number of relations participating.
   */
  explicit IntermediateResult(size_t max_column_n);
  /**
   * Deallocates memory used by the row-id columns if necessery.
   */
  ~IntermediateResult();
  /**
   * Gives access to a column of the intermediate result.
   * Which is essentially the row-ids of the join result for the specified relation.
   * @note When refering to "relation_index" we mean the index of the relation
   * as it appears in a query from clause. For example:
   * 4 5 3 | .... | ....
   * Here relation 4 has index 0, relation 5 has index 1 and finally relation 3 has index 2.
   * @param relation_index The index of the relation
   * @return A pointer to an array of row-ids. This address can be used for reads-writes
   * to the intermediate result.
   */
  uint64_t *get_column(size_t relation_index);
  bool is_empty();
  size_t column_count();
  size_t row_count();
  /**
   * Get's a boolean value specifying if there is allocated space for
   * the row-ids of the specified relation.
   * @param relation_index The index of the relation to check.
   * @return A boolean value.
   */
  bool column_is_allocated(size_t relation_index);
  /**
   * Allocates space for the row-ids of the specified column.
   * @param relation_index The index of the relation to allocate.
   * @param row_n Number of rows for the newly allocated relation.
   */
  void allocate_column(size_t relation_index, size_t row_n);
  /**
   * Deallocates the space of an existing relation row-id column.
   * This is needed due to size changes in the intermediate result
   * row numbers. @TODO maybe there's a better solution to multiple
   * new-deletes(for example: don't delete if the next join result is smaller).
   * @param relation_index The index of the relation to deallocate.
   */
  void deallocate_column(size_t relation_index);

 private:
  uint64_t **columns;
  size_t max_column_n;
  size_t column_n;
  size_t row_n;
};

#endif //SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_