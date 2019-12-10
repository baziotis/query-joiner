//
// Created by aris on 29/11/19.
//

#ifndef SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_
#define SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_

#include <zconf.h>
#include <cstdint>
#include "joinable.h"
#include "relation_storage.h"

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
  explicit IntermediateResult(RelationStorage &rs, size_t max_column_n);
  /**
   * Deallocates memory used by the row-id columns if necessery.
   */
  ~IntermediateResult();
  /**
   * Gives access to a column of the intermediate result.
   * Which is essentially the row-ids of the join result for the specified relation.
   * @note When refering to "relation_index" we mean the global index of the relation
   * NOT as it appears in a query from clause. For example:
   * 4 5 3 | .... | ....
   * Here relation 4 has index 4, relation 5 has index 5 and finally relation 3 has index 3.
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
//  /**
//   * Allocates space for the row-ids of the specified column.
//   * @param relation_index The index of the relation to allocate.
//   * @param row_n Number of rows for the newly allocated relation.
//   */
//  void allocate_column(size_t relation_index, size_t row_n);
//  /**
//   * Deallocates the space of an existing relation row-id column.
//   * This is needed due to size changes in the intermediate result
//   * row numbers. @TODO maybe there's a better solution to multiple
//   * new-deletes(for example: don't delete if the next join result is smaller).
//   * @param relation_index The index of the relation to deallocate.
//   */
//  void deallocate_column(size_t relation_index);

  /**
   * Creates a joinable object that contains <key, rowid> pairs.
   * The keys are fetched from relation_storage based on "relation_index"
   * and "key_index". The row-ids are evaluated as the index of each tuple
   * of the intermediate result.
   * @param relation_index Global index of the relation.
   * @param key_index Index of the key to use.
   * @return A Joinable object.
   */
  Joinable to_joinable(size_t relation_index, size_t key_index);

  /**
   * Accumulates relations to the intermediate result.
   * This function can be called at most "max_column_n-1" times
   * (the number of relations in the from clause of a query).
   * After this call the ir has more columns(relations).
   * @TODO make a wrapper that takes the parsing output as arguments.
   * @note the left-right order of the parameters is irrelevant.
   * @param left_relation_index Global index of first relation.
   * @param left_key_index Join column index in first relation.
   * @param right_relation index Global index of second relation.
   * @param right_key_index Join column index in second relation.
   */
  void execute_join(
      size_t left_relation_index, size_t left_key_index,
      size_t right_relation_index, size_t right_key_index);

  /**
   * Executes the select clause of the query and performs an aggregate sum on the join results.
   * All relation indices passed in the parameter should be present in the ir.
   *
   * @param relation_indices An ordered collection of pairs of <relation_index, column_index>
   * as they appear in the select clause.
   * @return An ordered collection of the aggregate sums for the join result.
   * The size of the collection should be equal to the parameter collection.
   */
  StretchyBuf<uint64_t> execute_select(StretchyBuf<Pair<size_t, size_t>> relation_indices);

 private:
  void join_existing_relations(
      size_t left_relation_index, size_t left_key_index,
      size_t right_relation_index, size_t right_key_index);
  void execute_initial_join(
      size_t left_relation_index, size_t left_key_index,
      size_t right_relation_index, size_t right_key_index);
  void execute_common_join(
      size_t existing_relation_index, size_t existing_relation_key_index,
      size_t new_relation_index, size_t new_relation_key_index);

  RelationStorage &relation_storage;
  uint64_t **columns;
  size_t max_column_n;
  size_t column_n;
  size_t row_n;
};

#endif //SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_