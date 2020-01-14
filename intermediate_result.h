//
// Created by aris on 29/11/19.
//

#ifndef SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_
#define SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_

#include <cstdint>
#include "joinable.h"
#include "relation_storage.h"
#include "parse.h"
#include "task_scheduler.h"

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
class IntermediateResult : public Array<StretchyBuf<u64>> {
 public:
  /**
   * Constructs an empty intermediate result that can hold up to
   * <max_column_n> columns corresponding to relations in the from clause
   * of a query.
   * @param max_column_n The maximum number of relations participating.
   */
  explicit IntermediateResult(RelationStorage &rs, const ParseQueryResult &pqr);

  /**
   * Deallocates memory used by the row-id columns if necessery.
   */
  void free();

  /**
   * @TODO this must be removed at some point.
   * execution of a query should be done by traversing a join tree.
   */
  StretchyBuf<uint64_t> execute_query();

  /**
   * Joins this intermediate result to another on the specified relation-column pairs.
   * The memory for the parameter ir is deallocated and it is no longer used.
   * @param ir Some arbitrary ir.
   * @param this_relation_index
   * @param this_key_index
   * @param right_relation_index
   * @param right_key_index
   * @return The existing ir updated.
   */
  IntermediateResult join_with_ir(IntermediateResult &ir,
                                  size_t this_relation_index, size_t this_key_index,
                                  size_t right_relation_index, size_t right_key_index);

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
   * Executes the select clause of the query and performs an aggregate sum on the join results.
   * All relation indices passed in the parameter should be present in the ir.
   *
   * @param relation_indices An ordered collection of pairs of <relation_index, column_index>
   * as they appear in the select clause.
   * @return An ordered collection of the aggregate sums for the join result.
   * The size of the collection should be equal to the parameter collection.
   */
  StretchyBuf<uint64_t> execute_select(Array<Pair<int, int>> relation_indices);

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

  void execute_join(const Predicate &predicate);

  static void execute_join_static(IntermediateResult *ir, const Predicate &predicate);

  /**
   * This Future object represents the return value of method addtask in task scheduler.
   */
//  Future<void> previous_join;

 private:



  /**
   * Creates a joinable object that contains <key, rowid> pairs.
   * The keys are fetched from relation_storage based on "relation_index"
   * and "key_index". The row-ids are evaluated as the index of each tuple
   * of the intermediate result. All filtering associated with this relation
   * is performed at this step.
   * @param relation_index Global index of the relation.
   * @param key_index Index of the key to use.
   * @return A Joinable object.
   */
  Joinable to_joinable(size_t relation_index, size_t key_index);



  void execute_join_as_filter(
      size_t left_relation_index, size_t left_key_index,
      size_t right_relation_index, size_t right_key_index);
  void execute_initial_join(
      size_t left_relation_index, size_t left_key_index,
      size_t right_relation_index, size_t right_key_index);
  void execute_common_join(
      size_t existing_relation_index, size_t existing_relation_key_index,
      size_t new_relation_index, size_t new_relation_key_index);
  /**
   * Get's all the associated where clauses for the specified relation in the query.
   * The data is drawn from the ParseQueryResult.
   * @note where clauses of the form 0.1 = 1.2 are not included in the resulting array.
   * @param relation_index The index of the specified relation.
   * @return An array of where predicates.
   */
  StretchyBuf<Predicate> get_relation_filters(size_t relation_index);

  size_t get_global_relation_index(size_t local_relation_index);

  static void free_join_result(StretchyBuf<Join::JoinRow> &join_result);

  bool relation_is_sorted(size_t relation_index, size_t key_index);

  struct Sorting {
    void set_none();
    int sorted_relation_index_1;
    int relation_1_sorting_key;
    int sorted_relation_index_2;
    int relation_2_sorting_key;
  } sorting;

  Joinable aux;
  StretchyBuf<Joinable::SortContext> sort_context;
  RelationStorage relation_storage;
  ParseQueryResult parse_query_result;
  size_t max_column_n;
  size_t column_n;
  size_t row_n;
};


#endif //SORT_MERGE_JOIN__INTERMEDIATE_RESULT_H_
