//
// Created by aris on 7/1/20.
//

#ifndef QUERY_JOINER__QUERYEXECUTOR_H_
#define QUERY_JOINER__QUERYEXECUTOR_H_

#include "stretchy_buf.h"
#include "array.h"
#include "intermediate_result.h"

/**
 * This class is used to perform query executions,
 * with the predicates being executed at any order.
 * TODO maybe later we keep statistics in here too.
 */
class QueryExecutor {
 public:
  explicit QueryExecutor(RelationStorage &rs);
  /**
   * Executes a query based on it's parse result.
   * @param pqr Parse result.
   * @return List of the sums.
   */
  StretchyBuf<uint64_t> execute_query(ParseQueryResult &pqr);
  void free();

 private:
  StretchyBuf<IntermediateResult> intermediate_results;
  RelationStorage &relation_storage;

  /**
   * Get's the index of the ir that contains relation 'r'
   * @param r Relation index.
   * @return The index of the target ir in the list. If none returns -1.
   */
  int get_target_ir_index(size_t r);

  /**
   * Removes the intermediate result at the specified index
   * from the list.
   * @param i Index.
   */
  void intermediate_results_remove_at(size_t i);
};

#endif //QUERY_JOINER__QUERYEXECUTOR_H_
