//
// Created by aris on 7/1/20.
//

#ifndef QUERY_JOINER__QUERY_EXECUTOR_H_
#define QUERY_JOINER__QUERY_EXECUTOR_H_

#include "stretchy_buf.h"
#include "array.h"
#include "intermediate_result.h"

struct TaskState {
  TaskState() : query_index{0U} {
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&notify, NULL);
  }
  pthread_mutex_t mutex;
  pthread_cond_t notify;
  size_t query_index;
};

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
  StretchyBuf<uint64_t> execute_query(ParseQueryResult pqr, char *query);

  /**
   * Executes a query and returns a future object that will yield it's result.
   *
   * @param pqr Parse result.
   * @return Future list of the sums.
   */
  Future<StretchyBuf<uint64_t>> execute_query_async(ParseQueryResult pqr, char *query, TaskState *state);

  void free();

 private:
  StretchyBuf<IntermediateResult> intermediate_results;
  RelationStorage relation_storage;

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

  static StretchyBuf<uint64_t> execute_query_static(QueryExecutor *this_qe, ParseQueryResult pqr, char *query,
      TaskState *state);
};

#endif //QUERY_JOINER__QUERY_EXECUTOR_H_
