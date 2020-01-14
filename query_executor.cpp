//
// Created by aris on 7/1/20.
//

#include <mutex>
#include "query_executor.h"

extern TaskScheduler scheduler;

QueryExecutor::QueryExecutor(RelationStorage &rs)
: intermediate_results(), relation_storage(rs) {}

StretchyBuf<uint64_t> QueryExecutor::execute_query(ParseQueryResult pqr, char *query) {
  // Clean up the ir's.
  intermediate_results.clear();
  intermediate_results.free();
  assert(pqr.predicates.size > 0);
  int is_chain = 0;
  for (auto predicate: pqr.predicates) {
    if (predicate.kind != PRED::JOIN)
      continue;
    auto r1 = predicate.lhs.first; // Left relation to join.
    auto r2 = predicate.rhs.first; // Right relation to join.
    int target_ir_index_1 = get_target_ir_index(r1);
    int target_ir_index_2 = get_target_ir_index(r2);

    if (target_ir_index_1 != -1 && target_ir_index_2 != -1 &&
          target_ir_index_1 != target_ir_index_2) {
      // If the relation is present in different ir's.
      auto &target_ir_1 = intermediate_results[target_ir_index_1];
      auto &target_ir_2 = intermediate_results[target_ir_index_2];
      // Don't forget to wait for the ir's to finish their joins.
//      target_ir_1.previous_join.wait();
//      target_ir_2.previous_join.wait();
      target_ir_1.join_with_ir(
          target_ir_2, predicate.lhs.first, predicate.lhs.second,
          predicate.rhs.first, predicate.rhs.second);
      intermediate_results_remove_at(target_ir_index_2);
    } else if (target_ir_index_1 == -1 && target_ir_index_2 == -1) {
      assert(++is_chain < 2); // Make sure we enter this case once only for chains.
      // If both relations are new add a new ir to the list.
      IntermediateResult new_ir(relation_storage, pqr);
      // TODO locks needed here.
      intermediate_results.push(new_ir); // first push and then start to execute...
      intermediate_results[intermediate_results.len-1].execute_join(predicate);
    } else {
      // This is the common case. What we did in previous versions.
      auto &target_ir = target_ir_index_1 == -1 ?
                       intermediate_results[target_ir_index_2] :
                       intermediate_results[target_ir_index_1];
      target_ir.execute_join(predicate);
    }
  }
  // Make sure that when there are no more join operations,
  // all join operations collapsed to a single ir.
  assert(intermediate_results.len == 1);
  // Don't forget to wait for the last join predicate
  // to finish before executing select clause.
//  intermediate_results[0].previous_join.wait();
  return intermediate_results[0].execute_select(pqr.sums);
}

int QueryExecutor::get_target_ir_index(size_t relation_index) {
  for (int i = 0; i < intermediate_results.len; ++i) {
    auto ir = intermediate_results[i];
//    ir.previous_join.wait();
    if (ir.column_is_allocated(relation_index))
      return i;
  }
  return -1;
}

void QueryExecutor::intermediate_results_remove_at(size_t index) {
  StretchyBuf<IntermediateResult> new_list;
  for (size_t i = 0; i < intermediate_results.len; ++i) {
    if (i == index)
      continue;
    new_list.push(intermediate_results[i]);
  }
  intermediate_results.free();
  intermediate_results = new_list;
}

void QueryExecutor::free() {
  intermediate_results.free();
}

Future<StretchyBuf<uint64_t>> QueryExecutor::execute_query_async(ParseQueryResult pqr, char *query) {
  return scheduler.add_task(execute_query_static, this, pqr, query);
}

StretchyBuf<uint64_t> QueryExecutor::execute_query_static(QueryExecutor *this_qe, ParseQueryResult pqr, char *query) {
  return this_qe->execute_query(pqr, query);
}

