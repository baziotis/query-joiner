//
// Created by aris on 7/1/20.
//

#include "QueryExecutor.h"

QueryExecutor::QueryExecutor(RelationStorage &rs)
: intermediate_results(), relation_storage(rs) {}

StretchyBuf<uint64_t> QueryExecutor::execute_query(ParseQueryResult &pqr) {
  // Clean up the ir's.
  intermediate_results.clear();
  intermediate_results.free();
  size_t i = 0;
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
      auto& target_ir_1 = intermediate_results[target_ir_index_1];
      auto& target_ir_2 = intermediate_results[target_ir_index_2];
      target_ir_1.join_with_ir(
          target_ir_2, predicate.lhs.first, predicate.lhs.second,
          predicate.rhs.first, predicate.rhs.second);
      intermediate_results_remove_at(target_ir_index_2);
      if (target_ir_1.row_count() == 0)
        return target_ir_1.execute_select(pqr.sums);
    } else if (target_ir_index_1 == -1 && target_ir_index_2 == -1) {
      // If both relations are new add a new ir to the list.
      IntermediateResult new_ir(relation_storage, pqr);
      new_ir.execute_join(predicate.lhs.first, predicate.lhs.second,
                          predicate.rhs.first, predicate.rhs.second);
      intermediate_results.push(new_ir);
      if (new_ir.row_count() == 0)
        return new_ir.execute_select(pqr.sums);
    } else {
      // This is the common case. What we did in previous versions.
      auto& target_ir = target_ir_index_1 == -1 ?
                       intermediate_results[target_ir_index_2] :
                       intermediate_results[target_ir_index_1];
      target_ir.execute_join(predicate.lhs.first, predicate.lhs.second,
                             predicate.rhs.first, predicate.rhs.second);
      if (target_ir.row_count() == 0)
        return target_ir.execute_select(pqr.sums);
    }
  }
  // Make sure that when there are no more join operations,
  // all join operations collapsed to a single ir.
  assert(intermediate_results.len == 1);
  return intermediate_results[0].execute_select(pqr.sums);
}

int QueryExecutor::get_target_ir_index(size_t relation_index) {
  for (int i = 0; i < intermediate_results.len; ++i) {
    auto ir = intermediate_results[i];
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
