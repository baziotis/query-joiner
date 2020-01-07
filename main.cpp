#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "intermediate_result.h"
#include "QueryExecutor.h"

void print_sums(StretchyBuf<uint64_t> sums) {
  for (size_t i = 0; i < sums.len; i++) {
    auto sum = sums[i];
    if (sum == 0)
      printf("%s", "NULL");
    else
      printf("%lu", sum);

    if (i == sums.len - 1)
      printf("\n");
    else
      printf(" ");
  }
}

int main(int argc, char *args[]) {
  // Αdd a file here that contains the full input. (filenames, queries).
  FILE *fp = fopen(args[1], "r");
  assert(fp);

  CommandInterpreter interpreter{fp};
  interpreter.read_relation_filenames();
  RelationStorage relation_storage(interpreter.remaining_commands());
  relation_storage.insert_from_filenames(interpreter.begin(), interpreter.end());
  QueryExecutor query_executor(relation_storage);

  while (interpreter.read_query_batch()) {
    for (char *query : interpreter) {
      ParseQueryResult pqr = parse_query(query);
//      IntermediateResult intermediate_result(relation_storage, pqr);
//      auto sums = intermediate_result.execute_query();
      auto sums = query_executor.execute_query(pqr);
      print_sums(sums);
      sums.free();
      pqr.predicates.clear_and_free();
      pqr.sums.clear_and_free();
//      intermediate_result.free();
    }
  }

  query_executor.free();
  relation_storage.free();
  fclose(fp);
  return 0;
}
