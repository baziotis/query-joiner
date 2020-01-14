#include "../command_interpreter.h"
#include "../parse.h"
#include "../relation_storage.h"
#include "../query_executor.h"

TaskScheduler scheduler{8, 1000};

/**
 * This is for a specific input...
 * @param pqr
 */
void reorder_predicates(ParseQueryResult &pqr) {
  Array<Predicate> new_arr(4);
  new_arr.size = 4;
  new_arr[0] = pqr.predicates[0];
  new_arr[1] = pqr.predicates[1];
  new_arr[2] = pqr.predicates[3];
  new_arr[3] = pqr.predicates[2];
  pqr.predicates = new_arr;
}

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
//      reorder_predicates(pqr);
//      auto sums = query_executor.execute_query_async(pqr);
//      print_sums(sums);
//      sums.free();
      pqr.predicates.clear_and_free();
      pqr.sums.clear_and_free();
    }
  }
  // Should output: 103260116758 17416413522 59644305653

  query_executor.free();
  relation_storage.free();
  fclose(fp);
  return 0;
}

