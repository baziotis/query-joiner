#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "intermediate_result.h"

/*
 * I'm going to write here a use case indicating how to create a RelationStorage by reading the input filenames
 * CommandInterpreter interpreter{};
 * interpreter.read_relation_filenames();
 * RelationStorage storage(interpreter.remaining_commands());
 * storage.insert_from_filenames(interpreter.begin(), interpreter.end());
 */

int main() {
  // Αdd a file here that contains the full input. (filenames, queries).
  FILE *fp = fopen("../workloads/small/input", "r");
  assert(fp);

  CommandInterpreter interpreter{fp};
  interpreter.read_relation_filenames();
  RelationStorage relation_storage(interpreter.remaining_commands());
  relation_storage.insert_from_filenames(interpreter.begin(), interpreter.end());

  interpreter.read_query_batch();
  for (char *query : interpreter) {
    ParseQueryResult pqr = parse_query(query);
    IntermediateResult intermediate_result(relation_storage, pqr);
    auto sums = intermediate_result.execute_query();
    for (auto sum: sums) {
      if (sum == 0) {
        printf("%s ", "NULL");
      } else {
        printf("%ld ", sum);
      }
    }
    printf("\n");
  }

  fclose(fp);
  return 0;
}
