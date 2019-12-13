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

void print_sums(StretchyBuf<uint64_t > sums) {
  for (size_t i = 0; i < sums.len; i++) {
    auto sum = sums[i];
    if (sum == 0)
      printf("%s", "NULL");
    else
      printf("%lu", sum);

    if (i == sums.len-1)
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

  interpreter.read_query_batch();
  for (char *query : interpreter) {
    ParseQueryResult pqr = parse_query(query);
    IntermediateResult intermediate_result(relation_storage, pqr);
    auto sums = intermediate_result.execute_query();
    print_sums(sums);
  }

  fclose(fp);
  return 0;
}
