#include "command_interpreter.h"
#include "parse.h"

/*
 * I'm going to write here a use case indicating how to create a RelationStorage by reading the input filenames
 * CommandInterpreter interpreter{};
 * interpreter.read_relation_filenames();
 * RelationStorage storage(interpreter.remaining_commands());
 * storage.insert_from_filenames(interpreter.begin(), interpreter.end());
 */

int main() {
  FILE *fp = fopen("./tests/command_interpreter/queries.txt", "r");
  assert(fp);
  CommandInterpreter interpreter{fp};
  interpreter.read_query_batch();
  for (char *query : interpreter) {
    ParseQueryResult pqr = parse_query(query);
    for (Predicate p : pqr.predicates) {
      p.print();
    }
  }
  fclose(fp);
  return 0;
}
