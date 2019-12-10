#include "command_interpreter.h"
#include "parse.h"

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
