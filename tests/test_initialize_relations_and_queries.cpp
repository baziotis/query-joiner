#include "../relation_storage.h"
#include "../command_interpreter.h"

int main() {
  CommandInterpreter command_interpreter;
  RelationStorage *relation_storage =
      command_interpreter.get_relation_storage();

  for (size_t i = 0; i < relation_storage->relation_count(); i++) {
    printf("%ld\n", relation_storage->get_relation(i)->row_count());
  }

  while (true) {
    char *query;
    int command = command_interpreter.next_query(&query);
    if (command == CommandInterpreter::CommandQueryGroupEnd) {
      printf("End of query group\n");
    }
    else if (command == CommandInterpreter::CommandReadNext) {
      printf("query: %s\n", query);
      delete query;
      continue;
    }
    else if (command == CommandInterpreter::CommandEndOfInput) {
      break;
    }
  }

  delete relation_storage;
  return 0;
}