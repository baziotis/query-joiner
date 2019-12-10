#include <cstdlib>
#include <cassert>
#include "../../command_interpreter.h"
#include "../../report_utils.h"

void test_filenames() {
  FUNCTION_TEST();
  constexpr char *input_fname = (char *const) "filenames.txt";
  constexpr char *filenames[] = {
      (char *const) "Test filename 1",
      (char *const) "TheBestFilename",
      (char *const) "wow!_what_a_filename"
  };

  FILE *fp = fopen(input_fname, "r");
  assert(fp);
  CommandInterpreter interpreter{fp};
  interpreter.read_relation_filenames();
  fclose(fp);
  size_t i = 0U;
  for (char *command : interpreter) {
    report(R"(Command = "%s")", command);
    assert(strcmp(command, filenames[i]) == 0);
    ++i;
  }
}

void test_queries() {
  FUNCTION_TEST();
  constexpr char *input_fname = (char *const) "queries.txt";
  constexpr char *queries[] = {
    (char *const) "0 1 2|0.1=1.2&1.0=2.1&0.1>3000|0.0 0.1",
    (char *const) "10 5 14|0.19=1.2 & 2.20=5000| 1.10"
  };
  FILE *fp = fopen(input_fname, "r");
  assert(fp);
  CommandInterpreter interpreter{fp};
  interpreter.read_query_batch();
  size_t i = 0U;
  for (char *query : interpreter) {
    report(R"(Query = "%s")", query);
    assert(strcmp(query, queries[i]) == 0);
    ++i;
  }
  fclose(fp);
}

int main() {
  test_filenames();
  test_queries();
  return EXIT_SUCCESS;
}
