#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "query_executor.h"

size_t nr_threads = static_cast<size_t>(sysconf(_SC_NPROCESSORS_ONLN));
TaskScheduler scheduler{nr_threads};

int main(int argc, char *args[]) {
  scheduler.start();
  // Αdd a file here that contains the full input. (filenames, queries).
  FILE *fp = fopen(args[1], "r");
  assert(fp);

  CommandInterpreter interpreter{fp};
  interpreter.read_relation_filenames();
  RelationStorage relation_storage(interpreter.remaining_commands());
  relation_storage.insert_from_filenames(interpreter.begin(), interpreter.end());

  TaskState state{};

  QueryExecutor *executor;
  while (interpreter.read_query_batch()) {
    for (char *query : interpreter) {
      executor = new QueryExecutor{relation_storage};
      ParseQueryResult pqr = parse_query(query);
      executor->execute_query_async(pqr, query, &state);

      pthread_mutex_lock(&state.mutex);
      ++state.query_index;
      while (state.query_index == (nr_threads / 2)) {
        pthread_cond_wait(&state.notify, &state.mutex);
      }
      pthread_mutex_unlock(&state.mutex);
    }
  }

  fclose(fp);
  return 0;
}