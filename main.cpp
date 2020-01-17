#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "query_executor.h"

size_t nr_threads = static_cast<size_t>(sysconf(_SC_NPROCESSORS_ONLN) * 2);
TaskScheduler scheduler{nr_threads};

int main(int argc, char *args[]) {
//    Joinable lhs{10};
//    Joinable rhs{10};
//    lhs.push({1, 0});
//    lhs.push({1, 1});
//    lhs.push({1, 2});
//    lhs.push({1, 3});
//    lhs.push({2, 4});
//    lhs.push({2, 5});
//    lhs.push({4, 6});
//    lhs.push({10, 7});
//
//    rhs.push({1, 10});
//    rhs.push({1, 20});
//    rhs.push({1, 30});
//    rhs.push({2, 40});
//    rhs.push({3, 50});
//    rhs.push({4, 500});
//    rhs.push({10, 600});
//    rhs.push({10, 700});
//
//    Join join{};
//    auto res = join(lhs, rhs);
//    for (auto v : res) {
//      for (auto rv : v.second) {
//        printf("LHS rowid = %lu, RHS rowid = %lu\n", v.first.v, rv.v);
//      }
//    }

//  8 0 13 13|0.2=1.0&1.0=2.2&2.1=3.2&0.1>7860|3.3 2.1 3.6


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
      executor->execute_query_async(pqr, &state);

      pthread_mutex_lock(&state.mutex);
      ++state.query_index;
      while (state.query_index >= (nr_threads / 2)) {
        pthread_cond_wait(&state.notify, &state.mutex);
      }
      pthread_mutex_unlock(&state.mutex);
    }
  }

  scheduler.wait_remaining_and_stop();

  fclose(fp);
  return 0;
}