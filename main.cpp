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
    StretchyBuf<Future<StretchyBuf<uint64_t>>> future_sums{interpreter.remaining_commands()};
    for (char *query : interpreter) {
      executor = new QueryExecutor{relation_storage};
      ParseQueryResult pqr = parse_query(query);
      future_sums.push(executor->execute_query_async(pqr, &state));

      pthread_mutex_lock(&state.mutex);
      ++state.query_index;
      while (state.query_index >= (nr_threads / 3)) {
        pthread_cond_wait(&state.notify, &state.mutex);
      }
      pthread_mutex_unlock(&state.mutex);
    }

    for (auto &future_sum : future_sums) {
      size_t index = 0;
      auto sums = future_sum.get_value();
      for (uint64_t sum : sums) {
        const char *separator = (index != sums.len - 1) ? " " : "\n";
        if (sum != 0) {
          printf("%lu%s", sum, separator);
        } else {
          printf("NULL%s", separator);
        }
        ++index;
      }
    }

    future_sums.free();
  }

  fclose(fp);
  return 0;
}