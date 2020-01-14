#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "query_executor.h"

size_t nr_threads = static_cast<size_t>(sysconf(_SC_NPROCESSORS_ONLN));
TaskScheduler scheduler{nr_threads};

//void print_sums(StretchyBuf<uint64_t> sums) {
//  for (size_t i = 0; i < sums.len; i++) {
//    auto sum = sums[i];
//    if (sum == 0)
//      printf("%s", "NULL");
//    else
//      printf("%lu", sum);
//
//    if (i == sums.len - 1)
//      printf("\n");
//    else
//      printf(" ");
//  }
//}

//void print_batch_sums(StretchyBuf<Future<StretchyBuf<uint64_t>>> batch_sums) {
//  for (auto &sums: batch_sums) {
//    print_sums(sums.get_value());
//    sums.free();
//  }
//}

void free_query_executors(StretchyBuf<QueryExecutor *> executors) {
  for (auto &executor: executors) {
    executor->free();
  }
  executors.free();
}

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

  while (interpreter.read_query_batch()) {
//    Future<StretchyBuf<uint64_t>> future_batch_sums[4];
//    StretchyBuf<Future<StretchyBuf<uint64_t>>> future_batch_sums;
    StretchyBuf<QueryExecutor *> query_executors;
    for (char *query : interpreter) {
      QueryExecutor *p_executor = new QueryExecutor(relation_storage);
      ParseQueryResult pqr = parse_query(query);
      pthread_mutex_lock(&state.mutex);
      p_executor->execute_query_async(pqr, query, &state);
      query_executors.push(p_executor);

      ++state.query_index;
      while (state.query_index == (nr_threads / 2)) {
        pthread_cond_wait(&state.notify, &state.mutex);
      }
      pthread_mutex_unlock(&state.mutex);
    }

//    print_batch_sums(future_batch_sums);
//    future_batch_sums.free();
    //    print_batch_sums(future_batch_sums);
//    free_query_executors(query_executors);
  }

  relation_storage.free();
  fclose(fp);
  return 0;
}
