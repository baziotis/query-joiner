#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "query_executor.h"

TaskScheduler scheduler{4, 1000};

void print_sums(StretchyBuf<uint64_t> sums) {
  for (size_t i = 0; i < sums.len; i++) {
    auto sum = sums[i];
    if (sum == 0)
      printf("%s", "NULL");
    else
      printf("%lu", sum);

    if (i == sums.len - 1)
      printf("\n");
    else
      printf(" ");
  }
}

void print_batch_sums(StretchyBuf<Future<StretchyBuf<uint64_t>>> batch_sums) {
  for (auto &sums: batch_sums) {
    print_sums(sums.get_value());
    sums.free();
  }
}

void free_query_executors(StretchyBuf<QueryExecutor*> executors) {
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

  while (interpreter.read_query_batch()) {
    size_t index = 0;
    StretchyBuf<Future<StretchyBuf<uint64_t>>> future_batch_sums;
    StretchyBuf<QueryExecutor*> query_executors;
    for (char *query : interpreter) {
      QueryExecutor *p_executor = new QueryExecutor(relation_storage);
      ParseQueryResult pqr = parse_query(query);
      future_batch_sums.push(
          p_executor->execute_query_async(pqr, query));
      query_executors.push(p_executor);
      if (index == 2) {
        print_batch_sums(future_batch_sums);
        future_batch_sums.clear();
        index = 0U;
      }
      ++index;
    }
    print_batch_sums(future_batch_sums);
//    print_batch_sums(future_batch_sums);
//    future_batch_sums.free();
//    free_query_executors(query_executors);
  }

  relation_storage.free();
  fclose(fp);
  return 0;
}
