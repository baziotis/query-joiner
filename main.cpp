#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "query_executor.h"

#include <math.h>

size_t nr_threads = static_cast<size_t>(sysconf(_SC_NPROCESSORS_ONLN) * 2);
TaskScheduler scheduler{nr_threads};

struct ColumnStat {
  double l, u, f, d;

  void print() const {
    printf("l: %.1lf, u: %.1lf, f: %.1lf, d: %.1lf\n", l, u, f, d);
  }
};

struct Stats {
  Array<Array<ColumnStat>> relations;
};

ColumnStat compute_stats_for_col(Array<u64> col) {
  ColumnStat res;
  size_t row_n = col.size;
  size_t min = col[0];
  size_t max = col[0];
  for (size_t i = 1; i < row_n; ++i) {
    if (col[i] > max)
      max = col[i];
    if (col[i] < min)
      min = col[i];
  }
  res.l = min;
  res.u = max;
  res.f = row_n;

  
  // Do second pass to find the distinct values
  size_t alloc_size = max - min + 1;
  // TODO: If it's not < 50.000.000, use 50M
  // as a size and modulo.
  assert(alloc_size < 50000000);
  Array<bool> map(alloc_size);
  for (size_t i = 0; i != alloc_size; ++i)
    map.push(false);
  int num_distinct_values = 0;
  for (size_t i = 0; i < row_n; ++i) {
    size_t ndx = col[i].v - min;
    if (!map[ndx]) {
      map[ndx] = true;
      num_distinct_values++;
    }
  }
  res.d = num_distinct_values;
  return res;
}

void compute_stats(RelationStorage rs) {
  assert(rs.size);
  Stats stats;
  stats.relations = Array<Array<ColumnStat>>(rs.size);
  size_t k = 0;
  for (RelationData &rd : rs) {
    size_t col_n = rd.size;
    Array<ColumnStat> stat_arr(col_n);
    for (size_t i = 0; i != col_n; ++i) {
      auto col_stat = compute_stats_for_col(rd[i]);
      stat_arr.push(col_stat);
      //col_stat.print();
    }
    stats.relations.push(Array<ColumnStat>(col_n));
  }
}

ParseQueryResult reorder_query(ParseQueryResult pqr, Stats stats) {
  for (Predicate p : pqr.predicates) {
    // if same relation and predicate is join
    p.print();

    ColumnStat stats_left = stats.relations[p.lhs.first][p.lhs.second];
    ColumnStat stats_right = stats.relations[p.rhs.first][p.rhs.second];
    
    // Compute new values
    double new_l = std::max(stats_left.l, stats_right.l);   
    double new_u = std::min(stats_left.u, stats_right.u);   
    double n = new_u - new_l + 1;
    double new_f = stats_left.f / n;

    double fA = stats_left.f;
    double dA = stats_left.d;
    assert(fA);
    assert(dA);
    double f_ratio = new_f/fA;
    double power = pow(1-f_ratio, fA/dA);
    double new_d = stats_left.d * (1.0-power);

    ColumnStat new_stat{new_l, new_u, new_f, new_d};
    new_stat.print();

    // Update the 2 relations that participated.
    stats.relations[p.lhs.first][p.lhs.second] = new_stat;
    stats.relations[p.rhs.first][p.rhs.second] = new_stat;

    // Update _all_ the rest of the relations.
    int k = 0;
    for (ColumnStat &col_stat : stats.relations[p.lhs.first]) {
      if (k != p.lhs.second && k != p.rhs.second) {
        double power = pow(1-f_ratio, col_stat.f/col_stat.d);
        col_stat.d = col_stat.d * (1 - power);
        col_stat.f = new_stat.f;
      }
    }
  }

  /*
  for (ColumnStat &col_stat : stats.relations[0]) {
    col_stat.print();
  }
  */
}

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

  const char *input = "10 | 0.1=0.2 & 0.0=0.1 | 0.2 2.5 2.2";
  ParseQueryResult pqr = parse_query(input);

  //compute_stats(relation_storage);

  Stats stats;
  stats.relations = Array<Array<ColumnStat>>(1);
  Array<ColumnStat> col_stats(3);
  col_stats.push({1, 2, 3, 4});
  col_stats.push({1, 2, 3, 4});
  col_stats.push({1, 2, 3, 4});
  stats.relations.push(col_stats);
  //reorder_query(pqr, stats);

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
