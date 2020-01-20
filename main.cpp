#include "command_interpreter.h"
#include "parse.h"
#include "relation_storage.h"
#include "query_executor.h"

#include <math.h>

size_t nr_threads = static_cast<size_t>(sysconf(_SC_NPROCESSORS_ONLN) * 2);
TaskScheduler scheduler{nr_threads};

struct ColumnStat {
  double l, u, f, d;

  bool operator==(ColumnStat rhs) const {
    return (rhs.l == l && rhs.u == u && rhs.f == f && rhs.d == d);
  }

  void print() const {
    //printf("l: %.1lf, u: %.1lf, f: %.1lf, d: %.1lf\n", l, u, f, d);
  }
};

struct Stats {
  Array<Array<ColumnStat>> relations;

  ColumnStat get_column_stat(Pair<int, int> p) const {
    return relations[p.first][p.second];
  }
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

Stats compute_stats(RelationStorage rs) {
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
    stats.relations.push(stat_arr);
  }
  return stats;
}

void check_and_add_connections(Pair<int, int> connected[max_relations][max_columns][max_joins],
                               Pair<int, int> left, Pair<int, int> right) {
    Pair<int, int> *c = connected[left.first][left.second];
    int j = 0;
    while (j < max_joins) {
      if (c[j].first == -1)
        break;
      if (c[j].first == right.first && c[j].second == right.second)
        break;
      ++j;
    }
    assert(j != max_joins);
    c[j] = right;
}

constexpr int max_join_parts = 2 * max_joins;

static int num_join_parts = 0;

struct Triple {
  int first, second, third;
};

struct L2_Triple : public Triple {
  L2_Triple() {}

  L2_Triple(int f, int s, int t, Stats st) {
    first = f;
    second = s;
    third = t;
    stats = st;
  }

  Stats stats;
};

void check_and_add_join_part(Pair<int, int> join_parts[max_join_parts],
                             Pair<int, int> join_part) {
  int i = 0;
  for (; i != max_join_parts; ++i) {
    if (join_parts[i].first == -1)
      break;
    if (join_parts[i] == join_part)
      return;
  }
  assert(i != max_join_parts);
  //printf("Add: %d, (%d, %d)\n", i, join_part.first, join_part.second);
  join_parts[i] = join_part;
  ++num_join_parts;
}

bool check_connected(Pair<int, int> connected[max_relations][max_columns][max_joins],
                     Pair<int, int> a, Pair<int, int> b) {
  Pair<int, int> *c = connected[a.first][a.second];
  for (int i = 0; i != max_joins; ++i) {
    if (c[i] == b)
      return true;
  }
  return false;
}

int __num_relations;

Stats alloc_new_stats() {
  // TODO: Update the arrays to hold actual data.
  Stats stats;
  stats.relations = Array<Array<ColumnStat>>(__num_relations);
  for (int i = 0; i < __num_relations; ++i) {
    Array<ColumnStat> col_stats(max_columns);
    col_stats.push({1, 2, 3, 4});
    col_stats.push({1, 2, 3, 4});
    col_stats.push({1, 2, 3, 4});
    col_stats.push({1, 2, 3, 4});
    col_stats.push({1, 2, 3, 4});
    col_stats.push({1, 2, 3, 4});
    col_stats.push({1, 2, 3, 4});
    col_stats.push({1, 2, 3, 4});

    for (int i = col_stats.size; i < max_columns; ++i)
      col_stats.push({1, 2, 3, 4});

    stats.relations.push(col_stats);
  }
  return stats;
}

void update_stats(Stats stats, Pair<int, int> left, Pair<int, int> right) {
  ColumnStat stats_left = stats.relations[left.first][left.second];
  ColumnStat stats_right = stats.relations[right.first][right.second];
  
  // Compute new values
  double new_l = std::max(stats_left.l, stats_right.l);
  double new_u = std::min(stats_left.u, stats_right.u);

  double new_f, new_d;
  double f_ratio;
  double n = new_u - new_l + 1;
  if (left.first == right.first) { // on same relation
    new_f = stats_left.f / n;
    
    double fA = stats_left.f;
    double dA = stats_left.d;
    assert(fA);
    assert(dA);
    f_ratio = new_f/fA;
    double power = pow(1-f_ratio, fA/dA);
    new_d = stats_left.d * (1.0-power);
  } else {
    new_f = (stats_left.f * stats_right.f) / n;
    new_d = (stats_left.d * stats_right.d) / n;
  }

  ColumnStat new_stat{new_l, new_u, new_f, new_d};

  // Update the 2 join parts that participated.
  stats.relations[left.first][left.second] = new_stat;
  stats.relations[right.first][right.second] = new_stat;


  if (left.first == right.first) { // on same relation
    // Update _all_ the rest of the relations.
    for (int k = 0; k != stats.relations[left.first].size; ++k) {
      if (k != left.second) {
        ColumnStat &col_stat = stats.relations[left.first][k];
        double power = pow(1-f_ratio, col_stat.f/col_stat.d);
        col_stat.d = col_stat.d * (1 - power);
        col_stat.f = new_stat.f;
      }
    }

    for (int k = 0; k != stats.relations[right.first].size; ++k) {
      if (k != right.second) {
        ColumnStat &col_stat = stats.relations[right.first][k];
        double power = pow(1-f_ratio, col_stat.f/col_stat.d);
        col_stat.d = col_stat.d * (1 - power);
        col_stat.f = new_stat.f;
      }
    }
  } else {
    double d_ratio = new_stat.d / stats_left.d;
    // Update _all_ the rest of the relations.
    for (int k = 0; k != stats.relations[left.first].size; ++k) {
      if (k != left.second) {
        ColumnStat &col_stat = stats.relations[left.first][k];
        double power = pow(1-d_ratio, col_stat.f/col_stat.d);
        col_stat.d = col_stat.d * (1 - power);
        col_stat.f = new_stat.f;
      }
    }
    d_ratio = new_stat.d / stats_right.d;
    for (int k = 0; k != stats.relations[right.first].size; ++k) {
      if (k != right.second) {
        ColumnStat &col_stat = stats.relations[right.first][k];
        double power = pow(1-f_ratio, col_stat.f/col_stat.d);
        col_stat.d = col_stat.d * (1 - power);
        col_stat.f = new_stat.f;
      }
    }
  }
}

// IMPORTANT - TODO: Change that to actual
void copy_stats(Stats a, Stats b) {
  for (int i = 0; i != b.relations.size; ++i)
    for (int j = 0; j != max_columns; ++j) {
      a.relations[i][j] = b.relations[i][j];
    }
}

double compute_cost(Stats stats, Pair<int, int> p) {
  return stats.get_column_stat(p).f;
}

Stats get_partial_stats_from_initial_stats(Stats initial,
                                           int actual_relations[max_relations + 1]) {
  Stats stats = alloc_new_stats();
  for (int i = 0; i < __num_relations; ++i) {
    int actual_rel = actual_relations[i];
    for (int j = 0; j < initial.relations[actual_rel].size; ++j) {
      stats.relations[i][j] = initial.relations[actual_rel][j];
    }
  }
  return stats;
}

static int queries_reordered = 0;

void update_filter(Stats stats, Predicate p) {
  assert(p.kind == PRED::FILTER);
  assert(p.op == '=');
  double k = p.filter_val;
  ColumnStat new_stat = {k, k, 1, 0};
  stats.relations[p.lhs.first][p.lhs.second] = new_stat;

  // Update all the other relations.
  for (int k = 0; k != stats.relations[p.lhs.first].size; ++k) {
    if (k != p.lhs.second) {
      ColumnStat &col_stat = stats.relations[p.lhs.first][k];
      double f_ratio = new_stat.f / col_stat.f;
      double power = pow(1-f_ratio, col_stat.f/col_stat.d);
      col_stat.d = col_stat.d * (1 - power);
    }
  }
}

ParseQueryResult rewrite_query(ParseQueryResult pqr, Stats stats) {
  Pair<int, int> connected[max_relations][max_columns][max_joins];

  for (ssize_t i = 0; i < pqr.predicates.size; ++i) {
    Predicate p = pqr.predicates[i];
    if (p.kind != PRED::FILTER)
      break;
    if (p.op == '=')
      update_filter(stats, p);
  }

  // Initialize to no connections
  for (int i = 0; i < max_relations; ++i)
    for (int j = 0; j < max_columns; ++j)
      for (int k = 0; k < max_joins; ++k)
        connected[i][j][k].first = -1;
  
  // TODO: What about filters?
  Pair<int, int> join_parts[max_join_parts];

  for (int i = 0; i != max_join_parts; ++i)
    join_parts[i].first = -1;

  
  num_join_parts = 0;
  int num_joins = 0;
  // Add connections to both parts of every join.
  for (ssize_t i = pqr.predicates.size - 1; i >= 0; --i) {
    Predicate p = pqr.predicates[i];
    if (p.kind != PRED::JOIN)
      break;
    //p.print();
    ++num_joins;
    Pair<int, int> left = {p.lhs.first, p.lhs.second};
    Pair<int, int> right = {p.rhs.first, p.rhs.second};

    check_and_add_join_part(join_parts, left);
    check_and_add_join_part(join_parts, right);

    check_and_add_connections(connected, left, right);
    check_and_add_connections(connected, right, left);
  }

  if (!num_joins || num_joins == 1 || num_join_parts != num_joins + 1) {
    return pqr;
  }

  //printf("num_join_parts: %d\n", num_join_parts);


  Pair<int, int> level_1[3];
  L2_Triple level_2[3];

  // Level 1
  int k = 0;
  for (int i = 0; i < num_join_parts; ++i) {
    for (int j = i + 1; j < num_join_parts; ++j) {
      if (check_connected(connected, join_parts[i], join_parts[j])) {
        //printf("%d %d\n", i, j);
        //printf("%d.%d, %d.%d\n", join_parts[i].first, join_parts[i].second,
        //       join_parts[j].first, join_parts[j].second);
        //printf("\n");
        assert(k < 3);
        level_1[k] = {i, j};
        ++k;
      }
    }
  }

  // Find minimum
  double min_cost;
  for (int i = 0; i < k; ++i) {
    Pair<int, int> left = join_parts[level_1[i].first];
    Pair<int, int> right = join_parts[level_1[i].second];

    //printf("left: %d.%d\n", left.first, left.second);
    //printf("right: %d.%d\n", right.first, right.second);

    // Create new stats - make a copy of the old one
    Stats new_stats = alloc_new_stats();
    copy_stats(new_stats, stats);
    
    //stats.get_column_stat(left).print();
    update_stats(new_stats, left, right);
    new_stats.get_column_stat(left).print();
    new_stats.get_column_stat(right).print();
    assert(new_stats.get_column_stat(left) == new_stats.get_column_stat(right));
    double c = compute_cost(new_stats, left);
    //printf("cost: %lf\n\n", c);
    if (i == 0 || c < min_cost)
      min_cost = c;
    new_stats.relations.clear_and_free();
  }
  
  if (num_joins == 2) {
    Pair<int, int> first = join_parts[level_1[0].first];
    Pair<int, int> second = join_parts[level_1[0].second];
    
    ssize_t i = pqr.predicates.size - num_joins;
    pqr.predicates[i].lhs = first;
    pqr.predicates[i].rhs = second;
    ++i;

    pqr.predicates[i].lhs = second;

    for (int j = 0; j != num_join_parts; ++j) {
      if (j != level_1[0].first && j != level_1[0].second) {
        pqr.predicates[i].rhs = join_parts[j];
        break;
      }
    }

    ++queries_reordered;


    return pqr;
  }

  int num_level_2 = 0;
  for (int i = 0; i < k; ++i) {
    int join_part_left = level_1[i].first;
    int join_part_right = level_1[i].second;
    Pair<int, int> left = join_parts[join_part_left];
    Pair<int, int> right = join_parts[join_part_right];

    // Create new stats - make a copy of the old one
    Stats new_stats = alloc_new_stats();
    copy_stats(new_stats, stats);
    
    update_stats(new_stats, left, right);
    assert(new_stats.get_column_stat(left) == new_stats.get_column_stat(right));
    double c = compute_cost(new_stats, left);

    if (c == min_cost) {

      //printf("here: %d %d\n", join_part_left, join_part_right);
      // Start from second because it is always bigger.
      for (int j = 0; j < num_join_parts; ++j) {
        if (j != join_part_left && j != join_part_right) {
          if (check_connected(connected, left, join_parts[j]) ||
              check_connected(connected, right, join_parts[j]))
          {
            //printf("\t%d %d %d\n", join_part_left, join_part_right, j);
            level_2[num_level_2] = L2_Triple(join_part_left, join_part_right, j, new_stats);
            num_level_2++;
          }
        }
      }

    }
  }
  if (!num_level_2)
    return pqr;

  //printf("\n\n\n");

  // Find minimum
  Triple min_triple;
  for (int i = 0; i < num_level_2; ++i) {
    Pair<int, int> first = join_parts[level_2[i].first];
    Pair<int, int> second = join_parts[level_2[i].second];
    Pair<int, int> third = join_parts[level_2[i].third];

    /*
    printf("first: %d.%d\nsecond: %d.%d\nthird: %d.%d\n",
           first.first, first.second,
           second.first, second.second,
           third.first, third.second);
    */

    // Create new stats - make a copy of the old one
    Stats new_stats = alloc_new_stats();
    copy_stats(new_stats, stats);
    
    stats.get_column_stat(third).print();
    update_stats(new_stats, first, third);
    new_stats.get_column_stat(third).print();
    double c = compute_cost(new_stats, third);
    //printf("cost: %lf\n\n", c);
    if (i == 0 || c < min_cost) {
      min_cost = c;
      min_triple = Triple{level_2[i].first, level_2[i].second, level_2[i].third};
    }
  }

  Pair<int, int> first = join_parts[min_triple.first];
  Pair<int, int> second = join_parts[min_triple.second];
  Pair<int, int> third = join_parts[min_triple.third];

  /*
  printf("%d %d %d\n\n", min_triple.first, min_triple.second, min_triple.third);
  printf("first: %d.%d\nsecond: %d.%d\nthird: %d.%d\n",
         first.first, first.second,
         second.first, second.second,
         third.first, third.second);
  */
  
  ssize_t i = pqr.predicates.size - num_joins;
  pqr.predicates[i].lhs = first;
  pqr.predicates[i].rhs = second;
  ++i;

  pqr.predicates[i].lhs = second;
  pqr.predicates[i].rhs = third;
  ++i;

  pqr.predicates[i].lhs = third;

  for (int j = 0; j != num_join_parts; ++j) {
    if (j != min_triple.first && j != min_triple.second && j != min_triple.third) {
      pqr.predicates[i].rhs = join_parts[j];
      break;
    }
  }

  queries_reordered++;

  return pqr;
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

  Stats initial_stats = compute_stats(relation_storage);

  //const char *input = "2 3 | 0.0=0.1 & 0.1=0.2 & 0.2 = 0.3 | 0.2 2.5 2.2";
  //const char *input = "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1";
  //const char *input = "4 1 2 11|0.1=1.0&1.0=2.1&1.0=3.1&0.1>2493|3.2 2.2 2.1";
//  const char *input = "9 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3 0.4";
//  ParseQueryResult pqr = parse_query(input);
//
//  __num_relations = pqr.num_relations;
//  // TODO: get part of stats from all stats
//  Stats stats = alloc_new_stats();
//  stats = get_partial_stats_from_initial_stats(initial_stats, pqr.actual_relations);
//  rewrite_query(pqr, stats);

  //return 0;

  TaskState state{};

  QueryExecutor *executor;
  int count_queries = 0;
  while (interpreter.read_query_batch()) {
    StretchyBuf<Future<StretchyBuf<uint64_t>>> future_sums{interpreter.remaining_commands()};
    for (char *query : interpreter) {
      executor = new QueryExecutor{relation_storage};
      ParseQueryResult pqr = parse_query(query);
      ++count_queries;
      __num_relations = pqr.num_relations;
//      printf("\n\nQUERY: %s\n\n", query);
      Stats stats = get_partial_stats_from_initial_stats(initial_stats, pqr.actual_relations);
      rewrite_query(pqr, stats);
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

//  printf("queries_reordered: %d / %d\n", queries_reordered, count_queries);
  fclose(fp);
  return 0;
}
