#ifndef PARSE_H
#define PARSE_H

#include <cassert>

#include "array.h"
#include "pair.h"

enum class PRED {
  UNDEFINED,
  JOIN,
  FILTER,
};

struct Predicate {
  PRED kind;
  Pair<int, int> lhs;
  union {
    struct {
      int filter_val;
      int op;
    };
    Pair<int, int> rhs;
  };

  void print() const {
    assert(kind != PRED::UNDEFINED);
    if (kind == PRED::JOIN) {
      printf("%d.%d = %d.%d\n", lhs.first, lhs.second, rhs.first, rhs.second);
    } else {
      printf("%d.%d %c %d\n", lhs.first, lhs.second, op, filter_val); 
    }
  }
};

constexpr int max_relations = 20;
constexpr int max_columns = 20;
constexpr int max_joins = 4;
struct ParseQueryResult {
  int num_relations;
  int actual_relations[max_relations + 1];
  Array<Predicate> predicates;
  Array<Pair<int, int>> sums;
};

ParseQueryResult parse_query(const char *query);

#endif
