#pragma once

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
      printf("%d.%d=%d.%d\n", lhs.first, lhs.second, rhs.first, rhs.second);
    } else {
      printf("%d.%d %c %d\n", lhs.first, lhs.second, op, filter_val); 
    }
  }
};

struct ParseQueryResult {
  Array<Predicate> predicates;
  Array<Pair<int, int>> sums;
};

