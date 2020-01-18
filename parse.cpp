#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <vector>

#include "parse.h"

using namespace std;

static constexpr int Read_Int_Succ = 0;
static constexpr int Read_Int_Err = 1;

static const char *input;

// Read an integer found in `input`. Assume that
// there is at least one non-whitespace character in the input.
// Save value in `val`.
static int read_int(int *out) {
  assert(input);
  // Skip leading whitespace.
  while (isspace(*input))
    ++input;
  if (!isdigit(*input)) {
    return Read_Int_Err;
  }
  int sum = 0;
  while (isdigit(*input)) {
    char c = *input;
    int dec = c - '0';
    sum = sum * 10 + dec;
    ++input;
    // Oveflow check
    assert(sum >= 0);
  }
  *out = sum;
  return Read_Int_Succ;
}

static void eat_whitespace() {
  while (isspace(*input))
    ++input;
}

// Assume that input contains whitespace-separated numbers
// that end with '|'. Fill the map `actual_relations` which maps
// virtual relations to actual relations.
static void parse_actual_relations(int actual_relations[max_relations+1]) {
  int i = 0;
  int val;
  // Start reading ints
  while (read_int(&val) == Read_Int_Succ) {
    assert(val >= 0 && val <= max_relations);
    actual_relations[i] = val;
    ++i;
  }
  assert(*input == '|');
  ++input;
}

static void test_parse_actual_relations() {
  int actual_relations[max_relations+1];
  input = "1 2 4|";
  parse_actual_relations(actual_relations);
  assert(actual_relations[0] == 1);
  assert(actual_relations[1] == 2);
  assert(actual_relations[2] == 4);

  input = "3 1 7|  ";
  parse_actual_relations(actual_relations);
  assert(actual_relations[0] == 3);
  assert(actual_relations[1] == 1);
  assert(actual_relations[2] == 7);
}

// Parse parts of predicates in that form: x.y
static Pair<int, int> parse_dotted_part() {
  int v1, v2;
  eat_whitespace();
  assert(read_int(&v1) == Read_Int_Succ);
  eat_whitespace();
  assert(*input == '.');
  ++input;
  assert(read_int(&v2) == Read_Int_Succ);
  return {v1, v2};
}

static int is_pred_op(char c) {
  return (c == '=' || c == '<' || c == '>');
}

static Predicate parse_predicate() {
  int val;
  // We can assume that LHS is always a dotted part.
  // Parse the LHS
  Predicate ret;
  Pair<int, int> lhs = parse_dotted_part();
  ret.lhs = lhs;
  
  // Get the op
  eat_whitespace();
  char op = *input++;
  assert(is_pred_op(op));
  if (op == '=') {
    // At this point, we can't really know if we have a filter
    // predicate or a join predicate. So, we read an int in
    // any case.
    assert(read_int(&val) == Read_Int_Succ);
    eat_whitespace();
    if (*input == '.') {
      // We have a join predicate. The RHS is
      // a dotted part (which turn, has 2 parts, the left
      // part of the dot and the right). But we have already
      // read the left part (`v`) so we will read manually the
      // right part.
      ++input;
      int left_part_of_dot, right_part_of_dot;
      left_part_of_dot = val;
      assert(read_int(&right_part_of_dot) == Read_Int_Succ);
      Pair<int, int> rhs = {left_part_of_dot, right_part_of_dot};
      ret.kind = PRED::JOIN;
      ret.rhs = rhs;
    } else {
      // It's a filter predicate with the filter val being
      // the int we just read (`v`).
      ret.kind = PRED::FILTER;
      ret.filter_val = val;
      ret.op = op;
    }
  } else {
      // It's definitely a filter predicate.
      assert(read_int(&val) == Read_Int_Succ);
      ret.kind = PRED::FILTER;
      ret.filter_val = val;
      ret.op = op;
  }
  return ret;
}

// Assuming that the input is correct.
static int find_num_predicates() {
  const char *temp = input;
  int res = 0;
  while (*temp) {
    if (*temp == '&')
      res++;
    ++temp;
  }
  return res + 1;
}

// Assuming that the input is correct.
static int find_num_sums() {
  const char *temp = input;
  int res = 0;
  while (*temp) {
    if (*temp == '.')
      res++;
    ++temp;
  }
  return res;
}

static void reverse(Array<Predicate> predicates, int start, int end) {
  int i = start;
  int j = end;
  while (i < j) {
    Predicate temp = predicates[i];
    predicates[i] = predicates[j];
    predicates[j] = temp;
    i++;
    --j;
  }
}

static void parse_all_predicates(Array<Predicate> predicates) {
  assert(predicates.size > 0);
  ssize_t left = -1, right = predicates.size;
  ssize_t num_join_predicates = 0;
  eat_whitespace();
  while (true) {
    Predicate pr = parse_predicate();
    assert(left < right);
    if (pr.kind == PRED::FILTER) {
      predicates[++left] = pr;
    } else {
      ++num_join_predicates;
      predicates[--right] = pr;
    }
    eat_whitespace();
    int c = *input;
    assert(c == '&' || c == '|');
    ++input;
    if (c == '|')
      break;
    eat_whitespace();
  }

  reverse(predicates, right, predicates.size - 1);

  // Make sure the join predicates are serially connected.
  // Note: It is assumed that a connection exists.
  if (num_join_predicates > 1) {
    int relations_used[max_relations] = { 0 };
    ssize_t num_predicates = predicates.size;
    ssize_t first_join_predicate = num_predicates - num_join_predicates;
    // Mark the 2 relations that the first
    // predicate uses.
    // TODO: Take into considerations the filters that always come
    // before.
    relations_used[predicates[first_join_predicate].lhs.first] = 1;
    relations_used[predicates[first_join_predicate].rhs.first] = 1;
    // Start in the second join predicate.
    for (ssize_t i = first_join_predicate + 1;
         i < num_predicates;
         ++i)
    {
      // Search for a predicate that has
      // at least one relation that has already been used.
      int flag = 0;
      for (ssize_t j = i; j < num_predicates; ++j) {
        if ((relations_used[predicates[j].lhs.first] ||
            relations_used[predicates[j].rhs.first]) && i != j)
        {
          // Swap it.
          auto tmp = predicates[i];
          predicates[i] = predicates[j];
          predicates[j] = tmp;
        }
      }
    }
  }
}

static void test_parse_predicate() {
  int dummy[max_relations + 1];

  input = "10 2 9 1| 0.2=1.0 & 2.0=3.0 & 1.0=2.2 & 0.1=209|0.2 2.5 2.2";

  // We don't really need that
  parse_actual_relations(dummy);
  // Find the number of predicates
  int num_predicates = find_num_predicates();
  Array<Predicate> predicates(num_predicates);
  // Hacky way so that we don't have to push.
  predicates.size = num_predicates;

  parse_all_predicates(predicates);
  
  // Note that they're reordered to have connection.

  // 0.1 = 209
  //assert(predicates[0].kind == PRED::FILTER);
  //assert(predicates[0].lhs.first == 0);
  //assert(predicates[0].lhs.second == 1);
  //assert(predicates[0].op == '=');
  //assert(predicates[0].filter_val == 209);

  //// 1.0 = 2.2
  //assert(predicates[1].kind == PRED::JOIN);
  //assert(predicates[1].lhs.first == 1);
  //assert(predicates[1].lhs.second == 0);
  //assert(predicates[1].rhs.first == 2);
  //assert(predicates[1].rhs.second == 2);

  //// 0.2 = 1.0
  //assert(predicates[2].kind == PRED::JOIN);
  //assert(predicates[2].lhs.first == 0);
  //assert(predicates[2].lhs.second == 2);
  //assert(predicates[2].rhs.first == 1);
  //assert(predicates[2].rhs.second == 0);

  //// 2.0 = 3.0
  //assert(predicates[3].kind == PRED::JOIN);
  //assert(predicates[3].lhs.first == 2);
  //assert(predicates[3].lhs.second == 0);
  //assert(predicates[3].rhs.first == 3);
  //assert(predicates[3].rhs.second == 0);
}

ParseQueryResult parse_query(const char *query) {
  // TODO: Remove these 2.
  test_parse_actual_relations();
  test_parse_predicate();

  ParseQueryResult pqr;
  input = query;
  // Parse the actual relations and fill the `actual_relations` map.
  parse_actual_relations(pqr.actual_relations);

  // Find the number of predicates
  int num_predicates = find_num_predicates();
  Array<Predicate> predicates(num_predicates);
  // Hacky way so that we don't have to push.
  predicates.size = num_predicates;

  // Parse and save predicates.
  parse_all_predicates(predicates);

  // Parse and save sums.
  eat_whitespace();
  int num_sums = find_num_sums();
  Array<Pair<int, int>> sums(num_sums);
  while (*input) {
    sums.push(parse_dotted_part());
    eat_whitespace();
  }

  pqr.predicates = predicates;
  pqr.sums = sums;

  return pqr;
}
