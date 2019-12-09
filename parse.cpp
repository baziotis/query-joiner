#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <vector>

#include "array.h"
#include "pair.h"

using namespace std;

constexpr int Input_End = 0;
constexpr int Input_Cont = 1;

FILE *fp;

bool is_empty_or_whitespace(char *str) {
  size_t len = strlen(str);
  if (len == 0)
    return true;
  for (size_t i = 0; i < len; i++) {
    if (str[i] != ' ' && str[i] != '\n' && str[i] != '\t')
      return false;
  }
  return true;
}

int next_query(char **query_string) {
  char command_string[1024];
  char *status;

  FILE *input = stdin;
  do {
    status = fgets(command_string, 1024, input);
    if (status == nullptr)
      return Input_End;
    command_string[strlen(command_string) - 1] = '\0';
  } while (is_empty_or_whitespace(command_string));

  if (!strncmp("F", command_string, sizeof("F")))
    return Input_End; // TODO: Group end
  *query_string = new char[strlen(command_string) + 1];
  strcpy(*query_string, command_string);
  return Input_Cont;
}

constexpr int Read_Int_Succ = 0;
constexpr int Read_Int_Err = 1;

const char *input;
int val;

// Read an integer found in `input`. Assume that
// there is at least one non-whitespace character in the input.
// Save value in `val`.
int read_int() {
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
  val = sum;
  return Read_Int_Succ;
}

constexpr int max_relations = 12;
int actual_relations[max_relations + 1];

void eat_whitespace() {
  while (isspace(*input))
    ++input;
}

// Assume that input contains whitespace-separated numbers
// that end with '|'. Fill the map `actual_relations` which maps
// virtual relations to actual relations.
void parse_actual_relations() {
  int i = 0;
  // Start reading ints
  while (read_int() == Read_Int_Succ) {
    assert(val > 0 && val <= max_relations);
    actual_relations[i] = val;
    ++i;
  }
  assert(*input == '|');
  ++input;
}

void test_parse_actual_relations() {
  input = "1 2 4|";
  parse_actual_relations();
  assert(actual_relations[0] == 1);
  assert(actual_relations[1] == 2);
  assert(actual_relations[2] == 4);

  input = "3 1 7|  ";
  parse_actual_relations();
  assert(actual_relations[0] == 3);
  assert(actual_relations[1] == 1);
  assert(actual_relations[2] == 7);
}

// Parse parts of predicates in that form: x.y
Pair<int, int> parse_dotted_part() {
  eat_whitespace();
  read_int();
  int v1 = val;
  eat_whitespace();
  assert(*input == '.');
  ++input;
  read_int();
  int v2 = val;
  return {actual_relations[v1], v2};
}

int is_op(char c) {
  return (c == '=' || c == '<' || c == '>');
}

enum class PRED_TYPE {
  UNDEFINED,
  JOIN,
  FILTER,
};

struct Predicate {
  PRED_TYPE kind;
  Pair<int, int> lhs;
  union {
    struct {
      int filter_val;
      int op;
    };
    Pair<int, int> rhs;
  };

  void print() const {
    assert(kind != PRED_TYPE::UNDEFINED);
    if (kind == PRED_TYPE::JOIN) {
      printf("%d.%d=%d.%d\n", lhs.first, lhs.second, rhs.first, rhs.second);
    } else {
      printf("%d.%d %c %d\n", lhs.first, lhs.second, op, filter_val); 
    }
  }
};

Predicate parse_predicate() {
  // We can assume that LHS is always a dotted part.
  // Parse the LHS
  Predicate ret;
  Pair<int, int> lhs = parse_dotted_part();
  ret.lhs = lhs;
  
  // Get the op
  eat_whitespace();
  char op = *input++;
  assert(is_op(op));
  if (op == '=') {
    // At this point, we can't really know if we have a filter
    // predicate or a join predicate. So, we read an int in
    // any case.
    read_int();
    int v = val;
    eat_whitespace();
    if (*input == '.') {
      // We have a join predicate. The RHS is
      // a dotted part (which turn, has 2 parts, the left
      // part of the dot and the right). But we have already
      // read the left part (`v`) so we will read manually the
      // right part.
      ++input;
      read_int();
      int left_part_of_dot = actual_relations[v];
      int right_part_of_dot = val;
      Pair<int, int> rhs = {left_part_of_dot, right_part_of_dot};
      ret.kind = PRED_TYPE::JOIN;
      ret.rhs = rhs;
    } else {
      // It's a filter predicate with the filter val being
      // the int we just read (`v`).
      ret.kind = PRED_TYPE::FILTER;
      ret.filter_val = v;
      ret.op = op;
    }
  } else {
      // It's definitely a filter predicate.
      read_int();
      ret.kind = PRED_TYPE::FILTER;
      ret.filter_val = val;
      ret.op = op;
  }
  return ret;
}

// Assuming that the input is correct.
int find_num_predicates() {
  const char *temp = input;
  int res = 0;
  while (*temp) {
    if (*temp == '&')
      res++;
    ++temp;
  }
  return res + 1;
}

struct ParseQueryResult {
  Array<Predicate> predicates;
  Array<Pair<int, int>> sums;
};

// Assuming that the input is correct.
int find_num_sums() {
  const char *temp = input;
  int res = 0;
  while (*temp) {
    if (*temp == '.')
      res++;
    ++temp;
  }
  return res;
}

ParseQueryResult parse_query(const char *query) {
  test_parse_actual_relations();
  input = query;
  // Parse the actual relations and fill the `actual_relations` map.
  parse_actual_relations();

  // Find the number of predicates
  int num_predicates = find_num_predicates();
  Array<Predicate> predicates(num_predicates);

  // Parse and save predicates.
  eat_whitespace();
  while (*input != '|') {
    predicates.push(parse_predicate());
    eat_whitespace();
    int c = *input;
    assert(c == '&' || c == '|');
    ++input;
    if (c == '|')
      break;
    eat_whitespace();
  }

  // Parse and save sums.
  eat_whitespace();
  int num_sums = find_num_sums();
  Array<Pair<int, int>> sums(num_sums);
  while (*input) {
    sums.push(parse_dotted_part());
    eat_whitespace();
  }

  return {predicates, sums};
}

int main() {
  ParseQueryResult pqr =
    parse_query("3 1 7 | 0.1 > 1000 & 0.1=1.2 & 1.2=2.3 & 2.1=0.2 | 0.0 1.1");

  for (Predicate p : pqr.predicates) {
    p.print();
  }
  printf("\n");
  for (Pair<int, int> s : pqr.sums) {
    printf("%d, %d\n", s.first, s.second);
  }
}
