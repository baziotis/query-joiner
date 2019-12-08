#ifndef _TOKENIZER_H_
#define _TOKENIZER_H_

#include "common.h"

struct Tokenizer {
  DEFAULT_CONSTRUCT(Tokenizer);

  explicit Tokenizer(char *stream, size_t length, char delimiter,
                     size_t index = 0U)
      : stream{stream}, length{length}, delimiter{delimiter}, index{index} {}

  bool has_next();
  char *next_token();
  size_t remaining_tokens();

  static Tokenizer emtpy_tokenizer();

private:
  char *stream;
  size_t length;
  char delimiter;
  size_t index;
};

#endif // _TOKENIZER_H_
