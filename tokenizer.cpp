#include "tokenizer.h"

bool Tokenizer::has_next() { return index != length; }

char *Tokenizer::next_token() {
  if (!has_next()) return nullptr;

  size_t old_pos = index;
  size_t current_index = index;
  while (current_index != length && stream[current_index] != delimiter) {
    ++current_index;
  }

  size_t delimiter_offset = 0U;
  while (current_index != length && stream[current_index] == delimiter) {
    ++current_index;
    ++delimiter_offset;
  }

  stream[current_index - delimiter_offset] = '\0';
  index = current_index;
  return &stream[old_pos];
}

size_t Tokenizer::remaining_tokens() {
  if (index == length)
    return 0U;
  size_t current_index = index;
  size_t remaining_tokens = stream[length - 1U] == delimiter ? 0U : 1U;
  while (current_index != length) {
    if (stream[current_index] == delimiter) {
      ++remaining_tokens;

      while (current_index != length && stream[current_index] == delimiter) {
        ++current_index;
      }
    } else {
      ++current_index;
    }
  }
  return remaining_tokens;
}

Tokenizer Tokenizer::emtpy_tokenizer() {
  return Tokenizer{nullptr, 0, '\0', 0};
}
