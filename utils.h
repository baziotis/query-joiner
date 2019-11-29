#ifndef _UTILS_H_
#define _UTILS_H_

#include <cstddef>
#include <cstdint>

struct File {
  char *contents;
  size_t size;

  ~File();

  [[nodiscard]]
  static bool read_entire_file_into_memory(const char *filename ,File *out_file);
};

bool string_to_u64(char *string, uint64_t *out);

#endif
