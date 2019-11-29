#include "utils.h"
#include "common.h"
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

File::~File() {
  delete[] contents;
}

bool File::read_entire_file_into_memory(const char *filename, File *out_file) {
  int fd = open(filename, O_RDONLY);
  if (fd == -1)
    return false;
  struct stat info{};
  fstat(fd, &info);
  char *contents = new char[info.st_size];
  ssize_t res = read(fd, contents, info.st_size) == info.st_size;
  assert(res);
  out_file->contents = contents;
  out_file->size = info.st_size;
  close(fd);
  return true;
}

bool string_to_u64(char *string, uint64_t *out) {
  char *valid;
  *out = strtoul(string, &valid, 10);
  return *valid == '\0';
}


