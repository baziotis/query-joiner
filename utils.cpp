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

size_t read_line_from_stream(StretchyBuf<char> &buffer, int fd) {
  char ch;
  size_t total_read{0U};
  ssize_t bytes_read;
  while ((bytes_read = read(fd, &ch, sizeof(char))) > 0 && ch != EOF) {
    total_read += bytes_read;
    buffer.push((char) ch);
    if (ch == '\n') break;
  }
  // This is an edge case where the file descriptor is coming from an actual file
  // and we encountered the end of file but the last line hasn't a newline character
  // at it so we add it explicitly for uniformity reasons
  if (bytes_read == 0 && buffer[buffer.len - 1] != '\n') {
    buffer.push('\n');
    ++total_read;
  }
  return bytes_read != -1 ? total_read : -1;
}

bool file_exists(const char *filename) {
  return access(filename, F_OK) != -1;
}

