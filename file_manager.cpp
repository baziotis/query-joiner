#include <cstdio>
#include <cassert>
#include "file_manager.h"

RelationData *FileManager::create_relation(const char *filename) {
  FILE *fp = fopen(filename, "rb");
  assert(fp != nullptr);
  uint64_t dimensions[2], bytes_read;
  bytes_read = fread(dimensions, sizeof(uint64_t), 2, fp);
  assert(bytes_read > 0);

  auto *relation = new RelationData(dimensions[0], dimensions[1]);
  for (size_t c = 0; c < dimensions[1]; ++c) {
    bytes_read = fread(relation[c].data, sizeof(uint64_t), dimensions[0], fp);
    assert(bytes_read > 0);
  }

  fclose(fp);
  return relation;
}
