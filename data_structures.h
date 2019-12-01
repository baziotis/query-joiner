#ifndef QUERY_JOINER__DATA_STRUCTURES_H_
#define QUERY_JOINER__DATA_STRUCTURES_H_

#include "array.h"
#include "stretchy_buf.h"
#include "pair.h"

struct ColumnCollection;

struct SortContext {
  size_t from;
  size_t to;
  size_t byte_pos;
};

struct MemoryContext {
  ColumnCollection &aux;
  StretchyBuf<SortContext> stack;
};

using ColumnEntry = Pair<u64, u64>;

struct ColumnCollection : public Array<ColumnEntry> {
  explicit ColumnCollection(u64 size) : Array(size) {}
  ColumnCollection(Array<ColumnEntry> collection) : Array(collection) {}

  void sort(MemoryContext mem_context);

 private:
  using MinMaxPair = Pair<int, int>;
  MinMaxPair construct_histogram(size_t out_hist[256], size_t byte_pos);
};

#endif //QUERY_JOINER__DATA_STRUCTURES_H_
