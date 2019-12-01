#include "data_structures.h"

ColumnCollection::MinMaxPair ColumnCollection::construct_histogram(size_t out_hist[256], size_t byte_pos) {
  int min = 255;
  int max = 0;
  for (ColumnEntry entry : *this) {
    int b = entry.first.byte(byte_pos);
    min = std::min(min, b);
    max = std::max(max, b);
    ++out_hist[b];
  }
  return make_pair(min, max);
}

void ColumnCollection::sort(MemoryContext mem_context) {
  ColumnCollection copy = *this;
  ColumnCollection aux_copy = mem_context.aux;

  auto stack = mem_context.stack;
  stack.push({0, this->size, 0});

  while (!stack.empty()) {
    size_t hist[256] = {0};
    SortContext context = stack.pop();
    size_t byte_pos = context.byte_pos;

    if (byte_pos == 8) continue;

    if ((byte_pos & 1) != 0) {
      copy = mem_context.aux;
      aux_copy = *this;
    } else {
      copy = *this;
      aux_copy = mem_context.aux;
    }

    ColumnCollection curr = copy.subarray(context.from, context.to);
    ColumnCollection curr_aux = aux_copy.subarray(context.from, context.to);

    MinMaxPair min_max = curr.construct_histogram(hist, byte_pos);

  }
}

