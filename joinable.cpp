#include <cstring>
#include "joinable.h"

Joinable::Joinable(size_t size) : Array(size) {}

Joinable::Joinable(Array<JoinableEntry> entries) : Array(entries) {}

Joinable::MinMaxPair Joinable::construct_histogram(size_t *out_hist, size_t byte_pos) {
  size_t min = 255;
  size_t max = 0;
  for (JoinableEntry entry : *this) {
    size_t b = entry.first.byte(byte_pos);
    min = std::min(min, b);
    max = std::max(max, b);
    ++out_hist[b];
  }
  return make_pair(min, max);
}

void Joinable::construct_prefix_sum(JoinableEntry *base_addr, const size_t *hist,
                                    JoinableEntry **out_prefix_sum,
                                    Joinable::MinMaxPair min_max) {
  for (size_t i = min_max.first; i <= min_max.second; ++i) {
    out_prefix_sum[i] = base_addr;
  }
  size_t i = min_max.first + 1;
  for (size_t prev_i = i - 1U; i <= min_max.second; ++i) {
    if (hist[i] != 0U) {
      out_prefix_sum[i] = out_prefix_sum[prev_i] + hist[prev_i];
      prev_i = i;
    }
  }
}

void Joinable::copy_data(Joinable &dest, JoinableEntry **prefix_sum, const size_t byte_pos) {
  for (const JoinableEntry entry : *this) {
    size_t b = entry.first.byte(byte_pos);
    JoinableEntry *to_insert = prefix_sum[b];
    *to_insert = entry;
    ++prefix_sum[b];
  }
}

void Joinable::sort(Joinable::MemoryContext mem_context, size_t sort_threshold) {
  Joinable copy = *this;
  Joinable aux_copy = mem_context.aux;

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

    Joinable curr = copy.subarray(context.from, context.to);
    Joinable curr_aux = aux_copy.subarray(context.from, context.to);

    MinMaxPair min_max = curr.construct_histogram(hist, byte_pos);
    JoinableEntry *prefix_sum[256] = {nullptr};
    JoinableEntry *prefix_sum_copy[256] = {nullptr};
    Joinable::construct_prefix_sum(curr_aux.data, hist, prefix_sum, min_max);

    for (size_t i = min_max.first; i <= min_max.second; ++i) {
      prefix_sum_copy[i] = prefix_sum[i];
    }

    curr.copy_data(curr_aux, prefix_sum, byte_pos);
    ColumnEntry *curr_aux_base = curr_aux.data;
    for (size_t i = min_max.first; i <= min_max.second; ++i) {
      size_t nr_elements = hist[i];
      ColumnEntry *psum_base = prefix_sum_copy[i];
      ptrdiff_t from_index = context.from + (psum_base - curr_aux_base);
      if (nr_elements > 1) {
        if ((nr_elements * sizeof(ColumnEntry)) <= sort_threshold) {
//          quick_sort(psum_base, 0, nelements - 1);
          if ((byte_pos & 1) == 0) {
            memcpy(this->data + from_index, psum_base, nr_elements * sizeof(ColumnEntry));
          }
        } else {
          ptrdiff_t to_index = from_index + nr_elements;
          stack.push(SortContext{(size_t)(from_index), (size_t)(to_index), byte_pos + 1});
        }
      } else if (nr_elements == 1 && (byte_pos & 1) == 0) {
        (*this)[from_index] = mem_context.aux[from_index];
      }
    }
  }
}

StretchyBuf<Join::RowIdPair> Join::operator()(Joinable lhs, Joinable rhs) {

}

