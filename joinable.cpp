#include <cstring>
#include <random>
#include "joinable.h"
#include "report_utils.h"

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

static std::default_random_engine generator;

static void insertion_sort(JoinableEntry *data, size_t length) {
  for (size_t i = 1U; i < length; ++i) {
    JoinableEntry key = data[i];
    ssize_t j = i - 1U;
    while (j >= 0 && data[j] > key) {
      data[j + 1U] = data[j];
      --j;
    }
    data[j + 1U] = key;
  }
}

static ssize_t partition(JoinableEntry *data, ssize_t left_index, ssize_t right_index) {
  std::uniform_int_distribution<ssize_t> distribution(left_index, right_index);
  ssize_t random_index = distribution(generator);
  std::swap(data[random_index], data[right_index]);
  JoinableEntry pivot = data[right_index];
  ssize_t i = left_index - 1;
  for (ssize_t j = left_index; j != right_index; ++j) {
    if (data[j] <= pivot) {
      ++i;
      std::swap(data[i], data[j]);
    }
  }
  std::swap(data[i + 1], data[right_index]);
  return i + 1;
}

static void quicksort(JoinableEntry *data, ssize_t left_index, ssize_t right_index) {
  while (left_index < right_index) {
    ssize_t length = right_index - left_index + 1;
    if (length <= 16) {
      insertion_sort(data + left_index, length);
      return;
    }
    ssize_t partition_index = partition(data, left_index, right_index);
    if (partition_index - left_index < right_index - partition_index) {
      quicksort(data, left_index, partition_index - 1);
      left_index = partition_index + 1;
    } else {
      quicksort(data, partition_index + 1, right_index);
      right_index = partition_index - 1;
    }
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

    Joinable curr = (Joinable) copy.subarray(context.from, context.to);
    Joinable curr_aux = (Joinable) aux_copy.subarray(context.from, context.to);

    MinMaxPair min_max = curr.construct_histogram(hist, byte_pos);
    JoinableEntry *prefix_sum[256] = {nullptr};
    JoinableEntry *prefix_sum_copy[256] = {nullptr};
    Joinable::construct_prefix_sum(curr_aux.data, hist, prefix_sum, min_max);

    for (size_t i = min_max.first; i <= min_max.second; ++i) {
      prefix_sum_copy[i] = prefix_sum[i];
    }

    curr.copy_data(curr_aux, prefix_sum, byte_pos);
    JoinableEntry *curr_aux_base = curr_aux.data;
    for (size_t i = min_max.first; i <= min_max.second; ++i) {
      size_t nr_elements = hist[i];
      JoinableEntry *psum_base = prefix_sum_copy[i];
      ptrdiff_t from_index = context.from + (psum_base - curr_aux_base);
      if (nr_elements > 1) {
        if ((nr_elements * sizeof(JoinableEntry)) <= sort_threshold) {
          quicksort(psum_base, 0, nr_elements - 1);
          if ((byte_pos & 1) == 0) {
            memcpy(this->data + from_index, psum_base, nr_elements * sizeof(JoinableEntry));
          }
        } else {
          ptrdiff_t to_index = from_index + nr_elements;
          stack.push(SortContext{(size_t) (from_index), (size_t) (to_index), byte_pos + 1});
        }
      } else if (nr_elements == 1 && (byte_pos & 1) == 0) {
        (*this)[from_index] = mem_context.aux[from_index];
      }
    }
  }
}

void Joinable::print(int fd) {
  for (JoinableEntry e : *this) {
    freport(fd, "Key = %lu, RowId = %lu", e.first.v, e.second.v);
  }
}

int Joinable::compare_entry(const void *v1, const void *v2) {
  JoinableEntry lhs = *(JoinableEntry *) v1;
  JoinableEntry rhs = *(JoinableEntry *) v2;
  return lhs < rhs ? -1 : lhs == rhs ? 0 : 1;
}

StretchyBuf<Join::RowIdPair> Join::operator()(Joinable lhs, Joinable rhs) {

}

