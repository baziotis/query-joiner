#include <cstring>
#include <random>
#include "joinable.h"
#include "report_utils.h"

Joinable::Joinable() : Array() {}

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
  stack.free();
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
Joinable Joinable::empty() {
  Joinable empty(1);
  empty.size = 0;
  return empty;
}

using GroupIndex = Pair<size_t, size_t>;
using GroupIndexes = Pair<GroupIndex, GroupIndex>;

struct JoinThreadArgs {
  StretchyBuf<GroupIndexes> *group_indexes;
  StretchyBuf<Join::JoinRow> *result;
  volatile size_t *next_index;
  Joinable *lhs;
  Joinable *rhs;
};

static StretchyBuf<GroupIndexes> calculate_group_indexes(Joinable lhs, Joinable rhs) {
  StretchyBuf<GroupIndexes> res{};
  size_t prev_i = 0U;
  size_t prev_j = 0U;

  for (size_t i = 0U; i < lhs.size; ++i) {
    u64 prev_lhs_key = lhs[prev_i].first;
    while (i < lhs.size && prev_lhs_key == lhs[i].first) ++i;

    size_t j = prev_j;
    while (j < rhs.size && prev_lhs_key > rhs[j].first) ++j;
    if (j == rhs.size) continue;

    prev_j = j;

    while (j < rhs.size && prev_lhs_key == rhs[j].first) ++j;

    res.push({{prev_i, i}, {prev_j, j}});
    prev_i = i;
    prev_j = j;
  }

  return res;
}

void *run_merge(void *args) {
  JoinThreadArgs *info = (JoinThreadArgs *) args;
  size_t next_index;
  while ((next_index = __sync_fetch_and_add(info->next_index, 1)) < info->group_indexes->len) {
    GroupIndexes group_indexes = (*info->group_indexes)[next_index];
    GroupIndex lhs_gindex = group_indexes.first;
    GroupIndex rhs_gindex = group_indexes.second;

    size_t rhs_gsize = rhs_gindex.second - rhs_gindex.first;
    StretchyBuf<u64> rhs_row_ids{rhs_gsize};
    for (size_t j = rhs_gindex.first; j < rhs_gindex.second; ++j) {
      rhs_row_ids.push((*info->rhs)[j].second);
    }

    for (size_t i = lhs_gindex.first; i < lhs_gindex.second; ++i) {
      StretchyBuf<u64> _rhs_row_ids{rhs_gsize};
      for (u64 v : rhs_row_ids) {
        _rhs_row_ids.push(v);
      }
      (*info->result)[i] = make_pair((*info->lhs)[i].second, _rhs_row_ids);
    }
  }
  pthread_exit(NULL);
}

StretchyBuf<Join::JoinRow> Join::operator()(Joinable lhs, Joinable rhs) {
  StretchyBuf<GroupIndexes> group_indexes = calculate_group_indexes(lhs, rhs);
//  for (auto g : group_indexes) {
//    printf("LHS start = %lu, LHS end = %lu | RHS start = %lu, RHS end = %lu\n",
//           g.first.first, g.first.second, g.second.first, g.second.second);
//  }
  StretchyBuf<Join::JoinRow> *result = new StretchyBuf<Join::JoinRow>{lhs.size};
  result->len = group_indexes[group_indexes.len - 1].first.second;
  memset(result->data, '\0', lhs.size * sizeof(Join::JoinRow));
  volatile size_t next_index = 0U;
  JoinThreadArgs *args = new JoinThreadArgs{};
  args->group_indexes = &group_indexes;
  args->next_index = &next_index;
  args->lhs = &lhs;
  args->rhs = &rhs;
  args->result = result;
  size_t nr_threads = sysconf(_SC_NPROCESSORS_ONLN);
  pthread_t *threads = new pthread_t[nr_threads];

  for (size_t i = 0U; i != nr_threads; ++i) {
    pthread_create(&threads[i], NULL, run_merge, args);
  }
  for (size_t i = 0U; i != nr_threads; ++i) {
    pthread_join(threads[i], NULL);
  }

  group_indexes.free();
  delete args;
  delete[] threads;
//  StretchyBuf<Join::JoinRow>
//      res{};
//  size_t prev_j = 0U;
//  for (size_t i = 0U; i != lhs.size; ++i) {
//    StretchyBuf<u64> right_row_ids{};
//    size_t j = prev_j;
//    u64 lhs_key = lhs[i].first;
//    while (j < rhs.size && lhs_key > rhs[j].first) ++j;
//    if (j == rhs.size) continue;
//    prev_j = j;
//    while (j < rhs.size && lhs_key == rhs[j].first) {
//      right_row_ids.push(rhs[j].second);
//      ++j;
//    }
//
//    if (right_row_ids.len) {
//      right_row_ids.shrink_to_fit();
//      res.push(make_pair(lhs[i].second, right_row_ids));
//      right_row_ids = StretchyBuf<u64>{};
//    }
//  }
//  if (res.len != 0)
//    res.shrink_to_fit();
  return *result;
}

