#include <cstdlib>
#include "../joinable.h"
#include "../report_utils.h"

static void test_joinable_sort_without_using_quicksort(size_t size, bool print_joinable = false) {
  FUNCTION_TEST();
  Joinable data(size);
  Joinable copy(size);
  Joinable aux(size);
  Joinable::MemoryContext context{aux, StretchyBuf<Joinable::SortContext>()};

  for (size_t i = 0U; i != size; ++i) {
    auto p = make_pair(u64(size - i), u64(size - i));
    data.push(p);
    copy.push(p);
  }
  if (print_joinable) {
    report("Original");
    data.print();
    report("\n");
  }
  // Use 1 as sort threshold so we don't call quicksort
  data.sort(context, 1);
  std::qsort(copy.data, copy.size, sizeof(JoinableEntry), Joinable::compare_entry);
  if (print_joinable) {
    report("Sorted");
    data.print();
    report("\n");
  }
  for (size_t i = 0U; i != size; ++i) {
    assert(data[i] == copy[i]);
  }

  data.clear_and_free();
  copy.clear_and_free();
  aux.clear_and_free();
  context.stack.free();
}

static void test_joinable_sort_using_quicksort(size_t size, bool print_joinable = false) {
  FUNCTION_TEST();

  Joinable data(size);
  Joinable copy(size);
  Joinable aux(size);
  Joinable::MemoryContext context{aux, StretchyBuf<Joinable::SortContext>()};

  for (size_t i = 0U; i != size; ++i) {
    auto p = make_pair(u64(size - i), u64(size - i));
    data.push(p);
    copy.push(p);
  }

  if (print_joinable) {
    report("Original");
    data.print();
    report("\n");
  }

  data.sort(context, 32 * 1024);
  std::qsort(copy.data, copy.size, sizeof(JoinableEntry), Joinable::compare_entry);

  if (print_joinable) {
    report("Sorted");
    data.print();
    report("\n");
  }

  for (size_t i = 0U; i != size; ++i) {
    assert(data[i] == copy[i]);
  }

  data.clear_and_free();
  copy.clear_and_free();
  aux.clear_and_free();
  context.stack.free();
}

static void test_join(size_t size, bool print_join = false) {
  FUNCTION_TEST();
  constexpr size_t sort_threshold = 32 * 1024;
  Joinable ldata(size);
  Joinable rdata(size);
  Joinable aux(size);
  Joinable::MemoryContext context{aux, StretchyBuf<Joinable::SortContext>()};

  for (size_t i = 0U; i != size; ++i) {
    auto p = make_pair(u64(size - i), u64(size - i));
    ldata.push(p);
    rdata.push(p);
  }

  ldata.sort(context, sort_threshold);
  context.stack.reset();
  rdata.sort(context, sort_threshold);
  Join join{};

  auto res = join(ldata, rdata);
  assert(res.len == size);
  size_t total_size{0U};
  for (Join::JoinRow &r : res) {
    total_size += r.second.len;
  }

  // That is for only this case where we have only every row id only once in every relation
  assert(total_size == size);

  if (print_join) {
    for (Join::JoinRow &r : res) {
      for (u64 r_row_id : r.second) {
        report("Left RowId = %lu, Right RowId = %lu", r.first.v, r_row_id.v);
      }
    }
  }

  ldata.clear_and_free();
  rdata.clear_and_free();
  aux.clear_and_free();
  context.stack.free();
}

int main() {
  constexpr size_t size = 1000;
  test_joinable_sort_without_using_quicksort(size);
  test_joinable_sort_using_quicksort(size);
  test_join(size);
  return EXIT_SUCCESS;
}