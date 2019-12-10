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
}

int main() {
  constexpr size_t size = 100;
  test_joinable_sort_without_using_quicksort(size);
  test_joinable_sort_using_quicksort(size);
  return EXIT_SUCCESS;
}