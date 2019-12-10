#ifndef SORT_MERGE_JOIN__JOINABLE_H_
#define SORT_MERGE_JOIN__JOINABLE_H_

#include <cstdint>
#include <unistd.h>
#include "array.h"
#include "pair.h"
#include "stretchy_buf.h"

/**
 * It represents an entry in a joinable object
 * The first value is the key (column value)
 * and the second is the row_id of the context
 * we are using the joinable.
 * Context can be either a RelationData or an Intermediate Result
 */
using JoinableEntry = Pair<u64, u64>;

/**
 * An object which stores only the column values and the row_ids
 * of a Relation or an Intermediate result
 */
struct Joinable : public Array<JoinableEntry> {
  explicit Joinable(size_t size);
  explicit Joinable(Array<JoinableEntry> entries);

  struct SortContext {
    size_t from;
    size_t to;
    size_t byte_pos;
  };

  struct MemoryContext {
    Joinable &aux;
    StretchyBuf<SortContext> stack;
  };

  /**
   * Sorts the joinable using Radix Sort like algorithm
   * @param mem_context: Memory context that keeps pre-allocated data structures needed for the sorting
   * @param sort_threshold: A threshold that determines when to use quicksort for element groups
   */
  void sort(MemoryContext mem_context, size_t sort_threshold);

  void print(int fd = STDERR_FILENO);

  static int compare_entry(const void *v1, const void *v2);

 private:
  /**
   * A pair which holds two numbers
   * First: Minimum value
   * Second: Maximum value
   */
  using MinMaxPair = Pair<size_t, size_t>;

  /**
   * Constructs a histogram from a Joinable
   * @param out_hist: The constructed histogram. It's an output argument
   * @param byte_pos: The byte position to create the histogram for
   * @return A pair which indicates the minimum and maximum byte for the byte_position given
   * That enables us to start our process from the minimum byte up to the maximum and not the
   * whone possible range of bytes (255)
   */
  MinMaxPair construct_histogram(size_t out_hist[256], size_t byte_pos);

  /**
   * Creates the prefix sum of a histogram produced by a Joinable
   * @param base_addr: The base address of the Joinable's data we are goind to write to
   * @param hist: The histogram produced by the Joinable which is going to write
   * @param out_prefix_sum: The output prefix_sum. It's an output argument
   * @param min_max: The min and max byte pair which has meaning to compute the prefix sum for.
   */
  static void construct_prefix_sum(JoinableEntry *base_addr, const size_t hist[256],
                                   JoinableEntry *out_prefix_sum[256], MinMaxPair min_max);

  /**
   * Copies the JoinableEntries to the Joinable destination based on the prefix sum and the byte_pos
   * @param dest: The destination Joinable we are going to copy the entries to
   * @param prefix_sum: The prefix sum to use in order to copy to the correct positions
   * @param byte_pos: The byte position we are using
   */
  void copy_data(Joinable &dest, JoinableEntry *prefix_sum[256], const size_t byte_pos);
};

/**
 * An object which represents the Join clause
 */
struct Join {
  /**
   * A row of a join result.
   * First: The row_id of the left Joinable
   * Second: An array of row_ids belonging to the right Joinable matched to the row_id of the left Joinable
   */
  using JoinRow = Pair<u64, StretchyBuf<u64>>;

  /**
   * The () (call) operator which does the actual join.
   * @param lhs: The left hand side Joinable
   * @param rhs: The right hand side Joinable
   * @return An array of Join Rows to be used in order to fill the intermediate result
   */
  StretchyBuf<JoinRow> operator()(Joinable lhs, Joinable rhs);
};

#endif //SORT_MERGE_JOIN__JOINABLE_H_
