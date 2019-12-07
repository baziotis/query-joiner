//#include "CUnit/Basic.h"
//#include "common.h"
//#include "data_structures.h"
//#include "scoped_timer.h"
//#include "sort.h"
//#include "stretchy_buf.h"
//#include "utils.h"
//#include "report_utils.h"
//#include <cstddef>
//#include <cstdint>
//#include <cstdio>
//#include <cstdlib>
//#include <cstring>
//#include <fcntl.h>
//
//#define RELA_OPTION "-r"
//#define RELB_OPTION "-s"
//#define PRINT_TO_FILE_OPTION "--print-to-file"
//#define INVALID_OPTION_ARGUMENT_MSG "Argument \"%s\" is invalid for option \"%s\""
//
//static inline bool is_option(const char *str) {
//    size_t str_len = strlen(str);
//    return !strncmp(str, RELA_OPTION, str_len) ||
//        !strncmp(str, RELB_OPTION, str_len) ||
//        !strncmp(str, PRINT_TO_FILE_OPTION, str_len);
//}
//
//MinMaxPair construct_histogram(const RelationColumns rel_columns,
//                               size_t out_hist[256], size_t byte_pos) {
//#if LOG
//    log("\n---- CONSTRUCT HISTOGRAM ----\n\n\n");
//    size_t i = 0;
//#endif
//    int min = 255;
//    int max = 0;
//    for (const ColumnStore &c : rel_columns.columns) {
//        uint8_t b = c.key.byte(byte_pos);
//        if (b < min) {
//            min = b;
//        }
//        if (b > max) {
//            max = b;
//        }
//        ++out_hist[b];
//
//#if LOG
//        print_pointed_columns_and_hist(rel_columns, i, out_hist, byte_pos, b);
//        log("\n");
//        ++i;
//#endif
//    }
//    return {min, max};
//}
//
//void construct_prefix_sum(ColumnStore *s_base, const size_t hist[256],
//                          ColumnStore *out_prefix_sum[256], MinMaxPair minmax) {
//
//#if LOGk
//    log("\n---- CONSTRUCT PREFIX SUM----\n\n\n");
//#endif
//    for (int i = minmax.min; i <= minmax.max; ++i) {
//        out_prefix_sum[i] = s_base;
//    }
//    int i = minmax.min + 1;
//    for (int prev_i = i - 1U; i <= minmax.max; ++i) {
//        if (hist[i] != 0U) {
//            out_prefix_sum[i] = out_prefix_sum[prev_i] + hist[prev_i];
//            prev_i = i;
//#if LOG
//            print_pointed_histogram(hist, i, out_prefix_sum, minmax);
//            log("\n");
//#endif
//        }
//    }
//}
//
//void copy_data_to_aux(RelationColumns r, RelationColumns aux,
//                      ColumnStore *prefix_sum[256], size_t byte_pos,
//                      ColumnStore *real_aux_base = nullptr) {
//#if LOG
//    int i = 0;
//    ColumnStore *aux_base = aux.columns.data;
//    if (aux_base == real_aux_base) {
//        log("    --- R ---");
//        log("                            ");
//        log("--- S ---\n\n");
//    } else {
//        log("    --- S ---");
//        log("                            ");
//        log("--- R ---\n\n");
//    }
//#endif
//  // Before the loop, every element points to where
//  // we start for each byte. After we insert for a byte `b`,
//  // we increment by one the pointer so that the next time
//  // we insert in the next position
//  for (const ColumnStore &c : r.columns) {
//    int b = c.key.byte(byte_pos);
//    ColumnStore *to_insert = prefix_sum[b];
//    *to_insert = c;
//    // Move to next tuple for that byte.
//    ++prefix_sum[b];
//
//#if LOG
//    int just_changed = to_insert - aux_base;
//    print_r_and_aux(r, aux, i, just_changed, byte_pos);
//    log("\n");
//    ++i;
//#endif
//    }
//}
//
//inline void build_relation_columns_for_join(Relation r,
//                                            RelationColumns rel_columns) {
//    assert(r.tuples.size == rel_columns.columns.size);
//    for (size_t i = 0U; i != r.tuples.size; ++i) {
//        Tuple *t = &r.tuples[i];
//        rel_columns.columns[i] = ColumnStore{t->key, t};
//    }
//}
//
//RelationColumns sort_relation(RelationColumns r, RelationColumns aux,
//                              StretchyBuf<Relation_Context> stack,
//                              size_t sort_threshold) {
//  // MEASURE_FUNCTION();
//  RelationColumns r_copy = r;
//  RelationColumns aux_copy = aux;
//
//  size_t from_index = 0;
//  size_t to_index = r.columns.size;
//  // A Relation_Context essentially describes the slice (2 indexes)
//  // of the space and the current byte position in which we sort (i.e.
//  // the current round).
//  Relation_Context context{from_index, to_index, 0};
//
//  assert(stack.empty());
//  stack.push(context);
//
//  // ----------------------------------------
//
//  // Notes:
//  // `r` is the initial primary.
//  // `aux` is the initial auxiliary.
//  // Then the change places etc.
//
//  // ----------------------------------------
//
//  // With `stack` we create an artificial recursion.
//  while (!stack.empty()) {
//    size_t hist[256] = {0};
//    ColumnStore *prefix_sum[256] = {nullptr};
//    ColumnStore *prefix_sum_copy[256] = {nullptr};
//
//    context = stack.pop();
//
//    size_t byte_pos = context.byte_pos;
//
//    if (byte_pos == 8)
//      continue;
//
//    // Decide which is the current primary and current
//    // auxiliary according to whether we are in an even or odd
//    // round.
//    if ((byte_pos & 1) != 0) {
//      r_copy = aux;
//      aux_copy = r;
//    } else {
//      r_copy = r;
//      aux_copy = aux;
//    }
//
//    // Take slices on the current primary space and auxiliary space.
//    RelationColumns curr{
//        r_copy.columns.subarray(context.from_index, context.to_index)};
//    RelationColumns curr_aux{
//        aux_copy.columns.subarray(context.from_index, context.to_index)};
//
//    // Construct the histogram and take back the minimum and maximum
//    // bytes found.
//    MinMaxPair minmax = construct_histogram(curr, hist, byte_pos);
//    // Construct the prefix sum, which is a map of bytes to of pointers. Each pointer
//    // points to where we start to put elements of the respective byte in the
//    // auxiliary space.
//    construct_prefix_sum(curr_aux.columns.data, hist, prefix_sum, minmax);
//    // Copy prefix sum
//    for (int i = minmax.min; i <= minmax.max; ++i) {
//      prefix_sum_copy[i] = prefix_sum[i];
//    }
//    // Copy all the data to aux (which in effect, sorts them up to byte_pos).
//    copy_data_to_aux(curr, curr_aux, prefix_sum, byte_pos);
//
//    ColumnStore *curr_aux_base = curr_aux.columns.data;
//    for (int i = minmax.min; i <= minmax.max; ++i) {
//      size_t nelements = hist[i];
//      if (nelements > 1) {
//        ColumnStore *psum_base = prefix_sum_copy[i];
//        // If we're under the threshold, sort in the current auxiliary.
//        if ((nelements * sizeof(ColumnStore)) <= sort_threshold) {
//          quick_sort(psum_base, 0, nelements - 1);
//          // Copy the elements only if they're not in the final space.
//          if ((byte_pos & 1) == 0) {
//            // The compiler won't traverse this to memcpy() because
//            // with indexing operator [] there is bounds-checking (with assert);
//            ptrdiff_t from_index =
//                context.from_index + (psum_base - curr_aux_base);
//            memcpy(r.columns.data + from_index,
//                   psum_base,
//                   nelements * sizeof(ColumnStore));
//          }
//        } else {
//          // Otherwise, pass them to the next round (which will sort according
//          // to the next byte position).
//          assert(psum_base >= curr_aux_base);
//          ptrdiff_t from_index =
//              context.from_index + (psum_base - curr_aux_base);
//          assert(from_index >= 0);
//          ptrdiff_t to_index = from_index + nelements;
//          assert(to_index >= from_index);
//          stack.push(Relation_Context{(size_t)from_index, (size_t)to_index,
//                                      context.byte_pos + 1});
//        }
//      // Only one element. Copy it only if it is not in the final space.
//      } else if (nelements && (byte_pos & 1) == 0) {
//        ColumnStore *psum_base = prefix_sum_copy[i];
//        ptrdiff_t from_index =
//            context.from_index + (psum_base - curr_aux_base);
//        r.columns[from_index] = aux.columns[from_index];
//      }
//    }
//  }
//  // We eventually save all the results to `r`, which we consider
//  // as our final space.
//  return r;
//}
//
//Result merge_relations(RelationColumns r, RelationColumns s) {
//  Result res{};
//  static constexpr size_t pairs_n = 1024U * 1024U / sizeof(TupleGroup);
//  Array<TupleGroup> bucket{pairs_n};
//  StretchyBuf<PayloadType> right_payloads{};
//
//  // For each left tuple
//  for (size_t i = 0U; i != r.columns.size; ++i) {
//    size_t j = 0U;
//    u64 r_key = r.columns[i].key;
//    // Skip keys in the right relation.
//    while (j < s.columns.size && r_key > s.columns[j].key)
//      ++j;
//
//    // Reached the end of the right relation, go to the next
//    // left tuple and start again.
//    if (j == s.columns.size)
//      continue;
//
//    // For the current left tuple (and payload), save
//    // all the right payloads.
//    while (j < s.columns.size && r_key == s.columns[j].key) {
//      right_payloads.push(s.columns[j].row->payload);
//      ++j;
//    }
//    // If we have at least one saved result.
//    if (right_payloads.len) {
//      bucket.push(TupleGroup{*r.columns[i].row, right_payloads});
//      // Reset the right payloads for the next left tuple.
//      right_payloads = StretchyBuf<PayloadType>();
//      if (bucket.is_full()) {
//        res.result.add_back(bucket);
//        // reallocate
//        bucket = Array<TupleGroup>(pairs_n);
//      }
//    }
//  }
//  if (bucket.size != 0U) {
//    res.result.add_back(bucket);
//  }
//  return res;
//}
//
//Result SortMergeJoin(Relation r, Relation s) {
//  MEASURE_FUNCTION();
//  size_t l1_dcache_size = sysconf(_SC_LEVEL1_DCACHE_SIZE);
//  size_t sort_threshold = l1_dcache_size;
//
//   We abstract the memory in a memory context structure so
//   that each of the called functions don't have to manage memory.
//  // We get size for a primary space, an auxiliary space and
//  // a automatically-stretchy stack.
//  MemoryContext memctx =
//      MemoryContext::get_context(r.tuples.size, s.tuples.size);
//
//  // Create the cols for the relation
//  memctx.r_cols.set(r.tuples.size, r.tuples.capacity);
//  // Set the same sizes in the auxiliary
//  memctx.aux.set(r.tuples.size, r.tuples.capacity);
//  memctx.stack.clear();
//  build_relation_columns_for_join(r, memctx.r_cols);
//  RelationColumns r_cols =
//      sort_relation(memctx.r_cols, memctx.aux, memctx.stack, sort_threshold);
//
//  // Same as above
//  memctx.s_cols.set(s.tuples.size, s.tuples.capacity);
//  memctx.aux.set(s.tuples.size, s.tuples.capacity);
//  memctx.stack.clear();
//  build_relation_columns_for_join(s, memctx.s_cols);
//  RelationColumns s_cols =
//      sort_relation(memctx.s_cols, memctx.aux, memctx.stack, sort_threshold);
//
//  Result res = merge_relations(r_cols, s_cols);
//  memctx.free();
//  return res;
//}
//
//struct RelationPair {
//    Relation first, second;
//};
//
//RelationPair load_relations(const char *relA_filename,
//                            const char *relB_filename) {
//    File relA_file{};
//    if (!read_entire_file_into_memory(relA_filename, &relA_file)) {
//        fprintf(stderr, "Could not read file %s\n", relA_filename);
//        exit(-1);
//    }
//
//    File relB_file{};
//    if (!read_entire_file_into_memory(relB_filename, &relB_file)) {
//        fprintf(stderr, "Could not read file %s\n", relB_filename);
//        exit(-1);
//    }
//
//    Relation relA = Relation::from_file(relA_file);
//    Relation relB = Relation::from_file(relB_file);
//
//    free_file(relA_file);
//    free_file(relB_file);
//    return {relA, relB};
//}
//
//struct Program_Options {
//    char *relA_filename{nullptr};
//    char *relB_filename{nullptr};
//    bool print_to_file{false};
//};
//
//[[noreturn]] static void usage() {
//    report("Usage:\n"
//           "\t./main -r <relA_filename> -s <relB_filename> (--print-to-file)\n");
//    exit(EXIT_FAILURE);
//}
//
//static Program_Options get_program_options(int argc, char *args[]) {
//    if (argc < 5) usage();
//    Program_Options options{};
//    for (int i = 1; i < argc; ++i) {
//        const char *arg = args[i];
//        char *next_arg = args[i + 1];
//        size_t arg_len = strlen(arg);
//        if (!strncmp(arg, RELA_OPTION, arg_len)) {
//            if (is_option(next_arg)) {
//                report_error(INVALID_OPTION_ARGUMENT_MSG, next_arg, arg);
//                usage();
//            }
//            options.relA_filename = next_arg;
//            ++i;
//        } else if (!strncmp(arg, RELB_OPTION, arg_len)) {
//            if (is_option(next_arg)) {
//                report_error(INVALID_OPTION_ARGUMENT_MSG, next_arg, arg);
//                usage();
//            }
//            options.relB_filename = next_arg;
//            ++i;
//        } else if (!strncmp(arg, PRINT_TO_FILE_OPTION, arg_len)) {
//            options.print_to_file = true;
//        } else {
//            report_error("Unknown option %s", arg);
//            usage();
//        }
//    }
//    return options;
//}
//
//static void run_with_command_line_arguments(int argc, char *args[]) {
//    Program_Options options = get_program_options(argc, args);
//    RelationPair relations = load_relations(options.relA_filename, options.relB_filename);
//    Result res = SortMergeJoin(relations.first, relations.second);
//    if (options.print_to_file) {
//        char *out_filename;
//        asprintf(&out_filename, "/tmp/result.out");
//        int fd = open(out_filename,
//                      O_WRONLY | O_CREAT | O_TRUNC,
//                      S_IRWXU | S_IRGRP | S_IROTH);
//        if (fd == -1) {
//            report_error("Could not open or create file %s", out_filename);
//        } else {
//            res.print(fd);
//            close(fd);
//        }
//        free(out_filename);
//    }
//}
//
//
//#include "tests.cpp"
//
//int main(int argc, char *args[]) {
//#ifdef TESTS
//    return tests();
//#endif
//
//#if LOG
//    // Uncomment to check logging.
//    sort_relation_test_handwritten();
//#endif
//    run_with_command_line_arguments(argc, args);
//    return 1;
//}

int main() {
  return 0;
}