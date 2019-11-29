#ifndef SORT_MERGE_JOIN__COMMON_H_
#define SORT_MERGE_JOIN__COMMON_H_

#define DEBUG 1
#define LOG 0

#if DEBUG
#include <cassert>
#else
#define assert(cond)
#endif

#include <cstdint>
#include <cstdio>
#include <utility>
#include <type_traits>
#include "metaprogramming.h"

using i64 = int64_t;
using u32 = uint32_t;
using i32 = int32_t;
using u16 = uint16_t;
using i16 = int16_t;
using u8 = uint8_t;
using i8 = int8_t;
using byte = i8;

// A custom uint64_t. Mainly to add a utility that gets
// a specific byte without weird macros.
struct u64 {
  uint64_t v;

  template<typename T, enable_if_integral_t<T> = true>
  u64(T value) {
    this->v = value;
  }

  template<typename T, enable_if_integral_t<T> = true>
  u64 &operator=(T value) {
    v = value;
    return *this;
  }

  template<typename T, enable_if_integral_t<T> = true>
  operator T() { return v; }

  uint8_t byte(size_t i) const {
    assert(i < 8);
    return (v >> ((7U - i) * 8U)) & 0xFFU;
  }
};

#define DEFAULT_CONSTRUCTOR(Typename) Typename() = default;

#define DEFAULT_COPY(Typename)                                                 \
  Typename(const Typename &rhs) = default;                                     \
  Typename &operator=(const Typename &rhs) = default;

#define DEFAULT_MOVE(Typename)                                                 \
  Typename(Typename &&rhs) noexcept = default;                                 \
  Typename &operator=(Typename &&rhs) noexcept = default;

#define DEFAULT_COPY_AND_MOVE(Typename)                                        \
  DEFAULT_COPY(Typename)                                                       \
  DEFAULT_MOVE(Typename)

#define DEFAULT_CONSTRUCT(Typename)                                            \
  DEFAULT_CONSTRUCTOR(Typename)                                                \
  DEFAULT_COPY_AND_MOVE(Typename)


#if LOG

// Fight the preprocessor.
struct RelationColumns;
struct ColumnStore;
struct Relation;
struct MinMaxPair;

void log(const char *fmt, ...);
void red_on();
void red_off();
void yellow_on();
void yellow_off();
void bold_on();
void bold_off();
void print_pointer(size_t i, size_t j);
void print_pointed_columns_and_hist(RelationColumns r, size_t i,
                                    size_t hist[256], int byte_pos,
                                    size_t just_changed);
void print_pointed_histogram(const size_t hist[256], int i,
                             ColumnStore *prefix_sum[256], MinMaxPair minmax);
void print_r_and_aux(RelationColumns r, RelationColumns s, size_t i,
                     size_t just_changed, int byte_pos);
#endif

#endif
