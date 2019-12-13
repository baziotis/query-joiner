#ifndef STRETCHY_BUF_H
#define STRETCHY_BUF_H

#include "common.h"
#include <cstdlib>

#include <limits>

#define MAX(a, b) ((a > b) ? a : b)

// A stretchy buffer without C++ magic.
// This is essentially a simple std::vector

template<typename T>
struct StretchyBuf {
  // Members
  size_t cap, len;
  T *data;

  StretchyBuf() {
    cap = len = 0;
    data = nullptr;
  }

  StretchyBuf(size_t n) : StretchyBuf() { reserve(n); }

  // No destructor, no copy and rvalue constructors.
  // It works like a C struct in those moves.

 private:
  void _grow(size_t new_len) {
    constexpr size_t size_t_max = std::numeric_limits<size_t>::max();
    assert(cap <= (size_t_max - 1) / 2);
    size_t new_cap = MAX(MAX(2 * cap, new_len), 16);
    assert(new_len <= new_cap);
    assert(new_cap <= (size_t_max) / sizeof(T));
    size_t new_size = new_cap * sizeof(T);
    data = (T *) realloc(data, new_size);
    assert(data);
    cap = new_cap;
  }

 public:
  void push(T v) {
    constexpr size_t size_t_max = std::numeric_limits<size_t>::max();
    assert(len < size_t_max);
    size_t new_len = len + 1;
    if (new_len > cap)
      _grow(new_len);
    data[len] = v;
    len = new_len;
  }

  T pop() {
    assert(!empty());
    --len;
    return data[len];
  }

  bool empty() { return len == 0; }

  void reserve(size_t n) {
    assert(len == 0 && cap == 0);
    _grow(n);
    cap = n;
  }

  void reset() {
    len = 0;
  }

  void clear() {
    cap = 0;
    len = 0;
  }

  void free() {
    if (data != nullptr)
      ::free(data);
    data = nullptr;
    len = 0;
    cap = 0;
  }

  void shrink_to_fit() {
    assert(len);

    data = (T *) realloc(data, len * sizeof(T));
    assert(data);
    cap = len;
  }

  T &operator[](size_t i) {
    assert(i < len);
    return data[i];
  }

  const T &operator[](size_t i) const {
    assert(i < len);
    return data[i];
  }

  inline T *begin() { return this->data; }
  inline const T *begin() const { return this->data; }

  inline T *end() { return &this->data[len]; }
  inline const T *end() const { return &this->data[len]; }
};

#endif
