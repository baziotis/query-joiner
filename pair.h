#ifndef EXERCISE_II__PAIR_H_
#define EXERCISE_II__PAIR_H_

#include <utility>
#include <bits/unique_ptr.h>
#include "metaprogramming.h"

template<typename T, typename U>
struct Pair {
  T first;
  U second;

  inline bool operator==(Pair<T, U> &rhs) {
    return first == rhs.first && second == rhs.second;
  }

  inline bool operator<(Pair<T, U> &rhs) {
    return first < rhs.first || (first == rhs.first && second < rhs.second);
  }

  inline bool operator>(Pair<T, U> &rhs) {
    return first > rhs.first || (first == rhs.first && second > rhs.second);
  }

  inline bool operator<=(Pair<T, U> &rhs) {
    return first <= rhs.first || (first == rhs.first && second <= rhs.second);
  }

  inline bool operator>=(Pair<T, U> &rhs) {
    return first >= rhs.first || (first == rhs.first && second >= rhs.second);
  }

  inline bool operator!=(Pair<T, U> &rhs) {
    return first != rhs.first || second != rhs.second;
  }
};

template <typename T, typename U>
inline Pair<typename decay_and_strip<T>::Type, typename decay_and_strip<U>::Type>
make_pair(T &&first, U &&second) {
  using DS_First = typename decay_and_strip<T>::Type;
  using DS_Second = typename decay_and_strip<U>::Type;
  using Pair_Type = Pair<DS_First, DS_Second>;
  return Pair_Type{std::forward<T>(first), std::forward<U>(second)};
}

#endif //EXERCISE_II__PAIR_H_
