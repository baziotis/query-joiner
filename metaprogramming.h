#ifndef EXERCISE_II__METAPROGRAMMING_H_
#define EXERCISE_II__METAPROGRAMMING_H_

#include <type_traits>

// We maybe want to use in the future. I'll keep them as a comment for now

//template<typename... Conditions>
//struct and_ : std::true_type {};
//
//template<typename Condition, typename... Conditions>
//struct and_<Condition, Conditions...>
//    : std::conditional<Condition::value, and_<Conditions...>, std::false_type>::type {
//};
//
//template<typename Target, typename... Ts>
//using areT = and_<std::is_same<Target, Ts>...>;

template<typename T>
struct strip_reference {
  using Type = T;
};

template<typename T>
struct strip_reference<std::reference_wrapper<T>> {
  using Type = T &;
};

template<typename T>
struct decay_and_strip {
  using Type = typename strip_reference<typename std::decay<T>::type>::Type;
};

template<typename T>
using enable_if_integral_t = typename std::enable_if<std::is_integral<T>::value, bool>::type;

#endif //EXERCISE_II__METAPROGRAMMING_H_
