#ifndef JOB_SCHEDULER__QUEUE_H_
#define JOB_SCHEDULER__QUEUE_H_

#include <cstddef>
#include <pthread.h>
#include <utility>
#include <stdexcept>

template<typename T>
struct Queue {
  Queue(size_t capacity);

  T &pop();
  void push(const T &value);
  void push(T &&value);

  [[gnu::always_inline]]
  inline bool emtpy();

  [[gnu::always_inline]]
  inline bool full();
 private:
  T *data;
  size_t capacity;
  size_t left;
  size_t right;
};

template<typename T>
Queue<T>::Queue(size_t capacity)
    : capacity{capacity}, left{0U}, right{0U}, data{new T[capacity]} {}

template<typename T>
T &Queue<T>::pop() {
  if (emtpy()) {
    throw std::runtime_error("Cannot pop from empty queue");
  }
  T &elem = data[left++];
  left = (left != capacity) ? left : 0U;
  return elem;
}

template<typename T>
void Queue<T>::push(const T &value) {
  if (full()) {
    throw std::runtime_error("Cannot push to a full queue");
  }
  data[right++] = value;
  right = (right != capacity) ? right : 0U;
}

template<typename T>
void Queue<T>::push(T &&value) {
  if (full()) {
    throw std::runtime_error("Cannot push to a full queue");
  }
  data[right++] = std::move(value);
  right = (right != capacity) ? right : 0U;
}

template<typename T>
bool Queue<T>::emtpy() {
  bool res = left == right;
  return res;
}

template<typename T>
bool Queue<T>::full() {
  bool res = ((left == right + 1) || (right == capacity && left == 0));
  return res;
}

#endif //JOB_SCHEDULER__QUEUE_H_
