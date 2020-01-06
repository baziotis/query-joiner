#include <pthread.h>
#include <functional>
#include "queue.h"

// Forward declare Future type so we can use it in the Task type
template<typename R>
struct Future;

/**
 * A type that represents a task to be executed by the task scheduler
 * @tparam F: The type of the callable that the task will execute
 * @tparam Args: The type(s) of the argument(s) that the callable will get
 */
template<typename F, typename... Args>
struct Task {
  using ReturnType = typename std::result_of<F(Args...)>::type;

  Task(F callable, Args... args) : future{Future<ReturnType>()} {
    /*
     * We wrap the call inside a lambda so we save both the callable object
     * and the argument pack of the callable.
     * When we finish the execution we notify everyone that waits for the return value
     */
    call_wrapper = [this, callable, args...]() {
      ReturnType res = callable(args...);
      future.set_value(res);
    };
  }

  void run() { call_wrapper(); }

  Future<ReturnType> &get_future() { return future; }

 private:
  Future<ReturnType> future;
  std::function<void()> call_wrapper;
};

template<typename F, typename... Args>
Task<F, Args...> make_task(F callable, Args... args) {
  return Task<F, Args...>{callable, args...};
}

template<typename F, typename... Args>
Task<F, Args...> *allocate_task(F callable, Args... args) {
  return new Task<F, Args...>{callable, args...};
}

/**
 * A type that represents the return value of the callable
 * that the Task will execute. This is somewhat of a contract
 * that the value will be set in the future
 * @tparam R: The return type of the callable
 */
template<typename R>
struct Future {
  // We declare the generic Task type as friend so it can access private methods of the Future
  template<typename F, typename... Args>
  friend
  struct Task;

  /**
   * A type that will hold the shared state of the Future object
   */
  struct FutureState {
    FutureState() : ready{false} {
      pthread_mutex_init(&mutex, NULL);
      pthread_cond_init(&ready_cond, NULL);
    }

    pthread_mutex_t mutex;
    pthread_cond_t ready_cond;
    R value;
    bool ready;
  };

  Future() : state{new FutureState()} {}

  /**
   * It retrieves the value from the Future object.
   * If the value hasn't been set yet because the task
   * related to the Future hasn't been executed yet then
   * the calling thread will block until the value is set
   * @return The return value of the Callable
   */
  R &get_value() {
    pthread_mutex_lock(&state->mutex);
    if (!state->ready) {
      pthread_cond_wait(&state->ready_cond, &state->mutex);
    }
    R &value = state->value;
    pthread_mutex_unlock(&state->mutex);
    return value;
  }

 private:

  void set_value(const R &val) {
    pthread_mutex_lock(&state->mutex);
    state->value = val;
    state->ready = true;
    pthread_cond_broadcast(&state->ready_cond);
    pthread_mutex_unlock(&state->mutex);
  }

  void set_value(R &&val) {
    pthread_mutex_lock(&state->mutex);
    state->value = std::move(val);
    state->ready = true;
    pthread_cond_broadcast(&state->ready_cond);
    pthread_mutex_unlock(&state->mutex);
  }

  FutureState *state;
};

/**
 * A structure that hold the shared state between the worker threads of the TaskScheduler
 */
struct ThreadState {
  ThreadState(size_t queue_cap) : stop{false}, task_queue{queue_cap} {
    pthread_mutex_init(&queue_mutex, NULL);
    pthread_cond_init(&cond, NULL);
  }

  Queue<std::function<void()>> task_queue;
  pthread_mutex_t queue_mutex;
  pthread_cond_t cond;
  bool stop;
};

struct TaskScheduler {
  TaskScheduler() = delete;
  TaskScheduler(size_t nr_threads);

  void start();

  /**
   * Add a task for execution to the task queue
   * @tparam F: The type of the Callable object that the task will execute
   * @tparam Args: The type(s) of the argument(s) that the Callable will get
   * @param callable: The callable object
   * @param args: The argumnent(s) of the callable
   * @return: A Future object related to the task
   */
  template<typename F, typename... Args>
  Future<typename std::result_of<F(Args...)>::type> &add_task(F callable, Args... args);

  /**
   * Add a task for execution to the task queue
   * @tparam F: The type of the Callable object that the task will execute
   * @tparam Args: The type(s) of the argument(s) that the Callable will get
   * @param task: The task to execute
   * @return: A Future object related to the task
   */
  template<typename F, typename... Args>
  Future<typename std::result_of<F(Args...)>::type> &add_task(const Task<F, Args...> &task);

 private:
  pthread_t *threads;
  size_t nr_threads;
  ThreadState *state;
};

template<typename F, typename... Args>
Future<typename std::result_of<F(Args...)>::type> &TaskScheduler::add_task(F callable, Args... args) {
  using ReturnType = typename std::result_of<F(Args...)>::type;
  Task<F, Args...> *task = allocate_task(callable, args...);
  Future<ReturnType> &future = task->get_future();
  pthread_mutex_lock(&state->queue_mutex);
  state->task_queue.push([task]() { task->run(); });
  pthread_cond_signal(&state->cond);
  pthread_mutex_unlock(&state->queue_mutex);
  return future;
}

template<typename F, typename... Args>
Future<typename std::result_of<F(Args...)>::type> &TaskScheduler::add_task(const Task<F, Args...> &task) {
  using ReturnType = typename std::result_of<F(Args...)>::type;
  Future<ReturnType> &future = task.get_future();
  pthread_mutex_lock(&state->queue_mutex);
  state->task_queue.push([task]() { task.run(); });
  pthread_cond_signal(&state->cond);
  pthread_mutex_unlock(&state->queue_mutex);
  return future;
}
