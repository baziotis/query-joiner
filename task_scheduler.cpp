#include <pthread.h>
#include "task_scheduler.h"

static void *worker(void *arg) {
  ThreadState *state = (ThreadState *) arg;
  for (;;) {
    pthread_mutex_lock(&state->queue_mutex);
    while (!state->stop && state->task_queue.emtpy()) {
      pthread_cond_wait(&state->cond, &state->queue_mutex);
    }
    if (state->stop && state->task_queue.emtpy()) {
      pthread_mutex_unlock(&state->queue_mutex);
      return NULL;
    }
    std::function<void()> &task = state->task_queue.pop();
    pthread_mutex_unlock(&state->queue_mutex);
    task();
  }
  return NULL;
}

TaskScheduler::TaskScheduler(size_t nr_threads)
    : nr_threads{nr_threads}, threads{new pthread_t[nr_threads]}, state{new ThreadState(nr_threads)} {}

void TaskScheduler::start() {
  for (size_t i = 0U; i != nr_threads; ++i) {
    pthread_create(&threads[i], nullptr, worker, state);
  }
}


