#include <pthread.h>
#include "task_scheduler.h"
#include "report_utils.h"

static void *worker(void *arg) {
  ThreadState *state = (ThreadState *) arg;
  for (;;) {
    pthread_mutex_lock(&state->queue_mutex);
    while (!state->stop && state->task_queue.emtpy()) {
      pthread_cond_wait(&state->cond, &state->queue_mutex);
    }
    if (state->stop && state->task_queue.emtpy()) {
      pthread_mutex_unlock(&state->queue_mutex);
      break;
    }
    std::function<void()> &task = state->task_queue.pop();
    pthread_mutex_unlock(&state->queue_mutex);
    task();
  }
  report("Stopped!");
  pthread_exit(NULL);
}

TaskScheduler::TaskScheduler(size_t nr_threads, size_t queue_size)
    : nr_threads{nr_threads}, threads{new pthread_t[nr_threads]}, state{new ThreadState(queue_size)} {}

void TaskScheduler::start() {
  state->stop = false;
  for (size_t i = 0U; i != nr_threads; ++i) {
    pthread_create(&threads[i], nullptr, worker, state);
  }
}

void TaskScheduler::wait_remaining_and_stop() {
  pthread_mutex_lock(&state->queue_mutex);
  state->stop = true;
  pthread_cond_broadcast(&state->cond);
  pthread_mutex_unlock(&state->queue_mutex);
  for (size_t i = 0U; i != nr_threads; ++i) {
    pthread_join(threads[i], NULL);
  }
}