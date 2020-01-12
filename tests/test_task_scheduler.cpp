#include "../report_utils.h"
#include "../task_scheduler.h"

int sum(int x, int y) {
  int sum = x + y;
  report("%s: x + y = %d", __PRETTY_FUNCTION__, sum);
  return sum;
}

int sum_range(int from, int to) {
  int sum = 0;
  for (int i = from; i <= to; ++i) {
    sum += i;
  }
  report("%s: Result = %d", __PRETTY_FUNCTION__, sum);
  return sum;
}

void print_msg(const char *msg) {
  report("Message = %s", msg);
}

int main() {
  TaskScheduler scheduler{3};
  scheduler.start();
  auto &f1 = scheduler.add_task(sum, 10, 20);
  auto &f2 = scheduler.add_task(sum_range, 1, 5);
  auto &f3 = scheduler.add_task([](int x) {
    report("Lambda: Value = %d", x);
    return x + 1;
  }, 100);
  auto &f4 = scheduler.add_task(print_msg, "Hello World");
  report("From f1 got %d", f1.get_value());
  report("From f2 got %d", f2.get_value());
  report("From f3 got %d", f3.get_value());
  f4.wait();
  report("F4 finished");
  f1.free();
  f2.free();
  f3.free();
  f4.free();
  scheduler.wait_remaining_and_stop();
  return 0;
}