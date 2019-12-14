#ifndef SORT_MERGE_JOIN__SCOPED_TIMER_H_
#define SORT_MERGE_JOIN__SCOPED_TIMER_H_

#include <chrono>
#include <iostream>

#define MEASURE_FUNCTION()                                                     \
  Scoped_Timer timer { __func__ }

class Scoped_Timer {
public:
  using clock_type = std::chrono::steady_clock;

  explicit Scoped_Timer(const char *function)
      : function_{function}, start_{clock_type::now()} {}

  ~Scoped_Timer() {
    using namespace std::chrono;
    const auto stop = clock_type::now();
    const auto duration = stop - start_;
    const auto ms = duration_cast<milliseconds>(duration).count();
    std::cout << ms << " ms " << function_ << std::endl;
  }

private:
  const char *function_{};
  const clock_type::time_point start_{};
};

#endif // SORT_MERGE_JOIN__SCOPED_TIMER_H_
