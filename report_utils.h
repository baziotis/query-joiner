#ifndef _REPORT_UTILS_H_
#define _REPORT_UTILS_H_

#define FUNCTION_TEST() report(R"([TEST]: =============== "%s" ===============)", __PRETTY_FUNCTION__)

[[gnu::format(printf, 1, 2)]]
void report_error(const char *fmt = nullptr, ...);

[[gnu::format(printf, 1, 2)]]
void report(const char *fmt = nullptr, ...);

[[gnu::format(printf, 2, 3)]]
void freport(int fd, const char *fmt = nullptr, ...);

#endif