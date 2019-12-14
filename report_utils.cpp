#include <cstdarg>
#include <cstdio>
#include <unistd.h>
#include "report_utils.h"

#define GET_ARGS() \
va_list args; \
va_start(args, fmt);

static void report_base(const char *tag, const char *fmt, va_list args, int fd = STDERR_FILENO) {
    if (tag != nullptr) {
        dprintf(fd, "[%s]: ", tag);
    }

    if (fmt) {
      vdprintf(fd, fmt, args);
    }

    dprintf(fd, "\n");
    va_end(args);
}

void report_error(const char *fmt, ...) {
    GET_ARGS();
    report_base("ERROR", fmt, args);
}

void report(const char *fmt, ...) {
    GET_ARGS();
    report_base(nullptr, fmt, args);
}

void freport(int fd, const char *fmt, ...) {
  GET_ARGS();
  report_base(nullptr, fmt, args, fd);
}
