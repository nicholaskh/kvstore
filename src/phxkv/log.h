#pragma once

#include "logger.h"

void stderrprintf(const char *fmt...);

#define STDERR_LOG(fmt, ...) \
    stderrprintf("%s:%d:%s: " fmt, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
