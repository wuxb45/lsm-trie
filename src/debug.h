/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <sys/time.h>
#include <stdint.h>

uint64_t
debug_time_usec(void);

double
debug_time_sec(void);

uint64_t
debug_diff_usec(const uint64_t last);

double
debug_diff_sec(const double last);

uint64_t
debug_tv_diff(const struct timeval * const t0, const struct timeval * const t1);

void
debug_print_tv_diff(char *tag, const struct timeval t0, const struct timeval t1);

void
debug_trace(void);
