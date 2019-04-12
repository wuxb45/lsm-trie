/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <stdint.h>

void
conc_set_affinity_0(void);

void
conc_set_affinity_n(const uint64_t cpu);

void
conc_fork_reduce(const uint64_t nr, void *(*func) (void *), void * const arg);
