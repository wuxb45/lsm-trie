/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <stdint.h>
#include <stdio.h>

struct Stat {
  uint64_t nr_get;
  uint64_t nr_get_miss;
  uint64_t nr_get_at_hit[2];
  uint64_t nr_get_vc_hit[64];

  uint64_t nr_fetch_barrel;
  uint64_t nr_fetch_bc;

  uint64_t nr_true_negative;
  uint64_t nr_false_positive;
  uint64_t nr_true_positive;

  uint64_t nr_set;
  uint64_t nr_set_retry;

  uint64_t nr_compaction;
  uint64_t nr_active_dumped;

  uint64_t nr_write[64];
  uint64_t nr_write_bc;
};

  void
stat_inc(uint64_t *const p);

  void
stat_inc_n(uint64_t *const p, const uint64_t n);

  void
stat_show(struct Stat * const stat, FILE * const out);

uint32_t*
latency_initial(void);

  void
latency_record(const uint64_t usec, uint32_t *const counters);

  void
latency_show(const char * const tag, uint32_t *const counters, FILE * const out);

  void
latency_95_99_999(uint32_t * const counters, FILE * const out);
