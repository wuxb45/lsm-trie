/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <inttypes.h>

#include "stat.h"

  void
stat_inc(uint64_t *const p) { __sync_fetch_and_add(p, 1); }

  void
stat_inc_n(uint64_t *const p, const uint64_t n) { __sync_fetch_and_add(p, n); }

void
stat_show(struct Stat * const stat, FILE * const out)
{
  const struct Stat snapshot = *stat;

  // hit all
  const uint64_t nr_hit_at = snapshot.nr_get_at_hit[0] + snapshot.nr_get_at_hit[1];
  uint64_t nr_hit_vc = 0;
  for (int i = 0; i < 64; i++) {
    nr_hit_vc += snapshot.nr_get_vc_hit[i];
  }
  const uint64_t nr_hit_all = nr_hit_at + nr_hit_vc;

  // bloom
  const uint64_t nr_all_probes = snapshot.nr_false_positive + snapshot.nr_true_positive + snapshot.nr_true_negative;
  const double fp = (typeof(fp))snapshot.nr_false_positive;
  // false-positive rate: false-positve/all-probes
  const double fprate = fp * 100.0 / ((double)nr_all_probes);

  // read
  const uint64_t nr_fetch_all = snapshot.nr_fetch_barrel + snapshot.nr_fetch_bc;
  const double all_fetch_eff = ((double)nr_hit_vc) * 100.0 / ((double)nr_fetch_all);
  const double read_amp = ((double)nr_fetch_all) / ((double)nr_hit_all);

  // compaction (write)
  const uint64_t all_dumped = snapshot.nr_compaction * 8 + snapshot.nr_active_dumped;
  uint64_t nr_write_table = 0;
  for (int i = 0; i < 64; i++) {
    nr_write_table += snapshot.nr_write[i];
  }
  const uint64_t nr_write_all = snapshot.nr_write_bc + nr_write_table;
  const double write_amp = ((double)nr_write_all) / ((double)snapshot.nr_write[0]);

  fprintf(out, "====STAT====\n");
  if (snapshot.nr_get) {
    fprintf(out, "nr_get                 %10lu\n", snapshot.nr_get);
    fprintf(out, "nr_get_miss            %10lu\n", snapshot.nr_get_miss);

    fprintf(out, "nr_get_at_hit[all,0:1] %10lu %10lu %10lu\n",
        nr_hit_at, snapshot.nr_get_at_hit[0], snapshot.nr_get_at_hit[1]);
    fprintf(out, "nr_get_vc_hit[all,0:4] %10lu %10lu %10lu %10lu %10lu %10lu\n",
        nr_hit_vc, snapshot.nr_get_vc_hit[0], snapshot.nr_get_vc_hit[3],
        snapshot.nr_get_vc_hit[6], snapshot.nr_get_vc_hit[9], snapshot.nr_get_vc_hit[12]);

    fprintf(out, "nr_hit_all*            %10lu\n", nr_hit_all);
    fprintf(out, "nr_fetch_barrel        %10lu\n", snapshot.nr_fetch_barrel);
    fprintf(out, "nr_fetch_bc            %10lu\n", snapshot.nr_fetch_bc);
    fprintf(out, "nr_fetch_all*          %10lu\n", nr_fetch_all);

    fprintf(out, "nr_true_negative       %10lu\n", snapshot.nr_true_negative);
    fprintf(out, "nr_false_positive      %10lu\n", snapshot.nr_false_positive);
    fprintf(out, "nr_true_positive       %10lu\n", snapshot.nr_true_positive);
    fprintf(out, "false-post. rate*      %10.4lf%%\n", fprate);
    fprintf(out, "all-fetch-efficiency*  %10.4lf%%\n", all_fetch_eff);
    fprintf(out, "read_amplification*    %10.4lf\n", read_amp);
  }
  if (snapshot.nr_set) {
    fprintf(out, "nr_set                 %10lu\n", snapshot.nr_set);
    fprintf(out, "nr_set_retry           %10lu\n", snapshot.nr_set_retry);
    fprintf(out, "nr_compaction          %10lu\n", snapshot.nr_compaction);
    fprintf(out, "nr_active_dumped       %10lu\n", snapshot.nr_active_dumped);
    fprintf(out, "all_dumped*            %10lu\n", all_dumped);

    fprintf(out, "nr_4K_write[table,0:4] %10lu %10lu %10lu %10lu %10lu %10lu\n",
        nr_write_table, snapshot.nr_write[0], snapshot.nr_write[3],
        snapshot.nr_write[6], snapshot.nr_write[9], snapshot.nr_write[12]);
    fprintf(out, "nr_4K_write_bc         %10lu\n", snapshot.nr_write_bc);
    fprintf(out, "nr_4K_write_all*       %10lu\n", nr_write_all);
    fprintf(out, "write_amplification*   %10.4lf\n", write_amp);
  }
}

#define STAT_COUNTER_CAP ((UINT64_C(100000)))
#define STAT_COUNTER_NUSEC ((UINT64_C(1)))
  uint32_t*
latency_initial(void)
{
  const uint64_t size = sizeof(uint32_t) * STAT_COUNTER_CAP;
  void *const m = malloc(size);
  assert(m);
  bzero(m, size);
  return (uint32_t *)m;
}

  void
latency_record(const uint64_t usec, uint32_t *const counters)
{
  const uint64_t id = usec/STAT_COUNTER_NUSEC;
  if (id < STAT_COUNTER_CAP) {
    __sync_add_and_fetch(&(counters[id]), 1);
  }
}

  void
latency_show(const char * const tag, uint32_t *const counters, FILE * const out)
{
  uint64_t sum = 0;
  uint64_t max = 0;
  // sum & max
  for (uint64_t i = 0; i < STAT_COUNTER_CAP; i++) {
    if (counters[i] > 0) {
      sum += counters[i];
      max = i;
    }
  }
  if (sum == 0) return;
  fprintf(out, "====Latency Stat:%s\n", tag);
  fprintf(out, "[x<L<x+%lu] %10s %10s\n", STAT_COUNTER_NUSEC, "[COUNT]", "[%]");

  // 1/1024
  const uint64_t c1 = sum >> 10;
  const double sumd = (double)sum;
  const double d1 = sumd * 0.01;

  const uint64_t p95 = (uint64_t)(sumd * 0.95);
  const uint64_t p99 = (uint64_t)(sumd * 0.99);
  const uint64_t p999 = (uint64_t)(sumd * 0.999);
  uint64_t i95=0, i99=0, i999=0;

  uint64_t count = 0;
  for (uint64_t i = 0; i < STAT_COUNTER_CAP; i++) {
    if (counters[i] > 0) {
      count += counters[i];
    }

    if (count && (count >= p95) && (i95 == 0)) i95 = i;
    if (count && (count >= p99) && (i99 == 0)) i99 = i;
    if (count && (count >= p999) && (i999 == 0)) i999 = i;

    if (counters[i] > c1) {
      const double p = ((double)counters[i]) / d1;
      fprintf(out, "%10lu %10u %10.3lf\n", i * STAT_COUNTER_NUSEC, counters[i], p);
    }
  }
  fprintf(out, "%6s  %8lu us\n%6s  %8lu us\n%6s  %8lu us\n%6s  %8lu us\n",
      "MAX", max * STAT_COUNTER_NUSEC, "95%", i95 * STAT_COUNTER_NUSEC,
      "99%", i99 * STAT_COUNTER_NUSEC, "99.9%", i999 * STAT_COUNTER_NUSEC);
}

  void
latency_95_99_999(uint32_t * const counters, FILE * const out)
{
  uint64_t sum = 0;
  uint64_t max = 0;
  // sum & max
  for (uint64_t i = 0; i < STAT_COUNTER_CAP; i++) {
    if (counters[i] > 0) {
      sum += counters[i];
      max = i;
    }
  }
  if (sum == 0) return;

  // 1/1024
  const double sumd = (double)sum;

  const uint64_t p95 = (uint64_t)(sumd * 0.95);
  const uint64_t p99 = (uint64_t)(sumd * 0.99);
  const uint64_t p999 = (uint64_t)(sumd * 0.999);
  uint64_t i95=0, i99=0, i999=0;

  uint64_t count = 0;
  for (uint64_t i = 0; i < STAT_COUNTER_CAP; i++) {
    if (counters[i] > 0) {
      count += counters[i];
    }

    if (count && (count >= p95) && (i95 == 0)) i95 = i;
    if (count && (count >= p99) && (i99 == 0)) i99 = i;
    if (count && (count >= p999) && (i999 == 0)) i999 = i;
  }
  fprintf(out, "%s %6lu %s %6lu %s %6lu %s %6lu\n",
      "MAX", max * STAT_COUNTER_NUSEC, "95%", i95 * STAT_COUNTER_NUSEC,
      "99%", i99 * STAT_COUNTER_NUSEC, "99.9%", i999 * STAT_COUNTER_NUSEC);
}
