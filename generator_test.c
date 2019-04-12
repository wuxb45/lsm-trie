/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>

#include "debug.h"
#include "generator.h"

static int
uint64_compare(const void * p1, const void * p2)
{
  const uint64_t v1 = *((uint64_t * const)p1);
  const uint64_t v2 = *((uint64_t * const)p2);
  if (v1 < v2) return -1;
  else if (v1 > v2) return 1;
  else return 0;
}

static void
test_gen(struct GenInfo * gi, char * const tag, const uint64_t count)
{
  printf("#### %s generator test\n", tag);
  const uint64_t t0 = debug_time_usec();
  uint64_t *r = (typeof(r))malloc(sizeof(uint64_t) * count);
  for (uint64_t i = 0; i < count; i++) {
    const uint64_t x = gi->next(gi);
    r[i] = x;
  }
  const uint64_t dt = debug_diff_usec(t0);
  const uint64_t p1 = count / 100u;

  // stat
  qsort(r, count, sizeof(r[0]), uint64_compare);

  // count r[] to pairs[]
  struct Pair64 *pairs = (typeof(pairs))calloc(count, sizeof(*pairs));
  uint64_t pid = 0;
  pairs[pid].a = r[0];
  for (uint64_t i = 0; i < count; i++) {
    if (r[i] == pairs[pid].a) {
      pairs[pid].b++;
    } else {
      pid++;
      pairs[pid].a = r[i];
      pairs[pid].b = 1;
    }
  }

  printf("usec: %"PRIu64 " MIN: %" PRIu64 ", MAX: %" PRIu64 "\n", dt, r[0], r[count-1]);
  for (int i = 5; i < 100; i+=5) {
    printf("|%d%%:%" PRIu64 "", i, r[count * i / 100]);
  }
  printf("\n");

  uint64_t pcount = 0;
  for (uint64_t i = 0; i <= pid; i++) {
    if (pairs[i].b >= p1) {
      pcount++;
      printf("@%" PRIu64 "[%" PRIu64 "]", pairs[i].a, pairs[i].b);
    }
  }
  printf("\n");
  free(r);
}

  int
main(int argc, char ** argv)
{
  (void)argc;
  (void)argv;
  struct GenInfo * gis[11];
  gis[0] = generator_new_constant(1984);
  gis[1] = generator_new_counter(1984);
  gis[2] = generator_new_exponential(95.0, 100.0);
  gis[3] = generator_new_zipfian(20, 2000);
  gis[4] = generator_new_zipfian(0, UINT64_C(0x10000000000));
  gis[5] = generator_new_zipfian(0, UINT64_C(0x40000000000));
  gis[6] = generator_new_zipfian(0, UINT64_C(0x80000000000));
  gis[7] = generator_new_zipfian(0, UINT64_C(0xc0000000000));
  gis[8] = generator_new_zipfian(0, UINT64_C(0x100000000000));
  gis[9] = generator_new_xzipfian(20, 2000);
  gis[10] = generator_new_uniform(20, 2000);

  const uint64_t gen_nr = 1000000;
  char * tags[11] = {"CONST", "COUNTER", "EXP", "ZIPF",
  "Zipf 1<<40", "Zipf 4<<40", "Zipf 8<<40",
  "Zipf 12<<40", "Zipf 16<<40", "XZIPF", "UNIFORM"};
  for (int i = 0; i < 11; i++) {
    test_gen(gis[i], tags[i], gen_nr);
    generator_destroy(gis[i]);
  }

  // test zipfian gen build time
  for (uint64_t i = 0x100000; i < 0x1000000; i<<=1) {
    struct timeval t0, t1;
    printf("gen 0x%"PRIx64" zipfian .. ", i);
    gettimeofday(&t0, NULL);
    struct GenInfo * const gi = generator_new_zipfian(0, i);
    gettimeofday(&t1, NULL);
    debug_print_tv_diff("time elasped", t0, t1);
    generator_destroy(gi);
  }
  return 0;
}
