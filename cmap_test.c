/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <unistd.h>
#include <assert.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

#include "cmap.h"

  static void
cmap_test(void)
{
  const char * const raw_fn = "/tmp/raw_test";
  const char * const meta_fn = "/tmp/cm_test";
  const uint64_t cap = UINT64_C(1024 * 1024) * 32 * 64;

  // create
  struct ContainerMap * const cm = containermap_create(raw_fn, cap);
  assert(cm);
  containermap_show(cm);
  // alloc
  uint64_t offs[64];
  for (uint64_t i = 0; i< 64; i++) {
    const uint64_t off = containermap_alloc(cm);
    assert(off < cap);
    offs[i] = off;
  }
  const uint64_t offx = containermap_alloc(cm);
  assert(offx > cap);
  containermap_show(cm);
  // release
  for (uint64_t i = 0; i< 64; i++) {
    containermap_release(cm, offs[i]);
  }
  containermap_show(cm);
  for (uint64_t i = 0; i< 32; i++) {
    (void)containermap_alloc(cm);
  }
  containermap_show(cm);
  // store
  containermap_dump(cm, meta_fn);
  close(cm->raw_fd);
  struct ContainerMap * const cm1 = containermap_load(meta_fn, raw_fn);
  assert(cm1);
  containermap_show(cm1);
  assert(cm->nr_units == cm1->nr_units);
  assert(cm->nr_used == cm1->nr_used);
  assert(cm->total_cap == cm1->total_cap);
  assert(cm->discard == cm1->discard);
  assert(memcmp(cm->bits, cm1->bits, cm->nr_units/8) == 0);
  free(cm);
  containermap_destroy(cm1);
}

int
main(int argc, char ** argv)
{
  (void)argc;
  (void)argv;
  cmap_test();
  return 0;
}
