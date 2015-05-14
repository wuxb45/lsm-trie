/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

#include "cmap.h"

int
main(int argc, char **argv)
{
  if (argc < 2) {
    printf("usage: %s <CM filename>\n", argv[0]);
    exit(0);
  }
  FILE * const fi = fopen(argv[1], "rb");
  assert(fi);
  uint64_t nr_units = 0;
  const size_t nun = fread(&nr_units, sizeof(nr_units), 1, fi);
  assert(nun == 1);

  const size_t nr_bytes = (nr_units + 7) >> 3;
  struct ContainerMap * const cm = (typeof(cm))malloc(sizeof(*cm) + nr_bytes);
  pthread_mutex_init(&(cm->mutex_cm), NULL);
  const size_t nus = fread(&(cm->nr_used), sizeof(cm->nr_used), 1, fi);
  assert(nus == 1);

  const size_t nby = fread(cm->bits, sizeof(cm->bits[0]), nr_bytes, fi);
  fclose(fi);
  assert(nby == nr_bytes);
  cm->nr_units = nr_units;
  containermap_show(cm);
  free(cm);
  return 0;
}
