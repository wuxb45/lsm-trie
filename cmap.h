/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

struct ContainerMap {
  uint64_t nr_units;
  uint64_t nr_used;
  uint64_t total_cap;
  bool discard;
  int raw_fd;
  pthread_mutex_t mutex_cm;      // lock on operating on ContainerMap
  uint8_t bits[];
};

  struct ContainerMap *
containermap_create(const char * const raw_fn, const uint64_t cap_hint);

  struct ContainerMap *
containermap_load(const char * const meta_fn, const char * const raw_fn);

  void
containermap_dump(struct ContainerMap * const cm, const char * const meta_fn);

  void
containermap_show(struct ContainerMap * const cm);

  uint64_t
containermap_alloc(struct ContainerMap * const cm);

  bool
containermap_release(struct ContainerMap * const cm, const uint64_t offset);

  void
containermap_destroy(struct ContainerMap * const cm);

  uint64_t
containermap_unused(const struct ContainerMap * const cm);
