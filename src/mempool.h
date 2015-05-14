/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

struct Mempool;

  void *
huge_alloc(const uint64_t cap);

  void
huge_free(void * const ptr, const uint64_t cap);

struct Mempool *
mempool_new(const size_t cap);

uint8_t *
mempool_alloc(struct Mempool * const p, const size_t cap);

void
mempool_free(struct Mempool * const p);

void
mempool_show(struct Mempool * const p);
