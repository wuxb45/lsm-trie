/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <math.h>

#include "generator.h"

  uint64_t
random_uint64(void)
{
  // 62 bit random value;
  const uint64_t rand64 = (((uint64_t)random()) << 31) + ((uint64_t)random());
  return rand64;
}

#define RAND64_MAX   ((((uint64_t)RAND_MAX) << 31) + ((uint64_t)RAND_MAX))
#define RAND64_MAX_D ((double)(RAND64_MAX))

  static double
random_double(void)
{
  // random between 0.0 - 1.0
  const double r = (double)random_uint64();
  const double rd = r / RAND64_MAX_D;
  return rd;
}

  static uint64_t
gen_constant(struct GenInfo * const gi)
{
  return gi->gen.constant.constant;
}

  struct GenInfo *
generator_new_constant(const uint64_t constant)
{
  struct GenInfo * const gi = (typeof(gi))malloc(sizeof(*gi));
  gi->gen.constant.constant = constant;

  gi->type = GEN_CONSTANT;
  gi->next = gen_constant;
  return gi;
}

  static uint64_t
gen_counter(struct GenInfo * const gi)
{
  const uint64_t v = __sync_fetch_and_add(&(gi->gen.counter.counter), 1lu);
  return v;
}

  struct GenInfo *
generator_new_counter(const uint64_t start)
{
  struct GenInfo * const gi = (typeof(gi))malloc(sizeof(*gi));
  gi->gen.counter.counter = start;

  gi->type = GEN_COUNTER;
  gi->next = gen_counter;
  return gi;
}

  static uint64_t
gen_exponential(struct GenInfo * const gi)
{
  const double d = - log(random_double()) / gi->gen.exponential.gamma;
  return (uint64_t)d;
}

  struct GenInfo *
generator_new_exponential(const double percentile, const double range)
{
  struct GenInfo * const gi = (typeof(gi))malloc(sizeof(*gi));
  gi->gen.exponential.gamma = - log(1.0 - (percentile/100.0)) / range;

  gi->type = GEN_EXPONENTIAL;
  gi->next = gen_exponential;
  return gi;
}

  static uint64_t
gen_zipfian(struct GenInfo * const gi)
{
  // simplified: no increamental update
  const struct GenInfo_Zipfian *gz = &(gi->gen.zipfian);
  const double u = random_double();
  const double uz = u * gz->zetan;
  if (uz < 1.0) {
    return gz->base + 0lu;
  } else if (uz < (1.0 + pow(0.5, gz->theta))) {
    return gz->base + 1lu;
  }
  const double x = ((double)gz->nr_items) * pow(gz->eta * (u - 1.0) + 1.0, gz->alpha);
  const uint64_t ret = gz->base + (uint64_t)x;
  return ret;
}

#define FNV_OFFSET_BASIS_64 ((UINT64_C(0xCBF29CE484222325)))
#define FNV_PRIME_64 ((UINT64_C(1099511628211)))

  static uint64_t
FNV_hash64(const uint64_t value)
{
  uint64_t hashval = FNV_OFFSET_BASIS_64;
  uint64_t val = value;
  for (int i = 0; i < 8; i++)
  {
    const uint64_t octet=val & 0x00fflu;
    val = val >> 8;
    // FNV-1a
    hashval = (hashval ^ octet) * FNV_PRIME_64;
  }
  return hashval;
}

  static double
zeta_range(const uint64_t start, const uint64_t count, const double theta)
{
  double sum = 0.0;
  if (count > 0x10000000) {
    fprintf(stderr, "zeta_range would take a long time... kill me our wait\n");
  }
  for (uint64_t i = 0lu; i < count; i++) {
    sum += (1.0 / pow((double)(start + i + 1lu), theta));
  }
  return sum;
}

static const uint64_t zetalist_u64[] = {0,
  UINT64_C(0x4040437dd948c1d9), UINT64_C(0x4040b8f8009bce85),
  UINT64_C(0x4040fe1121e564d6), UINT64_C(0x40412f435698cdf5),
  UINT64_C(0x404155852507a510), UINT64_C(0x404174d7818477a7),
  UINT64_C(0x40418f5e593bd5a9), UINT64_C(0x4041a6614fb930fd),
  UINT64_C(0x4041bab40ad5ec98), UINT64_C(0x4041cce73d363e24),
  UINT64_C(0x4041dd6239ebabc3), UINT64_C(0x4041ec715f5c47be),
  UINT64_C(0x4041fa4eba083897), UINT64_C(0x4042072772fe12bd),
  UINT64_C(0x4042131f5e380b72), UINT64_C(0x40421e53630da013),
};

static const double * zetalist_double = (typeof(zetalist_double))zetalist_u64;
static const uint64_t zetalist_step = UINT64_C(0x10000000000);
static const uint64_t zetalist_count = 16;
static const double zetalist_theta = 0.99;

  static double
zeta(const uint64_t n, const double theta)
{
  assert(theta == zetalist_theta);
  const uint64_t zlid0 = n / zetalist_step;
  const uint64_t zlid = (zlid0 > zetalist_count) ? zetalist_count : zlid0;
  const double sum0 = zetalist_double[zlid];
  const uint64_t start = zlid * zetalist_step;
  const uint64_t count = n - start;
  assert(n > start);
  const double sum1 = zeta_range(start, count, theta);
  return sum0 + sum1;
}

  struct GenInfo *
generator_new_zipfian(const uint64_t min, const uint64_t max)
{
  struct GenInfo * const gi = (typeof(gi))malloc(sizeof(*gi));
  struct GenInfo_Zipfian * const gz = &(gi->gen.zipfian);

  const uint64_t items = max - min + 1;
  gz->nr_items = items;
  gz->base = min;
  gz->zipfian_constant = ZIPFIAN_CONSTANT;
  gz->theta = ZIPFIAN_CONSTANT;
  gz->zeta2theta = zeta(2, ZIPFIAN_CONSTANT);
  gz->alpha = 1.0 / (1.0 - ZIPFIAN_CONSTANT);
  double zetan = zeta(items, ZIPFIAN_CONSTANT);
  gz->zetan = zetan;
  gz->eta = (1.0 - pow(2.0 / (double)items, 1.0 - ZIPFIAN_CONSTANT)) / (1.0 - (gz->zeta2theta / zetan));
  gz->countforzeta = items;
  gz->min = min;
  gz->max = max;

  gi->type = GEN_ZIPFIAN;
  gi->next = gen_zipfian;
  return gi;
}

  static uint64_t
gen_xzipfian(struct GenInfo * const gi)
{
  const uint64_t z = gen_zipfian(gi);
  const uint64_t xz = gi->gen.zipfian.min + (FNV_hash64(z) % gi->gen.zipfian.nr_items);
  return xz;
}

  struct GenInfo *
generator_new_xzipfian(const uint64_t min, const uint64_t max)
{
  struct GenInfo * gi = generator_new_zipfian(min, max);

  gi->type = GEN_XZIPFIAN;
  gi->next = gen_xzipfian;
  return gi;
}

  static uint64_t
gen_uniform(struct GenInfo * const gi)
{
  const uint64_t off = (uint64_t)(random_double() * gi->gen.uniform.interval);
  return gi->gen.uniform.min + off;
}

  struct GenInfo *
generator_new_uniform(const uint64_t min, const uint64_t max)
{
  struct GenInfo * const gi = (typeof(gi))malloc(sizeof(*gi));
  gi->gen.uniform.min = min;
  gi->gen.uniform.max = max;
  gi->gen.uniform.interval = (double)(max - min);

  gi->type = GEN_UNIFORM;
  gi->next = gen_uniform;

  return gi;
}

  void
generator_destroy(struct GenInfo * const geninfo)
{
  if (geninfo) free(geninfo);
}
