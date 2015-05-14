/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <assert.h>
#include <openssl/sha.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <inttypes.h>

#include "generator.h"
#include "debug.h"
#include "mempool.h"
#include "bloom.h"


  void
uncached_probe_test(void)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srandom(tv.tv_usec);
  const uint64_t nr_units = 1024*256;
  const uint64_t usize = 4096;
  uint8_t * const bf0 = malloc(usize * nr_units);
  struct BloomFilter * bfs[nr_units];

  for (uint64_t i = 0; i < nr_units; i++) {
    struct BloomFilter * const bf = (typeof(bf))(bf0 + (i * usize));
    memset(bf, -1, usize);
    bf->bytes = 4000;
    bfs[i] = bf;
  }
  const uint64_t times = UINT64_C(80000000);
  struct timeval t0, t1;
  gettimeofday(&t0, NULL);
  for (uint64_t i = 0; i < times; i++) {
    const uint64_t r = random_uint64();
    const uint64_t k = random_uint64();
    const bool b = bloom_match(bfs[r % nr_units], k);
    assert(b == true);
  }
  gettimeofday(&t1, NULL);
  free(bf0);
  const uint64_t dt = debug_tv_diff(&t0, &t1);
  printf("probe %" PRIu64 " times over 1GB, %" PRIu64" usec, %.2lf p/s\n",
      times, dt, ((double)times) * 1000000.0 / ((double)dt));
}

  void
false_positive_test(void)
{
  // random seed
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srandom(tv.tv_usec);
  uint64_t probes = 0;
  uint64_t fps = 0;

  for (int i = 0; i < 32; i++) {
    struct Mempool * const p = mempool_new(4096*4096);
    const uint64_t nr_keys = 64;
    const uint64_t nr_probes = nr_keys * 65536;
    uint64_t * const keys = (typeof(keys))malloc(sizeof(keys[0]) * nr_keys);

    struct BloomFilter *bf = bloom_create(nr_keys, p);
    // put nr_keys keys
    for (uint64_t j = 0; j < nr_keys; j++) {
      const uint64_t h = random_uint64();
      bloom_update(bf, h);
      keys[j] = h;
    }
    // true-positive
    for (uint64_t j = 0; j < nr_keys; j++) {
      assert(bloom_match(bf, keys[j]));
    }

    uint64_t fp = 0;
    uint64_t j = 0;
    while(j < nr_probes) {
      const uint64_t h = random_uint64();
      bool nonex = true;
      for (uint64_t k = 0; k < nr_keys; k++) {
        if(h == keys[k]) {
          nonex = false;
          break;
        }
      }

      if (nonex) {
        if (bloom_match(bf, h)) {
          fp++;
        }
        j++;
      }
    }
    const double fprate = ((double)fp) / ((double)nr_probes);
    printf("%" PRIu64 " out of %" PRIu64 ": %lf, ", fp, nr_probes, fprate);
    printf(" 8 %5.2lf  ", fprate *  8.0 * 100.0);
    printf("32 %5.2lf  ", fprate * 32.0 * 100.0);
    printf("40 %5.2lf  ", fprate * 40.0 * 100.0);
    printf("48 %5.2lf  ", fprate * 48.0 * 100.0);
    printf("56 %5.2lf  ", fprate * 56.0 * 100.0);
    printf("64 %5.2lf\n", fprate * 64.0 * 100.0);
    mempool_free(p);
    fps += fp;
    probes += nr_probes;
  }
  const double fprateall = ((double)fps) / ((double)probes);
  printf("%" PRIu64 " out of %" PRIu64 ": %lf, ", fps, probes, fprateall);
  printf(" 8: %5.2lf  ", fprateall *  8.0 * 100.0);
  printf("32: %5.2lf  ", fprateall * 32.0 * 100.0);
  printf("40: %5.2lf  ", fprateall * 40.0 * 100.0);
  printf("48: %5.2lf  ", fprateall * 48.0 * 100.0);
  printf("56: %5.2lf  ", fprateall * 56.0 * 100.0);
  printf("64: %5.2lf\n", fprateall * 64.0 * 100.0);
}

  void
multi_level_false_positive_test(void)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srandom(tv.tv_usec);

  struct Mempool *p = mempool_new(256*1024*1024);
  const uint64_t nr_keys = 1024;
  const uint64_t nr_levels = 64;
  const uint64_t nr_all_keys = nr_keys * nr_levels;

  uint64_t * const keys = (typeof(keys))malloc(sizeof(keys[0]) * nr_all_keys);
  struct BloomFilter *bfs[nr_levels] = {NULL};

  for (uint64_t l = 0; l < nr_levels; l++) {
    bfs[l] = bloom_create(nr_keys, p);
    // put nr_keys keys and remember in keys[]
    for (uint64_t k = 0; k < nr_keys; k++) {
      const uint64_t h = random_uint64();
      bloom_update(bfs[l], h);
      keys[l * nr_keys + k] = h;
    }
    // true-positive
    for (uint64_t k = 0; k < nr_keys; k++) {
      assert(bloom_match(bfs[l], keys[l * nr_keys + k]));
    }
  }

  // check keys in the last level
  uint64_t fp = 0;
  const uint64_t level = nr_levels - 1;
  for (uint64_t l = 0; l < level; l++) {
    for (uint64_t k = 0; k < nr_keys; k++) {
      const bool m = bloom_match(bfs[l], keys[level * nr_keys + k]);
      if (m) fp++;
    }
  }
  printf("multi-level exist keys fp: %" PRIu64 " levels, %" PRIu64 " keys/level, %" PRIu64 " probes, %" PRIu64 " f-p, %.3lf%%\n",
      nr_levels, nr_keys, level * nr_keys, fp, ((double)fp) * 100.0 /((double)nr_keys));
  fp = 0;
  const uint64_t nr_nonprobe = 10000;
  for (uint64_t l = 0; l < level; l++) {
    for (uint64_t k = 0; k < nr_nonprobe; k++) {
      const uint64_t h = random_uint64();
      const bool m = bloom_match(bfs[l], h);
      if (m) fp++;
    }
  }
  printf("multi-level non-exist keys fp: %" PRIu64 " levels, %" PRIu64 " probes, %" PRIu64 " f-p, %.3lf%%\n",
      nr_levels, level * nr_nonprobe, fp, ((double)fp) * 100.0 /((double)nr_nonprobe));

  mempool_free(p);
}

// test bloom-container
  void
containertest(void)
{
  const uint64_t xcap = 32;
  struct Mempool *mp = mempool_new(xcap * 4096 * 8);
  struct BloomFilter *bfs[8][xcap];
  struct BloomTable *bts[8];
  struct BloomContainer *bcs[8];
  uint8_t hash[20];
  struct Stat stat;
  bzero(&stat, sizeof(stat));

  // bf & bt
  for (uint64_t z = 0; z < 8; z++) { // level
    for (uint64_t i = 0; i < xcap; i++) { // index
      bfs[z][i] = bloom_create(64, mp);
      for (uint64_t j = 0; j < 64; j++) { // key id
        const uint64_t h = i + (j << 20) + (j << 30) + (j << 40) + (z << 50);
        SHA1((const unsigned char *)(&h), 8, hash);
        const uint64_t sha = *((uint64_t *)(&hash[7]));
        bloom_update(bfs[z][i], sha);
      }
    }
    bts[z] = bloomtable_build(bfs[z], xcap);
    assert(bts[z]);
  }

  // bc
  const int rawfd = open("/tmp/bctest", O_CREAT | O_TRUNC | O_RDWR | O_LARGEFILE, 00666);
  assert(rawfd >= 0);
  bcs[0] = bloomcontainer_build(bts[0], rawfd, 0, &stat);
  assert(bcs[0]);
  uint64_t match=0;
  uint64_t nomatch =0;
  for (uint64_t i = 0; i < xcap; i++) {
    for (uint64_t j = 0; j < 64; j++) {
      const uint64_t h = i + (j << 20) + (j << 30) + (j << 40);
      SHA1((const unsigned char *)(&h), 8, hash);
      const uint64_t sha = *((uint64_t *)(&hash[7]));
      const uint8_t m = bloomcontainer_match(bcs[0], i, sha);
      assert(m);
      const uint8_t n = bloomcontainer_match(bcs[0], i, sha+1);
      if (n) match ++; else nomatch++;
    }
  }
  printf("match %" PRIu64 ", nomatch %" PRIu64 " (m/n should < 1%%)\n", match, nomatch);
  printf("build[0] ok\n");

  bcs[1] = bloomcontainer_update(bcs[0], bts[1], rawfd, 0, &stat);
  printf("update[1] ok\n");
  uint64_t match01[4]={0};
  for (uint64_t i = 0; i < xcap; i++) {
    for (uint64_t j = 0; j < 64; j++) {
      const uint64_t h0 = i + (j << 20) + (j << 30) + (j << 40);
      SHA1((const unsigned char *)(&h0), 8, hash);
      const uint64_t sha0 = *((uint64_t *)(&hash[7]));
      const uint8_t m0 = bloomcontainer_match(bcs[1], i, sha0);
      if (m0 < 4) match01[m0]++;
      const uint64_t h1 = i + (j << 20) + (j << 30) + (j << 40) + (UINT64_C(1) << 50);
      SHA1((const unsigned char *)(&h1), 8, hash);
      const uint64_t sha1 = *((uint64_t *)(&hash[7]));
      const uint8_t m1 = bloomcontainer_match(bcs[1], i, sha1);
      if (m1 < 4) match01[m1]++;
    }
  }
  printf("match1:%" PRIu64 ", 2:%" PRIu64 "\n", match01[1], match01[2]);

  bcs[2] = bloomcontainer_update(bcs[1], bts[2], rawfd, 0, &stat);
  printf("update[2] ok\n");
  bcs[3] = bloomcontainer_update(bcs[2], bts[3], rawfd, 0, &stat);
  printf("update[3] ok\n");
  bcs[4] = bloomcontainer_update(bcs[3], bts[4], rawfd, 0, &stat);
  printf("update[4] ok\n");
  bcs[5] = bloomcontainer_update(bcs[4], bts[5], rawfd, 0, &stat);
  printf("update[5] ok\n");
  bcs[6] = bloomcontainer_update(bcs[5], bts[6], rawfd, 0, &stat);
  printf("update[6] ok\n");
  bcs[7] = bloomcontainer_update(bcs[6], bts[7], rawfd, 0, &stat);
  printf("update[7] ok\n");

  // match
  match = 0;
  nomatch = 0;
  uint64_t mc[8] = {0};
  uint64_t mismatch = 0;
  for (uint64_t z = 0; z < 8; z++) {
    for (uint64_t i = 0; i < xcap; i++) {
      for (uint64_t j = 0; j < 64; j++) {
        const uint64_t h = i + (j << 20) + (j << 30) + (j << 40) + (z << 50);
        SHA1((const unsigned char *)(&h), 8, hash);
        const uint64_t sha = *((uint64_t *)(&hash[7]));
        const uint8_t m = bloomcontainer_match(bcs[7], i, sha);
        assert(m);
        mc[z]++;
        if ((m & (1 << z)) == 0) mismatch++;
        const uint8_t n = bloomcontainer_match(bcs[7], i, sha+1);
        if (n) match ++; else nomatch++;
      }
    }
  }
  printf("match count[0-7]:%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " mismatch %" PRIu64 "\n",
      mc[0], mc[1], mc[2], mc[3], mc[4], mc[5], mc[6], mc[7], mismatch);
  printf("containertest: passed\n");
}

  int
main(int argc, char **argv)
{
  (void)argc;
  (void)argv;
  uncached_probe_test();
  false_positive_test();
  multi_level_false_positive_test();
  containertest();
}
