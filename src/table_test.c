/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <execinfo.h>
#include <unistd.h>
#include <string.h>
#include <openssl/sha.h>
#include <inttypes.h>

#include "debug.h"
#include "table.h"
#include "generator.h"
#include "stat.h"

  static void
table_test(const uint64_t max_value_size)
{
  srandom(debug_time_usec());
  const double t0 = debug_time_sec();
  uint8_t key[64] __attribute__((aligned(8)));
  uint8_t hash[HASHBYTES] __attribute__((aligned(8)));
  uint8_t value[1024] __attribute__((aligned(8)));
  bzero(value, 1024);
  struct Table * const table = table_alloc_default(1.5);
  struct GenInfo * const gi = generator_new_uniform(1, max_value_size);
  struct KeyValue kv;
  kv.klen = 16;
  kv.pk = key;
  kv.pv = value;
  uint64_t count = 0;
  while (true) {
    sprintf((char *)key, "%016" PRIx64, count);
    kv.vlen = gi->next(gi);
    const bool ri = table_insert_kv_safe(table, &kv);
    if (ri == false) {
      break;
    }
    count++;
  }
  free(gi);
  mempool_show(table->mempool);
  //table_show(table, stdout);
  const double t1 = debug_time_sec();
  uint64_t found1 = 0;
  for (uint64_t i = 0; i < count; i++) {
    sprintf((char *)key, "%016" PRIx64, i);
    SHA1(key, 16, hash);
    struct KeyValue * const kv = table_lookup(table, 16, key, hash);
    if (kv) {
      found1++;
      free(kv);
    }
  }
  const double t2 = debug_time_sec();
  {
    const bool rbt = table_build_bloomtable(table);
    assert(rbt == true);
    const bool rre = table_retain(table);
    assert(rre == true);
  }
  //table_show(table, stdout);
  const double t3 = debug_time_sec();
  {
    const int fd_out = open("/tmp/raw", O_CREAT | O_WRONLY | O_LARGEFILE, 00666);
    const uint64_t nr_dump = table_dump_barrels(table, fd_out, 0);
    assert(nr_dump == count);
    close(fd_out);
  }
  const bool rdm = table_dump_meta(table, "/tmp/meta", 0);
  const double t4 = debug_time_sec();
  //metatable
  assert(rdm);
  const int fd_in = open("/tmp/raw", O_RDONLY | O_LARGEFILE, 00666);
  struct Stat stat;
  bzero(&stat, sizeof(stat));
  struct MetaTable * const mt = metatable_load("/tmp/meta", fd_in, true, &stat);
  assert(mt);
  const double t5 = debug_time_sec();
  uint64_t found2 = 0;
  for (uint64_t i = 0; i < count; i++) {
    sprintf((char *)key, "%016" PRIx64, i);
    SHA1(key, 16, hash);
    struct KeyValue * const kv = metatable_lookup(mt, 16, key, hash);
    if (kv) {
      found2++;
      free(kv);
    }
  }
  const double t6 = debug_time_sec();
  stat_show(&stat, stdout);
  table_analysis_verbose(table, stdout);
  char buffer[1024];
  table_analysis_short(table, buffer);
  fprintf(stdout, "%s\n", buffer);
  printf("insert %lf\n", t1-t0);
  printf("lookup %lf\n", t2-t1);
  printf("retain %lf\n", t3-t2);
  printf("dump   %lf\n", t4-t3);
  printf("load   %lf\n", t5-t4);
  printf("lookup %lf\n", t6-t5);
  table_free(table);
  metatable_free(mt);
}

  int
main(int argc, char ** argv)
{
  (void)argc;
  (void)argv;
  table_test(200);
  table_test(300);
  table_test(400);
  table_test(500);
  table_test(600);
  return 0;
}
