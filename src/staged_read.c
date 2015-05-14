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
#include <signal.h>
#include <inttypes.h>
#include <pthread.h>

#include "coding.h"
#include "stat.h"
#include "conc.h"
#include "table.h"
#include "db.h"
#include "debug.h"
#include "generator.h"

// one for each thread
struct DBParams {
  char * tag;
  uint64_t vlen;
  char * meta_dir;
  char * cm_conf_fn;
  uint64_t nr_readers;
  uint64_t nr_cycle; // *100
};

// single one
struct TestState {
  struct GenInfo *gc; // counter
  struct GenInfo *gr; // random
  struct DB * db;
  uint64_t time_finish;
  uint64_t nr_all;
  uint8_t buf[BARREL_ALIGN];
  uint32_t * latency;
  bool test_running;
  uint64_t token;
};

static struct TestState __ts;

  static void
show_dbparams(const struct DBParams * const ps)
{
  printf("Staged Read: %s\n", ps->tag);
  printf("    -v #vlen:       %" PRIu64 "\n", ps->vlen);
  printf("    -d #meta_dir:   %s\n", ps->meta_dir);
  printf("    -c #cm_conf_fn: %s\n", ps->cm_conf_fn);
  printf("    -a #nr_readers  %" PRIu64 "\n", ps->nr_readers);
  printf("    -n #cycle(*100):%" PRIu64 "\n", ps->nr_cycle);
  fflush(stdout);
}

  static void *
read_th(void * const p)
{
  (void)p;
  const uint64_t token = __sync_fetch_and_add(&(__ts.token), 1);
  conc_set_affinity_n(token % 4);
  // read keys
  uint64_t key __attribute__((aligned(8)));
  uint64_t t;
  uint64_t nr = 0;
  do {
    for (uint64_t k = 0; k < 100; k++) {
      key = __ts.gr->next(__ts.gr);
      const uint64_t t0 = debug_time_usec();
      struct KeyValue * const kv = db_lookup(__ts.db, sizeof(key), (const uint8_t *)(&(key)));
      const uint64_t t1 = debug_time_usec();
      if (kv) { free(kv); }
      latency_record(t1 - t0, __ts.latency);
    }
    t = debug_time_usec();
    nr += 100;
  } while (t < __ts.time_finish);
  __sync_add_and_fetch(&__ts.nr_all, nr);
  pthread_exit(NULL);
}

  static void
do_read(const char * const tag, const uint64_t nr_readers, const uint64_t sec)
{
  sleep(2);
  // read
  db_stat_clean(__ts.db);
  __ts.latency = latency_initial();
  const uint64_t time_start = debug_time_usec();
  __ts.time_finish = time_start + (sec * 1000000);
  __ts.nr_all = 0;
  conc_fork_reduce(nr_readers, read_th, NULL);
  // print
  fprintf(stdout, "%s-QPS %"  PRIu64 "\n", tag, __ts.nr_all/sec);
  db_stat_show(__ts.db, stdout);
  latency_show(tag, __ts.latency, stdout);
  fflush(stdout);
  free(__ts.latency);
  generator_destroy(__ts.gr);
  __ts.gr = NULL;
}

  static void
staged_worker(const struct DBParams * const ps)
{
  uint64_t keys[100];
  const uint64_t klen = sizeof(klen);

  struct KeyValue kvs[100] __attribute__((aligned(8)));
  for (uint64_t i = 0; i < 100u; i++) {
    kvs[i].klen = klen;
    kvs[i].pk = (uint8_t *)(&keys[i]);
    kvs[i].vlen = ps->vlen;
    kvs[i].pv = __ts.buf;
  }
  for (uint64_t x = 1; __ts.test_running == true; x++) {
    // write
    for (uint64_t j = 0; j < ps->nr_cycle; j++) {
      // random keys
      for (uint64_t i = 0; i < 100u; i++) {
        const uint64_t rkey = __ts.gc->next(__ts.gc);
        keys[i] = rkey;
        (void)keys[i];
      }
      const bool rmi = db_multi_insert(__ts.db, 100, kvs);
      assert(rmi);
    }
    // wait for compaction
    do { sleep(1); } while (db_doing_compaction(__ts.db));

    // read
    const uint64_t max = __ts.gc->gen.counter.counter;
    // uniform existed
    __ts.gr = generator_new_uniform(0, UINT64_MAX>>2);
    do_read("WARM", ps->nr_readers, 10);
    __ts.gr = generator_new_uniform(0, max - (ps->nr_cycle * 50));
    do_read("EUNI", ps->nr_readers, 30);
    // uniform nonexisted
    __ts.gr = generator_new_uniform(0, UINT64_MAX>>2);
    do_read("NUNI", ps->nr_readers, 30);
    // zipfian nonexisted
    __ts.gr = generator_new_xzipfian(0, UINT64_C(0x100000000000));
    do_read("NZIP", ps->nr_readers, 30);
  }
}

  static void
sig_handler_int(const int sig)
{
  (void)sig;
  __ts.test_running = false;
}

  static void
sig_handler_dump(const int sig)
{
  (void)sig;
  db_force_dump_meta(__ts.db);
  debug_trace();
}

  void
sig_install_all(void)
{
  struct sigaction sa;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESTART;

  sa.sa_handler = SIG_IGN;
  sigaction(SIGHUP, &sa, NULL);
  sigaction(SIGALRM, &sa, NULL);

  sa.sa_handler = sig_handler_int;
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
  sigaction(SIGQUIT, &sa, NULL);

  sa.sa_handler = sig_handler_dump;
  sigaction(SIGUSR1, &sa, NULL);
}

  static void
staged_test(const struct DBParams * const p)
{
  __ts.test_running = true;
  sig_install_all();
  srandom(debug_time_usec());
  show_dbparams(p);
  __ts.gc = generator_new_counter(0);
  __ts.db = db_touch(p->meta_dir, p->cm_conf_fn);
  assert(__ts.db);
  memset(__ts.buf, 0x5au, BARREL_ALIGN);
  staged_worker(p);
  generator_destroy(__ts.gc);
  db_close(__ts.db);
}

static const uint64_t nr_configs = 1;
static struct DBParams pstable[] = {
  {"XX", 100, "dbtmp", "cm_conf1.txt", 16, 10000},
};
  int
main(int argc, char ** argv)
{
  printf("!Staged Read Test: Compiled at %s %s\n", __DATE__, __TIME__);
  int opt;
  // default opts
  struct DBParams ps = pstable[0];
  while ((opt = getopt(argc, argv,
          "x:" // select pre-defined configuration
          "v:" // constant value size
          "d:" // meta_dir
          "c:" // cm_conf
          "a:" // nr_threads
          "n:" // nr_cycle
          "h"  // help
          "l"  // list pre-defined params
          )) != -1) {
    switch(opt) {
      case 'x': {
                  const uint64_t id = strtoull(optarg, NULL, 10);
                  if (id < nr_configs) ps = pstable[id];
                  break;
                }
      case 'v': ps.vlen       = strtoull(optarg, NULL, 10); break;
      case 'd': ps.meta_dir   = strdup(optarg); break;
      case 'c': ps.cm_conf_fn = strdup(optarg); break;
      case 'a': ps.nr_readers = strtoull(optarg, NULL, 10); break;
      case 'n': ps.nr_cycle   = strtoull(optarg, NULL, 10); break;
      case 'h': {
                  show_dbparams(&ps);
                  exit(1);
                }
      case 'l': {
                  for (uint64_t i = 0; i < nr_configs; i++) {
                    printf("====param %" PRIu64 "====\n", i);
                    show_dbparams(&(pstable[i]));
                  }
                  exit(1);
                }
      case '?': perror("arg error\n"); exit(1);
      default : perror("arg error\n"); exit(1);
    }
  }
  staged_test(&ps);
  return 0;
}
