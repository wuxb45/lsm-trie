/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <getopt.h>
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
  uint64_t nr_threads;
  uint64_t p_writer;
  char * generator;
  uint64_t range;
  uint64_t sec; // run time
  uint64_t nr_report;
};

static const uint64_t nr_configs = 1;
static struct DBParams pstable[] = {
  //tag    vlen  meta_dir       cm_conf_fn     th  pw   gen        range                     sec   nr
  {"Dummy", 100, "lsmtrie_tmp", "cm_conf1.txt", 1, 100, "uniform", UINT64_C(0x100000000000), 3000, 100000},
};

// singleton
struct TestState {
  uint64_t token;
  uint64_t nr_100;
  uint64_t usec_last;
  uint64_t usec_start;
  bool test_running;
  pthread_mutex_t test_lock;
  struct GenInfo *gi;
  struct DB * db;
  uint32_t * latency;
  uint8_t buf[BARREL_ALIGN];
};

static struct TestState __ts = {0,0,0,0,false,PTHREAD_MUTEX_INITIALIZER,NULL,NULL,NULL,{0,},};

  static void
show_dbparams(const struct DBParams * const ps)
{
  printf("MIX_TEST: %s\n", ps->tag);
  printf("    -v #vlen:       %" PRIu64 "\n", ps->vlen);
  printf("    -d #dir:        %s\n",          ps->meta_dir);
  printf("    -c #cm_conf_fn: %s\n",          ps->cm_conf_fn);
  printf("    -a #nr_threads: %" PRIu64 "\n", ps->nr_threads);
  printf("    -w #p_writer:   %" PRIu64 "\n", ps->p_writer);
  printf("    -g #generator:  %s\n",          ps->generator);
  printf("    -r #range:      %" PRIu64 "\n", ps->range);
  printf("    -t #sec:        %" PRIu64 "\n", ps->sec);
  printf("    -n #nr_report:  %" PRIu64 "\n", ps->nr_report);
  fflush(stdout);
}

  static void
mixed_worker(const struct DBParams * const ps)
{
  uint64_t keys[100];
  assert(ps->p_writer <= 100u);

  struct KeyValue kvs[100] __attribute__((aligned(8)));
  for (uint64_t i = 0; i < 100u; i++) {
    kvs[i].klen = sizeof(keys[i]);
    kvs[i].pk   = (typeof(kvs[i].pk))(&(keys[i]));
    kvs[i].vlen = ps->vlen;
    kvs[i].pv   = __ts.buf;
  }

  // wait for instruction
  for (;;) {
    if (__ts.test_running == true) break;
    usleep(100);
  }
  // loop
  while (__ts.test_running) {
    // random keys
    for (uint64_t i = 0; i < 100u; i++) {
      const uint64_t rkey = __ts.gi->next(__ts.gi);
      keys[i] = rkey;
    }

    // write items
    if (ps->p_writer > 0) {
      const bool r = db_multi_insert(__ts.db, ps->p_writer, kvs);
      assert(r);
    }

    // read keys
    for (uint64_t i = ps->p_writer; i < 100u; i++) {
      const uint64_t t0 = debug_time_usec();
      struct KeyValue * const kv = db_lookup(__ts.db, sizeof(keys[i]), (const uint8_t *)(&(keys[i])));
      const uint64_t t1 = debug_time_usec();
      latency_record(t1 - t0, __ts.latency);
      if (kv) {
        free(kv);
      }
    }

    const uint64_t nr_100 = __sync_add_and_fetch(&(__ts.nr_100), 1);
    if ((nr_100 % ps->nr_report) == 0) {
      pthread_mutex_lock((&__ts.test_lock));
      {
        const uint64_t usec = debug_time_usec();
        const double udiff = (usec - __ts.usec_last) / 1000000.0;
        const double elapsed = (usec - __ts.usec_start) / 1000000.0;
        const double qps = ((double)ps->nr_report) * 100.0 / udiff;
        printf("@@%14" PRIu64 " %12lf   %12lf   %12.2lf\n", nr_100 * 100u, elapsed, udiff, qps);
        __ts.usec_last = usec;
        db_stat_show(__ts.db, stdout);
        fflush(stdout);
      }
      pthread_mutex_unlock(&(__ts.test_lock));
    }
  }
}

  static void *
mixed_thread(void *p)
{
  const uint64_t token = __sync_fetch_and_add(&(__ts.token), 1u);
  conc_set_affinity_n(token % 8);
  mixed_worker((struct DBParams *)p);
  pthread_exit(NULL);
  return NULL;
}

  static struct GenInfo *
gen_initial(const char * const name, const uint64_t range)
{
  //static const uint64_t range = UINT64_C(0x20000000000);
  if (name == NULL) {
    return generator_new_uniform(0, range);
  }

  const int len = strlen(name);
  if (0 == strncmp(name, "counter", len)) {
    return generator_new_counter(0);
  } else if (0 == strncmp(name, "exponential", len)) {
    return generator_new_exponential(95.0, (double)range);
  } else if (0 == strncmp(name, "zipfian", len)) {
    return generator_new_zipfian(0, range);
  } else if (0 == strncmp(name, "xzipfian", len)) {
    return generator_new_xzipfian(0, range);
  } else if (0 == strncmp(name, "uniform", len)) {
    return generator_new_uniform(0, range);
  } else {
    return generator_new_uniform(0, range);
  }
}

  static uint64_t
wait_for_deadline(const uint64_t sec)
{
  printf("######## Start\n");
  sleep(1);
  const uint64_t dur = sec * 1000000u;
  const uint64_t start = debug_time_usec();
  __ts.usec_start = start;
  __ts.usec_last = start;

  __ts.test_running = true;
  while(__ts.test_running == true) {
    sleep(1);
    const uint64_t now = debug_time_usec();
    if (now - start > dur) break;
  }
  __ts.test_running = false;

  const uint64_t finish = debug_time_usec();
  printf("######## Finish\n");
  return finish - start;
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

  static void
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
mixed_test(const struct DBParams * const p)
{
  sig_install_all();
  srandom(debug_time_usec());
  show_dbparams(p);
  __ts.gi = gen_initial(p->generator, p->range);
  __ts.db = db_touch(p->meta_dir, p->cm_conf_fn);
  assert(__ts.db);
  memset(__ts.buf, 0x5au, BARREL_ALIGN);
  __ts.latency = latency_initial();

  const uint64_t nth = p->nr_threads;
  pthread_t pth[nth];
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for (uint64_t i = 0; i < nth; i++) {
    const int rc = pthread_create(&(pth[i]), &attr, mixed_thread, (void *)p);
    assert(rc == 0);
    pthread_setname_np(pth[i], "Worker");
  }

  const uint64_t dur = wait_for_deadline(p->sec);

  for (uint64_t i = 0; i < nth; i++) {
    pthread_join(pth[i], NULL);
  }
  printf("Op %" PRIu64 "\n", __ts.nr_100 * 100u);
  printf("time_usec %" PRIu64 "\n", dur);
  printf("QPS %.4lf\n", ((double)__ts.nr_100) * 100000000.0 / ((double)dur));
  generator_destroy(__ts.gi);
  __ts.gi = NULL;

  db_stat_show(__ts.db, stdout);
  latency_show("GET", __ts.latency, stdout);
  free(__ts.latency);
  fflush(stdout);
  db_close(__ts.db);
}

  int
main(int argc, char ** argv)
{
  printf("!Mixed Test: Compiled at %s %s\n", __DATE__, __TIME__);
  int opt;
  // default opts
  struct DBParams ps = pstable[0];
  while ((opt = getopt(argc, argv,
          "x:" // test case id
          "v:" // constant value size
          "a:" // nr_threads
          "w:" // p_writers
          "t:" // seconds
          "n:" // nr_report
          "r:" // range of gen
          "d:" // meta dir
          "c:" // cm_conf_fn
          "g:" // generator c,e,z,x,u
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
      case 'a': ps.nr_threads = strtoull(optarg, NULL, 10); break;
      case 'w': ps.p_writer   = strtoull(optarg, NULL, 10); break;
      case 't': ps.sec        = strtoull(optarg, NULL, 10); break;
      case 'n': ps.nr_report  = strtoull(optarg, NULL, 10); break;
      case 'r': ps.range      = strtoull(optarg, NULL, 10); break;

      case 'd': ps.meta_dir   = strdup(optarg); break;
      case 'c': ps.cm_conf_fn = strdup(optarg); break;
      case 'g': ps.generator  = strdup(optarg); break;
      case 'h': { show_dbparams(&ps); exit(1); }
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
  mixed_test(&ps);
  return 0;
}
