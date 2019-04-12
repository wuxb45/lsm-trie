/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE
#include <unistd.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>

#include "rwlock.h"

struct Magic {
  struct RWLock rwlock;
  pthread_rwlock_t rwlock_1;
  bool gameover;
  uint64_t rcount;
  uint64_t wcount;
};

  static void *
th_reader_p(void *p)
{
  struct Magic *m = (typeof(m))p;
  uint64_t rcount = 0;
  while (m->gameover == false) {
    pthread_rwlock_rdlock(&(m->rwlock_1));
    rcount++;
    pthread_rwlock_unlock(&(m->rwlock_1));
  }
  __sync_add_and_fetch(&(m->rcount), rcount);
  pthread_exit(NULL);
}

  static void *
th_writer_p(void *p)
{
  struct Magic *m = (typeof(m))p;
  uint64_t wcount = 0;
  while (m->gameover == false) {
    pthread_rwlock_wrlock(&(m->rwlock_1));
    wcount++;
    pthread_rwlock_unlock(&(m->rwlock_1));
  }
  __sync_add_and_fetch(&(m->wcount), wcount);
  pthread_exit(NULL);
}

  static void *
th_reader(void *p)
{
  struct Magic *m = (typeof(m))p;
  uint64_t rcount = 0;
  while (m->gameover == false) {
    const uint64_t t = rwlock_reader_lock(&(m->rwlock));
    rcount++;
    rwlock_reader_unlock(&(m->rwlock), t);
  }
  __sync_add_and_fetch(&(m->rcount), rcount);
  pthread_exit(NULL);
}

  static void *
th_writer(void *p)
{
  struct Magic *m = (typeof(m))p;
  uint64_t wcount = 0;
  while (m->gameover == false) {
    const uint64_t t = rwlock_writer_lock(&(m->rwlock));
    wcount++;
    rwlock_writer_unlock(&(m->rwlock), t);
  }
  __sync_add_and_fetch(&(m->wcount), wcount);
  pthread_exit(NULL);
}

  static void
run_test(const char * const tag, const uint64_t nr_readers, const uint64_t nr_writers, void * (*th_r)(void *), void * (*th_w)(void *))
{
  struct Magic x;
  rwlock_initial(&(x.rwlock));
  pthread_rwlock_init(&(x.rwlock_1), NULL);
  x.gameover = false;
  x.wcount = 0;
  x.rcount = 0;
  pthread_t pt_readers[nr_readers];
  pthread_t pt_writers[nr_writers];
  for (uint64_t i = 0; i< nr_readers; i++) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&(pt_readers[i]), &attr, th_r, &x);
  }
  for (uint64_t i = 0; i< nr_writers; i++) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&(pt_writers[i]), &attr, th_w, &x);
  }
  const int sec = 5;
  fflush(stdout);
  sleep(sec);
  x.gameover = true;
  for (uint64_t i = 0; i< nr_readers; i++) pthread_join(pt_readers[i], NULL);
  for (uint64_t i = 0; i< nr_writers; i++) pthread_join(pt_writers[i], NULL);
  printf("%s: W %2" PRIu64 ", R %2" PRIu64 ", w %9" PRIu64 ", r %9" PRIu64 "\n", tag, nr_writers, nr_readers, x.wcount, x.rcount);
}

  int
main(int argc, char **argv)
{
  (void)argc;
  (void)argv;
  for (uint64_t w = 0; w < 8; w++) {
    run_test("TICKET ", 0, w, th_reader, th_writer);
    for (uint64_t r = 4; r < 256; r<<=2) {
      run_test("TICKET ", r, w, th_reader, th_writer);
    }
  }
  for (uint64_t w = 0; w < 8; w++) {
    run_test("PTHREAD", 0, w, th_reader_p, th_writer_p);
    for (uint64_t r = 4; r < 256; r<<=2) {
      run_test("PTHREAD", r, w, th_reader_p, th_writer_p);
    }
  }
  return 0;
}
