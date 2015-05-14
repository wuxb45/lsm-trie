/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <assert.h>
#include <stdint.h>
#include <pthread.h>

#include "conc.h"

  void
conc_set_affinity_n(const uint64_t cpu)
{
  // bind to one cpu
  cpu_set_t cpuset;
  pthread_t thread;
  thread = pthread_self();
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
}

  void
conc_fork_reduce(const uint64_t nr, void *(*func) (void *), void * const arg)
{
  assert((nr > UINT64_C(0)) && (nr < UINT64_C(1024)));
  pthread_t ths[nr];
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for (uint64_t j = 0; j < nr; j++) {
    pthread_create(&(ths[j]), &attr, func, arg);
  }
  for (uint64_t j = 0; j < nr; j++) {
    pthread_join(ths[j], NULL);
  }
}
