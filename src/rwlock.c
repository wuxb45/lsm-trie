/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <stdio.h>
#include <pthread.h>
#include <stdint.h>
#include <assert.h>
#include <inttypes.h>

#include "rwlock.h"

  void
rwlock_show(struct RWLock *bo)
{
  printf("****nxt %" PRIu64 " r %" PRIu64 " w %" PRIu64 " r0 %c %" PRIu64 " r1 %c %" PRIu64 "\n",
         bo->next_ticket, bo->reader_ticket, bo->writer_ticket,
         bo->rl[0].open?'+':'_', bo->rl[0].nr_readers,
         bo->rl[1].open?'+':'_', bo->rl[1].nr_readers);
}

  void
rwlock_initial(struct RWLock *bo)
{
  bo->next_ticket = 0;
  bo->reader_ticket = 0;
  bo->writer_ticket = 0;
  pthread_mutex_init(&(bo->mutex_any), NULL);
  pthread_cond_init(&(bo->cond_writer), NULL);

  bo->rl[0].nr_readers = 0;
  bo->rl[1].nr_readers = 0;
  pthread_cond_init(&(bo->rl[0].cond_reader), NULL);
  pthread_cond_init(&(bo->rl[1].cond_reader), NULL);
  bo->rl[0].open = true; // open 0
  bo->rl[1].open = false; // close 1
}

  uint64_t
rwlock_reader_lock(struct RWLock *bo)
{
  pthread_mutex_lock(&(bo->mutex_any));
  // buy one reader-ticket
  const uint64_t ticket = bo->reader_ticket;
  // find the room and line up
  struct ReaderLock *const rl = &(bo->rl[ticket & 1]);
  rl->nr_readers++;
  while (rl->open == false) { // wait for open
    pthread_cond_wait(&(rl->cond_reader), &(bo->mutex_any));
  }
  // do what you want
  pthread_mutex_unlock(&(bo->mutex_any));
  return ticket;
}

  void
rwlock_reader_unlock(struct RWLock *bo, const uint64_t ticket)
{
  pthread_mutex_lock(&(bo->mutex_any));
  struct ReaderLock *const rl = &(bo->rl[ticket & 1]);
  assert(rl->nr_readers);
  // leave
  rl->nr_readers--;
  // if I'm the last, wake up the writer
  if (rl->nr_readers == 0) {
    pthread_cond_broadcast(&(bo->cond_writer));
  }
  pthread_mutex_unlock(&(bo->mutex_any));
}

  uint64_t
rwlock_writer_lock(struct RWLock *bo)
{
  pthread_mutex_lock(&(bo->mutex_any));
  // early register as writer
  // buy one writer-ticket
  const uint64_t ticket = bo->next_ticket;
  bo->next_ticket++;
  // wait for my writer-ticket
  while (ticket != bo->writer_ticket) {
    pthread_cond_wait(&(bo->cond_writer), &(bo->mutex_any));
  }
  // tell readers to go to next room
  assert(bo->reader_ticket == bo->writer_ticket);

  bo->reader_ticket++;
  // close door
  struct ReaderLock *const rl = &(bo->rl[ticket & 1]);
  // wait for readers to leave
  while (rl->nr_readers > 0) {
    pthread_cond_wait(&(bo->cond_writer), &(bo->mutex_any));
  }
  rl->open = false;
  // work time
  pthread_mutex_unlock(&(bo->mutex_any));
  return ticket;
}

  void
rwlock_writer_unlock(struct RWLock *bo, const uint64_t ticket)
{
  pthread_mutex_lock(&(bo->mutex_any));
  struct ReaderLock *const rl_curr = &(bo->rl[ticket & 1]);
  assert(rl_curr->nr_readers == 0);
  // work done. only now can open next door
  struct ReaderLock *const rl_next = &(bo->rl[(ticket + 1) & 1]);
  rl_next->open = true;
  if (rl_next->nr_readers > 0) {
    pthread_cond_broadcast(&(rl_next->cond_reader));
  }
  // wake up next writer
  bo->writer_ticket++;
  // wake up writers anyway
  pthread_cond_broadcast(&(bo->cond_writer));
  pthread_mutex_unlock(&(bo->mutex_any));
}
