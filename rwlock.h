/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>

struct ReaderLock {
  uint64_t nr_readers;
  bool open;
  pthread_cond_t cond_reader;
  uint64_t pad1[8];
};

struct RWLock {
  pthread_mutex_t mutex_any;
  uint64_t next_ticket; // sell to writers
  uint64_t reader_ticket;
  uint64_t writer_ticket;
  uint64_t pad1[8];
  pthread_cond_t cond_writer;
  uint64_t pad2[8];
  struct ReaderLock rl[2]; // mod 2
};

void
rwlock_show(struct RWLock *bo);
void
rwlock_initial(struct RWLock *bo);
uint64_t
rwlock_reader_lock(struct RWLock *bo);
void
rwlock_reader_unlock(struct RWLock *bo, const uint64_t ticket);
uint64_t
rwlock_writer_lock(struct RWLock *bo);
void
rwlock_writer_unlock(struct RWLock *bo, const uint64_t ticket);
