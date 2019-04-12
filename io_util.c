/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <sys/ioctl.h>
#include <linux/fs.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <inttypes.h>

#include "stat.h"
#include "debug.h"
#include "generator.h"

static volatile bool running = true;
static uint32_t * latency = NULL;

struct RR {
  pthread_t pt;
  size_t mask;
  struct timeval t0;
  uint64_t duration_sec;
  int cpu;
  int fd;
  bool write;
  uint64_t nr_io;
  double perc;
};

  static void
sig_handler(const int sig)
{
  (void)sig;
  running = false;
}

  static int
opendev(char * path)
{
  // Write and DIRECT I/O
  return open(path, O_LARGEFILE | O_SYNC | O_RDWR | O_DIRECT);
}

static inline ssize_t
do_random_rw(const int fd, const size_t mask, uint8_t * const buf, const bool is_write, const uint64_t cap) {
  const size_t size = (mask + 1u);
  const off_t off = (random_uint64() % (cap - size)) & (~mask);

  const uint64_t t0 = debug_time_usec();
  const ssize_t r = is_write?pwrite(fd, buf, size, off):pread(fd, buf, size, off);
  assert((typeof(size))r == size);
  const uint64_t dt = debug_diff_usec(t0);
  latency_record(dt, latency);
  return r;
}

  static void
setup_affinity(const struct RR *const rr)
{
  // bind to one cpu
  cpu_set_t cpuset;
  pthread_t thread;
  thread = pthread_self();
  CPU_ZERO(&cpuset);
  CPU_SET(rr->cpu, &cpuset);
  pthread_setaffinity_np(thread, sizeof(cpuset), &cpuset);
}

  static uint8_t *
alloc_buf(const struct RR * const rr)
{
  const size_t mask = rr->mask;
  assert((mask | (mask + 1u)) == ((mask << 1) + 1u));
  const size_t alloc_size = mask + 1u;
  uint8_t * const buf = aligned_alloc(4096, alloc_size);
  if (rr->write) memset(buf, 0x5a, alloc_size);
  return buf;
}

  void *
rw4k_thread(void *p)
{
  struct RR *rr = (typeof(rr))p;
  uint8_t * const buf = alloc_buf(rr);
  uint64_t cap = 0;
  ioctl(rr->fd, BLKGETSIZE64, &cap);
  if (cap == 0) {
    cap = lseek(rr->fd, 0, SEEK_END);
  }
  assert(cap);
  cap = (rr->perc * (double)cap);
  setup_affinity(rr);
  const uint64_t start = debug_time_usec();
  const uint64_t dur = rr->duration_sec * 1000000;
  uint64_t count = 0;
  const uint64_t round = 10;
  for (;;) {
    for (uint64_t i = 0; i < round; i++) {
      do_random_rw(rr->fd, rr->mask, buf, rr->write, cap);
    }
    count += round;
    if (debug_time_usec() - start > dur) break;
  }
  // record
  rr->nr_io = count;
  free(buf);
  pthread_exit(NULL);
}

  static void
show_io(const char * const tag, const uint64_t io, const uint64_t dur, const size_t mask, const uint64_t nr_th)
{
  const double dio = (double)io;
  const double ddur = (double)dur;
  const double unit_size = (double)(mask + 1u);
  const double bytes = unit_size * dio;
  const double mb = bytes / (1024.0 * 1024.0);
  const double th = mb / ddur;
  printf("[%s] #th %" PRIu64 " #IO %" PRIu64 " Unit %.0lf IOPS %0.2lf Vo %6.0lfMiB Thr %.2lfMiB/s ",
      tag, nr_th, io, unit_size, dio / ddur, mb, th);
}

  void
threaded_rw(const int fd, const size_t mask, const int dur, const int nr_r, const int nr_w, const double perc)
{
  struct timeval t0;
  gettimeofday(&t0, NULL);
  latency = latency_initial();
  const int nr_threads = nr_r + nr_w;
  struct RR rr[nr_threads];
  // initial
  for (int i = 0; i < nr_threads; i++) {
    rr[i].mask = mask;
    rr[i].t0 = t0;
    rr[i].duration_sec = dur;
    rr[i].cpu = i % 8;
    rr[i].fd = fd;
    rr[i].write = (i<nr_w)?true:false;
    rr[i].nr_io = 0;
    rr[i].perc = perc;
  }
  // start workers
  for (int i = 0; i < nr_threads; i++) {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&(rr[i].pt), &attr, rw4k_thread, &(rr[i]));
  }

  uint64_t io_r = 0;
  uint64_t io_w = 0;
  // wait all
  for (int i = 0; i < nr_threads; i++) {
    pthread_join(rr[i].pt, NULL);
    if (i < nr_w) {
      io_w += rr[i].nr_io;
    } else {
      io_r += rr[i].nr_io;
    }
  }
  if (nr_r > 0) show_io("R", io_r, dur, mask, nr_r);
  if (nr_w > 0) show_io("W", io_w, dur, mask, nr_w);
  latency_95_99_999(latency, stdout);
  free(latency);
}

  int
main(int argc, char ** argv)
{
  if (sizeof(off_t) != 8) {
    printf("off_t: %zu\n", sizeof(off_t));
    exit(1);
  }

  if (argc < 6) {
    printf("usage: %s <dev> <nr_r> <nr_w> <shift> <dur> [<perc>]\n", argv[0]);
    exit(1);
  }

  const int fd = opendev(argv[1]);
  assert(fd >= 0);
  const int nr_r = atoi(argv[2]);
  const int nr_w = atoi(argv[3]);
  const int shift = atoi(argv[4]);
  const int dur = atoi(argv[5]);
  double perc = 1.0;
  if (argc == 7) {
    int iperc = 0;
    sscanf(argv[6], "%3d", &iperc);
    perc = ((double)iperc) / 100.0;
  }
  assert((shift < 32) && (shift >= 12));
  const size_t mask = (1u << shift) - 1u;
  struct sigaction sa;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESTART;
  sa.sa_handler = sig_handler;
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
  threaded_rw(fd, mask, dur, nr_r, nr_w, perc);
  return 0;
}
