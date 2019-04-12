/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>

#include "table.h"
#include "generator.h"
#include "cmap.h"

#define CONTAINER_UNIT_SIZE ((TABLE_ALIGN))

  static int
containermap_open_raw(const char * const raw_fn, const off_t cap_hint)
{
  struct stat rawst;
  int raw_fd = -1;
  assert(raw_fn);
  const int rst0 = stat(raw_fn, &rawst); // test filename
  if ((rst0 == 0) && S_ISBLK(rawst.st_mode)) { // blk device
    const int blk_flags = O_RDWR | O_LARGEFILE | O_SYNC | O_DIRECT;
    raw_fd = open(raw_fn, blk_flags);
    if (raw_fd < 0) return -1;
  } else { // is a normal file anyway
    const int normal_flags = O_CREAT | O_RDWR | O_LARGEFILE;
    raw_fd = open(raw_fn, normal_flags, 00644);
    if (raw_fd < 0) return -1;
    const int rst1 = stat(raw_fn, &rawst);
    assert(rst1 == 0);
    if (rawst.st_size < cap_hint) {// increase file size
      const int rt = ftruncate(raw_fd, cap_hint);
      if (rt != 0) return -1;
    }
  }
  return raw_fd;
}

// nr_units, total_cap, discard
  static bool
containermap_probe(struct ContainerMap * const cm, const int raw_fd)
{
  struct stat st;
  assert(raw_fd >= 0);
  const int r = fstat(raw_fd, &st);
  assert(0 == r);
  if (S_ISBLK(st.st_mode)) { // block device
    ioctl(raw_fd, BLKGETSIZE64, &(cm->total_cap));
    cm->discard = true;
  } else { // regular file
    cm->total_cap = st.st_size;
    cm->discard = false;
  }
  cm->nr_units = cm->total_cap / CONTAINER_UNIT_SIZE;
  return (cm->nr_units > 0) && (cm->total_cap > 0);
}

  struct ContainerMap *
containermap_create(const char * const raw_fn, const uint64_t cap_hint)
{
  struct ContainerMap cm0;
  bzero(&cm0, sizeof(cm0));
  assert(sizeof(off_t) == sizeof(uint64_t));
  const int raw_fd = containermap_open_raw(raw_fn, (off_t)cap_hint);
  if (raw_fd < 0) return NULL;
  const bool rp = containermap_probe(&cm0, raw_fd);
  if (rp == false) return NULL;

  const size_t nr_bytes = (cm0.nr_units + 7u) >> 3;
  struct ContainerMap * const cm = (typeof(cm))malloc(sizeof(*cm) + nr_bytes);
  bzero(cm, sizeof(*cm) + nr_bytes);
  cm->nr_units = cm0.nr_units;
  cm->nr_used = 0;
  cm->total_cap = cm0.total_cap;
  cm->discard = cm0.discard;
  cm->raw_fd = raw_fd;
  pthread_mutex_init(&(cm->mutex_cm), NULL);
  return cm;
}

  struct ContainerMap *
containermap_load(const char * const meta_fn, const char * const raw_fn)
{
  assert(meta_fn);
  FILE * const cmap_in = fopen(meta_fn, "rb");
  if (cmap_in == NULL) return NULL;
  // 1: nr_units
  uint64_t nr_units = 0;
  const size_t nun = fread(&nr_units, sizeof(nr_units), 1, cmap_in);
  assert(nun == 1);
  assert(nr_units > 0);
  // device smaller than CM

  // get device
  struct ContainerMap cm0;
  bzero(&cm0, sizeof(cm0));
  const int raw_fd = containermap_open_raw(raw_fn, nr_units * CONTAINER_UNIT_SIZE);
  assert(raw_fd >= 0);
  const bool rp = containermap_probe(&cm0, raw_fd);
  assert(rp == true);
  // check if cap fits
  assert(cm0.nr_units >= nr_units);

  const size_t nr_bytes = (nr_units + 7) >> 3;
  struct ContainerMap * const cm = (typeof(cm))malloc(sizeof(*cm) + nr_bytes);
  bzero(cm, sizeof(*cm) + nr_bytes);
  // 2: nr_used
  const size_t nus = fread(&(cm->nr_used), sizeof(cm->nr_used), 1, cmap_in);
  assert(nus == 1);
  // 3: bits
  const size_t nby = fread(cm->bits, sizeof(cm->bits[0]), nr_bytes, cmap_in);
  assert(nby == nr_bytes);
  // copy values
  cm->nr_units = nr_units;
  cm->total_cap = cm0.total_cap;
  cm->discard = cm0.discard;
  cm->raw_fd = raw_fd;
  pthread_mutex_init(&(cm->mutex_cm), NULL);
  fclose(cmap_in);
  return cm;
}

  void
containermap_dump(struct ContainerMap * const cm, const char * const meta_fn)
{
  assert(meta_fn);
  FILE * const cmap_out = fopen(meta_fn, "wb");
  assert(cmap_out);
  // 1: nr_units
  pthread_mutex_lock(&(cm->mutex_cm));
  const size_t nun = fwrite(&(cm->nr_units), sizeof(cm->nr_units), 1, cmap_out);
  assert(nun == 1);
  const size_t nr_bytes = (cm->nr_units + 7) >> 3;
  // 2: nr_used
  const size_t nus = fwrite(&(cm->nr_used), sizeof(cm->nr_used), 1, cmap_out);
  assert(nus == 1);
  // 3: bits
  const size_t nby = fwrite(cm->bits, sizeof(cm->bits[0]), nr_bytes, cmap_out);
  assert(nby == nr_bytes);
  pthread_mutex_unlock(&(cm->mutex_cm));
  fclose(cmap_out);
}

  void
containermap_show(struct ContainerMap * const cm)
{
  pthread_mutex_lock(&(cm->mutex_cm));
  uint64_t ucount = 0;
  // HEADER
  static const char * XX = "--------------------------------";
  printf("Container Map\n  INDEX  /%s%s\\\n", XX, XX);
  // bits
  char line[256]={0};
  for (uint64_t i = 0; i < cm->nr_units; i++) {
    const uint8_t byte = cm->bits[i >> 3];
    const uint8_t bit = byte & (1u << (i & 7u));
    // line header
    if (i % 64u == 0) sprintf(line, "%08" PRIu64 ":|", i);
    line[10 + (i % 64u)] = (bit == 0) ? ' ':'*';
    if (bit != 0) ucount++;
    // line end
    if (i % 64u == 63u) {
      line[10+64] = '\0';
      printf("%s|\n", line);
    }
  }
  if ((cm->nr_units % 64u) != 0) {
    uint64_t j = cm->nr_units;
    while ((j % 64u) != 0) {
      line[10 + (j % 64)] = '#';
      j++;
    }
    line[10+64] = '\0';
    printf("%s|\n", line);
  }
  printf("   END   \\%s%s/\n", XX, XX);
  printf("Container usage: (%" PRIu64 "/%" PRIu64 ", %.2lf%%)\n",
      ucount, cm->nr_units, ((double)ucount)/((double)cm->nr_units)*100.0);
  pthread_mutex_unlock(&(cm->mutex_cm));
  fflush(stdout);
}

// return device offset within the reasonable range
  uint64_t
containermap_alloc(struct ContainerMap * const cm)
{
  pthread_mutex_lock(&(cm->mutex_cm));
  const uint64_t rid = random_uint64();

  for (uint64_t i = 0; i < cm->nr_units; i++) {
    const uint64_t id = (i + rid) % cm->nr_units;
    const uint8_t byte = cm->bits[id >> 3];
    const uint8_t bit = byte & (1u << (id & 7u));
    if (bit == 0) { // hit
      const uint8_t new_byte = byte | (1u << (id & 7u));
      cm->bits[id >> 3] = new_byte;
      cm->nr_used++;
      pthread_mutex_unlock(&(cm->mutex_cm));
      return (id * CONTAINER_UNIT_SIZE);
    }
  }
  // full
  pthread_mutex_unlock(&(cm->mutex_cm));
  // on full returning invalid offset > last byte
  return (cm->nr_units + 100u) * CONTAINER_UNIT_SIZE;
}

  bool
containermap_release(struct ContainerMap * const cm, const uint64_t offset)
{
  pthread_mutex_lock(&(cm->mutex_cm));
  assert((offset & (CONTAINER_UNIT_SIZE - 1u)) == 0);
  const uint64_t id = offset / CONTAINER_UNIT_SIZE;
  assert(id < cm->nr_units);
  const uint8_t byte = cm->bits[id >> 3];
  const uint8_t new_byte = byte & (~ (1u << (id & 7u)));
  assert(new_byte < byte);
  cm->bits[id >> 3] = new_byte;
  cm->nr_used--;
  if ((cm->discard == true) && (cm->raw_fd >= 0)) { // issue TRIM
    const uint64_t range[2] = {offset, CONTAINER_UNIT_SIZE};
    ioctl(cm->raw_fd, BLKDISCARD, range);
  }
  pthread_mutex_unlock(&(cm->mutex_cm));
  return true;
}

  void
containermap_destroy(struct ContainerMap * const cm)
{
  assert(cm);
  close(cm->raw_fd);
  free(cm);
}

  uint64_t
containermap_unused(const struct ContainerMap * const cm)
{
  return (cm->nr_units - cm->nr_used);
}
