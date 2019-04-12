/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <stdint.h>
#include <stdbool.h>

#include "stat.h"
#include "bloom.h"
#include "mempool.h"

struct KeyValue {
  uint16_t klen;
  uint16_t vlen;
  uint8_t * pk;
  uint8_t * pv;
  uint8_t kv[]; // don't access it
};

#define HASHBYTES ((20))

#define TABLE_MAX_BARRELS ((UINT64_C(8192)))
// a Prime number
#define TABLE_NR_BARRELS  ((UINT64_C(8191)))
// 4KB
#define BARREL_ALIGN      ((UINT64_C(4096)))
// 32MB
#define TABLE_ALIGN       ((BARREL_ALIGN * TABLE_MAX_BARRELS))
// 8MB
#define TABLE_NR_IO       ((UINT64_C(2048)))

#define TABLE_ILOCKS_NR ((UINT64_C(64)))

struct Table {
  uint64_t volume;
  uint64_t capacity;
  struct Mempool * mempool; // store items
  struct Barrel *barrels;
  uint8_t *io_buffer;
  uint64_t nr_mi;
  struct MetaIndex * mis;
  struct BloomTable *bt;
  pthread_mutex_t ilocks[TABLE_ILOCKS_NR]; // used for parallel compaction feed
};

struct MetaFileHeader {
  uint64_t off;
  uint64_t volume;
  uint64_t nr_mi;
} __attribute__ ((packed));

struct MetaTable {
  struct MetaFileHeader mfh;
  int raw_fd;
  uint64_t mtid;
  struct MetaIndex * mis;
  struct BloomTable * bt;
  struct Stat * stat;
};

// ----Table
uint16_t
table_select_barrel(const uint8_t * const hash);

bool
table_retain(struct Table * const table);

struct Table *
table_alloc_new(const double cap_percent, const double mempool_factor);

struct Table *
table_alloc_default(const double mempool_factor);

bool
table_insert_kv_safe(struct Table * const table, const struct KeyValue * const kv);

bool
table_full(const struct Table *const table);

struct KeyValue *
table_lookup(struct Table * const table, const uint16_t klen,
    const uint8_t * const key, const uint8_t * const hash);

bool
table_build_bloomtable(struct Table * const table);

bool
table_dump_meta(struct Table *const table, const char * const metafn, const uint64_t off);

uint64_t
table_dump_barrels(struct Table * const table, const int fd, const uint64_t off);

void
table_free(struct Table * const table);

void
table_analysis_verbose(struct Table * const table, FILE * const out);

void
table_analysis_short(struct Table * const table, char * const buffer);

void
table_show(struct Table * const table, FILE * const fo);

// ----MetaTable
struct MetaTable *
metatable_load(const char * const metafn, const int raw_fd, const bool load_bf, struct Stat * const stat);

struct KeyValue *
metatable_lookup(struct MetaTable * const mt, const uint16_t klen,
    const uint8_t * const key, const uint8_t * const hash);

void
metatable_free(struct MetaTable * const mt);

bool
metatable_feed_barrels_to_tables(struct MetaTable * const mt, const uint16_t start,
    const uint16_t nr, uint8_t * const arena, struct Table * const * const tables,
    uint64_t (*select_table)(const uint8_t * const, const uint64_t), const uint64_t arg2);
