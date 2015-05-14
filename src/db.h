/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "table.h"

  struct DB *
db_touch(const char * const meta_dir, const char * const cm_conf_fn);

void
db_close(struct DB * const db);

bool
db_insert(struct DB * const db, struct KeyValue * const kv);

bool
db_multi_insert(struct DB * const db, const uint64_t nr_items, const struct KeyValue * const kvs);

struct KeyValue *
db_lookup(struct DB * const db, const uint16_t klen, const uint8_t * const key);

//----misc

void
db_force_dump_meta(struct DB * const db);

void
db_stat_show(struct DB * const db, FILE * const fo);

void
db_stat_clean(struct DB * const db);

bool
db_doing_compaction(struct DB * const db);
