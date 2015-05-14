/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <openssl/sha.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <inttypes.h>
#include <stdarg.h>

#include "rwlock.h"
#include "debug.h"
#include "table.h"
#include "cmap.h"
#include "generator.h"
#include "conc.h"

#include "db.h"

#define DB_CONTAINER_NR ((20))
struct Container { // a container of tables
  uint64_t count;
  struct BloomContainer *bc;
  struct MetaTable *metatables[DB_CONTAINER_NR]; // count
};

struct VirtualContainer {
  uint64_t start_bit; // 2^bit -> horizontal barrel groups
  struct Container cc;
  struct VirtualContainer *sub_vc[8];
};

/**
 * Bloom Container: 512KB:32MB (1/64)
 * Level-0:   4MB:256MB
 * Level-3:   32MB:2GB
 * Level-6:   256MB:16GB
 * Level-9:   2GB:128GB
 * Level-12:  16GB:1TB
 * Level-15:  128GB:8TB
 */

#define BC_START_BIT      ((UINT64_C(12)))
#define DB_COMPACTION_CAP (((uint64_t)(TABLE_ALIGN * 7.2)))
// NR = 8
#define DB_COMPACTION_NR         ((UINT64_C(8)))
#define DB_COMPACTION_THREADS_NR ((UINT64_C(4)))
#define DB_FEED_UNIT ((TABLE_MAX_BARRELS/8))
#define DB_FEED_NR   ((TABLE_MAX_BARRELS/DB_FEED_UNIT))
#define DB_NR_LEVELS ((5))

struct ContainerMapConf {
  char * raw_fn[6]; // at most 6 raw files
  uint64_t hints[6]; // corresponds to raw_fn
  uint64_t bc_id;
  uint64_t data_id[DB_NR_LEVELS]; // at most 5 levels
};

struct DB {
  char * persist_dir;
  double sec_start;
  FILE * log;
  struct Table *active_table[2];
  struct ContainerMap *cms[DB_NR_LEVELS];
  struct ContainerMap *cm_bc;
  struct ContainerMap *cms_dump[6];
  struct VirtualContainer *vcroot;

  // locks
  pthread_mutex_t mutex_active;  // lock on dumpping active table
  pthread_mutex_t mutex_current; // lock on operating on active and vcroot
  pthread_mutex_t mutex_root; // lock on compacting root
  pthread_mutex_t mutex_token[DB_COMPACTION_NR];

  // rwlock
  struct RWLock rwlock;

  // cond
  pthread_cond_t cond_root_producer;      // notify between dump thread & compaction thread
  pthread_cond_t cond_root_consumer;      // notify between dump thread & compaction thread
  pthread_cond_t cond_active;    // notify active-dumper thread
  pthread_cond_t cond_writer;    // notify writers

  // pthread_t
  pthread_t t_compaction[DB_COMPACTION_NR];
  pthread_t t_active_dumper;
  pthread_t t_meta_dumper;
  //
  bool closing;
  bool need_dump_meta;
  uint64_t next_mtid;
  uint64_t compaction_token;
  uint64_t compaction_running_counter;
  // stat
  struct Stat stat;
};

struct Compaction {
  // nums
  uint64_t start_bit;
  uint64_t sub_bit; // +3
  bool gen_bc;
  uint64_t nr_feed;
  uint64_t feed_id;
  uint64_t feed_token;
  uint64_t bt_token;
  uint64_t dump_token;
  uint64_t bc_token;
  // cms
  struct ContainerMap * cm_to;
  // pointers
  struct DB * db;
  struct VirtualContainer * vc;
  uint8_t * arena;

  // level(n)
  struct MetaTable *mts_old[DB_CONTAINER_NR];
  uint64_t mtids_old[DB_CONTAINER_NR];
  // tmp
  struct Table * tables[8];
  // level(n+1)
  struct MetaTable *mts_new[8];
  uint64_t mtids_new[8];
  // BC
  struct BloomContainer *mbcs_old[8];
  struct BloomContainer *mbcs_new[8];
};


#define DB_META_MAIN             ("META")
#define DB_META_CMAP_PREFIX      ("CONTAINER_MAP")
#define DB_META_ACTIVE_TABLE     ("ACTIVE_TABLE")
#define DB_META_LOG              ("LOG")
#define DB_META_BACKUP_DIR       ("META_BACKUP")


// free metafn after use!
  static void
db_generate_meta_fn(struct DB * const db, const uint64_t mtid, char * const path)
{
  sprintf(path, "%s/%02" PRIx64 "/%016" PRIx64, db->persist_dir, mtid % 256, mtid);
}

  static struct MetaTable *
db_load_metatable(struct DB * const db, const uint64_t mtid, const int raw_fd, const bool load_bf)
{
  char metafn[2048];
  db_generate_meta_fn(db, mtid, metafn);
  struct MetaTable * const mt = metatable_load(metafn, raw_fd, load_bf, &(db->stat));
  assert(mt);
  mt->mtid = mtid;
  return mt;
}

  static void
db_destory_metatable(struct DB * const db, const uint64_t mtid)
{
  char metafn[2048];
  db_generate_meta_fn(db, mtid, metafn);
  unlink(metafn);
}

  static struct BloomContainer *
db_load_bloomcontainer_meta(struct DB * const db, const uint64_t mtid)
{
  char bcmeta_fn[2048];
  db_generate_meta_fn(db, mtid, bcmeta_fn);
  FILE * const fi = fopen(bcmeta_fn, "rb");
  assert(fi);
  struct BloomContainer *bc = bloomcontainer_load_meta(fi, db->cm_bc->raw_fd);
  assert(bc);
  bc->mtid = mtid;
  fclose(fi);
  return bc;
}

  static bool
db_dump_bloomcontainer_meta(struct DB * const db, const uint64_t mtid, struct BloomContainer * const bc)
{
  char bcmeta_fn[2048];
  db_generate_meta_fn(db, mtid, bcmeta_fn);
  FILE * const fo = fopen(bcmeta_fn, "wb");
  assert(fo);
  const bool r = bloomcontainer_dump_meta(bc, fo);
  fclose(fo);
  return r;
}

  static void
db_log(struct DB * const db, const char * const msg, ...)
{
  if (db->log == NULL) return;
  const double sec = debug_time_sec();
  char th_name[16] = {0};
  pthread_getname_np(pthread_self(), th_name, sizeof(th_name));
  char head[1024];
  char tail[1024];
  sprintf(head, "[%-15s|%10s->%10.3lf|%9s] ", th_name, "", sec - db->sec_start, "");

  va_list varg;
  va_start(varg, msg);
  vsnprintf(tail, sizeof(tail), msg, varg);
  va_end(varg);
  fprintf(db->log, "%s%s\n", head, tail);
}

  static void
db_log_diff(struct DB * const db, const double sec0, const char * const msg, ...)
{
  if (db->log == NULL) return;
  const double sec1 = debug_time_sec();
  char th_name[16] = {0};
  pthread_getname_np(pthread_self(), th_name, sizeof(th_name));
  char head[1024];
  char tail[1024];
  sprintf(head, "[%-15s|%10.3lf->%10.3lf|%9.6lf] ", th_name, sec0 - db->sec_start, sec1 - db->sec_start, sec1 - sec0);

  va_list varg;
  va_start(varg, msg);
  vsnprintf(tail, sizeof(tail), msg, varg);
  va_end(varg);
  fprintf(db->log, "%s%s\n", head, tail);
}

  static struct VirtualContainer *
vc_create(const uint64_t start_bit)
{
  struct VirtualContainer * const vc = (typeof(vc))malloc(sizeof(*vc));
  assert(vc);
  bzero(vc, sizeof(*vc));
  vc->start_bit = start_bit;
  return vc;
}

// must used under lock aquired on vc
  static bool
vc_insert_internal(struct VirtualContainer *const vc, struct MetaTable *const mt, struct BloomContainer *const bc)
{
  if (vc->cc.count < DB_CONTAINER_NR) {
    const uint64_t id = vc->cc.count;
    vc->cc.metatables[id] = mt;
    vc->cc.count++;
    vc->cc.bc = bc;
  } else {
    // This should never happen in correct program.
    // If killed, compaction may not have scanned all the levels
    // Add signal processing function to finish compaction?
    assert(false);
  }
  return true;
}

  static void
vc_recursive_free(struct VirtualContainer * const vc)
{
  for (uint64_t i = 0; i < DB_CONTAINER_NR; i++) {
    if (vc->cc.metatables[i]) { metatable_free(vc->cc.metatables[i]); }
  }
  if (vc->cc.bc) { bloomcontainer_free(vc->cc.bc); }

  for (uint64_t i = 0; i < 8; i++) {
    if (vc->sub_vc[i]) { vc_recursive_free(vc->sub_vc[i]); }
  }
  free(vc);
}

// return 8 ... (DB_CONTAINER_NR) for compaction, 0 for NO compaction
  static uint64_t
vc_count_feed(struct VirtualContainer * const vc)
{
  if (vc == NULL) return 0;
  uint64_t vc_cap = 0;
  for (uint64_t j = 0; j < vc->cc.count; j++) {
    assert(vc->cc.metatables[j]);
    vc_cap += (vc->cc.metatables[j]->mfh.volume);
    if (vc_cap >= DB_COMPACTION_CAP) {
      return (j + 1);
    }
  }
  return 0;
}

// pick from 8 vcs; return NULL for no compaction
  static struct VirtualContainer *
vc_pick_compaction(struct VirtualContainer * const * const vcs, const uint64_t start, const uint64_t inc)
{
  uint64_t max_id = 8;
  uint64_t max_height = 0;
  for (uint64_t i = start; i < 8; i += inc) {
    if (vcs[i] == NULL) continue;
    const uint64_t height = vc_count_feed(vcs[i]);
    if (height > max_height) {
      max_height = height;
      max_id = i;
    }
  }
  if ((max_height > 0) && (max_id < 8)) {
    return vcs[max_id];
  } else {
    return NULL;
  }
}

// pick one who is full
  static struct VirtualContainer *
vc_pick_full(struct VirtualContainer * const * const vcs, const uint64_t start, const uint64_t inc)
{
  for (uint64_t i = start; i < 8; i += inc) {
    if (vcs[i] == NULL) continue;
    if (vcs[i]->cc.count == DB_CONTAINER_NR) {
      return vcs[i];
    }
  }
  return NULL;
}

  static bool
recursive_dump(struct VirtualContainer * const vc, FILE * const out)
{
  // only the metatable's id is dumpped :)
  if (vc) {
    fprintf(out, "[ %" PRIu64 "\n", vc->start_bit);
    if (vc->cc.bc) {
      fprintf(out, "<!\n");
    } else {
      fprintf(out, "<\n");
    }
    // dump at most 8 MetaTable
    for (uint64_t j = 0; j < vc->cc.count; j++) {
      if (vc->cc.metatables[j]) {
        const uint64_t mtid = vc->cc.metatables[j]->mtid;
        fprintf(out, "%016" PRIx64 "\n", mtid);
      }
    }
    if (vc->cc.bc) {
      fprintf(out, ">!%016" PRIx64 "\n", vc->cc.bc->mtid);
    } else {
      fprintf(out, ">\n");
    }
    for (uint64_t j = 0; j < 8; j++) {
      recursive_dump(vc->sub_vc[j], out);
    }
    fprintf(out, "]\n");
  } else {
    fprintf(out, "[]\n");
  }
  return true;
}

  static struct VirtualContainer *
recursive_parse(FILE * const in, const uint64_t start_bit, struct DB * const db)
{
  char buf[128];
  fgets(buf, 120, in);
  assert(buf[0] == '[');
  if (buf[1] == ']') {
    return NULL;
  }
  struct VirtualContainer *vc = vc_create(start_bit);
  fgets(buf, 28, in);
  assert(buf[0] == '<');
  // '<!' : bloomcontainer
  // '<'  : bloomtable
  const bool load_bf = (buf[1] == '!')?false:true;
  for (uint64_t j = 0; j < DB_CONTAINER_NR; j++) {
    fgets(buf, 28, in);
    if (buf[0] == '>') break;

    const uint64_t mtid = strtoull(buf, NULL, 16);
    assert(db->cms[start_bit/3]);
    const int raw_fd = db->cms[start_bit/3]->raw_fd;
    struct MetaTable * const mt = db_load_metatable(db, mtid, raw_fd, load_bf);
    assert(mt);
    vc->cc.count++;
    vc->cc.metatables[j] = mt;
  }
  if (buf[0] != '>') { // read 8 in loop, eat '>'
    fgets(buf, 28, in);
    assert(buf[0] == '>');
  }
  if (load_bf == false) { // no bf, load bc
    assert(buf[1] == '!');
    // load bloomcontainer
    assert(buf[2] != '\0');
    const uint64_t mtid_bc = strtoull(buf+2, NULL, 16);
    struct BloomContainer * const bc = db_load_bloomcontainer_meta(db, mtid_bc);
    vc->cc.bc = bc;
  }
  for (uint64_t i = 0; i < 8; i++) {
    vc->sub_vc[i] = recursive_parse(in, start_bit + 3, db);
  }
  fgets(buf, 28, in);
  assert(buf[0] == ']');
  return vc;
}

  static void
db_initial(struct DB * const db, const char * const meta_dir, struct ContainerMapConf * const cm_conf)
{
  // Load Meta
  // dir (for dump)
  db->persist_dir = strdup(meta_dir);

  // set cms
  assert(cm_conf);
  for (uint64_t i = 0; i < DB_NR_LEVELS; i++) {
    db->cms[i] = db->cms_dump[cm_conf->data_id[i]];
    assert(db->cms[i]);
  }
  db->cm_bc = db->cms_dump[cm_conf->bc_id]; // hi?
  assert(db->cm_bc);

  // active tables
  db->active_table[0] = table_alloc_default(15.0);
  db->active_table[1] = NULL;

  // threading vars
  pthread_mutex_init(&(db->mutex_active), NULL);
  pthread_mutex_init(&(db->mutex_current), NULL);
  pthread_mutex_init(&(db->mutex_root), NULL);
  for (uint64_t i = 0; i < DB_COMPACTION_NR; i++) {
    pthread_mutex_init(&(db->mutex_token[i]), NULL);
  }
  // rwlock
  rwlock_initial(&(db->rwlock));

  // cond var
  pthread_cond_init(&(db->cond_root_consumer), NULL);
  pthread_cond_init(&(db->cond_root_producer), NULL);
  pthread_cond_init(&(db->cond_active), NULL);
  pthread_cond_init(&(db->cond_writer), NULL);
  db->compaction_token = 0;

  // log
  char path[4096];
  sprintf(path, "%s/%s", db->persist_dir, DB_META_LOG);
  FILE * const log = fopen(path, "a"); // NULL is OK
  db->log = log;

  // running
  db->sec_start = debug_time_sec();
  db->closing = false;
}

// backup db metadata
  static bool
db_dump_meta(struct DB * const db)
{
  char path_meta[256];
  char path_sym[256];

  const double sec0 = debug_time_sec();
  // prepare files
  sprintf(path_meta, "%s/%s/%s-%018.6lf", db->persist_dir, DB_META_BACKUP_DIR, DB_META_MAIN, sec0);
  FILE * const meta_out = fopen(path_meta, "w");
  assert(meta_out);

  const uint64_t ticket = rwlock_reader_lock(&(db->rwlock));
  // dump meta
  // write vc
  const bool r_meta = recursive_dump(db->vcroot, meta_out);
  assert(r_meta);
  // write mtid
  const uint64_t db_next_mtid = db->next_mtid;
  fprintf(meta_out, "%" PRIu64 "\n", db_next_mtid);
  fclose(meta_out);

  // create symlink for newest meta
  sprintf(path_sym, "%s/%s", db->persist_dir, DB_META_MAIN);
  if (access(path_sym, F_OK) == 0) {
    const int ru = unlink(path_sym);
    assert(ru == 0);
  }
  const int rsm = symlink(path_meta, path_sym);
  assert(rsm == 0);

  // dump container-maps
  for (int i = 0; db->cms_dump[i]; i++) {
    char path_cm_dump[256];
    sprintf(path_cm_dump, "%s/%s/%s-%01d-%018.6lf", db->persist_dir, DB_META_BACKUP_DIR, DB_META_CMAP_PREFIX, i, sec0);
    containermap_dump(db->cms_dump[i], path_cm_dump);
    // create symlink for newest meta
    sprintf(path_sym, "%s/%s-%01d", db->persist_dir, DB_META_CMAP_PREFIX, i);
    if (access(path_sym, F_OK) == 0) {
      const int ru = unlink(path_sym);
      assert(ru == 0);
    }
    const int rs = symlink(path_cm_dump, path_sym);
    assert(rs == 0);
  }

  // done
  rwlock_reader_unlock(&(db->rwlock), ticket);
  db_log_diff(db, sec0, "Dumping Metadata Finished (%06" PRIx64 ")", db_next_mtid);
  fflush(db->log);
  return true;
}

  static void
db_free(struct DB * const db)
{
  free(db->persist_dir);
  if (db->active_table[0]) {
    table_free(db->active_table[0]);
  }
  if (db->active_table[1]) {
    table_free(db->active_table[1]);
  }
  vc_recursive_free(db->vcroot);
  fclose(db->log);
  for (int i = 0; db->cms_dump[i]; i++) {
    containermap_destroy(db->cms_dump[i]);
  }
  free(db);
  return;
}

  static uint64_t
db_aquire_mtid(struct DB * const db)
{
  const uint64_t mtid = __sync_fetch_and_add(&(db->next_mtid), 1);
  return mtid;
}

  static uint64_t
db_cmap_safe_alloc(struct DB * const db, struct ContainerMap * const cm)
{
  while(db->closing == false) {
    const uint64_t off = containermap_alloc(cm);
    if (off < cm->total_cap) {
      return off;
    } else {
      sleep(1);
    }
  }
  return containermap_alloc(cm);
}

// takes 0.5s on average
// assume table has been detached from db (like memtable => imm)
  static uint64_t
db_table_dump(struct DB * const db, struct Table * const table, const uint64_t start_bit)
{
  const double sec0 = debug_time_sec();
  // aquire a uniq mtid;
  const uint64_t mtid = db_aquire_mtid(db);
  // post process table
  // must has bloom-filter
  assert(table->bt);

  // retaining
  const bool rr = table_retain(table);
  // logging on failed retaining
  if (rr == false) {
    //char buffer[4096];
    //table_analysis_verbose(table, buffer);
    db_log(db, "DUMP @%" PRIu64 " [%8" PRIx64 " FAILED!!]\n%s", start_bit/3, mtid, "");
    assert(false);
  }

  // analysis and log
  char buffer[1024];
  table_analysis_short(table, buffer);

  // alloc data area from containermap for items
  struct ContainerMap * const cm = db->cms[start_bit/3];
  const uint64_t off_main = db_cmap_safe_alloc(db, cm);
  assert(off_main < cm->total_cap);

  // dump table data
  const uint64_t nr_items = table_dump_barrels(table, cm->raw_fd, off_main);

  // dump meta
  char metafn[2048];
  db_generate_meta_fn(db, mtid, metafn);
  const bool rdm = table_dump_meta(table, metafn, off_main);
  assert(rdm);
  db_log_diff(db, sec0, "DUMP @%" PRIu64 " [%8" PRIx64 " #%08" PRIx64 "] [%08"PRIu64"] %s",
      start_bit/3, mtid, off_main/TABLE_ALIGN, nr_items, buffer);
  return mtid;
}

  static uint64_t
compaction_select_table(const uint8_t * const hash, const uint64_t start_bit)
{
  assert(start_bit >= 3);
  const uint64_t sel_bit = start_bit - 3;
  const uint8_t *start_byte = hash + (sel_bit >> 3);
  const uint64_t tmp = *((uint64_t *)start_byte);
  const uint64_t hv = tmp >> (sel_bit & 7u);
  const uint64_t tid = hv & 7u;
  return tid;
}

  static void
compaction_initial(struct Compaction * const comp, struct DB * const db,
    struct VirtualContainer * const vc, const uint64_t nr_feed)
{
  bzero(comp, sizeof(*comp));
  comp->start_bit = vc->start_bit;
  comp->sub_bit = vc->start_bit + 3;
  comp->gen_bc = (comp->sub_bit >= BC_START_BIT)?true:false;
  assert(nr_feed <= vc->cc.count);
  assert(vc->cc.count <= DB_CONTAINER_NR);
  comp->nr_feed = nr_feed;
  comp->db = db;
  comp->vc = vc;
  comp->cm_to = db->cms[comp->sub_bit/3];

  // alloc arenas
  uint8_t * const arena = huge_alloc(TABLE_ALIGN);
  assert(arena);
  comp->arena = arena;
  // old mts & mtids
  for (uint64_t i = 0; i < nr_feed; i++) {
    struct MetaTable * const mt = vc->cc.metatables[i];
    assert(mt);
    comp->mts_old[i] = mt;
    comp->mtids_old[i] = mt->mtid;
  }

  // new tables
  for (uint64_t i = 0; i < 8u; i++) {
    struct Table * const table = table_alloc_default(1.8);
    assert(table);
    comp->tables[i] = table;
  }

  // mbcs_old (if exists else NULL)
  for (uint64_t i = 0; i < 8u; i++) {
    if (vc->sub_vc[i] == NULL) {
      vc->sub_vc[i] = vc_create(comp->sub_bit);
    }
    comp->mbcs_old[i] = vc->sub_vc[i]->cc.bc;
  }
}

  static bool
compaction_feed(struct Compaction * const comp)
{
  const uint64_t token = __sync_fetch_and_add(&(comp->feed_token), DB_FEED_UNIT);
  assert(token < TABLE_MAX_BARRELS);
  struct MetaTable * const mt = comp->mts_old[comp->feed_id];
  if (token >= TABLE_NR_BARRELS) return true;
  const uint64_t nr_fetch = ((TABLE_NR_BARRELS - token) < DB_FEED_UNIT) ? (TABLE_NR_BARRELS - token) : DB_FEED_UNIT;
  uint8_t * const arena = comp->arena + (token * BARREL_ALIGN);
  assert((token + nr_fetch) <= TABLE_NR_BARRELS);
  metatable_feed_barrels_to_tables(mt, token, nr_fetch, arena, comp->tables, compaction_select_table, comp->sub_bit);
  return true;
}

  static void *
thread_compaction_feed(void * const p)
{
  struct Compaction * const comp = (typeof(comp))p;
  compaction_feed(comp);
  pthread_exit(NULL);
}

  static void
compaction_feed_all(struct Compaction * const comp)
{
  for (uint64_t i = 0; i < comp->nr_feed; i++) {
    const double sec0 = debug_time_sec();
    comp->feed_id = i;
    comp->feed_token = 0;
    // parallel feed threads
    conc_fork_reduce(DB_FEED_NR, thread_compaction_feed, comp);
    db_log_diff(comp->db, sec0, "FEED @%" PRIu64 " [%8" PRIx64 " #%08" PRIx64 "]",
      comp->start_bit/3, comp->mts_old[i]->mtid, comp->mts_old[i]->mfh.off/TABLE_ALIGN);
  }
  // free feed arenas
  huge_free(comp->arena, TABLE_ALIGN);
}

  static void *
thread_compaction_bt(void * const p)
{
  struct Compaction * const comp = (typeof(comp))p;
  const uint64_t i = __sync_fetch_and_add(&(comp->bt_token), 1);
  assert(i < 8);
  table_build_bloomtable(comp->tables[i]);
  pthread_exit(NULL);
}

  static void
compaction_build_bt_all(struct Compaction * const comp)
{
  comp->bt_token = 0;
  conc_fork_reduce(8, thread_compaction_bt, comp);
}

  static void *
thread_compaction_dump(void * const p)
{
  struct Compaction * const comp = (typeof(comp))p;
  const uint64_t i = __sync_fetch_and_add(&(comp->dump_token), 1);
  assert(i < 8);
  const uint64_t mtid = db_table_dump(comp->db, comp->tables[i], comp->sub_bit);
  comp->mtids_new[i] = mtid;
  struct MetaTable * const mt = db_load_metatable(comp->db, mtid, comp->cm_to->raw_fd, false);
  assert(mt);
  comp->mts_new[i] = mt;
  stat_inc_n(&(comp->db->stat.nr_write[comp->sub_bit]), TABLE_MAX_BARRELS);
  assert(mt->bt == NULL);
  if (comp->gen_bc == false) {
    mt->bt = comp->tables[i]->bt;
  }
  pthread_exit(NULL);
}

  static struct BloomContainer *
compaction_update_bc(struct DB * const db, struct BloomContainer * const old_bc, struct BloomTable * const bloomtable)
{
  const double sec0 = debug_time_sec();
  const uint64_t off_bc = db_cmap_safe_alloc(db, db->cm_bc);
  assert(off_bc < db->cm_bc->total_cap);
  const uint64_t mtid_bc = db_aquire_mtid(db);
  const int raw_fd = db->cm_bc->raw_fd;

  struct BloomContainer * const new_bc = (old_bc == NULL)?
    bloomcontainer_build(bloomtable, raw_fd, off_bc, &(db->stat)):
    bloomcontainer_update(old_bc, bloomtable, raw_fd, off_bc, &(db->stat));
  assert(new_bc);
  new_bc->mtid = mtid_bc;
  const uint64_t count = new_bc->nr_bf_per_box;
  assert(count > 0);

  const bool r = db_dump_bloomcontainer_meta(db, mtid_bc, new_bc);
  assert(r);
  db_log_diff(db, sec0, "BC   *%1" PRIu64 " [%8" PRIx64 " #%08" PRIx64 "] {%4" PRIu32 "}",
      count, mtid_bc, off_bc/TABLE_ALIGN, new_bc->nr_index);
  return new_bc;
}

  static void *
thread_compaction_bc(void * const p)
{
  struct Compaction * const comp = (typeof(comp))p;
  const uint64_t i = __sync_fetch_and_add(&(comp->bc_token), 1);
  assert(i < 8);
  struct BloomContainer * const new_bc = compaction_update_bc(comp->db, comp->mbcs_old[i], comp->tables[i]->bt);
  comp->mbcs_new[i] = new_bc;
  pthread_exit(NULL);
}

  static void
compaction_dump_and_bc_all(struct Compaction * const comp)
{
  comp->dump_token = 0;
  comp->bc_token = 0;
  pthread_t thd[8];
  pthread_t thb[8];
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for (uint64_t j = 0; j < 8; j++) {
    pthread_create(&(thd[j]), &attr, thread_compaction_dump, comp);
    if (comp->gen_bc == true) {
      pthread_create(&(thb[j]), &attr, thread_compaction_bc, comp);
    }
  }
  for (uint64_t j = 0; j < 8; j++) {
    pthread_join(thd[j], NULL);
    if (comp->gen_bc == true) {
      pthread_join(thb[j], NULL);
    }
  }
}

  static void
compaction_update_vc(struct Compaction * const comp)
{
  const uint64_t ticket = rwlock_writer_lock(&(comp->db->rwlock));

  struct VirtualContainer * const vc = comp->vc;
  // insert new mts
  for (uint64_t i = 0; i < 8; i++) {
    const bool ri = vc_insert_internal(vc->sub_vc[i], comp->mts_new[i], comp->mbcs_new[i]);
    assert(ri);
  }

  const uint64_t nr_keep = vc->cc.count - comp->nr_feed;
  // shift
  for (uint64_t i = 0; i < nr_keep; i++) {
    vc->cc.metatables[i] = vc->cc.metatables[i + comp->nr_feed];
  }
  // NULL
  for (uint64_t i = nr_keep; i < DB_CONTAINER_NR; i++) {
    vc->cc.metatables[i] = NULL;
  }
  vc->cc.count = nr_keep;

  rwlock_writer_unlock(&(comp->db->rwlock), ticket);
}

  static void
compaction_free_old(struct Compaction * const comp)
{
  // free n
  for (uint64_t i = 0; i < comp->nr_feed; i++) {
    containermap_release(comp->db->cms[comp->start_bit/3], comp->mts_old[i]->mfh.off);
    metatable_free(comp->mts_old[i]);
    db_destory_metatable(comp->db, comp->mtids_old[i]);
  }

  // free n+1
  for (uint64_t i = 0; i < 8; i++) {
    if (comp->mbcs_old[i]) {
      containermap_release(comp->db->cm_bc, comp->mbcs_old[i]->off_raw);
      bloomcontainer_free(comp->mbcs_old[i]);
    }
    if (comp->gen_bc == false) { // keep bloomtable
      comp->tables[i]->bt = NULL;
    }
    table_free(comp->tables[i]);
  }
}

  static void
compaction_main(struct DB * const db, struct VirtualContainer * const vc, const uint64_t nr_feed)
{
  struct Compaction comp;
  const double sec0 = debug_time_sec();
  compaction_initial(&comp, db, vc, nr_feed);
  // feed (must sequential)
  compaction_feed_all(&comp);
  // build bt
  compaction_build_bt_all(&comp);
  // dump table and bc
  compaction_dump_and_bc_all(&comp);
  // apply changes
  compaction_update_vc(&comp);
  // free old
  compaction_free_old(&comp);
  // log
  db_log_diff(db, sec0, "COMP @%" PRIu64 " %2" PRIu64, vc->start_bit/3u, nr_feed);
  stat_inc(&(db->stat.nr_compaction));
}

  static void
recursive_compaction(struct DB * const db, struct VirtualContainer * const vc)
{
  assert(vc);
  // disable compaction for last level
  if (vc->start_bit >= BC_START_BIT) return;

  const uint64_t nr_input = vc_count_feed(vc);
  if (nr_input == 0) { return; }

  compaction_main(db, vc, nr_input);

  // select most significant sub_vc
  struct VirtualContainer * const vc1 = vc_pick_compaction(vc->sub_vc, 0, 1);
  if (vc1) {
    recursive_compaction(db, vc1);
    for (;;) {
      struct VirtualContainer * const vcf = vc_pick_full(vc->sub_vc, 0, 1);
      if (vcf) {
        recursive_compaction(db, vcf);
      } else {
        break;
      }
    }
  }
}

  static void
db_root_compaction(struct DB * const db, const uint64_t token)
{
  struct VirtualContainer * const vc = db->vcroot;
  const uint64_t nr_input = vc_count_feed(vc);
  if (nr_input == 0) { return; }

  compaction_main(db, vc, nr_input);

  // Notify producer
  pthread_mutex_lock(&(db->mutex_current));
  pthread_cond_broadcast(&(db->cond_root_producer));
  pthread_mutex_unlock(&(db->mutex_current));
  pthread_mutex_unlock(&(db->mutex_root));

  // lock the child tree
  pthread_mutex_lock(&(db->mutex_token[token]));
  struct VirtualContainer * const vc1 = vc_pick_compaction(vc->sub_vc, token, DB_COMPACTION_NR);
  if (vc1) {
    recursive_compaction(db, vc1);
    for (;;) {
      struct VirtualContainer * const vcf = vc_pick_full(vc->sub_vc, token, DB_COMPACTION_NR);
      if (vcf) {
        recursive_compaction(db, vcf);
      } else {
        break;
      }
    }
  }

  // finish
  pthread_mutex_unlock(&(db->mutex_token[token]));
}

  static void *
thread_meta_dumper(void *ptr)
{
  struct DB * const db = (typeof(db))ptr;

  conc_set_affinity_n(1);
  do {
    const uint64_t last_mtid = db->next_mtid;
    for (uint64_t i = 0; i < 100; i++) {
      if (db->closing || db->need_dump_meta) break;
      sleep(6);
    }
    const uint64_t next_mtid = db->next_mtid;
    if (next_mtid != last_mtid) {
      db_dump_meta(db);
    }
    db->need_dump_meta = false;
  } while (false == db->closing);
  pthread_exit(NULL);
}

  static void *
thread_compaction(void *ptr)
{
  struct DB * const db = (typeof(db))ptr;
  while(true) {
    // single one
    pthread_mutex_lock(&(db->mutex_root));
    // get token
    const uint64_t token = (__sync_fetch_and_add(&(db->compaction_token), 1)) % DB_COMPACTION_NR;
    assert(token < DB_COMPACTION_NR);
    // wait for work, using 'current'
    pthread_mutex_lock(&(db->mutex_current));
    while ((db->closing == false) && (vc_count_feed(db->vcroot) == 0)) {
      pthread_cond_broadcast(&(db->cond_root_producer));
      pthread_cond_wait(&(db->cond_root_consumer), &(db->mutex_current));
    }
    if (db->closing && (vc_count_feed(db->vcroot) == 0)) {
      pthread_mutex_unlock(&(db->mutex_current));
      pthread_mutex_unlock(&(db->mutex_root));
      break;
    }
    pthread_mutex_unlock(&(db->mutex_current));
    conc_set_affinity_n(token);
    __sync_fetch_and_add(&(db->compaction_running_counter), 1);
    db_root_compaction(db, token);
    __sync_fetch_and_sub(&(db->compaction_running_counter), 1);
  }

  pthread_exit(NULL);
  return NULL;
}

// pthread
  static void *
thread_active_dumper(void *ptr)
{
  struct DB * const db = (typeof(db))ptr;

  conc_set_affinity_n(2);
  while (db->active_table[0]) {
    // active
    pthread_mutex_lock(&(db->mutex_active));
    if ((db->active_table[0]->volume == 0) && db->closing) {
      pthread_mutex_unlock(&(db->mutex_active));
      table_free(db->active_table[0]);
      db->active_table[0] = NULL;
      break;
    }
    while ((false == table_full(db->active_table[0])) && (false == db->closing)) {
      pthread_cond_wait(&(db->cond_active), &(db->mutex_active));
    }
    // shift active table
    const uint64_t ticket1 = rwlock_writer_lock(&(db->rwlock));
    db->active_table[1] = db->active_table[0];
    if (db->closing) {
      db->active_table[0] = NULL;
    } else {
      db->active_table[0] = table_alloc_default(15.0);
    }
    rwlock_writer_unlock(&(db->rwlock), ticket1);
    // notify writers
    pthread_cond_broadcast(&(db->cond_writer));
    pthread_mutex_unlock(&(db->mutex_active));

    struct Table * const table1 = db->active_table[1];
    if (containermap_unused(db->cms[0]) < 8u) {
      db_log(db, "ContainerMap is near full, dropping current active-table");
      sleep(10);
      const uint64_t ticket2 = rwlock_writer_lock(&(db->rwlock));
      db->active_table[1] = NULL;
      rwlock_writer_unlock(&(db->rwlock), ticket2);
    } else if (table1->volume > 0) {
      // build bt
      const bool rbt = table_build_bloomtable(table1);
      assert(rbt);
      // dump
      const uint64_t mtid = db_table_dump(db, table1, 0);
      struct MetaTable * const mt = db_load_metatable(db, mtid, db->cms[0]->raw_fd, false);
      assert(mt);
      stat_inc_n(&(db->stat.nr_write[0]), TABLE_NR_BARRELS);
      mt->bt = table1->bt;
      // mark active_table[1]->bt == NULL before free it

      // wait for room
      pthread_mutex_lock(&(db->mutex_current));
      while (db->vcroot->cc.count == DB_CONTAINER_NR) {
        pthread_cond_wait(&(db->cond_root_producer), &(db->mutex_current));
      }
      pthread_mutex_unlock(&(db->mutex_current));

      // insert
      const uint64_t ticket2 = rwlock_writer_lock(&(db->rwlock));
      const bool ri = vc_insert_internal(db->vcroot, mt, NULL);
      assert(ri);
      stat_inc(&(db->stat.nr_active_dumped));
      // alert compaction thread if have work to be done
      if (db->vcroot->cc.count >= 8) {
        pthread_mutex_lock(&(db->mutex_current));
        pthread_cond_broadcast(&(db->cond_root_consumer));
        pthread_mutex_unlock(&(db->mutex_current));
      }
      db->active_table[1] = NULL;
      rwlock_writer_unlock(&(db->rwlock), ticket2);

      // post process
      table1->bt = NULL;
    }
    table_free(table1);
  }
  pthread_exit(NULL);
  return NULL;
}

  static void
db_wait_active_table(struct DB * const db)
{
  pthread_mutex_lock(&(db->mutex_active));
  while (table_full(db->active_table[0])) {
    pthread_cond_signal(&(db->cond_active));
    pthread_cond_wait(&(db->cond_writer), &(db->mutex_active));
  }
  pthread_mutex_unlock(&(db->mutex_active));
}

  static struct KeyValue *
recursive_lookup(struct Stat * const stat, struct VirtualContainer * const vc, const uint64_t klen,
    const uint8_t * const key, const uint8_t * const hash)
{
  // lookup in current vc
  // test if using bloomcontainer
  uint64_t bitmap = UINT64_MAX;
  if (vc->cc.bc) {
    const uint64_t index = table_select_barrel(hash);
    assert(index < UINT64_C(0x100000000));
    const uint64_t *phv = ((const uint64_t*)(&(hash[12])));
    const uint64_t hv = *phv;
    bitmap = bloomcontainer_match(vc->cc.bc, (uint32_t)index, hv);
    stat_inc(&(stat->nr_fetch_bc));
  }
  for (int64_t j = vc->cc.count - 1; j >= 0; j--) {
    struct MetaTable * const mt = vc->cc.metatables[j];
    if (mt == NULL) continue;

    if ((bitmap & (1u << j)) == 0u) {
      stat_inc(&(stat->nr_true_negative));
      continue; // skip
    }
    struct KeyValue * const kv = metatable_lookup(mt, klen, key, hash);
    if (kv) {
      stat_inc(&(stat->nr_get_vc_hit[vc->start_bit]));
      return kv;
    }
  }
  // in sub_vc
  const uint64_t sub_id = compaction_select_table(hash, vc->start_bit + 3);
  if (vc->sub_vc[sub_id]) {
    return recursive_lookup(stat, vc->sub_vc[sub_id], klen, key, hash);
  } else {
    return NULL;
  }
}

  struct KeyValue *
db_lookup(struct DB * const db, const uint16_t klen, const uint8_t * const key)
{
  uint8_t hash[HASHBYTES] __attribute__ ((aligned(8)));
  SHA1(key, klen, hash);

  stat_inc(&(db->stat.nr_get));
  const uint64_t ticket = rwlock_reader_lock(&(db->rwlock));
  // 1st lookup at active table[0]
  // 2nd lookup at active table[1]
  for (uint64_t i = 0; i < 2; i++) {
    struct Table *t = db->active_table[i];
    if (t == NULL) continue;
    // immutable item
    struct KeyValue * const kv = table_lookup(t, klen, key, hash);
    if (kv) {
      rwlock_reader_unlock(&(db->rwlock), ticket);
      stat_inc(&(db->stat.nr_get_at_hit[i]));
      return kv;
    }
  }

  // 3rd lookup into vcroot
  struct KeyValue * const kv2 = recursive_lookup(&(db->stat), db->vcroot, klen, key, hash);
  rwlock_reader_unlock(&(db->rwlock), ticket);
  if (kv2 == NULL) {
    stat_inc(&(db->stat.nr_get_miss));
  }
  return kv2;
}

  static bool
db_insert_try(struct DB * const db, struct KeyValue * const kv)
{
  const uint64_t ticket = rwlock_writer_lock(&(db->rwlock));
  struct Table *at = db->active_table[0];
  const bool ri = table_insert_kv_safe(at, kv);
  rwlock_writer_unlock(&(db->rwlock), ticket);
  return ri;
}

  bool
db_insert(struct DB * const db, struct KeyValue * const kv)
{
  stat_inc(&(db->stat.nr_set));
  while (false == db_insert_try(db, kv)) {
    db_wait_active_table(db);
    stat_inc(&(db->stat.nr_set_retry));
  }
  return true;
}

  bool
db_multi_insert(struct DB * const db, const uint64_t nr_items, const struct KeyValue * const kvs)
{
  uint64_t i = 0;
  while (i < nr_items) {
    const uint64_t ticket = rwlock_writer_lock(&(db->rwlock));
    struct Table *at = db->active_table[0];
    while (i < nr_items) {
      const struct KeyValue * const kv = &(kvs[i]);
      const bool ri = table_insert_kv_safe(at, kv);
      if (ri == true) { i++; } else { break; }
    }
    rwlock_writer_unlock(&(db->rwlock), ticket);

    if (i < nr_items) {
      db_wait_active_table(db);
      stat_inc(&(db->stat.nr_set_retry));
    }
  }
  stat_inc_n(&(db->stat.nr_set), nr_items);
  return true;
}

  static bool
db_touch_dir(const char * const root_dir, const char * const sub_dir)
{
  char path[256];
  sprintf(path, "%s/%s", root_dir, sub_dir);
  struct stat stat_buf;
  mkdir(path, 00755);
  if (0 != access(path, F_OK)) { return false; }
  if (0 != stat(path, &stat_buf)) { return false; }
  if (!S_ISDIR(stat_buf.st_mode)) {return false; }
  return true;
}

// create empty db
  static struct DB *
db_create(const char * const meta_dir, struct ContainerMapConf * const cm_conf)
{
  const double sec0 = debug_time_sec();
  // touch dir
  if (false == db_touch_dir(meta_dir, "")) return NULL;
  // touch meta_backup dir
  if (false == db_touch_dir(meta_dir, DB_META_BACKUP_DIR)) return NULL;

  // pre make 256 sub-dirs
  char sub_dir[16];
  for (uint64_t i = 0; i < 256; i++) {
    sprintf(sub_dir, "%02" PRIx64, i);
    if (false == db_touch_dir(meta_dir, sub_dir)) return NULL;
  }

  struct DB * const db = (typeof(db))malloc(sizeof(*db));
  bzero(db, sizeof(*db));

  for (int i = 0; (i < 6) && cm_conf->raw_fn[i]; i++) {
    struct ContainerMap * const cm = containermap_create(cm_conf->raw_fn[i], cm_conf->hints[i]);
    assert(cm);
    db->cms_dump[i] = cm;
  }

  db_initial(db, meta_dir, cm_conf);

  // empty vc
  db->vcroot = vc_create(0);
  assert(db->vcroot);

  // mtid start from 1
  db->next_mtid = 1;

  // initial anything
  db_log_diff(db, sec0, "Initialized Metadata");
  return db;
}

  static struct DB *
db_load(const char * const meta_dir, struct ContainerMapConf * const cm_conf)
{
  const double sec0 = debug_time_sec();
  char path_meta[2048];

  // test files
  sprintf(path_meta, "%s/%s", meta_dir, DB_META_MAIN);
  if (0 != access(path_meta, F_OK)) return NULL;
  assert(cm_conf);
  // test cmaps
  for (int i = 0; (i < 6) && cm_conf->raw_fn[i]; i++) {
    char path_cm[2048];
    sprintf(path_cm, "%s/%s-%01d", meta_dir, DB_META_CMAP_PREFIX, i);
    if (0 != access(path_cm, F_OK)) return NULL;
  }

  // alloc db and load ContainerMap
  struct DB * const db = (typeof(db))malloc(sizeof(*db));
  assert(db);
  bzero(db, sizeof(*db));

  // load ContainerMaps
  for (int i = 0; (i < 6) && cm_conf->raw_fn[i]; i++) {
    char path_cm[2048];
    sprintf(path_cm, "%s/%s-%01d", meta_dir, DB_META_CMAP_PREFIX, i);
    struct ContainerMap * const cm = containermap_load(path_cm, cm_conf->raw_fn[i]);
    assert(cm);
    db->cms_dump[i] = cm;
  }

  db_initial(db, meta_dir, cm_conf);

  //// LOAD META
  // parse vc
  FILE * const meta_in = fopen(path_meta, "r");
  struct VirtualContainer * const vcroot = recursive_parse(meta_in, 0, db);
  assert(vcroot);
  db->vcroot = vcroot;

  // read mtid
  char buf_mtid[32];
  fgets(buf_mtid, 30, meta_in);
  const uint64_t mtid = strtoull(buf_mtid, NULL, 10);
  assert(mtid > 0);
  db->next_mtid = mtid;
  fclose(meta_in);

  // initial anything
  db_log_diff(db, sec0, "Loaded Metadata Done");
  return db;
}

// get a DB * for GET/SET
// cap_hint: only ragular files are affected. raw disk/ssd are all seen as its real cap
  void
db_spawn_threads(struct DB * const db)
{
  assert(db);
  // spawn compaction thread
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for (uint64_t n = 0; n < DB_COMPACTION_THREADS_NR; n++) {
    const int pc = pthread_create(&(db->t_compaction[n]), &attr, thread_compaction, (void *)db);
    assert(pc == 0);
    char th_name[128];
    sprintf(th_name, "Compaction[%"PRIu64"]", n);
    pthread_setname_np(db->t_compaction[n], th_name);
  }

  const int pca = pthread_create(&(db->t_active_dumper), &attr, thread_active_dumper, (void *)db);
  assert(pca == 0);
  pthread_setname_np(db->t_active_dumper, "Active-Dumper");

  const int pcm = pthread_create(&(db->t_meta_dumper), &attr, thread_meta_dumper, (void *)db);
  assert(pcm == 0);
  pthread_setname_np(db->t_meta_dumper, "Meta-Dumper");
  // db ready to use
}

  static struct ContainerMapConf *
db_load_cm_conf(const char * const fn)
{
  struct ContainerMapConf * const cm_conf = (typeof(cm_conf))malloc(sizeof(*cm_conf));
  bzero(cm_conf, sizeof(*cm_conf));

  FILE * const fi = fopen(fn, "r");
  char buf[1024];
  uint64_t count = 0;
  for (int i = 0; i < 6; i++) {
    // dev path
    buf[0] = 0;
    fgets(buf, 1000, fi);
    if (buf[0] == '$') break;
    char * const peol = strchr(buf, '\n');
    if (peol) {*peol = '\0';}
    cm_conf->raw_fn[i] = strdup(buf);
    // hint
    buf[0] = 0;
    fgets(buf, 1000, fi);
    const uint64_t hint = strtoull(buf, NULL, 10);
    cm_conf->hints[i] = hint * UINT64_C(1024) * UINT64_C(1024) * UINT64_C(1024); // *GB
    count++;
  }

  assert(buf[0] == '$');
  // bc
  buf[0] = 0;
  fgets(buf, 1000, fi);
  const uint64_t bc_id = strtoull(buf, NULL, 10);
  assert(bc_id < count);
  cm_conf->bc_id = bc_id;
  // 0-4
  for (int i = 0; i < DB_NR_LEVELS; i++) {
    buf[0] = 0;
    fgets(buf, 1000, fi);
    const uint64_t id = strtoull(buf, NULL, 10);
    assert(id < count);
    cm_conf->data_id[i] = id;
  }
  fclose(fi);
  return cm_conf;
}

  struct DB *
db_touch(const char * const meta_dir, const char * const cm_conf_fn)
{
  // cm conf
  assert(cm_conf_fn);
  // TODO: free cm_conf at later time
  struct ContainerMapConf * const cm_conf = db_load_cm_conf(cm_conf_fn);
  assert(cm_conf);
  // touch dir
  assert(meta_dir);
  const int r_dir = access(meta_dir, F_OK);
  struct DB * db = NULL;
  if (r_dir == 0) {// has dir
    // try load DB
    db = db_load(meta_dir, cm_conf);
  }
  // create anyway
  if (db == NULL) {
    db = db_create(meta_dir, cm_conf);
  }
  if (db) {
    db_spawn_threads(db);
  }
  return db;
}

  void
db_close(struct DB * const db)
{
  // Active Dumper thread
  db->closing = true;
  db_log(db, "CLOSE: Waiting Active Dumper thread");
  pthread_mutex_lock(&(db->mutex_active)); // lock so no active threads is working
  pthread_cond_broadcast(&(db->cond_active));
  pthread_mutex_unlock(&(db->mutex_active)); // lock so no active threads is working
  pthread_join(db->t_active_dumper, NULL);
  db_log(db, "CLOSE: Active Dumper thread exited");

  // Compaction thread
  db_log(db, "CLOSE: Waiting for compaction thread");
  pthread_mutex_lock(&(db->mutex_current));
  pthread_cond_broadcast(&(db->cond_root_consumer));
  pthread_cond_broadcast(&(db->cond_root_producer));
  pthread_mutex_unlock(&(db->mutex_current));
  for (uint64_t n = 0; n < DB_COMPACTION_THREADS_NR; n++) {
    pthread_join(db->t_compaction[n], NULL);
  }
  db_log(db, "CLOSE: Compaction threads exited");

  // Meta Dummper thread
  db_log(db, "CLOSE: Waiting for Meta Dumper thread");
  pthread_join(db->t_meta_dumper, NULL);
  db_log(db, "CLOSE: Meta Dummper thread exited");

  // cheap last dump
  db_dump_meta(db);
  db_free(db);
  return;
}

  void
db_force_dump_meta(struct DB * const db)
{
  db->need_dump_meta = true;
  do {
    sleep(1);
  } while (db->need_dump_meta);
}

  void
db_stat_show(struct DB * const db, FILE * const fo)
{
  stat_show(&(db->stat), fo);
}

  void
db_stat_clean(struct DB * const db)
{
  bzero(&(db->stat), sizeof(db->stat));
}

  bool
db_doing_compaction(struct DB * const db)
{
  return db->compaction_running_counter ? true : false;
}
