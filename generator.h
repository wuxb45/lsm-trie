/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */
#pragma once

#include <stdint.h>
#include <stdio.h>

enum GeneratorType {
  GEN_CONSTANT = 0, // always a constant number
  GEN_COUNTER,      // +1 each fetch
  GEN_DISCRETE,     // gen within a set of values with its weight as probability
  GEN_EXPONENTIAL,  // exponential
  GEN_FILE,         // string from lines in file
  GEN_HISTOGRAM,    // histogram
  GEN_HOTSET,       // hot/cold set
  GEN_ZIPFIAN,      // Zipfian, 0 is the most popular.
  GEN_XZIPFIAN,     // ScrambledZipfian. scatters the "popular" items across the itemspace.
  GEN_LATEST,       // Generate a popularity distribution of items, skewed to favor recent items.
  GEN_UNIFORM,      // Uniformly distributed in an interval [a,b]
};

struct GenInfo_Constant {
  uint64_t constant;
};

struct GenInfo_Counter {
  uint64_t counter;
};

struct Pair64 {
  uint64_t a;
  uint64_t b;
};

struct GenInfo_Discrete {
  uint64_t nr_values;
  struct Pair64 *pairs;
};

struct GenInfo_Exponential {
  double gamma;
};

struct GenInfo_File {
  FILE * fin;
};

struct GenInfo_Histogram {
  uint64_t block_size;
  uint64_t area;
  uint64_t weighted_area;
  double main_size;
  uint64_t *buckets;
};

struct GenInfo_HotSet {
  uint64_t lower_bound;
  uint64_t upper_bound;
  uint64_t hot_interval;
  uint64_t cold_interval;
  double hot_set_fraction;
  double hot_op_fraction;
};

#define ZIPFIAN_CONSTANT ((0.99))
struct GenInfo_Zipfian {
  uint64_t nr_items;
  uint64_t base;
  double zipfian_constant;
  double theta;
  double zeta2theta;
  double alpha;
  double zetan;
  double eta;
  uint64_t countforzeta;
  uint64_t min;
  uint64_t max;
};

struct GenInfo_Latest {
  struct GenInfo_Zipfian zipfion;
  uint64_t max;
};

struct GenInfo_Uniform {
  uint64_t min;
  uint64_t max;
  double interval;
};

struct GenInfo {
  uint64_t (*next)(struct GenInfo * const);
  enum GeneratorType type;
  union {
    struct GenInfo_Constant     constant;
    struct GenInfo_Counter      counter;
    struct GenInfo_Discrete     discrete;
    struct GenInfo_Exponential  exponential;
    struct GenInfo_File         file;
    struct GenInfo_Histogram    histogram;
    struct GenInfo_HotSet       hotset;
    struct GenInfo_Zipfian      zipfian;
    struct GenInfo_Latest       latest;
    struct GenInfo_Uniform      uniform;
  } gen;
};

uint64_t random_uint64(void);

struct GenInfo * generator_new_constant(const uint64_t constant);

struct GenInfo * generator_new_counter(const uint64_t start);

struct GenInfo * generator_new_exponential(const double percentile, const double range);

struct GenInfo * generator_new_zipfian(const uint64_t min, const uint64_t max);

struct GenInfo * generator_new_xzipfian(const uint64_t min, const uint64_t max);

struct GenInfo * generator_new_uniform(const uint64_t min, const uint64_t max);

void generator_destroy(struct GenInfo * const geninfo);
