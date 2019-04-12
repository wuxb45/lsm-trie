/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>

#include "coding.h"

  uint8_t*
encode_uint64(uint8_t * const dst, const uint64_t v)
{
  static const uint64_t B = 0x80;
  static const uint64_t M = 0x7f;
  uint8_t * ptr = dst;
  uint64_t t = v;
  while (t >= B) {
    *ptr = (uint8_t)((t & M) | B);
    ++ptr;
    t >>= 7;
  }
  *ptr = (uint8_t) t;
  ++ptr;
  return ptr;
}

  const uint8_t *
decode_uint64(const uint8_t * const src, uint64_t * const value) {
  uint64_t result = 0;
  static const uint64_t B = 0x80;
  static const uint64_t M = 0x7f;
  const uint8_t* p = src;

  for (uint32_t shift = 0; shift <= 63; shift += 7) {
    const uint64_t byte = (uint64_t)(*p);
    ++p;
    if (byte & B) {
      // More bytes are present
      result |= ((byte & M) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  *value = 0;
  return src;
}

  uint8_t *
encode_uint16(uint8_t * const dst, const uint16_t v);

  uint8_t *
encode_uint32(uint8_t * const dst, const uint32_t v);

  const uint8_t *
decode_uint16(const uint8_t * const src, uint16_t * const value);

  const uint8_t *
decode_uint32(const uint8_t * const src, uint32_t * const value);
