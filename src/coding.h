/*
 * Copyright (c) 2014  Wu, Xingbo <wuxb45@gmail.com>
 *
 * All rights reserved. No warranty, explicit or implicit, provided.
 */

#pragma once

#include <stdint.h>

uint8_t *
encode_uint64(uint8_t * const dst, const uint64_t v);

const uint8_t *
decode_uint64(const uint8_t * const src, uint64_t * const value);

  inline uint8_t *
encode_uint16(uint8_t * const dst, const uint16_t v)
{
  return encode_uint64(dst, (uint64_t)v);
}

  inline uint8_t *
encode_uint32(uint8_t * const dst, const uint32_t v)
{
  return encode_uint64(dst, (uint64_t)v);
}

  inline const uint8_t *
decode_uint16(const uint8_t * const src, uint16_t * const value)
{
  uint64_t v = 0;
  const uint8_t * const p = decode_uint64(src, &v);
  // assert(v < UINT16_MAX)
  *value = (typeof(*value))v;
  return p;
}

  inline const uint8_t *
decode_uint32(const uint8_t * const src, uint32_t * const value)
{
  uint64_t v = 0;
  const uint8_t * const p = decode_uint64(src, &v);
  *value = (typeof(*value))v;
  return p;
}
