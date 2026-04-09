// SPDX-License-Identifier: Apache-2.0
//
// DingoFS Cache Engine - Public C API
//
// A high-performance cache engine for storing LLM KV cache on DingoFS.
// Uses TierBlockCache with bthread async callbacks, eventfd-based
// completion signaling, and an LRU exists cache.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

typedef struct cache_engine_t cache_engine_t;

typedef struct {
  uint64_t future_id;
  int ok;
  const char* error;
  const uint8_t* result_bytes;
  size_t result_len;
} cache_completion_t;

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Create a cache engine from a JSON config string.
//
// JSON fields:
//   "fs_id":                uint32 (default 1)
//   "ino":                  uint64 (default 1)
//   "cache_dir":            string (required)
//   "cache_size_mb":        uint32 (default 102400)
//   "exists_cache_capacity": uint64 (default 100000)
//
// Returns engine handle, or NULL on failure (check cache_engine_last_error).
cache_engine_t* cache_engine_create(const char* config_json);

void cache_engine_destroy(cache_engine_t* engine);

// ---------------------------------------------------------------------------
// Event notification
// ---------------------------------------------------------------------------

int cache_engine_event_fd(const cache_engine_t* engine);

// ---------------------------------------------------------------------------
// Submit operations (non-blocking, return future_id)
// ---------------------------------------------------------------------------

uint64_t cache_engine_submit_batch_get(cache_engine_t* engine,
                                       const char** keys, void** bufs,
                                       const size_t* lens, size_t count,
                                       size_t chunk_size);

uint64_t cache_engine_submit_batch_set(cache_engine_t* engine,
                                       const char** keys,
                                       const void** bufs,
                                       const size_t* lens, size_t count,
                                       size_t chunk_size);

uint64_t cache_engine_submit_batch_exists(cache_engine_t* engine,
                                          const char** keys, size_t count);

// ---------------------------------------------------------------------------
// Drain completions
// ---------------------------------------------------------------------------

int cache_engine_drain_completions(cache_engine_t* engine,
                                   cache_completion_t* out,
                                   size_t max_completions);

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

void cache_engine_close(cache_engine_t* engine);

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

const char* cache_engine_last_error(void);

#ifdef __cplusplus
}  // extern "C"
#endif
