// SPDX-License-Identifier: Apache-2.0

#include "cache_engine_capi.h"
#include "cache_engine.h"

#include <cstring>
#include <exception>
#include <string>
#include <vector>

// Minimal JSON parsing (no external dependency)
#include <json/reader.h>
#include <json/value.h>

static thread_local std::string tl_last_error;

static void set_error(const std::string& msg) { tl_last_error = msg; }
static void clear_error() { tl_last_error.clear(); }

struct cache_engine_t {
  dingofs::CacheEngine engine;
  std::vector<dingofs::Completion> cached_completions;

  explicit cache_engine_t(const dingofs::CacheEngineConfig& config)
      : engine(config) {}
};

extern "C" {

cache_engine_t* cache_engine_create(const char* config_json) {
  clear_error();
  try {
    dingofs::CacheEngineConfig config;

    // Parse JSON config
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::istringstream stream(config_json ? config_json : "{}");
    if (!Json::parseFromStream(builder, stream, &root, &errors)) {
      set_error("JSON parse error: " + errors);
      return nullptr;
    }

    if (root.isMember("fs_id")) {
      config.fs_id = root["fs_id"].asUInt();
    }
    if (root.isMember("ino")) {
      config.ino = root["ino"].asUInt64();
    }
    if (root.isMember("cache_dir")) {
      config.cache_dir = root["cache_dir"].asString();
    }
    if (root.isMember("cache_size_mb")) {
      config.cache_size_mb = root["cache_size_mb"].asUInt();
    }
    if (root.isMember("exists_cache_capacity")) {
      config.exists_cache_capacity = root["exists_cache_capacity"].asUInt64();
    }

    auto* engine = new cache_engine_t(config);
    auto status = engine->engine.Init();
    if (!status.ok()) {
      set_error(status.ToString());
      delete engine;
      return nullptr;
    }
    return engine;
  } catch (const std::exception& e) {
    set_error(e.what());
    return nullptr;
  }
}

void cache_engine_destroy(cache_engine_t* engine) {
  if (engine) {
    try {
      engine->engine.Close();
    } catch (...) {
    }
    delete engine;
  }
}

int cache_engine_event_fd(const cache_engine_t* engine) {
  if (!engine) return -1;
  return engine->engine.EventFd();
}

uint64_t cache_engine_submit_batch_get(cache_engine_t* engine,
                                       const char** keys, void** bufs,
                                       const size_t* lens, size_t count,
                                       size_t chunk_size) {
  clear_error();
  if (!engine || !keys || !bufs || !lens || count == 0) {
    set_error("invalid arguments");
    return 0;
  }
  try {
    std::vector<std::string> key_vec(keys, keys + count);
    std::vector<void*> buf_vec(bufs, bufs + count);
    std::vector<size_t> len_vec(lens, lens + count);
    return engine->engine.SubmitBatchGet(key_vec, buf_vec, len_vec,
                                         chunk_size);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

uint64_t cache_engine_submit_batch_set(cache_engine_t* engine,
                                       const char** keys,
                                       const void** bufs,
                                       const size_t* lens, size_t count,
                                       size_t chunk_size) {
  clear_error();
  if (!engine || !keys || !bufs || !lens || count == 0) {
    set_error("invalid arguments");
    return 0;
  }
  try {
    std::vector<std::string> key_vec(keys, keys + count);
    std::vector<void*> buf_vec(count);
    for (size_t i = 0; i < count; ++i) {
      buf_vec[i] = const_cast<void*>(bufs[i]);
    }
    std::vector<size_t> len_vec(lens, lens + count);
    return engine->engine.SubmitBatchSet(key_vec, buf_vec, len_vec,
                                         chunk_size);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

uint64_t cache_engine_submit_batch_exists(cache_engine_t* engine,
                                          const char** keys,
                                          size_t count) {
  clear_error();
  if (!engine || !keys || count == 0) {
    set_error("invalid arguments");
    return 0;
  }
  try {
    std::vector<std::string> key_vec(keys, keys + count);
    return engine->engine.SubmitBatchExists(key_vec);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

int cache_engine_drain_completions(cache_engine_t* engine,
                                   cache_completion_t* out,
                                   size_t max_completions) {
  clear_error();
  if (!engine || !out || max_completions == 0) {
    return 0;
  }
  try {
    engine->cached_completions = engine->engine.DrainCompletions();

    size_t n =
        std::min(engine->cached_completions.size(), max_completions);
    for (size_t i = 0; i < n; ++i) {
      const auto& c = engine->cached_completions[i];
      out[i].future_id = c.future_id;
      out[i].ok = c.ok ? 1 : 0;
      out[i].error = c.error.empty() ? nullptr : c.error.c_str();
      out[i].result_bytes =
          c.result_bytes.empty() ? nullptr : c.result_bytes.data();
      out[i].result_len = c.result_bytes.size();
    }
    return static_cast<int>(n);
  } catch (const std::exception& e) {
    set_error(e.what());
    return 0;
  }
}

void cache_engine_close(cache_engine_t* engine) {
  if (engine) {
    try {
      engine->engine.Close();
    } catch (...) {
    }
  }
}

const char* cache_engine_last_error(void) {
  return tl_last_error.empty() ? nullptr : tl_last_error.c_str();
}

}  // extern "C"
