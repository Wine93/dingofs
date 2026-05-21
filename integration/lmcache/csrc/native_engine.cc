// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "native_engine.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "cache/blockcache/block_cache.h"
#include "cache/remotecache/remote_block_cache.h"
#include "common/block/block_handle.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace integration {
namespace lmcache {

namespace {

// One engine per process: gflags storage is global, and so is the brpc /
// bthread / glog runtime. Subsequent constructions would silently corrupt
// the first engine's configuration.
std::atomic<bool> g_engine_alive{false};

std::once_flag g_glog_once;

void InitGlogOnce() {
  std::call_once(g_glog_once, [] {
    google::InitGoogleLogging("dingofs_lmcache_connector");
  });
}

void ApplyFlag(const std::string& name, const std::string& value) {
  std::string result =
      ::gflags::SetCommandLineOption(name.c_str(), value.c_str());
  LOG_IF(WARNING, result.empty()) << "Unknown gflag '" << name << "' (ignored)";
}

// Translate a dingofs Status into a (ok, err) pair. NotFound is a normal
// outcome for Exists/Get and is reported as ok=false with err="".
struct StatusReport {
  bool ok;
  std::string err;
};

StatusReport ReportFromStatus(const Status& s) {
  if (s.ok()) return {true, {}};
  if (s.IsNotFound()) return {false, {}};
  return {false, s.ToString()};
}

}  // namespace

// ---------- NativeEngine ----------

NativeEngine::NativeEngine() = default;

NativeEngine::~NativeEngine() { Shutdown(); }

std::unique_ptr<NativeEngine> NativeEngine::Create(const InitOptions& opts) {
  bool expected = false;
  if (!g_engine_alive.compare_exchange_strong(expected, true)) {
    throw std::runtime_error(
        "dingofs lmcache NativeEngine: only one instance allowed per process "
        "(gflags are process-global)");
  }

  if (opts.mds_addrs.empty()) {
    g_engine_alive.store(false);
    throw std::invalid_argument("mds_addrs is required");
  }
  if (opts.cache_group.empty()) {
    g_engine_alive.store(false);
    throw std::invalid_argument("cache_group is required");
  }

  std::unique_ptr<NativeEngine> engine(new NativeEngine());
  try {
    engine->InstallFlags(opts);
    InitGlogOnce();
    engine->StartCache();
  } catch (...) {
    g_engine_alive.store(false);
    throw;
  }

  engine->running_.store(true, std::memory_order_release);
  return engine;
}

void NativeEngine::InstallFlags(const InitOptions& opts) {
  // FLAGS_mds_addrs / FLAGS_cache_group live in namespace dingofs::cache.
  // Assigning directly is what dingo-client (src/client/main.cc:174) does;
  // SetCommandLineOption would also work but only after gflags has parsed.
  dingofs::cache::FLAGS_mds_addrs = opts.mds_addrs;
  dingofs::cache::FLAGS_cache_group = opts.cache_group;
  for (const auto& [name, value] : opts.extra_flags) {
    ApplyFlag(name, value);
  }
}

void NativeEngine::StartCache() {
  // storage_client is unused on the cache-group path (see
  // RemoteBlockCacheImpl ctor at
  // src/cache/remotecache/remote_block_cache.cc:55).
  block_cache_ =
      std::make_unique<dingofs::cache::RemoteBlockCacheImpl>(nullptr);
  Status s = block_cache_->Start();
  if (!s.ok()) {
    block_cache_.reset();
    throw std::runtime_error("RemoteBlockCache::Start() failed: " +
                             s.ToString());
  }
}

void NativeEngine::Shutdown() {
  bool was_running = running_.exchange(false, std::memory_order_acq_rel);
  if (!was_running) return;

  if (block_cache_) {
    block_cache_->Shutdown();
    block_cache_.reset();
  }
  g_engine_alive.store(false);
}

// ---------- sync ops ----------

bool NativeEngine::ExistsSync(const TensorKey& key, std::string* err) {
  if (!running_.load(std::memory_order_acquire)) {
    if (err) *err = "engine shut down";
    return false;
  }
  IOBuffer tmp;
  // retrieve_storage=false: ask the cache node to return NotFound rather
  // than falling through to the underlying object store. ~1 byte over wire.
  cache::RangeOption opt;
  opt.retrieve_storage = false;
  Status s = block_cache_->Range(BlockHandle(key), 0, 1, &tmp, opt);
  if (s.ok()) return true;
  if (s.IsNotFound()) return false;
  if (err) *err = s.ToString();
  return false;
}

bool NativeEngine::Ping(std::string* err) {
  // RemoteBlockCacheImpl does not expose a dedicated Ping RPC, so we probe
  // with a sentinel TensorKey. NotFound is the expected (healthy) outcome:
  // it proves the round-trip succeeded without finding the key.
  if (!running_.load(std::memory_order_acquire)) {
    if (err) *err = "engine shut down";
    return false;
  }
  TensorKey sentinel{"__ping__", 0, 0, "deadbeefdeadbeefdeadbeefdeadbeef",
                     "float16"};
  std::string probe_err;
  ExistsSync(sentinel, &probe_err);
  if (probe_err.empty()) return true;
  if (err) *err = std::move(probe_err);
  return false;
}

// ---------- async ops ----------

uint64_t NativeEngine::SubmitBatchSet(std::vector<SetItem> items) {
  if (!running_.load(std::memory_order_acquire)) {
    throw std::runtime_error("engine shut down");
  }
  if (items.empty()) return queue_.PushEmpty(OpType::kSet);

  FanIn* fan = queue_.NewFanIn(OpType::kSet, items.size());
  uint64_t fid = fan->future_id();

  size_t i = 0;
  for (auto& it : items) {
    // Zero-copy: hand the raw Python buffer to butil::IOBuf via AppendUserData.
    // The captured shared_ptr<BufferHandle> dies once IOBuf drops its last
    // reference to the block, releasing the Python ref under the GIL.
    IOBuffer io;
    io.AppendUserData(const_cast<void*>(it.data), it.size,
                      [pin = std::move(it.pin)](void*) {});

    block_cache_->AsyncPut(
        BlockHandle(it.key), std::move(io),
        [fan, i](Status s) {
          auto r = ReportFromStatus(s);
          fan->Report(i, r.ok, std::move(r.err));
        },
        cache::PutOption{});
    ++i;
  }
  return fid;
}

uint64_t NativeEngine::SubmitBatchGet(std::vector<GetItem> items) {
  if (!running_.load(std::memory_order_acquire)) {
    throw std::runtime_error("engine shut down");
  }
  if (items.empty()) return queue_.PushEmpty(OpType::kGet);

  FanIn* fan = queue_.NewFanIn(OpType::kGet, items.size());
  uint64_t fid = fan->future_id();

  size_t i = 0;
  for (auto& it : items) {
    // shared_ptr keeps the response IOBuffer alive across the async boundary;
    // brpc lands the payload in it, then we cutn() straight into the caller's
    // dst — the single unavoidable memcpy (brpc IOBuf -> user buffer). No
    // extra intermediate hop.
    auto io = std::make_shared<IOBuffer>();
    void* dst = it.dst;
    size_t size = it.size;

    block_cache_->AsyncRange(
        BlockHandle(it.key), 0, size, io.get(),
        [fan, i, io, pin = std::move(it.pin), dst, size](Status s) {
          if (s.ok()) {
            io->IOBuf().cutn(dst, size);  // TODO: zero copy
            fan->Report(i, true);
          } else if (s.IsNotFound()) {
            fan->Report(i, false);
          } else {
            fan->Report(i, false, s.ToString());
          }
        },
        cache::RangeOption{});
    ++i;
  }
  return fid;
}

uint64_t NativeEngine::SubmitBatchExists(std::vector<TensorKey> keys) {
  if (!running_.load(std::memory_order_acquire)) {
    throw std::runtime_error("engine shut down");
  }
  if (keys.empty()) return queue_.PushEmpty(OpType::kExists);

  FanIn* fan = queue_.NewFanIn(OpType::kExists, keys.size());
  uint64_t fid = fan->future_id();

  size_t i = 0;
  for (const auto& key : keys) {
    auto io = std::make_shared<IOBuffer>();
    cache::RangeOption opt;
    opt.retrieve_storage = false;

    block_cache_->AsyncRange(
        BlockHandle(key), 0, 1, io.get(),
        [fan, i, io](Status s) {
          if (s.ok()) {
            fan->Report(i, true);
          } else if (s.IsNotFound()) {
            fan->Report(i, false);
          } else {
            fan->Report(i, false, s.ToString());
          }
        },
        opt);
    ++i;
  }
  return fid;
}

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs
