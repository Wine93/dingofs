// SPDX-License-Identifier: Apache-2.0

#include "dingofs_connector.h"

#include <absl/strings/str_split.h>

#include <algorithm>
#include <cstdio>
#include <functional>
#include <future>
#include <limits>
#include <stdexcept>

#include "cache/common/context.h"
#include "cache/tiercache/tier_block_cache.h"
#include "common/options/cache.h"
#include "noop_block_accesser.h"

namespace dingofs {
namespace connector {

// ---------------------------------------------------------------------------
// Construction / Destruction
// ---------------------------------------------------------------------------

DingoFSConnector::DingoFSConnector(const std::string& cache_dir,
                                   uint32_t fs_id,
                                   uint64_t ino,
                                   uint32_t cache_size_mb,
                                   size_t exists_cache_capacity,
                                   int num_workers)
    : lmcache::connector::ConnectorBase<DingoFSConnection>(num_workers) {
  config_.cache_dir = cache_dir;
  config_.fs_id = fs_id;
  config_.ino = ino;
  config_.cache_size_mb = cache_size_mb;
  config_.exists_cache_capacity = exists_cache_capacity;
  config_.num_workers = num_workers;

  if (config_.cache_dir.empty()) {
    throw std::runtime_error("cache_dir must not be empty");
  }

  // GFlags are process-global; only one instance per process is safe.
  static std::atomic<int> instance_count{0};
  if (instance_count.fetch_add(1, std::memory_order_relaxed) > 0) {
    fprintf(stderr,
            "WARNING: multiple DingoFSConnector instances share global "
            "GFlags; only one instance per process is safe\n");
  }

  // Set GFlags for TierBlockCache
  cache::FLAGS_cache_store = "disk";
  cache::FLAGS_cache_dir = config_.cache_dir;
  cache::FLAGS_cache_size_mb = config_.cache_size_mb;
  cache::FLAGS_cache_group = "";            // local only
  cache::FLAGS_cache_dir_uuid = "lmcache";  // required by BlockCacheImpl

  // Create NoopBlockAccesser
  block_accesser_ = std::make_unique<NoopBlockAccesser>();

  // Create and start TierBlockCache
  auto tier_cache =
      std::make_unique<cache::TierBlockCache>(block_accesser_.get());
  auto status = tier_cache->Start();
  if (!status.ok()) {
    throw std::runtime_error("TierBlockCache start failed: " +
                             status.ToString());
  }
  block_cache_ = std::move(tier_cache);

  // Create ExistsCache
  exists_cache_ =
      std::make_unique<ExistsCache>(config_.exists_cache_capacity);

  // Start worker threads (must be last in constructor)
  start_workers();
}

DingoFSConnector::~DingoFSConnector() {
  // ConnectorBase destructor calls close() which joins workers.
  // We shut down block_cache_ here before members are destroyed.
  if (block_cache_) {
    block_cache_->Shutdown();
  }
}

// ---------------------------------------------------------------------------
// ConnectorBase overrides
// ---------------------------------------------------------------------------

DingoFSConnection DingoFSConnector::create_connection() {
  DingoFSConnection conn;
  conn.block_cache = block_cache_.get();
  conn.exists_cache = exists_cache_.get();
  conn.config = config_;
  return conn;
}

void DingoFSConnector::do_single_set(DingoFSConnection& /*conn*/,
                                     const std::string& key,
                                     const void* buf, size_t len,
                                     size_t /*chunk_size*/) {
  uint16_t num_shards = CalcNumShards(len);
  const char* src = static_cast<const char*>(buf);

  for (uint16_t s = 0; s < num_shards; ++s) {
    auto block_key = MapKey(key, s);
    size_t offset = static_cast<size_t>(s) * kMaxBlockSize;
    size_t shard_len = std::min(kMaxBlockSize, len - offset);

    // Zero-copy: wrap user buffer into IOBuffer
    IOBuffer io_buf;
    io_buf.AppendUserData(
        const_cast<char*>(src + offset), shard_len,
        [](void*) {});  // No-op deleter: caller holds the buffer

    cache::Block block(std::move(io_buf));
    auto status = SyncCache(block_key, block);
    if (!status.ok()) {
      throw std::runtime_error("DingoFS set failed: " +
                               status.ToString());
    }
  }

  exists_cache_->Insert(key);
}

void DingoFSConnector::do_single_get(DingoFSConnection& /*conn*/,
                                     const std::string& key,
                                     void* buf, size_t len,
                                     size_t /*chunk_size*/) {
  uint16_t num_shards = CalcNumShards(len);
  char* dst = static_cast<char*>(buf);

  for (uint16_t s = 0; s < num_shards; ++s) {
    auto block_key = MapKey(key, s);
    size_t offset = static_cast<size_t>(s) * kMaxBlockSize;
    size_t shard_len = std::min(kMaxBlockSize, len - offset);

    IOBuffer io_buf;
    auto status = SyncRange(block_key, 0, shard_len, &io_buf);
    if (!status.ok()) {
      exists_cache_->Remove(key);
      throw std::runtime_error("DingoFS get failed: " +
                               status.ToString());
    }

    io_buf.CopyTo(dst + offset, shard_len);
  }
}

bool DingoFSConnector::do_single_exists(DingoFSConnection& /*conn*/,
                                        const std::string& key) {
  // Fast path: ExistsCache LRU
  if (exists_cache_->Lookup(key)) {
    return true;
  }

  // Slow path: query TierBlockCache (shard 0 only)
  auto block_key = MapKey(key, 0);
  bool found = block_cache_->IsCached(block_key);
  if (found) {
    exists_cache_->Insert(key);
  }
  return found;
}

void DingoFSConnector::shutdown_connections() {
  // block_cache_ shutdown is handled in destructor
}

// ---------------------------------------------------------------------------
// Synchronous wrappers for bthread async APIs
// ---------------------------------------------------------------------------

Status DingoFSConnector::SyncCache(const cache::BlockKey& block_key,
                                   const cache::Block& block) {
  std::promise<Status> promise;
  auto future = promise.get_future();
  auto ctx = cache::NewContext();

  block_cache_->AsyncCache(
      ctx, block_key, block,
      [&promise](Status st) { promise.set_value(std::move(st)); });

  return future.get();
}

Status DingoFSConnector::SyncRange(const cache::BlockKey& block_key,
                                   size_t offset, size_t length,
                                   IOBuffer* io_buf) {
  std::promise<Status> promise;
  auto future = promise.get_future();
  auto ctx = cache::NewContext();

  cache::RangeOption opt;
  opt.retrieve_storage = false;  // Only read from cache layers

  block_cache_->AsyncRange(
      ctx, block_key, static_cast<off_t>(offset), length, io_buf,
      [&promise](Status st) { promise.set_value(std::move(st)); },
      opt);

  return future.get();
}

// ---------------------------------------------------------------------------
// BlockKey mapping (migrated from cache_engine.cpp)
// ---------------------------------------------------------------------------

uint32_t DingoFSConnector::ExtractChunkHash(const std::string& key) const {
  // CacheEngineKey: "model@ws@wid@chunk_hash_hex@dtype" (5 segments)
  // ObjectKey:      "model@kv_rank_hex@chunk_hash_hex"  (3 segments)
  std::vector<std::string_view> parts = absl::StrSplit(key, '@');

  std::string_view hex_str;
  if (parts.size() >= 5) {
    hex_str = parts[3];  // CacheEngineKey format
  } else if (parts.size() >= 3) {
    hex_str = parts[2];  // ObjectKey format
  } else {
    hex_str = key;  // fallback: hash the entire key
  }

  // Parse hex to uint32 (take lower 32 bits)
  // Fall back to std::hash if non-hex characters are detected
  uint64_t val = 0;
  for (char c : hex_str) {
    val <<= 4;
    if (c >= '0' && c <= '9') {
      val |= static_cast<uint64_t>(c - '0');
    } else if (c >= 'a' && c <= 'f') {
      val |= static_cast<uint64_t>(c - 'a' + 10);
    } else if (c >= 'A' && c <= 'F') {
      val |= static_cast<uint64_t>(c - 'A' + 10);
    } else {
      return static_cast<uint32_t>(
          std::hash<std::string>{}(key) & 0xFFFFFFFF);
    }
  }
  return static_cast<uint32_t>(val & 0xFFFFFFFF);
}

cache::BlockKey DingoFSConnector::MapKey(const std::string& key,
                                         uint16_t shard_id) const {
  uint32_t chunk_hash = ExtractChunkHash(key);
  uint64_t index =
      (static_cast<uint64_t>(chunk_hash) << 16) | shard_id;

  return cache::BlockKey(
      config_.fs_id,
      config_.ino,
      std::numeric_limits<uint64_t>::max(),  // UINT64_MAX sentinel
      index,
      0);
}

uint16_t DingoFSConnector::CalcNumShards(size_t data_size) const {
  if (data_size == 0) return 0;
  size_t n = (data_size + kMaxBlockSize - 1) / kMaxBlockSize;
  if (n > std::numeric_limits<uint16_t>::max()) {
    return std::numeric_limits<uint16_t>::max();
  }
  return static_cast<uint16_t>(n);
}

}  // namespace connector
}  // namespace dingofs
