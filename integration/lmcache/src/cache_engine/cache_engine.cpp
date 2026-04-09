// SPDX-License-Identifier: Apache-2.0

#include "cache_engine.h"

#include <absl/strings/str_split.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <stdexcept>

#include "cache/common/context.h"
#include "cache/tiercache/tier_block_cache.h"
#include "common/options/cache.h"
#include "noop_block_accesser.h"

namespace dingofs {

// ---------------------------------------------------------------------------
// ExistsCache LRU
// ---------------------------------------------------------------------------

class CacheEngine::ExistsCacheImpl {
 public:
  explicit ExistsCacheImpl(size_t capacity) : capacity_(capacity) {}

  bool Lookup(const std::string& key) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    list_.splice(list_.begin(), list_, it->second);
    return true;
  }

  void Insert(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      list_.splice(list_.begin(), list_, it->second);
      return;
    }
    if (map_.size() >= capacity_) {
      map_.erase(list_.back());
      list_.pop_back();
    }
    list_.push_front(key);
    map_[key] = list_.begin();
  }

  void Remove(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      list_.erase(it->second);
      map_.erase(it);
    }
  }

 private:
  size_t capacity_;
  mutable std::mutex mu_;
  mutable std::list<std::string> list_;
  std::unordered_map<std::string, std::list<std::string>::iterator> map_;
};

// ---------------------------------------------------------------------------
// CacheEngine
// ---------------------------------------------------------------------------

CacheEngine::CacheEngine(const CacheEngineConfig& config)
    : config_(config),
      exists_cache_(
          std::make_unique<ExistsCacheImpl>(config.exists_cache_capacity)) {}

CacheEngine::~CacheEngine() { Close(); }

Status CacheEngine::Init() {
  // Set GFlags for TierBlockCache
  FLAGS_cache_store = "disk";
  FLAGS_cache_dir = config_.cache_dir;
  FLAGS_cache_size_mb = config_.cache_size_mb;
  FLAGS_cache_group = "";  // Phase 1: local only

  // Create NoopBlockAccesser (never actually called in KVCache path)
  block_accesser_ = std::make_unique<NoopBlockAccesser>();

  // Create and start TierBlockCache
  auto tier_cache =
      std::make_unique<cache::TierBlockCache>(block_accesser_.get());
  auto status = tier_cache->Start();
  if (!status.ok()) return status;
  block_cache_ = std::move(tier_cache);

  // Create eventfd for async notification
  efd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (efd_ < 0) {
    return Status::Internal("failed to create eventfd");
  }

  return Status::OK();
}

// ---------------------------------------------------------------------------
// BlockKey mapping
// ---------------------------------------------------------------------------

uint32_t CacheEngine::ExtractChunkHash(const std::string& key) const {
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
  uint64_t val = 0;
  for (char c : hex_str) {
    val <<= 4;
    if (c >= '0' && c <= '9') {
      val |= static_cast<uint64_t>(c - '0');
    } else if (c >= 'a' && c <= 'f') {
      val |= static_cast<uint64_t>(c - 'a' + 10);
    } else if (c >= 'A' && c <= 'F') {
      val |= static_cast<uint64_t>(c - 'A' + 10);
    }
  }
  return static_cast<uint32_t>(val & 0xFFFFFFFF);
}

cache::BlockKey CacheEngine::MapKey(const std::string& key,
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

uint16_t CacheEngine::CalcNumShards(size_t data_size) const {
  if (data_size == 0) return 1;
  return static_cast<uint16_t>(
      (data_size + kMaxBlockSize - 1) / kMaxBlockSize);
}

// ---------------------------------------------------------------------------
// Submit operations
// ---------------------------------------------------------------------------

uint64_t CacheEngine::SubmitBatchSet(
    const std::vector<std::string>& keys,
    const std::vector<void*>& bufs,
    const std::vector<size_t>& lens,
    size_t /*chunk_size*/) {
  uint64_t fid = next_future_id_.fetch_add(1, std::memory_order_relaxed);

  // Count total shards across all keys
  uint32_t total_shards = 0;
  for (size_t i = 0; i < keys.size(); ++i) {
    total_shards += CalcNumShards(lens[i]);
  }

  auto batch = std::make_shared<BatchState>();
  batch->remaining.store(total_shards, std::memory_order_relaxed);
  batch->batch_op = Op::SET;

  for (size_t i = 0; i < keys.size(); ++i) {
    uint16_t ns = CalcNumShards(lens[i]);
    for (uint16_t s = 0; s < ns; ++s) {
      auto block_key = MapKey(keys[i], s);
      size_t offset = static_cast<size_t>(s) * kMaxBlockSize;
      size_t shard_len = std::min(kMaxBlockSize, lens[i] - offset);

      // Zero-copy: wrap user buffer into IOBuffer
      IOBuffer io_buf;
      io_buf.AppendUserData(
          static_cast<char*>(bufs[i]) + offset, shard_len,
          [](void*) {});  // No-op deleter: Python holds the buffer

      cache::Block block(std::move(io_buf));
      auto ctx = cache::NewContext();

      block_cache_->AsyncCache(
          ctx, block_key, block,
          [this, fid, batch](Status st) {
            HandleShardCompletion(fid, batch, st);
          });
    }
    exists_cache_->Insert(keys[i]);
  }

  return fid;
}

uint64_t CacheEngine::SubmitBatchGet(
    const std::vector<std::string>& keys,
    const std::vector<void*>& bufs,
    const std::vector<size_t>& lens,
    size_t /*chunk_size*/) {
  uint64_t fid = next_future_id_.fetch_add(1, std::memory_order_relaxed);

  uint32_t total_shards = 0;
  for (size_t i = 0; i < keys.size(); ++i) {
    total_shards += CalcNumShards(lens[i]);
  }

  auto batch = std::make_shared<BatchState>();
  batch->remaining.store(total_shards, std::memory_order_relaxed);
  batch->batch_op = Op::GET;

  for (size_t i = 0; i < keys.size(); ++i) {
    uint16_t ns = CalcNumShards(lens[i]);
    for (uint16_t s = 0; s < ns; ++s) {
      auto block_key = MapKey(keys[i], s);
      size_t offset = static_cast<size_t>(s) * kMaxBlockSize;
      size_t shard_len = std::min(kMaxBlockSize, lens[i] - offset);

      auto io_buf = std::make_shared<IOBuffer>();
      char* dst = static_cast<char*>(bufs[i]) + offset;
      auto ctx = cache::NewContext();

      cache::RangeOption opt;
      opt.retrieve_storage = false;  // Only read from cache layers

      block_cache_->AsyncRange(
          ctx, block_key, 0, shard_len, io_buf.get(),
          [this, fid, batch, io_buf, dst, shard_len,
           key_ref = keys[i]](Status st) {
            if (st.ok()) {
              io_buf->CopyTo(dst, shard_len);
            } else {
              exists_cache_->Remove(key_ref);
            }
            HandleShardCompletion(fid, batch, st);
          },
          opt);
    }
  }

  return fid;
}

uint64_t CacheEngine::SubmitBatchExists(
    const std::vector<std::string>& keys) {
  uint64_t fid = next_future_id_.fetch_add(1, std::memory_order_relaxed);

  Completion comp;
  comp.future_id = fid;
  comp.ok = true;
  comp.result_bytes.resize(keys.size());

  for (size_t i = 0; i < keys.size(); ++i) {
    bool found = false;

    // Fast path: ExistsCache LRU
    if (exists_cache_->Lookup(keys[i])) {
      found = true;
    } else {
      // Slow path: query TierBlockCache (shard 0 only)
      auto block_key = MapKey(keys[i], 0);
      found = block_cache_->IsCached(block_key);
      if (found) {
        exists_cache_->Insert(keys[i]);
      }
    }

    comp.result_bytes[i] = found ? 1 : 0;
  }

  PushCompletion(std::move(comp));
  return fid;
}

// ---------------------------------------------------------------------------
// Completion handling
// ---------------------------------------------------------------------------

void CacheEngine::HandleShardCompletion(
    uint64_t future_id,
    std::shared_ptr<BatchState> batch,
    Status status) {
  if (!status.ok()) {
    batch->any_failed.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lk(batch->err_mu);
    if (batch->first_error.empty()) {
      batch->first_error = status.ToString();
    }
  }

  uint32_t left =
      batch->remaining.fetch_sub(1, std::memory_order_acq_rel) - 1;

  if (left == 0) {
    Completion comp;
    comp.future_id = future_id;
    comp.ok = !batch->any_failed.load(std::memory_order_acquire);
    if (!comp.ok) {
      std::lock_guard<std::mutex> lk(batch->err_mu);
      comp.error = batch->first_error;
    }
    PushCompletion(std::move(comp));
  }
}

std::vector<Completion> CacheEngine::DrainCompletions() {
  DrainEventfd();

  std::vector<Completion> result;
  for (;;) {
    Completion c;
    {
      std::lock_guard<std::mutex> lk(comp_mu_);
      if (completions_.empty()) {
        signaled_.store(false, std::memory_order_release);
        if (!completions_.empty() &&
            !signaled_.exchange(true, std::memory_order_acq_rel)) {
          uint64_t x = 1;
          ::write(efd_, &x, sizeof(x));
        }
        break;
      }
      c = std::move(completions_.front());
      completions_.pop();
    }
    result.push_back(std::move(c));
  }

  return result;
}

void CacheEngine::PushCompletion(Completion&& c) {
  {
    std::lock_guard<std::mutex> lk(comp_mu_);
    completions_.push(std::move(c));
  }
  SignalEventfd();
}

// ---------------------------------------------------------------------------
// Eventfd helpers
// ---------------------------------------------------------------------------

void CacheEngine::SignalEventfd() {
  if (signaled_.exchange(true, std::memory_order_acq_rel)) return;

  uint64_t x = 1;
  for (;;) {
    ssize_t w = ::write(efd_, &x, sizeof(x));
    if (w == static_cast<ssize_t>(sizeof(x))) return;
    if (w < 0) {
      if (errno == EINTR) continue;
      break;
    }
    break;
  }
}

void CacheEngine::DrainEventfd() {
  for (;;) {
    uint64_t x;
    ssize_t r = ::read(efd_, &x, sizeof(x));
    if (r == static_cast<ssize_t>(sizeof(x))) continue;
    if (r < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN) break;
    }
    break;
  }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void CacheEngine::Close() {
  if (closed_.exchange(true, std::memory_order_acq_rel)) return;

  if (block_cache_) {
    block_cache_->Shutdown();
  }

  if (efd_ >= 0) {
    ::close(efd_);
    efd_ = -1;
  }
}

}  // namespace dingofs
