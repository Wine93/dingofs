// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <sys/eventfd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {

enum class Op : uint8_t { GET, SET, EXISTS };

struct BatchState {
  std::atomic<uint32_t> remaining{0};
  std::atomic<bool> any_failed{false};

  std::mutex err_mu;
  std::string first_error;

  std::vector<uint8_t> exists_results;
  Op batch_op;
  std::vector<std::string> cache_keys;  // Keys to cache on success
};

struct Completion {
  uint64_t future_id = 0;
  bool ok = true;
  std::vector<uint8_t> result_bytes;
  std::string error;
};

struct CacheEngineConfig {
  uint32_t fs_id = 1;
  uint64_t ino = 1;
  std::string cache_dir;
  uint32_t cache_size_mb = 102400;
  size_t exists_cache_capacity = 100000;
};

// High-performance cache engine backed by TierBlockCache.
// Uses bthread async callbacks instead of a worker thread pool.
class CacheEngine {
 public:
  explicit CacheEngine(const CacheEngineConfig& config);
  ~CacheEngine();

  CacheEngine(const CacheEngine&) = delete;
  CacheEngine& operator=(const CacheEngine&) = delete;

  Status Init();

  int EventFd() const { return efd_; }

  uint64_t SubmitBatchSet(const std::vector<std::string>& keys,
                          const std::vector<void*>& bufs,
                          const std::vector<size_t>& lens,
                          size_t chunk_size);

  uint64_t SubmitBatchGet(const std::vector<std::string>& keys,
                          const std::vector<void*>& bufs,
                          const std::vector<size_t>& lens,
                          size_t chunk_size);

  uint64_t SubmitBatchExists(const std::vector<std::string>& keys);

  std::vector<Completion> DrainCompletions();

  void Close();

 private:
  cache::BlockKey MapKey(const std::string& key,
                         uint16_t shard_id = 0) const;

  uint32_t ExtractChunkHash(const std::string& key) const;

  uint16_t CalcNumShards(size_t data_size) const;

  void SignalEventfd();
  void DrainEventfd();

  void PushCompletion(Completion&& c);

  void HandleShardCompletion(uint64_t future_id,
                             std::shared_ptr<BatchState> batch,
                             Status status);

  CacheEngineConfig config_;
  int efd_ = -1;
  std::atomic<bool> closed_{false};
  std::atomic<uint64_t> next_future_id_{1};
  std::atomic<bool> signaled_{false};

  std::mutex comp_mu_;
  std::queue<Completion> completions_;

  std::unique_ptr<cache::BlockCache> block_cache_;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_;

  // ExistsCache: thread-safe LRU
  class ExistsCacheImpl;
  std::unique_ptr<ExistsCacheImpl> exists_cache_;

  static constexpr size_t kMaxBlockSize = 4 * 1024 * 1024;
};

}  // namespace dingofs
