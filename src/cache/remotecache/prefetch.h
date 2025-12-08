/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-11-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCH_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCH_H_

#include <bthread/condition_variable.h>
#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <bthread/mutex.h>
#include <bthread/rwlock.h>
#include <butil/containers/flat_map.h>
#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <mutex>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/lru_cache.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/storage/closure.h"
#include "cache/storage/storage.h"
#include "cache/utils/bthread.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {

namespace cache {

const constexpr size_t kSegmentSize = 128 * 1024;  // 128KB

class SegmentLRUCache {
 public:
  // Cache() = default;

  // Cache(const Cache&) = delete;
  // Cache& operator=(const Cache&) = delete;

  //// Destroys all existing entries by calling the "deleter"
  //// function that was passed to the constructor.
  // virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle {};
  class HandleTable;

  virtual Handle* Insert(const std::string_view& key, void* value,
                         size_t charge,
                         void (*deleter)(const std::string_view& key,
                                         void* value));

  virtual Handle* Lookup(const std::string_view& key);

  virtual void Release(Handle* handle);

  virtual std::string_view Key(Handle* handle);

  virtual void* Value(Handle* handle);

  virtual void Erase(const std::string_view& key);

  virtual uint64_t NewId();

  virtual void Prune() {}

  virtual size_t TotalCharge() const;

 private:
  void LRURemove(Handle* e);
  void LRUAppend(Handle* list, Handle* e);

  void Ref(Handle* e);
  void Unref(Handle* e);

  bool FinishErase(Handle* e);

  mutable bthread::Mutex mutex_;
  size_t capacity_;
  size_t usage_;
  Handle lru_;
  Handle in_use_;
  HandleTable table_;
};

class SharedSegmentCache : public SegmentCache {
 public:
  explicit SharedSegmentCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (auto& s : shard_) {
      s.SetCapacity(per_shard);
    }
  }

  ~SharedSegmentCache() = default;

  Handle* Insert(const std::string_view& key, void* value, size_t charge,
                 void (*deleter)(const std::string_view& key,
                                 void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }

  Handle* Lookup(const std::string_view& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }

  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }

  void Erase(const std::string_view& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }

  std::string_view Key(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->key();
  }

  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }

  uint64_t NewId() override {
    std::lock_guard<BthreadMutex> l(id_mutex_);
    return ++(last_id_);
  }

  void Prune() override {
    for (auto& s : shard_) {
      s.Prune();
    }
  }

  size_t TotalCharge() const override {
    size_t total = 0;
    for (const auto& s : shard_) {
      total += s.TotalCharge();
    }
    return total;
  }

 private:
  static inline uint32_t HashSlice(const std::string_view& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

  LRUCache shard_[kNumShards];
  BthreadMutex id_mutex_;
  uint64_t last_id_;
};

using LRUCacheUPtr = std::unique_ptr<LRUCache>;
using LRUCacheSPtr = std::shared_ptr<LRUCache>;

// block will be sliced into multiple segments, each segment is 128KB
class BlockStat {
 public:
  enum kStatus : uint8_t {
    kNotExist = 0,
    kPrefetching = 1,
    kExist = 2,
  };

  struct SegmentStatus {
    SegmentStatus(int st) : st(st) {}

    bool IsPrfetching() const { return st == 1; }
    bool IsExist() const { return st == 2; }

    uint8_t st;
  };

  BlockStat() = default;

  SegmentStatus GetSegmentStatus(int index) {
    auto st = st_[index % kSegmentNum].load(std::memory_order_relaxed);
    return SegmentStatus(st);
  }

  bool SegmentPrefetching(int index) {
    return st_[index % kSegmentNum].load(std::memory_order_relaxed) == 1;
  }

  bool SegmentExists(int index) { return == 2; }

  void SetSegmentPrefetching(int index) {
    auto old_st = st_[index % kSegmentNum].compare_exchange_strong(
        kNotExist, kPrefetching, std::memory_order_relaxed,
        std::memory_order_relaxed);
    // TODO
  }

  void SetSegmentExist(int index) {
    // TODO
  }

  void RemoveSegment() {
    //
  }

 private:
  static constexpr size_t kSegmentNum = 32;  // 4MB/128KB

  int GetStatus(int index) {
    return st_[index % kSegmentNum].load(std::memory_order_relaxed);
  }

  // status:
  //   0: not exist
  //   1: prefecting
  //   2: cached
  std::atomic<uint8_t> st_[kSegmentNum]{};
};

using BlockStatSPtr = std::shared_ptr<BlockStat>;

class BlockMap {
 public:
  using BlockStatSPtr = std::shared_ptr<BlockStat>;

  BlockMap() = default;
  virtual ~BlockMap() = default;

  virtual BlockStatSPtr GetBlockStat(const BlockKey& key) {
    return GetOrCreateBlockStat(key);
  }

 private:
  static constexpr size_t kBlockNum = 16;  // 64MB/4MB

  BlockStatSPtr GetOrCreateBlockStat(const BlockKey& key);

  bthread::RWLock rwlock_;
  butil::FlatMap<uint64_t, BlockStatSPtr> blocks_[kBlockNum];  // key: slice_id
};

using BlockMapUPtr = std::unique_ptr<BlockMap>;

class SharedBlockMap : public BlockMap {
 public:
  SharedBlockMap() = default;

  BlockStatSPtr GetBlockStat(const BlockKey& key) override {
    return shard_[key.id % kShardNum].GetBlockStat(key);
  }

 private:
  static constexpr size_t kShardNum = 16;

  BlockMap shard_[kShardNum];  // hash by slice id
};

class BlockPrefetcher {
 public:
  class WaitingLobby;
  class OnSegmentFetched;

  struct Task {
    BlockKey key;
    int segment_index;
    size_t total_length;
    BlockStatSPtr stat;
    OnSegmentFetched* closure;
  };

  struct Waiter {
    BlockKey key;
    int segment_index;
    OnSegmentFetched* closure;
  };

  BlockPrefetcher();

  void Start();
  void Shutdown();

  void SubmitTasks(const std::vector<Task>& tasks);
  void AddWaiters(const std::vector<Waiter>& waiters);

 private:
  static int HandleTasks(void* meta,
                         bthread::TaskIterator<std::vector<Task>*>& iter);

  void DoPrefetch(const Task& task);

  bthread::RWLock rwlock_;
  LRUCacheSPtr cache_;
  RemoteCacheNodeSPtr remote_node_;
  BthreadJoinerUPtr joiner_;
  bthread::ExecutionQueue<std::vector<Task>*> task_queue_;
  WaitingLobbyUPtr waiting_lobby_;
};

using BlockPrefetcherUPtr = std::unique_ptr<BlockPrefetcher>;

// It will trigger prefetch
class CacheRetriever {
 public:
  CacheRetriever();

  // 这里需要做一个 batch 操作，对同一个 key 的做合并
  Status Range(const BlockKey& key, off_t offset, size_t length,
               size_t total_length, IOBuffer* buffer);

 private:
  off_t SegmentIndex(off_t offset) { return offset / kSegmentSize; }
  off_t SegmentOffset(off_t offset) {}
  size_t SegmentLength(off_t offset, size_t length) {
    // return std::min() - std::max();

    // return std::min(length, kSegmentSize - SegmentOffset(offset));
  }

  BlockPrefetcher::OnSegmentFetched NewSegmentClosure(segment_index, buffer);

  BlockMapUPtr block_map_;
  LRUCacheUPtr cache_;
  BlockPrefetcherUPtr prefetcher_;
  StorageSPtr storage_;
};

class SegmentHandler : public Closure {
 public:
  virtual ~SegmentHandler() = default;

  virtual Status Execute() = 0;
  virtual Status Retry() = 0;
};

class CacheHandler : public SegmentHandler {
 public:
  CacheHandler();
  CacheHandler(LRUCache* cache);

  Status Execute() override;
  Status Retry() override;

 private:
  off_t offset_;
  size_t length_;
  IOBuffer* buffer_;
  LRUCache* cache_;
  StorageUPtr storage_;
};

class WaitHandler : public SegmentHandler {
 public:
  explicit WaitHandler(Closure* closure);

  Status Execute() override;
  Status Retry() override;

 private:
  off_t offset_;
  size_t length_;
  IOBuffer* out_;
  Closure* done_;
  BlockPrefetcher::Task* task;
  StorageUPtr storage_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCH_H_
