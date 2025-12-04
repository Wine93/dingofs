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

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/lru_cache.h"
#include "cache/remotecache/remote_cache_node.h"
#include "cache/storage/closure.h"
#include "cache/storage/storage.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {

namespace cache {

const constexpr size_t kSegmentSize = 128 * 1024;  // 128KB

class LRUCache {
 public:
};

using LRUCacheUPtr = std::unique_ptr<LRUCache>;
using LRUCacheSPtr = std::shared_ptr<LRUCache>;

// block will be sliced into multiple segments, each segment is 128KB
class BlockStat {
 public:
  BlockStat() = default;

  bool SegmentPrefetching(int index) {
    return st_[index % kSegmentNum].load(std::memory_order_relaxed) == 1;
  }

  bool SegmentExists(int index) {
    return st_[index % kSegmentNum].load(std::memory_order_relaxed) == 2;
  }

  void SetSegmentPrefetching(int index) {
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
  struct Task {
    BlockKey key;
    off_t offset;
    size_t length;
    int segment_index;
    BlockStatSPtr stat;
    Closure* done;
  };

  BlockPrefetcher();

  void Start();
  void Shutdown();

  void Submit(const std::vector<Task>& tasks);

 private:
  static int HandleTasks(void* meta,
                         bthread::TaskIterator<std::vector<Task>*>& iter);
  void HandleTask(const Task& task);

  LRUCacheSPtr cache_;
  RemoteCacheNodeSPtr remote_node_;
  bthread::ExecutionQueue<std::vector<Task>*> queue_id_;
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

  BlockMapUPtr block_map_;
  LRUCacheUPtr cache_;
  BlockPrefetcherUPtr prefetcher_;
  StorageSPtr storage_;
};

class SegmentHandler {
 public:
  virtual ~SegmentHandler() = default;

  virtual Status Execute() = 0;
  virtual Status Retry() = 0;
};

class CacheHandler : public SegmentHandler {
 public:
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

class PrefetchHandler : public SegmentHandler {
 public:
  Status Execute() override;
  Status Retry() override;

 private:
  off_t offset_;
  size_t length_;
  IOBuffer* buffer_;
  BlockPrefetcher::Task* task;
  StorageUPtr storage_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCH_H_
