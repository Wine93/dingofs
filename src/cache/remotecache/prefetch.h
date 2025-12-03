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

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/lru_cache.h"
#include "cache/storage/closure.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {

namespace cache {

const constexpr size_t kSegmentSize = 128 * 1024;  // 128KB

class LRUCache {
 public:
};

using LRUCacheUPtr = std::unique_ptr<LRUCache>;

// block will be sliced into multiple segments, each segment is 128KB
class BlockStat {
 public:
  BlockStat() = default;

  bool Contain(int segment_index);

 private:
  static constexpr size_t kSegmentNum = 32;  // 4MB/128KB

  std::atomic<bool> st_[kSegmentNum];  // status: true iff exist
};

using BlockStatSPtr = std::shared_ptr<BlockStat>;

class BlockMap {
 public:
  using BlockStatSPtr = std::shared_ptr<BlockStat>;

  BlockMap() = default;
  virtual ~BlockMap() = default;

  virtual BlockStatSPtr GetBlockStat(const BlockKey& key);

 private:
  static constexpr size_t kBlockNum = 16;  // 64MB/4MB

  BlockStatSPtr GetOrCreate();

  bthread::RWLock rwlock_;
  butil::FlatMap<uint64_t, BlockStat> blocks_[kBlockNum];  // key: slice_id
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

class PrefetchClosure : public Closure {
 public:
  //
};

class PrefetchBatcher {
 public:
  void Add(Closure*);

 private:
};

class BlockPrefetcher {
 public:
  BlockPrefetcher();

  Status Start();

  void Submit(const BlockKey& key, off_t offset);

 private:
  static int HandleTask(void* meta,
                        bthread::TaskIterator<PrefetchBatcher*>& iter);

  bthread::ExecutionQueue<PrefetchBatcher*> queue_id_;
};

using BlockPrefetcherUPtr = std::unique_ptr<BlockPrefetcher>;

// It will trigger prefetch
class CacheRetriever {
 public:
  Status Range(const BlockKey& key, off_t offset, size_t length,
               size_t total_length, IOBuffer* buffer);

 private:
  off_t SegmentIndex(off_t offset) { return offset / kSegmentSize; }

  Status RetrieveMemory();
  Status RetrieveRemote();

  BlockPrefetcherUPtr prefetcher_;
  LRUCacheUPtr cache_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_PREFETCH_H_
