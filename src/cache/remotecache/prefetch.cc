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

#include "cache/remotecache/prefetch.h"

#include <glog/logging.h>

#include <atomic>
#include <cstdio>

#include "cache/blockcache/cache_store.h"
#include "cache/remotecache/prefetcher.h"

namespace dingofs {
namespace cache {

void BlockBitMap::Set(uint64_t segment_index, bool exist) {
  auto& s = status_[segment_index];
  s.compare_exchange_strong(!exist, exist, std::memory_order_relaxed);
}

BlockStatSPtr BlockBitMap::GetOrCreate(const BlockKey& key) {
  uint64_t slice_id = key.id;
  uint64_t block_index = key.index;
  auto& map = blocks_[block_index % kBlockNum];
  //
  {
    bthread::RWLockRdGuard guard(rwlock_);
    auto* block_map = map.seek(slice_id);
    if (block_map != nullptr) {
      return *block_map;
    }
  }

  {
    bthread::RWLockWrGuard guard(rwlock_);
    auto* block_map = map.seek(slice_id);
    if (block_map != nullptr) {
      return *block_map;
    }

    BlockStatSPtr stat = std::make_shared<BlockStat>();
    map.insert(slice_id, stat);

    return stat;
  }
}

Status BlockPrefetcher::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;

  CHECK_EQ(0, bthread::execution_queue_start(&queue_id_, &options, HandleTask,
                                             this));

  return Status::OK();
}

int BlockPrefetcher::HandleTask(void* meta,
                                bthread::TaskIterator<PrefetchBatcher*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<BlockPrefetcher*>(meta);
  for (; iter; iter++) {
    auto* task = *iter;
    //
  }

  return 0;
}

Status CacheRetriever::Range(const BlockKey& key, off_t offset, size_t length,
                             size_t total_length, IOBuffer* buffer) {
  CHECK_GT(total_length, 0);

  //
  uint64_t slice_id = key.id;
  uint64_t block_index = key.index;

  const auto& bmap = GetBlockBitMap(key);

  butil::FlatMap<uint32_t, bool> hash;
  bmap->ToHash(&hash);

  size_t size = hash.size();
  for (int i = 0; i < size; ++i) {
    //
  }

  std::vector<off_t> caching;
  std::vector<off_t> batch_prefetch;

  auto lindex = SegmentIndex(offset);
  auto rindex = SegmentIndex(offset + length);
  auto tindex = SegmentIndex(offset + total_length);
  for (off_t index = lindex; index <= tindex; ++index) {
    // 如果不存在，就触发预取

    // 如果是存在的，就加入缓存列表

    if (!bmap->Contain(index)) {
      TriggerPrefetch(key, offset, length, total_length);
    } else {
    }
    //
  }

  // auto

  //

  return Status::OK();
}

Status CacheRetriever::RetrieveMemory() {
  //
}

Status CacheRetriever::RetrieveRemote() {
  //
}

Status CacheRetriever::TriggerPrefetch(const BlockKey& key, off_t offset,
                                       size_t length, size_t total_length) {
  //
}

// BlockPrefetcher::BlockPrefetcher() {}

};  // namespace cache
};  // namespace dingofs
