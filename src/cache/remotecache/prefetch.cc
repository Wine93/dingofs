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

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdio>
#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/remotecache/prefetcher.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

BlockStatSPtr BlockMap::GetOrCreateBlockStat(const BlockKey& key) {
  uint64_t slice_id = key.id;
  uint64_t block_index = key.index;
  auto& m = blocks_[block_index % kBlockNum];

  {
    bthread::RWLockRdGuard guard(rwlock_);
    auto* stat = m.seek(slice_id);
    if (stat != nullptr) {
      return *stat;
    }
  }

  bthread::RWLockWrGuard guard(rwlock_);
  auto* stat = m.seek(slice_id);
  if (stat != nullptr) {
    return *stat;
  }

  return *m.insert(slice_id, std::make_shared<BlockStat>());
}

void BlockPrefetcher::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;

  // auto rc =
  //     bthread::execution_queue_start(&queue_id_, &options, HandleTask, this);
  // CHECK_EQ(rc, 0);
}

void BlockPrefetcher::Shutdown() {
  // bthread::execution_queue_stop(queue_id_);
  // bthread::execution_queue_join(queue_id_);
}

int BlockPrefetcher::HandleTasks(
    void* meta, bthread::TaskIterator<std::vector<Task>*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<BlockPrefetcher*>(meta);
  for (; iter; iter++) {
    auto* tasks = *iter;
    for (const auto& task : *tasks) {
      self->HandleTask(task);
    }
  }

  return 0;
}

void BlockPrefetcher::HandleTask(const Task& task) {
  auto segment_index = task.segment_index;
  if (task.stat->SegmentExists(segment_index)) {
  } else if (task.stat->SegmentPrefetching(segment_index)) {
  }

  //
}

CacheRetriever::CacheRetriever()
    : block_map_(std::make_unique<SharedBlockMap>()),
      prefetcher_(std::make_unique<BlockPrefetcher>()) {}

// NOTE: must gurantee takes less 50us per 128KB request
Status CacheRetriever::Range(const BlockKey& key, off_t offset, size_t length,
                             size_t total_length, IOBuffer* buffer) {
  CHECK_GT(total_length, 0);

  const auto& stat = block_map_->GetBlockStat(key);

  auto lindex = SegmentIndex(offset);
  auto rindex = SegmentIndex(offset + length);
  auto tindex = SegmentIndex(offset + total_length);

  std::vector<off_t> caching;
  std::vector<BlockPrefetcher::Task*> to_prefetch;
  std::vector<SegmentHandler> handlers;

  for (off_t index = lindex; index <= tindex; ++index) {
    bool exist = stat->SegmentExists(index);
    bool prefetching = stat->SegmentPrefetching(index);
    bool care = (index <= rindex);

    if (exist) {
      if (care) {
        caching.push_back(index);
        VLOG(3) << "Pick cache segment (" << index << ")";
      }
      continue;
    }

    auto* task = new BlockPrefetcher::Task();
    task->key = key;
    task->segment_index = index;
    to_prefetch.emplace_back(task);

    // not exist
  }

  // Submit prefetch task
  prefetcher_->Submit(to_prefetch);

  for (auto& handler : handlers) {
    auto status = handler.Execute();
    if (!status.ok()) {
      status = handler.Retry();
    }
  }

  //

  return Status::OK();
}

Status CacheHandler::Execute() {
  // 从 lrucache 里拿一下
  return Status::OK();
}

Status CacheHandler::Retry() {
  //
  return Status::OK();
}

Status PrefetchHandler::Execute() {
  CHECK_NOTNULL(task->done);

  // 在这里 wait
  // task->done->wait();
  // 一旦这个 prefetch 任务完成，先放进 lrucache，再将 buffer 回调给所有 waiter
  // 防止一旦 buffer 加进去就被淘汰了

  return Status::OK();
}

Status PrefetchHandler::Retry() {
  // 这里再去 retry 下 storage
  storage_->Range(key, )

      return Status::OK();
}

// BlockPrefetcher::BlockPrefetcher() {}

};  // namespace cache
};  // namespace dingofs
