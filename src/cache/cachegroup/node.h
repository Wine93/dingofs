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
 * Created Date: 2025-02-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_NODE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_NODE_H_

#include <functional>
#include <ostream>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/cachegroup/heartbeat.h"
#include "cache/cachegroup/task_tracker.h"
#include "cache/common/context.h"
#include "cache/common/mds_client.h"
#include "cache/common/storage_client.h"
#include "cache/common/storage_client_pool.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

class CacheNode {
 public:
  CacheNode();

  Status Start();
  Status Shutdown();

  Status Put(ContextSPtr ctx, const BlockContext& block_ctx,
             const Block& block);
  Status Range(ContextSPtr ctx, const BlockContext& block_ctx, off_t offset,
               size_t length, IOBuffer* buffer, size_t block_length);
  Status AsyncCache(ContextSPtr ctx, const BlockContext& block_ctx,
                    const Block& block);
  Status AsyncPrefetch(ContextSPtr ctx, const BlockContext& block_ctx,
                       size_t length);

  // RDMA zero-copy overload. Fills [dst, dst + dst_capacity) directly. The
  // caller owns `dst` (typically a slice of an RDMA-registered buffer) and
  // its registration; CacheNode treats it as a write-target. On success
  // *out_length is set to the number of bytes written.
  //
  // `io_uring_buf_index` is the global index of `dst` in io_uring's fixed
  // buffer table (from RDMAMemoryPool::Buffer::io_uring_buf_index). On the
  // cache-hit disk-read path this lets the disk read DMA straight into the
  // RDMA-registered memory — fully zero-copy from disk to RDMA wire. Pass
  // -1 if not registered with io_uring.
  Status Range(ContextSPtr ctx, const BlockContext& block_ctx, off_t offset,
               size_t length, char* dst, size_t dst_capacity,
               size_t* out_length, size_t block_length,
               int io_uring_buf_index = -1);

  // RDMA zero-copy overload. `src` is a contiguous block payload that the
  // RDMA transport has already filled (via RDMA_READ). `on_buffer_released`
  // runs once the underlying IOBuffer holding `src` drops its last reference
  // (i.e. after the async cache write completes); the caller uses it to
  // return the attachment buffer to its pool.
  //
  // `io_uring_buf_index` carries the same meaning as for Range above.
  Status AsyncCache(ContextSPtr ctx, const BlockContext& block_ctx,
                    char* src, size_t length,
                    std::function<void()> on_buffer_released,
                    int io_uring_buf_index = -1);

 private:
  bool IsRunning() const { return running_.load(std::memory_order_relaxed); }

  Status JoinGroup();
  Status LeaveGroup();

  Status RetrieveCache(ContextSPtr ctx, const BlockContext& block_ctx,
                       off_t offset, size_t length, IOBuffer* buffer);
  Status RetrieveStorage(ContextSPtr ctx, const BlockContext& block_ctx,
                         off_t offset, size_t length, IOBuffer* buffer,
                         size_t block_length);
  Status RetrievePartBlock(ContextSPtr ctx, const BlockContext& block_ctx,
                           off_t offset, size_t length, IOBuffer* buffer,
                           size_t block_length);
  Status RetrieveWholeBlock(ContextSPtr ctx, const BlockContext& block_ctx,
                            size_t block_length, IOBuffer* buffer);
  Status RunTask(StorageClient* storage_client, DownloadTaskSPtr task);
  Status WaitTask(DownloadTaskSPtr task);

 private:
  std::atomic<bool> running_;
  MDSClientSPtr mds_client_;
  StorageClientPoolSPtr storage_client_pool_;
  BlockCacheUPtr block_cache_;
  HeartbeatUPtr heartbeat_;
  TaskTrackerUPtr task_tracker_;

  bvar::Adder<int64_t> num_hit_cache_;
  bvar::Adder<int64_t> num_miss_cache_;
};

using CacheNodeSPtr = std::shared_ptr<CacheNode>;

std::ostream& operator<<(std::ostream& os, const CacheNode& node);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_NODE_H_
