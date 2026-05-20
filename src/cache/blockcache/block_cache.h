/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_

#include <brpc/server.h>
#include <json/value.h>

#include <ostream>

#include "cache/blockcache/cache_store.h"
#include "common/block/block_handle.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct PutOption {
  PutOption()
      : writeback(false),
        block_attr(BlockAttr::kFromUnknown),
        source_buffer_prepared(false),
        source_buffer(nullptr),
        source_buffer_capacity(0),
        source_buffer_index(-1) {}

  bool writeback;
  BlockAttr block_attr;
  bool source_buffer_prepared;
  const char* source_buffer;
  size_t source_buffer_capacity;
  int source_buffer_index;
};

struct RangeOption {
  bool retrieve_storage{true};
  size_t block_whole_length{0};
  bool is_subrequest{false};

  // If true, the caller has pre-attached a contiguous destination buffer to
  // the IOBuffer* (via IOBuffer::AppendUserData). Implementations read into
  // that storage directly instead of allocating a fresh one — the RDMA
  // service uses this to deliver disk reads straight into RDMA-registered
  // memory. Default false: implementation owns the buffer allocation.
  bool buffer_prepared{false};
  char* prepared_buffer{nullptr};
  size_t prepared_buffer_capacity{0};
  int prepared_buffer_index{-1};
};

struct CacheOption {
  CacheOption()
      : source_buffer_prepared(false),
        source_buffer(nullptr),
        source_buffer_capacity(0),
        source_buffer_index(-1) {}

  bool source_buffer_prepared;
  const char* source_buffer;
  size_t source_buffer_capacity;
  int source_buffer_index;
};
struct PrefetchOption {};

// async callback
using AsyncCallback = std::function<void(Status)>;

class BlockCache {
 public:
  virtual ~BlockCache() = default;

  virtual Status Start() { return Status::OK(); }
  virtual Status Shutdown() { return Status::OK(); }

  // block operations (sync)
  virtual Status Put(BlockHandle /*handle*/, IOBuffer /*block*/,
                     PutOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Range(BlockHandle /*handle*/, off_t /*offset*/,
                       size_t /*length*/, IOBuffer* /*buffer*/,
                       RangeOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Cache(BlockHandle /*handle*/, IOBuffer /*block*/,
                       CacheOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  virtual Status Prefetch(BlockHandle /*handle*/, size_t /*length*/,
                          PrefetchOption /*option*/ = {}) {
    return Status::NotSupport("not implemented");
  }

  // block operations (async)
  virtual void AsyncPut(BlockHandle /*handle*/, IOBuffer /*block*/,
                        AsyncCallback cb, PutOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncRange(BlockHandle /*handle*/, off_t /*offset*/,
                          size_t /*length*/, IOBuffer* /*buffer*/,
                          AsyncCallback cb, RangeOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncCache(BlockHandle /*handle*/, IOBuffer /*block*/,
                          AsyncCallback cb, CacheOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  virtual void AsyncPrefetch(BlockHandle /*handle*/, size_t /*length*/,
                             AsyncCallback cb, PrefetchOption /*option*/ = {}) {
    if (cb) {
      cb(Status::NotSupport("not implemented"));
    }
  }

  // utility
  virtual bool IsEnabled() const { return false; }
  virtual bool EnableStage() const { return false; }
  virtual bool EnableCache() const { return false; }
  virtual bool IsCached(const BlockHandle& /*handle*/) const { return false; }
  virtual bool Dump(Json::Value& /*value*/) const { return true; }
};

using BlockCachePtr = BlockCache*;
using BlockCacheSPtr = std::shared_ptr<BlockCache>;
using BlockCacheUPtr = std::unique_ptr<BlockCache>;

inline std::ostream& operator<<(std::ostream& os,
                                const BlockCache& block_cache) {
  os << "BlockCache{enable=" << block_cache.IsEnabled()
     << " stage=" << static_cast<int>(block_cache.EnableStage())
     << " cache=" << static_cast<int>(block_cache.EnableCache()) << "}";
  return os;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_BLOCK_CACHE_H_
