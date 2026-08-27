/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_BLOCKCACHE_BLOCK_SHARDED_BLOCK_CACHE_H_
#define DINGOFS_BLOCKCACHE_BLOCK_SHARDED_BLOCK_CACHE_H_

#include <glog/logging.h>

#include <string_view>
#include <utility>

#include "blockcache/block/block_cache.h"
#include "blockcache/common/route.h"
#include "blockcache/core/runtime/sharded.h"
#include "blockcache/core/runtime/smp.h"

namespace dingofs {
namespace blockcache {

// The half of a sharded cache facade that does not depend on which cache is
// behind it: route to the owning shard, run there, fold GetStats. What a
// concrete facade still owns is its own dependencies and how they start.
//
// Not a BlockCache: these are driven from the thread that owns main(), so
// Start/Shutdown are Status/void, while BlockCache's are Future<> because its
// implementations are driven on a shard.
template <typename Service>
class ShardedBlockCache {
 public:
  ShardedBlockCache(const ShardedBlockCache&) = delete;
  ShardedBlockCache& operator=(const ShardedBlockCache&) = delete;

  Future<Status> Put(BlockHandle handle, BufferViews block,
                     PutOption option = {}) {
    return InvokeOnOwner(handle, [=](Service& service) {
      return service.Put(handle, block, option);
    });
  }

  Future<Status> Get(BlockHandle handle, uint64_t offset, uint32_t length,
                     char* buffer, GetOption option = {}) {
    return InvokeOnOwner(handle, [=](Service& service) {
      return service.Get(handle, offset, length, buffer, option);
    });
  }

  Future<Status> Prefetch(BlockHandle handle, PrefetchOption option = {}) {
    return InvokeOnOwner(handle, [=](Service& service) {
      return service.Prefetch(handle, option);
    });
  }

  Future<Status> Delete(BlockHandle handle, DeleteOption option = {}) {
    return InvokeOnOwner(handle, [=](Service& service) {
      return service.Delete(handle, option);
    });
  }

  Future<CacheStats> GetStats() {
    return services_.MapReduce(
        CacheStats{}, [](Service& service) { return service.GetStats(); },
        [](CacheStats& sum, CacheStats part) { sum.Merge(part); });
  }

 protected:
  explicit ShardedBlockCache(std::string_view name) : name_(name) {}
  ~ShardedBlockCache() = default;

  template <typename Factory>
  Status StartShards(Factory factory) {
    CHECK(!running_) << name_ << " started twice";
    CHECK_GT(ShardCount(), 0u) << "Runtime must be up first";

    LOG(INFO) << name_ << " is starting...";
    const Status status = services_.StartOnAllShards(std::move(factory));
    if (!status.ok()) {
      return status;
    }

    running_ = true;
    LOG(INFO) << "Successfully start " << name_ << "{shards=" << ShardCount()
              << "}";
    return Status::OK();
  }

  void ShutdownShards() {
    LOG(INFO) << name_ << " is shutting down...";
    services_.ShutdownOnAllShards();
    running_ = false;
    LOG(INFO) << "Successfully shutdown " << name_;
  }

  bool running() const { return running_; }

 private:
  template <typename Fn>
  auto InvokeOnOwner(BlockHandle handle, Fn fn) {
    return services_.InvokeOn(OwnerShard(handle), std::move(fn));
  }

  std::string_view name_;
  bool running_ = false;
  Sharded<Service> services_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_BLOCK_SHARDED_BLOCK_CACHE_H_
