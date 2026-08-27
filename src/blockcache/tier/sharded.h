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

#ifndef DINGOFS_BLOCKCACHE_TIER_SHARDED_H_
#define DINGOFS_BLOCKCACHE_TIER_SHARDED_H_

#include <memory>

#include "blockcache/block/sharded_block_cache.h"
#include "blockcache/common/mds_client.h"
#include "blockcache/object/object.h"
#include "blockcache/tier/tier_cache.h"

namespace dingofs {
namespace blockcache {

class ShardedTierCache final : public ShardedBlockCache<TierCache> {
 public:
  ShardedTierCache(MDSClient* mds_client, ObjectStorageUPtr storage);
  ~ShardedTierCache();

  ShardedTierCache(const ShardedTierCache&) = delete;
  ShardedTierCache& operator=(const ShardedTierCache&) = delete;

  Status Start();
  void Shutdown();

 private:
  MDSClient* mds_client_;
  ObjectStorageUPtr storage_;
};

using ShardedTierCacheUPtr = std::unique_ptr<ShardedTierCache>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_TIER_SHARDED_H_
