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

#include "blockcache/tier/sharded.h"

#include <utility>

namespace dingofs {
namespace blockcache {

ShardedTierCache::ShardedTierCache(MDSClient* mds_client,
                                   ObjectStorageUPtr storage)
    : ShardedBlockCache("ShardedTierCache"),
      mds_client_(mds_client),
      storage_(std::move(storage)) {}

ShardedTierCache::~ShardedTierCache() { Shutdown(); }

Status ShardedTierCache::Start() {
  return StartShards(
      [this](unsigned) { return new TierCache(storage_.get(), mds_client_); });
}

void ShardedTierCache::Shutdown() {
  if (!running()) {
    return;
  }
  ShutdownShards();
}

}  // namespace blockcache
}  // namespace dingofs
