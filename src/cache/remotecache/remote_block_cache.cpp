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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_block_cache.h"

#include <butil/iobuf.h>

namespace dingofs {
namespace cache {
namespace remotecache {

RemoteBlockCacheImpl::RemoteBlockCacheImpl(RemoteBlockCacheOptions options)
    : inited_(false), options_(options), mds_client_(options_.mds_client) {}

bool RemoteBlockCacheImpl::Init() { return nodes_->Start(); }

void RemoteBlockCacheImpl::Shutdown() {}

Errno RemoteBlockCacheImpl::Range(const BlockKey& block_key,
                                  uint64_t block_size, uint64_t offset,
                                  uint64_t length, butil::IOBuf* buffer) {
  return nodes_->Get(block_key.Filename())
      ->Range(block_key, block_size, offset, length, buffer);
}

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs
