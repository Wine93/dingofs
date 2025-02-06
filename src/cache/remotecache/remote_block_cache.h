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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_

#include <butil/iobuf.h>

#include <memory>

#include "base/hash/con_hash.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "cache/remotecache/remote_node_manager.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using base::hash::ConHash;
using cache::blockcache::BlockKey;
using cache::common::Errno;
using cache::common::RemoteBlockCacheOptions;
using stub::rpcclient::MdsClient;

class RemoteBlockCache {
 public:
  virtual ~RemoteBlockCache() = default;

  virtual bool Init() = 0;

  virtual void Shutdown() = 0;

  virtual Errno Range(const BlockKey& block_key, uint64_t block_size,
                      uint64_t offset, uint64_t length,
                      butil::IOBuf* buffer) = 0;
};

class RemoteBlockCacheImpl : public RemoteBlockCache {
 public:
  explicit RemoteBlockCacheImpl(RemoteBlockCacheOptions options);

  bool Init() override;

  void Shutdown() override;

  Errno Range(const BlockKey& block_key, uint64_t block_size, uint64_t offset,
              uint64_t length, butil::IOBuf* buffer) override;

 private:
  std::atomic<bool> inited_;
  RemoteBlockCacheOptions options_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unique_ptr<RemoteNodeManager> nodes_;
};

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_BLOCK_CACHE_CLIENT_H_
