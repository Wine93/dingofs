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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_H_

#include <brpc/channel.h>
#include <butil/iobuf.h>

#include "cache/blockcache/block_cache.h"
#include "cache/common/errno.h"
#include "dingofs/cachegroup.pb.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using cache::blockcache::BlockKey;
using cache::common::Errno;

class RemoteNode {
 public:
  virtual ~RemoteNode() = default;

  virtual bool Init() = 0;

  virtual Errno Range(const BlockKey& block_key, uint64_t block_size,
                      uint64_t offset, uint64_t length,
                      butil::IOBuf* buffer) = 0;
};

class RemoteNodeImpl : public RemoteNode {
 public:
  explicit RemoteNodeImpl(const pb::mds::cachegroup::CacheGroupMember& member);

  bool Init() override;

  Errno Range(const BlockKey& block_key, uint64_t block_size, uint64_t offset,
              uint64_t length, butil::IOBuf* buffer) override;

 private:
  pb::mds::cachegroup::CacheGroupMember member_;
  std::unique_ptr<brpc::Channel> channel_;
};

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_NODE_H_
