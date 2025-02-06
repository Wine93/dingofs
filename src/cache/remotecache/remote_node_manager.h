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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_CACHE_GROUP_MEMBER_MANAGER_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_CACHE_GROUP_MEMBER_MANAGER_H_

#include <cstdint>
#include <memory>

#include "base/hash/con_hash.h"
#include "cache/remotecache/remote_block_cache.h"
#include "cache/remotecache/remote_node.h"
#include "dingofs/cachegroup.pb.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using base::hash::ConHash;
using base::timer::TimerImpl;
using pb::mds::cachegroup::CacheGroupMember;
using stub::rpcclient::MdsClient;
using utils::RWLock;

class RemoteNodeManager {
 public:
  virtual ~RemoteNodeManager() = default;

  RemoteNodeManager();

  virtual bool Start() = 0;

  virtual std::shared_ptr<RemoteNode> Get(const std::string& key);
};

class RemoteNodeManagerImpl : RemoteNodeManager {
  using NodesT = std::unordered_map<std::string, std::shared_ptr<RemoteNode>>;

 public:
  RemoteNodeManagerImpl(RemoteBlockCacheOptions options);

  bool Start() override;

  std::shared_ptr<RemoteNode> Get(const std::string& key) override;

 private:
  bool LoadMembers();

  bool FetchRemoteMembers(std::vector<CacheGroupMember>* members);

  bool IsSame(const std::vector<CacheGroupMember>& remote_members);

  std::vector<uint64_t> CalcWeights(
      const std::vector<CacheGroupMember>& members);

  std::shared_ptr<ConHash> BuildHash(
      const std::vector<CacheGroupMember>& members);

  std::shared_ptr<NodesT> CreateNodes(
      const std::vector<CacheGroupMember>& members);

 private:
  RWLock rwlock_;  // for chash_ & nodes_
  std::atomic<bool> running_;
  RemoteBlockCacheOptions options_;
  std::unique_ptr<TimerImpl> timer_;
  std::shared_ptr<ConHash> chash_;
  std::shared_ptr<NodesT> nodes_;
  std::shared_ptr<MdsClient> mds_client_;
};

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CLIENT_CACHE_GROUP_MEMBER_MANAGER_H_
