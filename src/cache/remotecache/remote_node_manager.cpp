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

#include "cache/remotecache/remote_node_manager.h"

#include <glog/logging.h>

#include <memory>

#include "base/hash/con_hash.h"
#include "base/hash/ketama_con_hash.h"
#include "dingofs/cachegroup.pb.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using base::hash::ConNode;
using base::hash::KetamaConHash;
using pb::mds::cachegroup::CacheGroupErrCode_Name;
using pb::mds::cachegroup::CacheGroupOk;
using utils::ReadLockGuard;
using utils::WriteLockGuard;

RemoteNodeManagerImpl::RemoteNodeManagerImpl(RemoteBlockCacheOptions options)
    : options_(options),
      timer_(std::make_unique<TimerImpl>()),
      chash_(std::make_shared<KetamaConHash>()),
      nodes_(std::make_shared<NodesT>()) {}

bool RemoteNodeManagerImpl::Start() {
  if (!LoadMembers()) {
    LOG(ERROR) << "Load cache group members failed.";
    return false;
  }

  CHECK(timer_->Start());
  timer_->Add([this] { LoadMembers(); }, options_.load_members_interval_ms);
  return true;
}

bool RemoteNodeManagerImpl::LoadMembers() {
  std::vector<CacheGroupMember> remote_members;
  bool succ = FetchRemoteMembers(&remote_members);
  if (!succ) {
    return false;
  } else if (IsSame(remote_members)) {
    return true;
  }

  auto chash = BuildHash(remote_members);
  auto nodes = CreateNodes(remote_members);

  // commit latest cache group members
  WriteLockGuard lk(rwlock_);
  chash_ = chash;
  nodes_ = nodes;
  return true;
}

bool RemoteNodeManagerImpl::FetchRemoteMembers(
    std::vector<CacheGroupMember>* members) {
  auto status =
      mds_client_->LoadCacheGroupMembers(options_.group_name, members);
  if (status != CacheGroupOk) {
    LOG(ERROR) << "Load cache group members failed: "
               << CacheGroupErrCode_Name(status);
    return false;
  }
  return true;
}

bool RemoteNodeManagerImpl::IsSame(
    const std::vector<CacheGroupMember>& remote_members) {
  if (remote_members.size() != nodes_->size()) {
    return false;
  }

  for (const auto& remote : remote_members) {
    auto iter = nodes_->find(std::to_string(remote.id()));
    if (iter == nodes_->end()) {  // not found
      return false;
    }

    auto& node = iter->second;
    // auto& local = node->GetMember();
    // if (local != remote) {
    //   return false;
    // }
  }
  return true;
}

std::vector<uint64_t> RemoteNodeManagerImpl::CalcWeights(
    const std::vector<CacheGroupMember>& members) {
  uint64_t gcd = 0;
  std::vector<uint64_t> weights;
  for (const auto& member : members) {
    weights.push_back(member.weight());
    gcd = std::gcd(gcd, member.weight());
  }
  CHECK_NE(gcd, 0);

  for (auto& weight : weights) {
    weight = weight / gcd;
  }
  return weights;
}

std::shared_ptr<ConHash> RemoteNodeManagerImpl::BuildHash(
    const std::vector<CacheGroupMember>& members) {
  auto weights = CalcWeights(members);
  CHECK_EQ(members.size(), weights.size());

  auto chash = std::make_shared<KetamaConHash>();
  for (size_t i = 0; i < members.size(); i++) {
    const auto& member = members[i];
    chash->AddNode(std::to_string(member.id()), weights[i]);
    LOG(INFO) << "Add cache group member (id=" << member.id()
              << ", endpoint=" << member.ip() << ":" << member.port()
              << ", weight=" << weights[i] << ") to cache group success.";
  }

  chash->Final();
  return chash;
}

std::shared_ptr<RemoteNodeManagerImpl::NodesT>
RemoteNodeManagerImpl::CreateNodes(
    const std::vector<CacheGroupMember>& members) {
  auto nodes = std::make_shared<NodesT>();
  for (const auto& member : members) {
    // auto node = std::make_shared<CacheGroupNodeImpl>(member);
    // if (!node->Init()) {
    //   LOG(WARNING) << "Init cache group node failed, id=" << member.id();
    // }
    // nodes->emplace(std::to_string(member.id()),
    //                std::make_shared<CacheGroupNodeImpl>(member));
  }
  return nodes;
}

std::shared_ptr<RemoteNode> RemoteNodeManagerImpl::Get(const std::string& key) {
  ReadLockGuard lk(rwlock_);

  ConNode cnode;
  bool find = chash_->Lookup(key, cnode);
  CHECK(find);

  auto iter = nodes_->find(cnode.key);
  CHECK(iter != nodes_->end());
  return iter->second;
}

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs
