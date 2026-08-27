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

#include "blockcache/infiniband/server/server.h"

#include <glog/logging.h>

#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/brpc_bridge.h"
#include "blockcache/net/brpc_server.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {
namespace {

// What brpc sees of rdma: one method, bridged to the shard that will own the
// wire.
class RdmaServiceNative final : public pb::cache::v2::RdmaService {
 public:
  RdmaServiceNative(net::BrpcServer* server,
                    std::span<HandshakeService* const> verbs)
      : server_(server), verbs_(verbs) {}

  void Handshake(google::protobuf::RpcController* cntl,
                 const pb::cache::v2::HandshakeRequest* request,
                 pb::cache::v2::HandshakeResponse* response,
                 google::protobuf::Closure* done) override {
    net::BridgeToShard(server_, verbs_, &HandshakeService::Handshake,
                       static_cast<::brpc::Controller*>(cntl), request,
                       response, done);
  }

 private:
  net::BrpcServer* server_;
  std::span<HandshakeService* const> verbs_;
};

}  // namespace

Server::~Server() { Shutdown(); }

void Server::AddService(std::vector<Service*> services) {
  CHECK(!started_) << "AddService after Start";
  services_.push_back(std::move(services));
}

Status Server::Start() {
  CHECK(!started_) << "Server already started";

  const unsigned shards = ShardCount();
  for (const std::vector<Service*>& services : services_) {
    CHECK_EQ(services.size(), shards) << "one instance per shard";
  }

  contexts_.resize(shards);
  registries_.resize(shards);
  listeners_.resize(shards);
  handshakes_.resize(shards);
  handshake_ptrs_.assign(shards, nullptr);

  Status status = RunOnAllAndWait([this](unsigned shard) -> Future<Status> {
    return OpenOnThisShard(shard);
  });
  if (!status.ok()) {
    StopShards();
    return status;
  }

  if (option_.brpc_server != nullptr) {
    option_.brpc_server->AddService(std::make_unique<RdmaServiceNative>(
        option_.brpc_server, handshake_ptrs_));
  }

  started_ = true;
  return Status::OK();
}

void Server::Shutdown() {
  if (!started_) {
    return;
  }
  started_ = false;
  StopShards();
}

Future<Status> Server::OpenOnThisShard(unsigned shard) {
  StatusOr<InfinibandUPtr> made = Infiniband::Create(option_.device_name);
  if (!made.ok()) {
    co_return made.status();
  }
  InfinibandUPtr context = std::move(made).value();
  BindThisInfiniband(context.get());
  contexts_[shard] = std::move(context);

  auto registry = std::make_unique<ServiceRegistry>();
  for (const std::vector<Service*>& services : services_) {
    registry->Add(services[shard]);
  }
  registries_[shard] = std::move(registry);

  listeners_[shard] = std::make_unique<Listener>(registries_[shard].get());
  handshakes_[shard] =
      std::make_unique<HandshakeService>(listeners_[shard].get());
  handshake_ptrs_[shard] = handshakes_[shard].get();
  co_return Status::OK();
}

// Sessions first (they serve through the registry), then the context; the
// brpc server carrying the handshake stopped before any of this runs.
Future<> Server::CloseOnThisShard(unsigned shard) {
  if (listeners_[shard] != nullptr) {
    co_await listeners_[shard]->Shutdown();
  }
  handshakes_[shard].reset();
  listeners_[shard].reset();
  if (contexts_[shard] != nullptr) {
    UnbindThisInfiniband(contexts_[shard].get());
    co_await contexts_[shard]->Shutdown();
    contexts_[shard].reset();
  }
  registries_[shard].reset();
}

void Server::StopShards() {
  if (contexts_.empty()) {
    return;
  }
  RunOnAllAndWait([this](unsigned shard) -> Future<> {
    return CloseOnThisShard(shard);
  });
  contexts_.clear();
  registries_.clear();
  listeners_.clear();
  handshakes_.clear();
  handshake_ptrs_.clear();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
