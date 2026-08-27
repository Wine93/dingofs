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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVER_H_

#include <string>
#include <utility>
#include <vector>

#include "blockcache/infiniband/infiniband.h"
#include "blockcache/infiniband/server/listener.h"
#include "blockcache/infiniband/server/service.h"

namespace dingofs {
namespace blockcache {

namespace net {
class BrpcServer;
}

namespace infiniband {

struct ServerOption {
  std::string device_name;
  // When set, Start registers the handshake's brpc face on it; must not have
  // started yet, and must stop before this server shuts down.
  net::BrpcServer* brpc_server = nullptr;
};

// The serving side, whole: one context, one listener and one service table
// per shard -- so every accepted session belongs here, through its shard's
// listener.
class Server {
 public:
  explicit Server(ServerOption option) : option_(std::move(option)) {}

  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // One verb set, one instance per shard; non-owning, registered before
  // Start.
  void AddService(std::vector<Service*> services);

  Status Start();

  void Shutdown();

 private:
  Future<Status> OpenOnThisShard(unsigned shard);
  Future<> CloseOnThisShard(unsigned shard);
  void StopShards();

  ServerOption option_;
  std::vector<std::vector<Service*>> services_;
  std::vector<InfinibandUPtr> contexts_;
  std::vector<ServiceRegistryUPtr> registries_;
  std::vector<ListenerUPtr> listeners_;
  std::vector<HandshakeServiceUPtr> handshakes_;
  std::vector<HandshakeService*> handshake_ptrs_;
  bool started_ = false;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVER_H_
