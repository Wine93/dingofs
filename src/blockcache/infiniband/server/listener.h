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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_LISTENER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_LISTENER_H_

#include <memory>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/server/session.h"
#include "blockcache/infiniband/session_registry.h"
#include "blockcache/net/controller.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class Infiniband;
class ServiceRegistry;

// One per serving shard, owner of every session it accepted. Constructed ON
// its shard, after the context opened: it takes it from ThisInfiniband().
class Listener {
 public:
  explicit Listener(ServiceRegistry* services);

  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;

  Future<Status> Accept(HandshakeMsg peer, HandshakeMsg* mine);

  // Shuts down every accepted session and closes the shard's gate.
  Future<> Shutdown();

 private:
  Infiniband* infiniband_;
  ServiceRegistry* services_;
  std::unique_ptr<SessionRegistry<ServerSession>> sessions_;
};

using ListenerUPtr = std::unique_ptr<Listener>;

class HandshakeService {
 public:
  explicit HandshakeService(Listener* listener) : listener_(listener) {}

  HandshakeService(const HandshakeService&) = delete;
  HandshakeService& operator=(const HandshakeService&) = delete;

  Future<> Handshake(net::Controller* cntl,
                     const pb::cache::v2::HandshakeRequest* request,
                     pb::cache::v2::HandshakeResponse* response);

 private:
  Listener* listener_;
};

using HandshakeServiceUPtr = std::unique_ptr<HandshakeService>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_LISTENER_H_
