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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CLIENT_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CLIENT_H_

#include <cstddef>
#include <memory>
#include <string>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/client/session.h"
#include "blockcache/infiniband/infiniband.h"
#include "blockcache/infiniband/session_registry.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// The dialing side of one shard: the context, and every session it dialed.
class Client {
 public:
  static Future<Status> InitOnThisShard(std::string device_name);
  static Future<> ShutdownOnThisShard();

  static Status RegisterOnAllShards(void* base, size_t bytes);

  // This shard's client; nullptr before InitOnThisShard.
  static Client* This();

  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  Infiniband* context() { return context_.get(); }
  SessionRegistry<ClientSession>& sessions() { return *sessions_; }

 private:
  static StatusOr<std::unique_ptr<Client>> Create(std::string device_name);

  Client() = default;

  Future<> Shutdown();

  InfinibandUPtr context_;
  std::unique_ptr<SessionRegistry<ClientSession>> sessions_;
};

using ClientUPtr = std::unique_ptr<Client>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CLIENT_H_
