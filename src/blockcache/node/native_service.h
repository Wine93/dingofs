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

#ifndef DINGOFS_BLOCKCACHE_NODE_NATIVE_SERVICE_H_
#define DINGOFS_BLOCKCACHE_NODE_NATIVE_SERVICE_H_

#include <span>

#include "blockcache/net/brpc_bridge.h"
#include "blockcache/node/service.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {

// What brpc sees. Every method is the same one move: find the shard that owns
// the key, run the coroutine there. The verb itself lives in CacheService,
// which rdma reaches through its own dispatch table.
class CacheServiceNative final : public pb::cache::v2::CacheService {
 public:
  // Qualified throughout: inside this class the injected base name
  // `CacheService` is the generated one, not the verb it forwards to.
  using Verbs = ::dingofs::blockcache::CacheService;

  CacheServiceNative(net::BrpcServer* server, std::span<Verbs* const> verbs)
      : server_(server), verbs_(verbs) {}

#define DINGOFS_BRIDGE_METHOD(name)                                          \
  void name(google::protobuf::RpcController* cntl,                           \
            const pb::cache::v2::name##Request* request,                     \
            pb::cache::v2::name##Response* response,                         \
            google::protobuf::Closure* done) override {                      \
    net::BridgeToShard(server_, verbs_, &Verbs::name,                        \
                       static_cast<::brpc::Controller*>(cntl), request,      \
                       response, done);                                      \
  }

  DINGOFS_BRIDGE_METHOD(Put)
  DINGOFS_BRIDGE_METHOD(Get)
  DINGOFS_BRIDGE_METHOD(Prefetch)
  DINGOFS_BRIDGE_METHOD(Delete)
  DINGOFS_BRIDGE_METHOD(Ping)
  DINGOFS_BRIDGE_METHOD(GetNodeInfo)

#undef DINGOFS_BRIDGE_METHOD

 private:
  net::BrpcServer* server_;
  std::span<Verbs* const> verbs_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NODE_NATIVE_SERVICE_H_
