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

#ifndef DINGOFS_BLOCKCACHE_REMOTE_BRPC_HANDSHAKE_CLIENT_H_
#define DINGOFS_BLOCKCACHE_REMOTE_BRPC_HANDSHAKE_CLIENT_H_

#include "blockcache/infiniband/client/dialer.h"
#include "blockcache/remote/brpc_dialer.h"

namespace dingofs {
namespace blockcache {

// Carries the rdma handshake over brpc, and is the only place that knows the
// two are related. The connection lives exactly as long as the one call.
class BrpcHandshakeClient final : public infiniband::HandshakeClient {
 public:
  explicit BrpcHandshakeClient(BrpcDialer* dialer) : dialer_(dialer) {}

  Future<Status> Handshake(const net::DialOption& option,
                           const pb::cache::v2::HandshakeRequest& request,
                           pb::cache::v2::HandshakeResponse* response) override;

 private:
  BrpcDialer* dialer_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_REMOTE_BRPC_HANDSHAKE_CLIENT_H_
