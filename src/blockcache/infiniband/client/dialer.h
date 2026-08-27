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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_DIALER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_DIALER_H_

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/dialer.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class HandshakeClient {
 public:
  virtual ~HandshakeClient() = default;
  HandshakeClient() = default;

  HandshakeClient(const HandshakeClient&) = delete;
  HandshakeClient& operator=(const HandshakeClient&) = delete;

  virtual Future<Status> Handshake(
      const net::DialOption& option,
      const pb::cache::v2::HandshakeRequest& request,
      pb::cache::v2::HandshakeResponse* response) = 0;
};

class Dialer final : public net::Dialer {
 public:
  explicit Dialer(HandshakeClient* handshake_client)
      : handshake_client_(handshake_client) {}

  Future<StatusOr<net::Channel*>> Dial(net::DialOption option) override;
  Future<> Close(net::Channel* channel) override;

 private:
  HandshakeClient* handshake_client_;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_DIALER_H_
