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

#include "blockcache/remote/brpc_handshake_client.h"

#include <google/protobuf/descriptor.h>

#include <utility>

#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {

// `option` and `request` outlive this frame: the caller is suspended on it.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-reference-coroutine-parameters)
Future<Status> BrpcHandshakeClient::Handshake(
    const net::DialOption& option,
    const pb::cache::v2::HandshakeRequest& request,
    pb::cache::v2::HandshakeResponse* response) {
  StatusOr<net::Channel*> channel = co_await dialer_->Dial(option);
  if (!channel.ok()) {
    co_return channel.status();
  }

  static const google::protobuf::MethodDescriptor* handshake =
      pb::cache::v2::RdmaService::descriptor()->FindMethodByName("Handshake");
  Status status = co_await channel.value()->CallMethod(net::Call{
      .method = handshake,
      .request = &request,
      .response = response,
  });

  co_await dialer_->Close(channel.value());
  co_return status;
}

}  // namespace blockcache
}  // namespace dingofs
