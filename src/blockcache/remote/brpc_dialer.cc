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

#include "blockcache/remote/brpc_dialer.h"

#include <glog/logging.h>

#include <utility>

#include "blockcache/core/runtime/smp.h"

namespace dingofs {
namespace blockcache {

Future<StatusOr<net::Channel*>> BrpcDialer::Dial(net::DialOption option) {
  BrpcChannel::Option brpc_option;
  brpc_option.server = std::move(option.server);
  brpc_option.connection_group = std::move(option.tag);

  StatusOr<std::unique_ptr<BrpcChannel>> channel =
      BrpcChannel::Create(ThisShardId(), brpc_option);
  if (!channel.ok()) {
    return MakeReadyFuture<StatusOr<net::Channel*>>(channel.status());
  }
  channels_.push_back(std::move(channel).value());
  return MakeReadyFuture<StatusOr<net::Channel*>>(
      static_cast<net::Channel*>(channels_.back().get()));
}

Future<> BrpcDialer::Close(net::Channel* channel) {
  for (auto it = channels_.begin(); it != channels_.end(); ++it) {
    if (it->get() == channel) {
      (*it)->Shutdown();
      channels_.erase(it);
      return MakeReadyFuture<>();
    }
  }
  LOG(FATAL) << "Close() got a channel this dialer never made";
  return MakeReadyFuture<>();
}

}  // namespace blockcache
}  // namespace dingofs
