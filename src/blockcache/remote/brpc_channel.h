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

#ifndef DINGOFS_BLOCKCACHE_REMOTE_BRPC_CHANNEL_H_
#define DINGOFS_BLOCKCACHE_REMOTE_BRPC_CHANNEL_H_

#include <atomic>
#include <memory>
#include <string>

#include "blockcache/common/status.h"
#include "blockcache/net/channel.h"

namespace brpc {
class Channel;
}

namespace dingofs {
namespace blockcache {

// The caller's side of the brpc transport, owned by one shard.
// Completions hop back through the shard's foreign queue to resolve there.
class BrpcChannel final : public net::Channel {
 public:
  // Who this connection talks to. Timeouts and retries are server-wide
  // (--remote_rpc_timeout_ms, --remote_connect_timeout_ms,
  // --remote_rpc_max_retry) and read where the channel is built.
  struct Option {
    std::string server;  // "ip:port"
    // brpc pools sockets by (address, group); per-shard group = own socket.
    std::string connection_group;
  };

  static StatusOr<std::unique_ptr<BrpcChannel>> Create(unsigned shard,
                                                       const Option& option);

  ~BrpcChannel() override;

  BrpcChannel(const BrpcChannel&) = delete;
  BrpcChannel& operator=(const BrpcChannel&) = delete;

  // Waits for calls already on the wire; must return before the runtime stops.
  void Shutdown();

  // Send is zero copy; Recv costs one memcpy out of brpc's blocks. brpc
  // resolves the verb from the descriptor, so nothing here carries an opcode:
  // no envelope, and the reply arrives already parsed.
  Future<Status> CallMethod(net::Call call) override;

  // In-flight accounting, kept by the request path.
  void CallStarted() { inflight_.fetch_add(1, std::memory_order_relaxed); }
  void CallFinished() { inflight_.fetch_sub(1, std::memory_order_release); }

  unsigned shard() const { return shard_; }

 private:
  BrpcChannel(unsigned shard, const Option& option);

  unsigned shard_;
  Option option_;
  std::unique_ptr<::brpc::Channel> channel_;
  std::atomic<int64_t> inflight_{0};
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_REMOTE_BRPC_CHANNEL_H_
