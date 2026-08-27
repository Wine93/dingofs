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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CHANNEL_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CHANNEL_H_

#include <cstdint>
#include <span>
#include <string_view>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/client/call_table.h"
#include "blockcache/infiniband/client/response.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include <memory>

#include "blockcache/infiniband/connection/receive_buffer.h"
#include "blockcache/net/channel.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class ClientSession;

// The callable face one dialed connection shows the rest of the process:
// turns a typed call into a request frame, matches the reply to it, and
// meters the in-flight requests. The wire it leaves to the session.
class Channel final : public net::Channel {
 public:
  Channel(ClientSession* session, Connection* conn);

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  Future<Status> CallMethod(net::Call call) override;

  // A response frame arrived; the reply borrows the receive buffer until the
  // caller drops it.
  void OnResponse(ReceiveBuffer* buffer, const FrameView& frame);

  void FailAll(const Status& error);

  bool Alive() const override { return conn_->Alive(); }
  ClientSession* session() const { return session_; }

 private:
  // Back pressure: slot, then credit.
  Future<StatusOr<Response>> Call(Opcode opcode, std::string_view payload,
                                  std::span<const RemoteRegion> regions,
                                  TransferDirection direction);

  ClientSession* session_;
  Connection* conn_;
  std::unique_ptr<CallTable<StatusOr<Response>>> slots_;
};

using ChannelUPtr = std::unique_ptr<Channel>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CHANNEL_H_
