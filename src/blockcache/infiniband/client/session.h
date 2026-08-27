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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_SESSION_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_SESSION_H_

#include <cstdint>
#include <memory>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/client/channel.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/infiniband/connection/event_handler.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class Infiniband;

// The dialing end: response frames feed its channel; a request frame is a
// peer talking to the wrong end and gets refused. Inbound frames are decoded
// and dispatched on this floor; the wire below owns the outbound path.
class ClientSession final : public EventHandler {
 public:
  // Builds its own wire from the ready `qps`; the context's gate holds the
  // dispatch coroutines open until the owner shuts down.
  ClientSession(Infiniband* context, QueuePairGroup qps,
                const HandshakeMsg& peer);

  // Opens the wire; the one fallible step between birth and calling.
  Status Start();

  void OnMessageReceived(ReceiveBuffer* buffer, const ibv_wc& wc) override;
  void OnMessageSent(SendBuffer* buffer, int retcode) override;
  void OnError(Status status) override;

  Future<> Shutdown();
  Connection& connection() { return *conn_; }

  Channel& channel() { return *channel_; }

  // Hands the slot back to the wire, which settles the credit it now owes.
  void Release(uint16_t recv_index) { conn_->OnMessageHandled(recv_index); }

 private:
  void HandleFrame(ReceiveBuffer* buffer, uint32_t len);

  Future<> Refuse(uint16_t recv_index, FrameView frame);

  ConnectionUPtr conn_;
  Gate* gate_;
  ChannelUPtr channel_;
  Status error_;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_SESSION_H_
