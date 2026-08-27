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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SESSION_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SESSION_H_

#include <cstdint>
#include <memory>
#include <string_view>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/infiniband/connection/event_handler.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class Infiniband;
class ServiceRegistry;

class ServerSession final : public EventHandler {
 public:
  ServerSession(Infiniband* context, QueuePairGroup qps,
                const HandshakeMsg& peer, ServiceRegistry* services);

  // Opens the wire; the one fallible step between birth and serving.
  Status Start();

  Future<> Shutdown();

  void OnMessageReceived(ReceiveBuffer* buffer, const ibv_wc& wc) override;
  void OnMessageSent(SendBuffer* buffer, int retcode) override;
  void OnError(Status status) override;

  Connection& connection() { return *conn_; }

 private:
  void OnNewMessage(ReceiveBuffer* buffer, uint32_t len);
  Future<> HandleRequest(uint16_t recv_index, FrameView frame);

  // One request, start to finish. `frame` points into the receive buffer,
  // which stays posted until the dispatch coroutine releases it.
  Future<Status> ServeRequest(FrameView frame);

  Future<Status> Reply(const FrameView& frame, ReplyCode code,
                       std::string_view payload);
  Future<Status> ReplyWithAttachment(const FrameView& frame, ReplyCode code,
                                     std::string_view payload,
                                     BufferView attachment) {
    if (attachment.empty()) {
      return Reply(frame, code, payload);
    }
    if (attachment.size > kMaxAttachmentBytes) {
      return Reply(frame, kReplyTooLarge, payload);
    }
    return DoReplyWithAttachment(frame, code, payload, attachment);
  }

  Future<Status> DoReplyWithAttachment(FrameView frame, ReplyCode code,
                                       std::string_view payload,
                                       BufferView attachment);

  // Hands the slot back to the wire, which settles the credit it now owes.
  void Release(uint16_t recv_index) { conn_->OnMessageHandled(recv_index); }

  bool Alive() const { return conn_->Alive(); }

  ConnectionUPtr conn_;
  Gate* gate_;
  ServiceRegistry* services_;
  Status error_;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SESSION_H_
