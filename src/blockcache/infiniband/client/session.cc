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

#include "blockcache/infiniband/client/session.h"

#include <glog/logging.h>

#include <cerrno>
#include <memory>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/common/wc_status.h"
#include "blockcache/infiniband/infiniband.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

ClientSession::ClientSession(Infiniband* context, QueuePairGroup qps,
                             const HandshakeMsg& peer)
    : conn_(std::make_unique<Connection>(this, context, std::move(qps),
                                         peer.rpc_credits)),
      gate_(&context->gate()),
      channel_(std::make_unique<Channel>(this, conn_.get())) {}

Status ClientSession::Start() { return conn_->Open(); }

void ClientSession::OnMessageReceived(ReceiveBuffer* buffer,
                                      const ibv_wc& wc) {
  conn_->ConsumeRecv();
  if (wc.status != IBV_WC_SUCCESS) {
    if (IsUnexpectedWc(wc.status, conn_->closing())) {
      OnError(WcStatus(wc.status, "receive rdma message"));
    }
    return;
  }
  conn_->Heard();
  if (wc.byte_len >= sizeof(FrameHeader) &&
      *reinterpret_cast<const uint32_t*>(buffer->data) == kFrameMagic) {
    HandleFrame(buffer, wc.byte_len);
    return;
  }
  Release(buffer->index);
}

void ClientSession::OnMessageSent(SendBuffer* buffer, int retcode) {
  conn_->ConsumeSend(buffer);
  if (IsUnexpectedWc(retcode, conn_->closing())) {
    OnError(WcStatus(static_cast<ibv_wc_status>(retcode), "send a frame"));
  }
}

void ClientSession::OnError(Status status) {
  if (!error_.ok()) {
    return;  // first failure wins
  }
  error_ = std::move(status);
  conn_->Fail(error_);
  channel_->FailAll(error_);
}

Future<> ClientSession::Shutdown() {
  // Done means drained: without this early-out a call in the done-but-not-
  // reaped window could suspend (a ready future still parks under
  // preemption) and resume on a connection the reaper already freed.
  if (conn_->shutdown_done()) {
    co_return;
  }
  if (!conn_->closing()) {
    conn_->BeginClose();
    OnError(ToStatus(ECANCELED, "keep the connection open"));
  }
  co_await conn_->Drain();
  conn_->MarkShutdownDone();
}

// `buffer` holds a whole frame; magic and length were checked on arrival.
void ClientSession::HandleFrame(ReceiveBuffer* buffer, uint32_t len) {
  FrameView frame;
  const char* reason = DecodeFrame(buffer->data, len, &frame);
  if (reason != nullptr) {
    LOG(ERROR) << "Fail to decode rdma frame: " << reason;
    Release(buffer->index);
    OnError(ToStatus(EPROTO, "decode an rdma frame"));
    return;
  }

  // Reclaim credits first: the sender may be parked waiting for them.
  if (frame.header->credit > 0) {
    conn_->RefillCredits(frame.header->credit);
  }

  switch (static_cast<FrameType>(frame.header->type)) {
    case FrameType::kResponse:
      channel_->OnResponse(buffer, frame);
      break;
    case FrameType::kRequest:
      // A dialling end serves nothing: refuse rather than crash.
      (void)Refuse(buffer->index, frame);
      break;
    case FrameType::kCreditGrant:
    case FrameType::kPing:
      Release(buffer->index);
      break;
    default:
      LOG(ERROR) << "Fail to dispatch rdma frame: unknown type="
                 << static_cast<int>(frame.header->type);
      Release(buffer->index);
      OnError(ToStatus(EPROTO, "dispatch an rdma frame"));
      break;
  }
}

// `frame` by value: it points into the buffer, posted until Release.
Future<> ClientSession::Refuse(uint16_t recv_index, FrameView frame) {
  Gate::Holder holder(*gate_);
  if (!holder.ok()) {
    Release(recv_index);
    co_return;  // shutting down: drop the request quietly
  }

  (void)co_await conn_->RDMASend({.type = FrameType::kResponse,
                                  .opcode = frame.header->opcode,
                                  .code = kReplyBadOpcode,
                                  .correlation = frame.header->correlation});
  Release(recv_index);
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
