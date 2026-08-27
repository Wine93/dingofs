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

#include "blockcache/infiniband/server/session.h"

#include <glog/logging.h>

#include <cerrno>
#include <memory>
#include <string>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/infiniband/common/wc_status.h"
#include "blockcache/infiniband/infiniband.h"
#include "blockcache/infiniband/server/service.h"
#include "blockcache/net/controller.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

ServerSession::ServerSession(Infiniband* context, QueuePairGroup qps,
                             const HandshakeMsg& peer,
                             ServiceRegistry* services)
    : conn_(std::make_unique<Connection>(this, context, std::move(qps),
                                         peer.rpc_credits)),
      gate_(&context->gate()),
      services_(services) {}

Status ServerSession::Start() { return conn_->Open(); }

Future<> ServerSession::Shutdown() {
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

void ServerSession::OnMessageReceived(ReceiveBuffer* buffer, const ibv_wc& wc) {
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
    OnNewMessage(buffer, wc.byte_len);
    return;
  }
  Release(buffer->index);
}

void ServerSession::OnMessageSent(SendBuffer* buffer, int retcode) {
  conn_->ConsumeSend(buffer);
  if (IsUnexpectedWc(retcode, conn_->closing())) {
    OnError(WcStatus(static_cast<ibv_wc_status>(retcode), "send a frame"));
  }
}

void ServerSession::OnError(Status status) {
  if (!error_.ok()) {
    return;  // first failure wins
  }
  error_ = std::move(status);
  conn_->Fail(error_);
}

// `buffer` holds a whole frame; magic and length were checked on arrival.
void ServerSession::OnNewMessage(ReceiveBuffer* buffer, uint32_t len) {
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
    case FrameType::kRequest:
      (void)HandleRequest(buffer->index, frame);
      break;
    case FrameType::kResponse:
      // This end never asks, so a reply here is stale by definition.
      Release(buffer->index);
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
Future<> ServerSession::HandleRequest(uint16_t recv_index, FrameView frame) {
  Gate::Holder holder(*gate_);
  if (!holder.ok()) {
    Release(recv_index);
    co_return;  // shutting down: drop the request quietly
  }

  const Status served = co_await ServeRequest(frame);
  if (!served.ok()) {
    LOG_EVERY_N(ERROR, 100) << "Fail to serve opcode " << frame.header->opcode
                            << ": " << served.ToString();
  }
  Release(recv_index);
}

// Every control path replies exactly once; only a failed attachment read may
// leave the caller unanswered, and then only when the wire is already gone.
Future<Status> ServerSession::ServeRequest(FrameView frame) {
  const Service::Method* method = services_->Find(frame.header->opcode);
  if (method == nullptr) {
    // A dead peer is owed nothing; a second reply would run the transport's
    // completion twice, and no path above replied yet.
    if (Alive()) {
      (void)co_await Reply(frame, kReplyBadOpcode, {});
    }
    co_return Status::OK();
  }

  net::Controller cntl;

  const uint32_t attachment_size = InboundAttachmentBytes(frame);
  cntl.set_request_attachment_size(attachment_size);
  if (attachment_size > 0) {
    // Slab-backed: 4 KiB-aligned address, so O_DIRECT can land straight in
    // it (SlabPool::kMinShift). The length stays the caller's logical size.
    Buffer attachment = Buffer::Alloc(attachment_size);
    if (attachment.empty()) {
      co_return co_await Reply(frame, kReplyTooLarge, {});
    }
    const Status read = co_await conn_->RDMARead(
        attachment.view(), {frame.regions, frame.region_count()});
    if (!read.ok()) {
      if (Alive()) {
        (void)co_await Reply(frame, kReplyHandlerError, {});
      }
      co_return read;
    }
    cntl.request_attachment() = std::move(attachment);
  }

  // The typed core: decode, invoke, encode. The payload lands in this frame
  // so it outlives the send.
  std::string payload;
  const ReplyCode code = co_await (*method)(&cntl, frame.payload, &payload);
  if (code != kReplyOk) {
    co_return co_await Reply(frame, code, {});
  }

  if (!cntl.has_response_body()) {
    co_return co_await Reply(frame, kReplyOk, payload);
  }
  // The borrowed response attachment stays alive: cntl is suspended here.
  co_return co_await ReplyWithAttachment(frame, kReplyOk, payload,
                                         cntl.response_view());
}

// The WRITE must complete before the reply: RC cannot order across QPs.
Future<Status> ServerSession::Reply(const FrameView& frame, ReplyCode code,
                                    std::string_view payload) {
  return conn_->RDMASend({.type = FrameType::kResponse,
                          .opcode = frame.header->opcode,
                          .code = code,
                          .correlation = frame.header->correlation,
                          .payload = payload});
}

Future<Status> ServerSession::DoReplyWithAttachment(FrameView frame,
                                                    ReplyCode code,
                                                    std::string_view payload,
                                                    BufferView attachment) {
  const Status moved = co_await conn_->RDMAWrite(
      attachment, {frame.regions, frame.region_count()});
  if (!moved.ok()) {
    // The caller is owed one answer even when its region was unusable.
    (void)co_await Reply(frame, kReplyHandlerError, {});
    co_return moved;
  }
  co_return co_await Reply(frame, code, payload);
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
