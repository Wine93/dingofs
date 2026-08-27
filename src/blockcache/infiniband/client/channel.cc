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

#include "blockcache/infiniband/client/channel.h"

#include "blockcache/infiniband/client/session.h"

#include <cerrno>
#include <memory>
#include <string>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/net/codec.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

static void ReturnReceiveBuffer(void* session, uint64_t recv_index) {
  static_cast<ClientSession*>(session)->Release(
      static_cast<uint16_t>(recv_index));
}

static Response MakeResponse(ClientSession* session, uint16_t recv_index,
                             ReplyCode code, std::string_view payload) {
  return Response(code, payload, &ReturnReceiveBuffer, session, recv_index);
}

Channel::Channel(ClientSession* session, Connection* conn)
    : session_(session),
      conn_(conn),
      slots_(std::make_unique<CallTable<StatusOr<Response>>>()) {
  slots_->Init(static_cast<uint16_t>(FLAGS_rdma_max_inflight_rpcs));
}

// A coroutine on purpose: the encoded payload and the RemoteRegion array must
// outlive the suspending call, so they live in this frame, not the caller's.
Future<Status> Channel::CallMethod(net::Call call) {
  const net::Encoded payload(*call.request);

  // One region per range; the peer sees a single stream either way.
  RemoteRegion regions[kMaxRegions];
  uint8_t count = 0;
  const auto add = [this, &regions, &count](BufferView range) -> Status {
    if (range.empty()) {
      return ToStatus(EINVAL, "send a request: an empty attachment range");
    }
    if (count == kMaxRegions) {
      return ToStatus(E2BIG, "send a request: too many attachment ranges");
    }
    StatusOr<uint32_t> rkey =
        conn_->memory_registry().GetRKey(range.data, range.size);
    if (!rkey.ok()) {
      return ToStatus(EINVAL,
                      "send a request: the attachment is not registered");
    }
    regions[count++] =
        RemoteRegion{.addr = reinterpret_cast<uint64_t>(range.data),
                     .len = range.size,
                     .rkey = rkey.value()};
    return Status::OK();
  };

  // The direction travels: the two look identical on the wire otherwise.
  TransferDirection direction = TransferDirection::kNone;
  if (!call.send.empty()) {
    direction = TransferDirection::kToServer;
    for (const BufferView& range : call.send) {
      const Status status = add(range);
      if (!status.ok()) {
        co_return status;
      }
    }
  } else if (!call.recv.empty()) {
    direction = TransferDirection::kToClient;
    const Status status = add(call.recv);
    if (!status.ok()) {
      co_return status;
    }
  }

  StatusOr<Response> reply =
      co_await Call(call.opcode, payload.view(),
                    std::span<const RemoteRegion>(regions, count), direction);
  if (!reply.ok()) {
    co_return reply.status();
  }
  if (!reply.value().accepted()) {
    co_return Status::Internal("rpc rejected: code=" +
                               std::to_string(reply.value().code()));
  }
  if (!net::Decode(reply.value().payload(), call.response)) {
    co_return Status::Internal("fail to decode the rpc reply");
  }
  co_return Status::OK();
}

void Channel::OnResponse(ReceiveBuffer* buffer, const FrameView& frame) {
  auto* entry = slots_->Match(frame.header->correlation);
  if (entry == nullptr) {
    // Stale or duplicated: the generation says this slot has moved on.
    session_->Release(buffer->index);
    return;
  }
  const uint16_t index = CorrelationSlot(frame.header->correlation);
  entry->promise.SetValue(StatusOr<Response>(MakeResponse(
      session_, buffer->index, frame.header->code, frame.payload)));
  slots_->Release(index);
}

void Channel::FailAll(const Status& error) { slots_->FailAll(error); }

Future<StatusOr<Response>> Channel::Call(Opcode opcode,
                                         std::string_view payload,
                                         std::span<const RemoteRegion> regions,
                                         TransferDirection direction) {
  // Failing after taking a slot or a credit strands both.
  if (regions.size() > kMaxRegions) {
    co_return ToStatus(EINVAL, "send a request: too many regions");
  }
  if (FrameSize(static_cast<uint8_t>(regions.size()), payload.size()) >
      conn_->frame_bytes()) {
    co_return ToStatus(EMSGSIZE, "send a request: it exceeds one frame");
  }
  if (!conn_->Alive()) {
    co_return ToStatus(ENOTCONN, "send a request: connection is down");
  }

  auto waiter = slots_->AcquireSlot();
  const uint16_t slot = co_await waiter;
  if (waiter.failed()) {
    co_return ToStatus(ECONNRESET, "acquire a request slot");
  }
  if (!conn_->Alive()) {
    slots_->Release(slot);
    co_return ToStatus(ECONNRESET, "acquire a request slot");
  }

  // Take the future before sending: the reply may arrive on the next poll.
  Future<StatusOr<Response>> reply = (*slots_)[slot].promise.GetFuture();

  co_await conn_->TakeCredit();
  if (!conn_->Alive()) {
    slots_->Release(slot);
    co_return ToStatus(ECONNRESET, "acquire a send credit");
  }

  Status sent = co_await conn_->RDMASend(
      {.type = FrameType::kRequest,
       .direction = direction,
       .opcode = opcode,
       .correlation = slots_->GetCorrelation(slot),
       .regions = regions,
       .payload = payload});
  if (!sent.ok()) {
    co_return sent;
  }
  co_return co_await std::move(reply);
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
