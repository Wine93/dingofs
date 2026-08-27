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

#include "blockcache/infiniband/connection/connection.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/connection/event_handler.h"
#include "blockcache/infiniband/infiniband.h"

namespace dingofs {
namespace blockcache {

// DEFINE must expand in the same namespace as flag_decls.h's DECLARE, or the
// two name different fLU:: symbols and the link fails.
DEFINE_uint32(rdma_max_inflight_rpcs, 128,
              "max in-flight rpcs per connection");
DEFINE_validator(rdma_max_inflight_rpcs,
                 [](const char* /*name*/, uint32_t value) {
                   return value > 0 && value <= UINT16_MAX;
                 });

DEFINE_uint32(rdma_frame_bytes, 16 << 10, "bytes in one message frame");
DEFINE_validator(rdma_frame_bytes, [](const char* /*name*/, uint32_t value) {
  return value > 0 && value <= UINT16_MAX;  // it is a uint16 on the wire
});

namespace infiniband {

static constexpr uint64_t kDrainReportNs = 5ull * 1000 * 1000 * 1000;

Connection::Connection(EventHandler* owner, Infiniband* context,
                       QueuePairGroup qps, uint16_t peer_credits)
    : memory_registry_(&context->memory_registry()),
      owner_(owner),
      recv_depth_(static_cast<uint16_t>(MsgRecvWr())),
      last_heard_ns_(CachedTimestampNs()),
      qps_(std::make_unique<QueuePairGroup>(std::move(qps))),
      recv_buffers_(
          std::make_unique<ReceiveBufferPool>(&context->buffer_pool())),
      receiver_(std::make_unique<Receiver>(&context->poller(), qps_.get(),
                                           recv_buffers_.get())),
      send_buffers_(std::make_unique<SendBufferPool>(&context->buffer_pool())),
      msg_sender_(std::make_unique<MsgSender>(qps_.get(), send_buffers_.get())),
      bulk_sender_(std::make_unique<BulkSender>(qps_.get(), memory_registry_)),
      credits_(std::make_unique<Credits>(peer_credits)) {}

Connection::~Connection() {
  // Destroying a QP with WRs outstanding lets the device write into freed
  // coroutine frames; Drain() guarantees this holds.
  CHECK(Inflights() == 0) << "connection destroyed with " << Inflights()
                          << " work requests still outstanding";
}

Status Connection::Open() {
  Status carved =
      send_buffers_->Init(FLAGS_rdma_frame_bytes, SendBufferCount(), owner_);
  if (!carved.ok()) {
    return carved;
  }
  carved = recv_buffers_->Init(FLAGS_rdma_frame_bytes, recv_depth_, owner_);
  if (!carved.ok()) {
    return carved;
  }
  receiver_->Open();
  return Status::OK();
}

// Not a coroutine: with a buffer in hand the send is synchronous.
Future<Status> Connection::RDMASend(const OutgoingFrame& frame) {
  SendBuffer* buffer = send_buffers_->TryAcquire();
  if (buffer == nullptr) {
    return RDMASendSlow(frame);
  }
  return MakeReadyFuture<Status>(PostFrame(buffer, frame));
}

Future<Status> Connection::RDMARead(BufferView dst,
                                    std::span<const RemoteRegion> from) {
  return bulk_sender_->Read(dst, from);
}

Future<Status> Connection::RDMAWrite(BufferView src,
                                     std::span<const RemoteRegion> to) {
  return bulk_sender_->Write(src, to);
}

void Connection::OnMessageHandled(uint16_t recv_index) {
  receiver_->Release(recv_index);
  ++owed_credits_;
  MaybeSendGrant();
}

void Connection::StartKeepalive(uint64_t interval_ns) {
  initiator_ = true;
  ping_interval_ns_ = interval_ns;
  ping_timer_.SetCallback([this] { MaybeSendPing(); });
  ping_timer_.ArmPeriodic(std::chrono::nanoseconds(interval_ns));
}

void Connection::Fail(const Status& status) {
  alive_ = false;

  // ERROR flushes posted WRs (Drain terminates) and fences late one-sided
  // writes out of buffers about to be handed back.
  receiver_->OnError();
  qps_->ModifyToError();

  credits_->FailAll();
  send_buffers_->FailAllWaiters();

  if (!closing_) {
    LOG(WARNING) << "Fail to keep rdma connection alive: " << status.ToString();
  }
}

void Connection::BeginClose() {
  closing_ = true;
  ping_timer_.Cancel();
}

Future<> Connection::Drain() {
  // Bounded: QPs in ERROR flush every posted WR; never-posted ones were
  // already failed by SendQueue::OnError.
  uint64_t next_report = TimestampNs() + kDrainReportNs;
  while (Inflights() > 0) {
    co_await Yield();
    if (TimestampNs() >= next_report) {
      LOG(ERROR) << "Fail to drain rdma connection in time, recv="
                 << receiver_->inflights() << " msg=" << msg_sender_->inflights()
                 << " bulk=" << bulk_sender_->inflights()
                 << " qps=" << static_cast<int>(qps_->qp_count());
      next_report += kDrainReportNs;
    }
  }
}

// By value: a coroutine outlives the caller's expression.
Future<Status> Connection::RDMASendSlow(OutgoingFrame frame) {
  SendBuffer* buffer = co_await send_buffers_->Acquire();
  if (buffer == nullptr) {
    co_return ToStatus(ECONNRESET, "acquire a send buffer");
  }
  co_return PostFrame(buffer, frame);
}

// The ONE place a frame becomes bytes on this end. Credits bound outstanding
// frames, so a refused post is broken accounting or the device rejecting it;
// either way the wire is done for, and this end pronounces it.
Status Connection::PostFrame(SendBuffer* buffer, const OutgoingFrame& frame) {
  const size_t len = EncodeFrame(buffer->data, frame, TakeOwedCredits());
  const Status posted = msg_sender_->Send(buffer, len);
  if (posted.ok()) {
    last_sent_ns_ = CachedTimestampNs();
  } else {
    owner_->OnError(posted);
  }
  return posted;
}

Future<Status> Connection::SendCreditGrant() {
  Status status = co_await RDMASend({.type = FrameType::kCreditGrant});
  grant_in_flight_ = false;
  co_return status;
}

// Credits normally ride on traffic; only a one-way burst needs a grant.
void Connection::MaybeSendGrant() {
  if (grant_in_flight_ || !alive_ || closing_) {
    return;
  }
  if (!GrantDue() || !TryTakeGrantCredit()) {
    return;
  }
  grant_in_flight_ = true;
  (void)SendCreditGrant();
}

// Skips when busy.
void Connection::MaybeSendPing() {
  if (!alive_ || closing_) {
    return;
  }
  if (CachedTimestampNs() - last_sent_ns_ < ping_interval_ns_) {
    return;
  }
  SendBuffer* buffer = send_buffers_->TryAcquire();
  if (buffer == nullptr) {
    return;
  }
  if (!credits_->TryAcquire()) {
    send_buffers_->Release(buffer);
    return;
  }
  (void)PostFrame(buffer, {.type = FrameType::kPing});
}

uint16_t Connection::TakeOwedCredits() {
  const uint32_t n = std::min<uint32_t>(owed_credits_, UINT16_MAX);
  owed_credits_ -= n;
  return static_cast<uint16_t>(n);
}

uint32_t Connection::Inflights() const {
  return receiver_->inflights() + msg_sender_->inflights() +
         bulk_sender_->inflights();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
