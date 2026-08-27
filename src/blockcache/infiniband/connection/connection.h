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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CONNECTION_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CONNECTION_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/credits.h"
#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/infiniband/connection/receive_buffer.h"
#include "blockcache/infiniband/connection/receiver.h"
#include "blockcache/infiniband/connection/send_buffer.h"
#include "blockcache/infiniband/connection/sender.h"
#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class EventHandler;
class Infiniband;
class MemoryRegistry;

// The wire: queue pairs and every registered buffer that feeds them -- the
// receive ring, the send-frame pool, the credits. ONE class for both ends,
// since after RTS an RC queue pair is symmetric, and the ONLY doorway other
// modules reach any of it through: it carves the frame pools from the shard
// pool and hands the rings buffers, never memory. Only born ready: `qps`
// already reached RTS, and `owner` is the session it lives inside, which
// receives every completion. `peer_credits` is how many frames the peer can
// absorb; what was said to learn it is the session's business. Pinned to one
// shard, not thread-safe by design.
class Connection {
 public:
  Connection(EventHandler* owner, Infiniband* context, QueuePairGroup qps,
             uint16_t peer_credits);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // Carves both frame pools, then posts the receive ring: the wire only
  // listens once it can be answered. Safe against RNR because the responder
  // opens before its handshake reply leaves, and the responder never sends
  // unprompted.
  Status Open();

  // The wire's verbs. A frame is encoded into the wire's own registered
  // memory, stamped with the credits owed; a refused post is pronounced
  // through the owner's OnError.
  Future<Status> RDMASend(const OutgoingFrame& frame);
  Future<Status> RDMARead(BufferView dst, std::span<const RemoteRegion> from);
  Future<Status> RDMAWrite(BufferView src, std::span<const RemoteRegion> to);

  // Back pressure for callers that meter themselves (the client channel).
  Credits::Waiter TakeCredit() { return credits_->Acquire(); }

  // Credits the peer granted arrived on an inbound frame.
  void RefillCredits(uint16_t n) { credits_->Refill(n); }

  // A good frame is a liveness proof for the idle sweep.
  void Heard() { last_heard_ns_ = CachedTimestampNs(); }

  // The frame is done with: the ring slot goes back, the credit it now owes
  // is remembered, and a grant leaves once too many pile up.
  void OnMessageHandled(uint16_t recv_index);

  // A receive ring slot came back, whatever its status.
  void ConsumeRecv() { receiver_->Consume(); }

  // A send frame came back, whatever its status: the slot and the frame.
  void ConsumeSend(SendBuffer* buffer) { msg_sender_->OnSent(buffer); }

  // The dialing end pings; the sweep on the other end reaps on silence.
  void StartKeepalive(uint64_t interval_ns);

  // Wire teardown only; Session::OnError is the caller and owns the
  // first-failure-wins guard.
  void Fail(const Status& status);

  // Flush completions are expected once this ran.
  void BeginClose();

  Future<> Drain();

  // Only after Drain(): the reaper frees any wire that claims this.
  void MarkShutdownDone() { shutdown_done_ = true; }

  bool Alive() const { return alive_; }
  bool closing() const { return closing_; }
  bool initiator() const { return initiator_; }
  bool shutdown_done() const { return shutdown_done_; }
  uint64_t last_heard_ns() const { return last_heard_ns_; }
  uint32_t frame_bytes() const { return send_buffers_->buffer_size(); }
  const MemoryRegistry& memory_registry() const { return *memory_registry_; }

 private:
  Future<Status> RDMASendSlow(OutgoingFrame frame);
  Status PostFrame(SendBuffer* buffer, const OutgoingFrame& frame);
  Future<Status> SendCreditGrant();
  void MaybeSendGrant();
  void MaybeSendPing();
  uint16_t TakeOwedCredits();
  uint32_t Inflights() const;

  // Credits normally ride on traffic; past this the peer needs a grant.
  bool GrantDue() const { return owed_credits_ * 2 >= recv_depth_; }

  // Grant frames only; a dead wire has no credit left to hand out, but then
  // the post itself reports it.
  bool TryTakeGrantCredit() {
    return credits_->failed() || credits_->TryTakeReserved();
  }

  const MemoryRegistry* memory_registry_;
  EventHandler* owner_;

  uint16_t recv_depth_;
  uint64_t last_heard_ns_;
  uint64_t last_sent_ns_ = 0;  // any frame counts as a liveness proof
  uint64_t ping_interval_ns_ = 0;
  uint32_t owed_credits_ = 0;
  bool alive_ = true;  // born on a wire that reached RTS
  bool initiator_ = false;
  bool closing_ = false;
  bool shutdown_done_ = false;
  bool grant_in_flight_ = false;
  Timer ping_timer_;

  QueuePairGroupUPtr qps_;
  ReceiveBufferPoolUPtr recv_buffers_;
  ReceiverUPtr receiver_;
  SendBufferPoolUPtr send_buffers_;
  MsgSenderUPtr msg_sender_;
  BulkSenderUPtr bulk_sender_;
  CreditsUPtr credits_;
};

using ConnectionUPtr = std::unique_ptr<Connection>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CONNECTION_H_
