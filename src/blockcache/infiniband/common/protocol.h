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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_PROTOCOL_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_PROTOCOL_H_

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/net/types.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

using net::kMaxAttachmentBytes;
using net::kOpUnspecified;
using net::kReplyBadOpcode;
using net::kReplyBadRequest;
using net::kReplyHandlerError;
using net::kReplyOk;
using net::kReplyTooLarge;
using net::Opcode;
using net::ReplyCode;

// Everything both ends must agree on lives here: the handshake that opens a
// connection, and the frame layout that crosses the message QP afterwards.
// Little-endian on both ends; asserted rather than byte-swapped.
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "the rdma protocol is little-endian");

inline constexpr uint16_t kHandshakeVersion = 3;

// Bulk queue pairs one connection may open. PROTOCOL CONSTANT: it sizes the
// handshake's QueuePairInfo array, so both ends must agree on it.
inline constexpr uint8_t kMaxBulkQps = 16;

// Wire geometry both roles must agree on: an initiator and a responder that
// disagree on frame size or credits cannot talk. Read once, where a context
// or a connection is built -- never per request. Send ring, receive ring and
// send-buffer pool all come off this one budget, which is what makes ENOBUFS
// unreachable.
inline uint32_t FrameBudget() {
  return (2u * FLAGS_rdma_max_inflight_rpcs) + 2u;
}

inline uint32_t MsgSendWr() { return FrameBudget(); }
inline uint32_t MsgRecvWr() { return FrameBudget(); }
inline uint32_t SendBufferCount() { return FrameBudget(); }

// The handshake is one rpc on whatever transport already serves the peer.
// This struct is the shape the queue-pair ladder wants; ToPb/FromPb convert
// it to and from the protobuf that actually crosses the wire -- the wire hop,
// and only it, speaks protobuf.
struct HandshakeMsg {
  uint16_t version;
  // Echo of the sender's shard, proving the route hint landed as aimed.
  uint16_t shard;
  uint8_t num_qps;  // 1 message QP + N bulk QPs
  uint8_t link_layer;
  uint8_t max_rd_atomic;  // informational; both ends use kMaxRdAtomic
  uint16_t rpc_credits;   // = my receive-ring depth
  uint16_t frame_bytes;   // = my receive-buffer size
  RemoteRegion exposed;   // the whole advertised arena
  QueuePairInfo qps[1 + kMaxBulkQps];

  void ToPb(pb::cache::v2::RdmaEndpoint* out) const;

  // False when the peer sent something this build cannot represent; every
  // other disagreement is CheckHandshake's to report.
  bool FromPb(const pb::cache::v2::RdmaEndpoint& in);
};

// Returns nullptr when acceptable, else a reason suitable for logging.
const char* CheckHandshake(const HandshakeMsg& msg, uint8_t local_qps,
                           LinkLayer local_link_layer);

// Frame layout for the message QP: FrameHeader (32B) + RemoteRegion*rc +
// payload. Bulk data moves through the advertised regions instead.

inline constexpr uint32_t kFrameMagic = 0x324e4344;  // "DCN2"

// Carried on the wire so the receiver need not know the verb.
enum class TransferDirection : uint8_t {
  kNone = 0,
  kToServer = 1,  // the caller supplies bytes the handler reads
  kToClient = 2,  // the handler supplies bytes the caller receives
};

enum class FrameType : uint8_t {
  kRequest = 1,
  kResponse = 2,
  kCreditGrant = 3,
  kPing = 4,
};

// PROTOCOL CONSTANT: both ends must agree. Covers a whole block's worth of the
// pages a writer accumulated into (4 MiB over 64 KiB pages is 64); the frame
// grows 20 bytes per region, so 32 + 64*20 = 1312 against 16 KiB.
inline constexpr uint8_t kMaxRegions = kMaxBufferViews;

struct __attribute__((packed)) FrameHeader {
  uint32_t magic;              // ASCII "DCN2"
  uint8_t type;                // FrameType
  uint8_t transfer_direction;  // TransferDirection: which way the advertised
                               // regions travel
  uint8_t region_count;
  uint8_t reserved0;
  uint16_t opcode;
  uint16_t credit;  // receive buffers the sender is returning
  ReplyCode code;   // responses only
  uint16_t reserved1;
  uint32_t payload_len;
  uint32_t reserved2;  // keeps correlation 8-aligned and the header at 32B
  uint64_t correlation;
};
static_assert(sizeof(FrameHeader) == 32, "FrameHeader is a protocol structure");
static_assert(offsetof(FrameHeader, correlation) == 24,
              "FrameHeader must have no implicit padding");

struct FrameView {
  const FrameHeader* header = nullptr;
  const RemoteRegion* regions = nullptr;
  std::string_view payload;

  uint8_t region_count() const { return header->region_count; }

  TransferDirection transfer_direction() const {
    return static_cast<TransferDirection>(header->transfer_direction);
  }

  uint32_t attachment_bytes() const {
    return static_cast<uint32_t>(GetLength({regions, region_count()}));
  }
};

// Only a caller-supplied attachment is bytes waiting to be read; one heading
// the other way is the handler's to produce.
inline uint32_t InboundAttachmentBytes(const FrameView& frame) {
  if (frame.transfer_direction() != TransferDirection::kToServer) {
    return 0;
  }
  return frame.attachment_bytes();
}

// A frame about to be encoded; `credit` joins at encode time because credits
// ride on whatever traffic happens to leave first.
struct OutgoingFrame {
  FrameType type;
  TransferDirection direction = TransferDirection::kNone;
  Opcode opcode = kOpUnspecified;
  ReplyCode code = kReplyOk;
  uint64_t correlation = 0;
  std::span<const RemoteRegion> regions;
  std::string_view payload;
};

inline size_t FrameSize(uint8_t region_count, uint32_t payload_len) {
  return sizeof(FrameHeader) + (size_t{region_count} * sizeof(RemoteRegion)) +
         payload_len;
}

// `dst` holds at least FrameSize() bytes; returns the bytes written.
size_t EncodeFrame(char* dst, const OutgoingFrame& frame, uint16_t credit);

// Returns nullptr when well formed, else a reason.
const char* DecodeFrame(const char* data, uint32_t len, FrameView* out);

// correlation = (generation << 16) | slot; generation drops late replies.
inline uint64_t MakeCorrelation(uint16_t slot, uint64_t generation) {
  return (generation << 16) | slot;
}
inline uint16_t CorrelationSlot(uint64_t correlation) {
  return static_cast<uint16_t>(correlation & 0xffff);
}
inline uint64_t CorrelationGeneration(uint64_t correlation) {
  return correlation >> 16;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_PROTOCOL_H_
