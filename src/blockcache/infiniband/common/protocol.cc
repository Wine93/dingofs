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

#include "blockcache/infiniband/common/protocol.h"

#include <cstring>

namespace dingofs {
namespace blockcache {
namespace infiniband {

void HandshakeMsg::ToPb(pb::cache::v2::RdmaEndpoint* out) const {
  out->set_version(version);
  out->set_shard(shard);
  out->set_link_layer(link_layer);
  out->set_rd_atomic(max_rd_atomic);
  out->set_rpc_credits(rpc_credits);
  out->set_frame_bytes(frame_bytes);
  pb::cache::v2::RdmaExposedRegion* out_exposed = out->mutable_exposed();
  out_exposed->set_addr(exposed.addr);
  out_exposed->set_length(exposed.len);
  out_exposed->set_rkey(exposed.rkey);
  for (uint8_t i = 0; i < num_qps; ++i) {
    const QueuePairInfo& qp = qps[i];
    pb::cache::v2::RdmaQpPeer* out_qp = out->add_qps();
    out_qp->set_qpn(qp.qpn);
    out_qp->set_psn(qp.psn);
    out_qp->set_lid(qp.lid);
    out_qp->set_port_num(qp.port_num);
    out_qp->set_link_layer(qp.link_layer);
    out_qp->set_mtu(qp.mtu);
    out_qp->set_rd_atomic(qp.max_rd_atomic);
    out_qp->set_gid(qp.gid, sizeof(qp.gid));
  }
}

bool HandshakeMsg::FromPb(const pb::cache::v2::RdmaEndpoint& in) {
  if (in.qps_size() < 1 || in.qps_size() > 1 + kMaxBulkQps) {
    return false;
  }
  std::memset(this, 0, sizeof(*this));
  version = static_cast<uint16_t>(in.version());
  shard = static_cast<uint16_t>(in.shard());
  num_qps = static_cast<uint8_t>(in.qps_size());
  link_layer = static_cast<uint8_t>(in.link_layer());
  max_rd_atomic = static_cast<uint8_t>(in.rd_atomic());
  rpc_credits = static_cast<uint16_t>(in.rpc_credits());
  frame_bytes = static_cast<uint16_t>(in.frame_bytes());
  exposed.addr = in.exposed().addr();
  exposed.len = in.exposed().length();
  exposed.rkey = in.exposed().rkey();
  for (int i = 0; i < in.qps_size(); ++i) {
    const pb::cache::v2::RdmaQpPeer& in_qp = in.qps(i);
    if (in_qp.gid().size() != sizeof(qps[i].gid)) {
      return false;
    }
    QueuePairInfo& qp = qps[i];
    qp.qpn = in_qp.qpn();
    qp.psn = in_qp.psn();
    qp.lid = static_cast<uint16_t>(in_qp.lid());
    qp.port_num = static_cast<uint8_t>(in_qp.port_num());
    qp.link_layer = static_cast<uint8_t>(in_qp.link_layer());
    qp.mtu = static_cast<uint8_t>(in_qp.mtu());
    qp.max_rd_atomic = static_cast<uint8_t>(in_qp.rd_atomic());
    std::memcpy(qp.gid, in_qp.gid().data(), sizeof(qp.gid));
  }
  return true;
}

const char* CheckHandshake(const HandshakeMsg& msg, uint8_t local_qps,
                           LinkLayer local_link_layer) {
  if (msg.version != kHandshakeVersion) {
    return "handshake version mismatch";
  }
  if (msg.num_qps != local_qps) {
    return "peer advertises a different number of QPs";
  }
  if (msg.num_qps == 0 || msg.num_qps > 1 + kMaxBulkQps) {
    return "peer advertises an invalid number of QPs";
  }
  if (static_cast<LinkLayer>(msg.link_layer) != local_link_layer) {
    return "link layer mismatch";
  }
  if (msg.rpc_credits == 0 || msg.frame_bytes == 0) {
    return "peer advertises no receive capacity";
  }
  return nullptr;
}

size_t EncodeFrame(char* dst, const OutgoingFrame& frame, uint16_t credit) {
  FrameHeader header{};
  header.magic = kFrameMagic;
  header.type = static_cast<uint8_t>(frame.type);
  header.transfer_direction = static_cast<uint8_t>(frame.direction);
  header.region_count = static_cast<uint8_t>(frame.regions.size());
  header.opcode = frame.opcode;
  header.credit = credit;
  header.code = frame.code;
  header.payload_len = static_cast<uint32_t>(frame.payload.size());
  header.correlation = frame.correlation;

  std::memcpy(dst, &header, sizeof(header));
  size_t at = sizeof(header);
  if (!frame.regions.empty()) {
    std::memcpy(dst + at, frame.regions.data(),
                frame.regions.size() * sizeof(RemoteRegion));
    at += frame.regions.size() * sizeof(RemoteRegion);
  }
  if (!frame.payload.empty()) {
    std::memcpy(dst + at, frame.payload.data(), frame.payload.size());
  }
  return at + frame.payload.size();
}

const char* DecodeFrame(const char* data, uint32_t len, FrameView* out) {
  if (len < sizeof(FrameHeader)) {
    return "frame shorter than its header";
  }
  const auto* header = reinterpret_cast<const FrameHeader*>(data);
  if (header->magic != kFrameMagic) {
    return "bad frame magic";
  }
  if (header->region_count > kMaxRegions) {
    return "too many regions";
  }
  if (header->transfer_direction >
      static_cast<uint8_t>(TransferDirection::kToClient)) {
    return "unknown transfer direction";
  }
  const size_t expected = FrameSize(header->region_count, header->payload_len);
  if (expected > len) {
    return "frame longer than the bytes received";
  }

  out->header = header;
  if (header->region_count == 0) {
    out->regions = nullptr;
  } else {
    out->regions =
        reinterpret_cast<const RemoteRegion*>(data + sizeof(FrameHeader));
  }
  out->payload = std::string_view(
      data + sizeof(FrameHeader) +
          (size_t{header->region_count} * sizeof(RemoteRegion)),
      header->payload_len);
  return nullptr;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
