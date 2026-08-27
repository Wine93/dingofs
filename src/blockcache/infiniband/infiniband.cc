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

#include "blockcache/infiniband/infiniband.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/memory/shard_allocator.h"
#include "blockcache/core/memory/slab_pool.h"
#include "blockcache/core/reactor/reactor.h"
#include "blockcache/infiniband/common/protocol.h"

namespace dingofs {
namespace blockcache {

// DEFINE must expand in the same namespace as flag_decls.h's DECLARE, or the
// two name different fLU:: symbols and the link fails.
static bool Positive(const char* /*name*/, uint32_t value) { return value > 0; }

DEFINE_uint32(rdma_max_connections, 64, "max rdma connections per shard");
DEFINE_validator(rdma_max_connections, Positive);

DEFINE_uint32(rdma_cq_entries, 4096,
              "initial completion queue entries; grows on demand");
DEFINE_validator(rdma_cq_entries, Positive);

DEFINE_uint32(rdma_ping_interval_s, 2, "seconds between keepalive pings");
DEFINE_validator(rdma_ping_interval_s, Positive);

DEFINE_uint32(rdma_idle_timeout_s, 10,
              "seconds of peer silence before a connection is reaped");
DEFINE_validator(rdma_idle_timeout_s, [](const char* /*name*/, uint32_t value) {
  return value >= 3 * FLAGS_rdma_ping_interval_s;
});

namespace infiniband {

namespace {

thread_local Infiniband* tls_this_infiniband = nullptr;

}  // namespace

uint64_t PingIntervalNs() {
  return uint64_t{FLAGS_rdma_ping_interval_s} * 1'000'000'000;
}

static const char* CheckTuning() {
  if (FLAGS_rdma_idle_timeout_s < 3 * FLAGS_rdma_ping_interval_s) {
    return "accept --rdma_idle_timeout_s below 3x --rdma_ping_interval_s";
  }
  return nullptr;
}

static size_t PoolSuperblocks() {
  const size_t per_connection =
      2 * size_t{FrameBudget()} * size_t{FLAGS_rdma_frame_bytes};
  const size_t bytes = per_connection * FLAGS_rdma_max_connections;
  return ((bytes + SlabPool::kSuperblockSize - 1) / SlabPool::kSuperblockSize) +
         1;
}

static SlabPoolOption PoolOption() {
  SlabPoolOption pool;
  pool.superblock_count = PoolSuperblocks();
  pool.numa_node = memory::LocalNumaNode();
  return pool;
}

static uint8_t QpCount() {
  return static_cast<uint8_t>(1 + FLAGS_rdma_bulk_qps);
}

static uint32_t WrsPerConnection() {
  return MsgSendWr() + MsgRecvWr() +
         (FLAGS_rdma_bulk_qps * FLAGS_rdma_bulk_send_wr);
}

StatusOr<InfinibandUPtr> Infiniband::Create(std::string device_name) {
  if (const char* reason = CheckTuning(); reason != nullptr) {
    return ToStatus(EINVAL, reason);
  }

  StatusOr<Device*> device = Device::Open(std::move(device_name));
  if (!device.ok()) {
    return device.status();
  }

  InfinibandUPtr infiniband(new Infiniband(device.value()));
  Status status = infiniband->Init();
  if (!status.ok()) {
    return status;
  }
  return infiniband;
}

Future<> Infiniband::Shutdown() {
  if (closing_) {
    co_return;
  }
  closing_ = true;

  co_await poller_->Disarm();
}

Status Infiniband::CheckPeer(const HandshakeMsg& peer) const {
  const char* reason =
      CheckHandshake(peer, QpCount(), device_->port_info().link_layer);
  if (reason != nullptr) {
    LOG(ERROR) << "Fail to handshake with peer: " << reason;
    return ToStatus(EPROTO, reason);
  }
  return Status::OK();
}

StatusOr<QueuePairGroup> Infiniband::CreateQueuePairGroup() {
  return QueuePairGroup::Create(device_, completion_queue_.get());
}

void Infiniband::FillHandshake(const QueuePairGroup& qps,
                               HandshakeMsg* msg) const {
  const PortInfo& port = device_->port_info();

  std::memset(msg, 0, sizeof(*msg));
  msg->version = kHandshakeVersion;
  msg->shard = static_cast<uint16_t>(HasReactor() ? ThisShardId() : 0);
  msg->num_qps = qps.qp_count();
  msg->link_layer = static_cast<uint8_t>(port.link_layer);
  msg->max_rd_atomic = QueuePair::kMaxRdAtomic;
  msg->rpc_credits = static_cast<uint16_t>(MsgRecvWr());
  msg->frame_bytes = static_cast<uint16_t>(FLAGS_rdma_frame_bytes);
  msg->exposed = exposed_region_;

  const std::vector<QueuePairInfo> infos = qps.GetInfos();
  std::ranges::copy(infos, msg->qps);
}

Status Infiniband::ReserveWrs() {
  return completion_queue_->Reserve(WrsPerConnection(), device_->max_cqe());
}

void Infiniband::UnreserveWrs(size_t sessions) {
  completion_queue_->Unreserve(static_cast<uint32_t>(sessions) *
                               WrsPerConnection());
}

Infiniband::Infiniband(Device* device)
    : device_(device),
      buffer_pool_(std::make_unique<BufferPool>(PoolOption())),
      memory_registry_(std::make_unique<MemoryRegistry>(device->pd())) {}

Status Infiniband::Init() {
  StatusOr<CompletionChannel> channel = CompletionChannel::Create(*device_);
  if (!channel.ok()) {
    return channel.status();
  }
  completion_channel_ =
      std::make_unique<CompletionChannel>(std::move(channel).value());

  StatusOr<CompletionQueue> cq = CompletionQueue::Create(
      *device_, FLAGS_rdma_cq_entries, *completion_channel_);
  if (!cq.ok()) {
    return cq.status();
  }
  completion_queue_ = std::make_unique<CompletionQueue>(std::move(cq).value());

  Status registered = buffer_pool_->Init(memory_registry_.get());
  if (!registered.ok()) {
    return registered;
  }

  if (SlabPool* local = blockcache::BufferPool::LocalPool(); local != nullptr) {
    StatusOr<const MemoryRegion*> mr =
        memory_registry_->Register(local->base(), local->total_bytes());
    if (!mr.ok()) {
      return mr.status();
    }
  }

  exposed_region_ =
      RemoteRegion{.addr = reinterpret_cast<uint64_t>(buffer_pool_->base()),
                   .len = buffer_pool_->total_bytes(),
                   .rkey = buffer_pool_->rkey()};
  poller_ = std::make_unique<InfinibandPoller>(completion_queue_.get(),
                                               completion_channel_->fd());
  return Status::OK();
}

Infiniband* ThisInfiniband() { return tls_this_infiniband; }

void BindThisInfiniband(Infiniband* infiniband) {
  CHECK(tls_this_infiniband == nullptr)
      << "one rdma context per shard: a process cannot both serve and dial "
         "rdma on the same shard";
  tls_this_infiniband = infiniband;
}

void UnbindThisInfiniband(Infiniband* infiniband) {
  if (tls_this_infiniband == infiniband) {
    tls_this_infiniband = nullptr;
  }
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
