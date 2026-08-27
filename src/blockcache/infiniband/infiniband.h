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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_INFINIBAND_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_INFINIBAND_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/base/buffer_pool.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/infiniband/poller.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

uint64_t PingIntervalNs();

struct HandshakeMsg;

class Infiniband;

using InfinibandUPtr = std::unique_ptr<Infiniband>;

// The device side of one shard: the verbs objects, the registered pools, the
// gate, the work-request budget. Who owns the sessions built on it is its
// owner's business (the Listener on a serving shard, the Client on a dialing
// one). One per shard, reached through the thread-local below.
class Infiniband {
 public:
  static StatusOr<InfinibandUPtr> Create(std::string device_name);

  Infiniband(const Infiniband&) = delete;
  Infiniband& operator=(const Infiniband&) = delete;

  Future<> Shutdown();

  Status CheckPeer(const HandshakeMsg& peer) const;

  // Every queue pair a connection needs, created in INIT.
  StatusOr<QueuePairGroup> CreateQueuePairGroup();

  // This end's half of the handshake: the group's endpoints plus everything
  // device-wide the peer must know.
  void FillHandshake(const QueuePairGroup& qps, HandshakeMsg* msg) const;

  Status ReserveWrs();
  void UnreserveWrs(size_t sessions = 1);

  Device& device() { return *device_; }
  CompletionQueue& completion_queue() { return *completion_queue_; }
  BufferPool& buffer_pool() { return *buffer_pool_; }
  MemoryRegistry& memory_registry() { return *memory_registry_; }
  const RemoteRegion& exposed_region() const { return exposed_region_; }
  InfinibandPoller& poller() { return *poller_; }
  Gate& gate() { return gate_; }

 private:
  explicit Infiniband(Device* device);

  Status Init();

  Device* device_ = nullptr;

  CompletionChannelUPtr completion_channel_;
  CompletionQueueUPtr completion_queue_;
  BufferPoolUPtr buffer_pool_;
  MemoryRegistryUPtr memory_registry_;
  RemoteRegion exposed_region_{};
  InfinibandPollerUPtr poller_;

  Gate gate_;
  bool closing_ = false;
};

Infiniband* ThisInfiniband();
void BindThisInfiniband(Infiniband* infiniband);
void UnbindThisInfiniband(Infiniband* infiniband);

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_INFINIBAND_H_
