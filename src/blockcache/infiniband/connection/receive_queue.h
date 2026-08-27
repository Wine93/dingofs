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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_QUEUE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_QUEUE_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/infiniband/connection/receive_buffer.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class QueuePair;

// Posts a carved ring of buffers and re-posts them in batches once consumed.
// Where the memory comes from is its owner's business: this class only
// operates on the buffers.
class ReceiveQueue {
 public:
  ReceiveQueue() = default;

  ReceiveQueue(const ReceiveQueue&) = delete;
  ReceiveQueue& operator=(const ReceiveQueue&) = delete;

  // `buffers` is already carved and stamped with its completion cookie.
  void Init(QueuePair* qp, ReceiveBufferPool* buffers);

  // Must run before the peer can send into this queue pair.
  void PostAllWorkRequests();

  // Queues the consumed buffer for re-posting; true means the caller now owes
  // this queue a FlushReplenish (schedule it on the poller's flush list).
  bool Recycle(uint16_t index);
  void FlushReplenish();

  void OnRecvWc() { --inflights_; }

  // After the QP is in ERROR: keep reclaiming completions, stop posting.
  void OnError() { broken_ = true; }

  uint32_t inflights() const { return inflights_; }

 private:
  static constexpr uint32_t kReplenishBatch = 32;

  void PostWorkRequests(const std::vector<ReceiveBuffer*>& buffers);

  ibv_qp* qp_ = nullptr;
  ReceiveBufferPool* buffers_ = nullptr;
  uint32_t inflights_ = 0;
  bool broken_ = false;
  bool queued_for_flush_ = false;

  std::vector<ReceiveBuffer*> pending_;
  std::vector<ibv_recv_wr> wrs_;
  std::vector<ibv_sge> sges_;
};

using ReceiveQueueUPtr = std::unique_ptr<ReceiveQueue>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_QUEUE_H_
