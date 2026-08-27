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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVER_H_

#include <cstdint>
#include <memory>

#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/infiniband/connection/receive_buffer.h"
#include "blockcache/infiniband/connection/receive_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class InfinibandPoller;

class Receiver {
 public:
  Receiver(InfinibandPoller* poller, QueuePairGroup* qps,
           ReceiveBufferPool* buffers)
      : poller_(poller), qps_(qps), buffers_(buffers) {}

  Receiver(const Receiver&) = delete;
  Receiver& operator=(const Receiver&) = delete;

  // Posts the ring; `buffers` must be carved by now.
  void Open();

  // A ring slot came back, whatever its status.
  void Consume() { queue_.OnRecvWc(); }

  // The frame is consumed; re-posting rides the poller's replenish batch.
  void Release(uint16_t recv_index);

  void OnError() { queue_.OnError(); }

  uint32_t inflights() const { return queue_.inflights(); }

 private:
  InfinibandPoller* poller_;
  QueuePairGroup* qps_;
  ReceiveBufferPool* buffers_;
  ReceiveQueue queue_;
};

using ReceiverUPtr = std::unique_ptr<Receiver>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVER_H_
