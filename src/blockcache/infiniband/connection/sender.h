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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SENDER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SENDER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/infiniband/connection/send_buffer.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class OpBatch;

class MsgSender {
 public:
  MsgSender(QueuePairGroup* qps, SendBufferPool* buffers)
      : qps_(qps), buffers_(buffers) {}

  MsgSender(const MsgSender&) = delete;
  MsgSender& operator=(const MsgSender&) = delete;

  Status Send(SendBuffer* buffer, size_t len);

  // A send completion: the slot and the frame come back together.
  void OnSent(SendBuffer* buffer);

  uint32_t inflights() const { return qps_->GetMsgQueue()->inflights(); }

 private:
  QueuePairGroup* qps_;
  SendBufferPool* buffers_;
};

using MsgSenderUPtr = std::unique_ptr<MsgSender>;

class BulkSender {
 public:
  BulkSender(QueuePairGroup* qps, const MemoryRegistry* registry)
      : qps_(qps), registry_(registry) {}

  BulkSender(const BulkSender&) = delete;
  BulkSender& operator=(const BulkSender&) = delete;

  Future<Status> Read(BufferView dst, std::span<const RemoteRegion> from) {
    return Move(dst, from, /*read=*/true);
  }
  Future<Status> Write(BufferView src, std::span<const RemoteRegion> to) {
    return Move(src, to, /*read=*/false);
  }

  uint32_t inflights() const { return qps_->bulk_inflights(); }

 private:
  Future<Status> Move(BufferView buffer, std::span<const RemoteRegion> regions,
                      bool read);
  StatusOr<uint32_t> Check(BufferView buffer,
                           std::span<const RemoteRegion> regions) const;
  Future<Status> AddRegion(OpBatch* batch, bool read, LocalRegion local,
                           RemoteRegion region);

  QueuePairGroup* qps_;
  const MemoryRegistry* registry_;
};

using BulkSenderUPtr = std::unique_ptr<BulkSender>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SENDER_H_
