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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_QUEUE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_QUEUE_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/infiniband/base/region.h"
#include "blockcache/utils/containers/park_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class BatchAwaiter;
class OpAwaiter;
class QueuePair;
class SendQueue;

struct DoorbellList {
  SendQueue* head = nullptr;
  void FlushAll();
};

// This shard's pending-flush list, bound by the poller that owns it;
// nullptr on a shard without one, and then every post rings immediately.
DoorbellList* ThisDoorbells();
void BindThisDoorbells(DoorbellList* doorbells);
void UnbindThisDoorbells(DoorbellList* doorbells);

class SendQueue {
 public:
  SendQueue() = default;

  // Inline capacity comes from the qp itself: the granted value, not the
  // requested one.
  SendQueue(QueuePair* qp, uint32_t depth);
  ~SendQueue();

  SendQueue(const SendQueue&) = delete;
  SendQueue& operator=(const SendQueue&) = delete;
  SendQueue(SendQueue&&) = default;
  SendQueue& operator=(SendQueue&&) = default;

  void Admit(OpAwaiter* op);

  // False when full: submit, then start anew.
  bool AddUnsignaled(ibv_wr_opcode opcode, LocalRegion local,
                     uint64_t remote_addr, uint32_t rkey,
                     ibv_send_wr** tail_out);
  void SealBatch(BatchAwaiter* batch, ibv_send_wr* tail);

  // Always signalled; false when full, broken, or the device rejects the
  // post -- the caller owns the buffer either way.
  bool PostMessage(uint64_t wr_id, LocalRegion message);

  // The completion side: frees `n` slots (`reads` of them READ slots) and
  // resubmits what parked on them.
  void ReleaseSlots(uint32_t n, uint32_t reads);

  // Reclaims never-posted slots so drains terminate.
  void OnError();

  uint32_t inflights() const { return outstanding_; }

 private:
  friend struct DoorbellList;

  // A multiple of rd_atomic: gating exactly at it starves the queue.
  static constexpr uint32_t kReadOversubscribe = 8;

  void Flush();
  ibv_send_wr* BuildWr(ibv_wr_opcode opcode, const LocalRegion& local,
                       uint64_t remote_addr, uint32_t rkey, uint64_t wr_id,
                       unsigned flags);
  void Submit(OpAwaiter* op);
  void Append(ibv_send_wr* wr);
  bool PostNow(ibv_send_wr* head, ibv_send_wr* tail);
  void ArmDoorbell();
  // Never reached the device, so no work completion will ever arrive.
  void FailUnposted(ibv_send_wr* wr, int32_t wc_status);
  void FailParked(int32_t wc_status);

  uint32_t parked_count() const { return parked_.size() + read_parked_.size(); }

  ibv_qp* qp_;
  uint32_t depth_;
  uint32_t max_inline_data_;

  // ibv_post_send copies before returning, so these are reusable.
  std::vector<ibv_send_wr> wrs_;
  std::vector<ibv_sge> sges_;
  uint32_t ring_pos_ = 0;

  ibv_send_wr* chain_head_ = nullptr;
  ibv_send_wr* chain_tail_ = nullptr;

  // Reads park separately: gated by both depth and the rd_atomic window.
  ParkQueue<OpAwaiter> parked_;
  ParkQueue<OpAwaiter> read_parked_;

  uint32_t outstanding_ = 0;
  uint32_t reads_outstanding_ = 0;
  uint32_t read_depth_ = 1;
  bool broken_ = false;

  bool doorbell_linked_ = false;
  SendQueue* doorbell_next_ = nullptr;
};


}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_SEND_QUEUE_H_
