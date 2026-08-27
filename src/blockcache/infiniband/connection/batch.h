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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_BATCH_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_BATCH_H_

#include <infiniband/verbs.h>

#include <cstdint>

#include "blockcache/core/reactor/io_awaiter.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/common/wc_status.h"
#include "blockcache/infiniband/common/wr_id.h"
#include "blockcache/infiniband/connection/send_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// The awaiter IS the completion object: its address is the wr_id.
class OpAwaiter final : public IoAwaiter<OpAwaiter> {
 public:
  OpAwaiter(SendQueue* queue, ibv_wr_opcode opcode, LocalRegion local,
            uint64_t remote_addr, uint32_t rkey) noexcept
      : queue_(queue),
        local_(local),
        remote_addr_(remote_addr),
        rkey_(rkey),
        opcode_(opcode) {}

  Status await_resume() const {
    return WcStatus(static_cast<ibv_wc_status>(result_),
                    "complete rdma operation");
  }

  void Arm() { queue_->Admit(this); }

  void Complete(int32_t wc_status) noexcept { ResumeLater(wc_status); }

  SendQueue* queue() const { return queue_; }
  bool IsRead() const { return opcode_ == IBV_WR_RDMA_READ; }

  OpAwaiter* park_next = nullptr;

 private:
  friend class SendQueue;

  SendQueue* queue_;
  LocalRegion local_;
  uint64_t remote_addr_ = 0;
  uint32_t rkey_ = 0;
  ibv_wr_opcode opcode_ = IBV_WR_RDMA_WRITE;
};

static_assert(alignof(OpAwaiter) > kWrTagMask,
              "wr_id steals the low bits of the awaiter address");

// Tail sentinel: only the last WR is signalled; RC ordering proves the rest.
class BatchAwaiter final : public IoAwaiter<BatchAwaiter> {
 public:
  BatchAwaiter(SendQueue* queue, ibv_send_wr* tail, uint32_t covered_wrs,
               uint32_t covered_reads) noexcept
      : queue_(queue),
        tail_(tail),
        covered_wrs_(covered_wrs),
        covered_reads_(covered_reads) {}

  bool await_ready() const noexcept { return covered_wrs_ == 0; }

  Status await_resume() const {
    if (covered_wrs_ == 0) {
      return Status::OK();
    }
    return WcStatus(static_cast<ibv_wc_status>(result_), "complete rdma batch");
  }

  void Arm() { queue_->SealBatch(this, tail_); }

  void Complete(int32_t wc_status) noexcept { ResumeLater(wc_status); }

  SendQueue* queue() const { return queue_; }
  uint32_t covered_wrs() const { return covered_wrs_; }
  uint32_t covered_reads() const { return covered_reads_; }

 private:
  SendQueue* queue_;
  ibv_send_wr* tail_;
  uint32_t covered_wrs_;
  uint32_t covered_reads_;
};

static_assert(alignof(BatchAwaiter) > kWrTagMask,
              "wr_id steals the low bits of the awaiter address");

class OpBatch {
 public:
  explicit OpBatch(SendQueue* queue) : queue_(queue) {}

  bool AddWrite(LocalRegion local, const RemoteRegion& remote) {
    return Add(IBV_WR_RDMA_WRITE, local, remote);
  }
  bool AddRead(LocalRegion local, const RemoteRegion& remote) {
    return Add(IBV_WR_RDMA_READ, local, remote);
  }

  BatchAwaiter Submit() { return BatchAwaiter(queue_, tail_, count_, reads_); }

  uint32_t count() const { return count_; }

 private:
  bool Add(ibv_wr_opcode opcode, const LocalRegion& local,
           const RemoteRegion& remote) {
    ibv_send_wr* tail = nullptr;
    if (!queue_->AddUnsignaled(opcode, local, remote.addr, remote.rkey,
                               &tail)) {
      return false;
    }
    tail_ = tail;
    ++count_;
    if (opcode == IBV_WR_RDMA_READ) {
      ++reads_;
    }
    return true;
  }

  SendQueue* queue_;
  ibv_send_wr* tail_ = nullptr;
  uint32_t count_ = 0;
  uint32_t reads_ = 0;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_BATCH_H_
