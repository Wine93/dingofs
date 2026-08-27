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

#include "blockcache/infiniband/connection/send_queue.h"

#include <glog/logging.h>

#include <cstring>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/common/wr_id.h"
#include "blockcache/infiniband/connection/batch.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

namespace {

thread_local DoorbellList* tls_doorbells = nullptr;

}  // namespace

DoorbellList* ThisDoorbells() { return tls_doorbells; }

void BindThisDoorbells(DoorbellList* doorbells) {
  DCHECK(tls_doorbells == nullptr);
  tls_doorbells = doorbells;
}

void UnbindThisDoorbells(DoorbellList* doorbells) {
  if (tls_doorbells == doorbells) {
    tls_doorbells = nullptr;
  }
}

void DoorbellList::FlushAll() {
  SendQueue* queue = head;
  head = nullptr;
  while (queue != nullptr) {
    SendQueue* next = queue->doorbell_next_;
    queue->doorbell_next_ = nullptr;
    queue->doorbell_linked_ = false;
    queue->Flush();
    queue = next;
  }
}

SendQueue::SendQueue(QueuePair* qp, uint32_t depth)
    : qp_(qp->get()),
      depth_(depth),
      max_inline_data_(qp->max_inline_data()),
      wrs_(depth),
      sges_(depth) {
  // Both ends run kMaxRdAtomic by design, so the read window is a constant.
  const uint32_t window = QueuePair::kMaxRdAtomic * kReadOversubscribe;
  read_depth_ = window > depth ? depth : window;
}

SendQueue::~SendQueue() {
  DCHECK(parked_.empty());
  DCHECK(read_parked_.empty());
}

void SendQueue::Admit(OpAwaiter* op) {
  if (broken_) {
    op->Complete(IBV_WC_WR_FLUSH_ERR);
    return;
  }

  const bool is_read = op->IsRead();
  if (is_read) {
    if (reads_outstanding_ >= read_depth_ || outstanding_ >= depth_) {
      read_parked_.Push(op);
      return;
    }
    ++reads_outstanding_;
  } else if (outstanding_ >= depth_) {
    parked_.Push(op);
    return;
  }

  ++outstanding_;
  Submit(op);
}

bool SendQueue::AddUnsignaled(ibv_wr_opcode opcode, LocalRegion local,
                              uint64_t remote_addr, uint32_t rkey,
                              ibv_send_wr** tail_out) {
  if (broken_ || outstanding_ >= depth_) {
    return false;
  }
  const bool is_read = opcode == IBV_WR_RDMA_READ;
  if (is_read && reads_outstanding_ >= read_depth_) {
    return false;
  }

  // wr_id 0 = no completion object; the poller must not dereference it.
  ibv_send_wr* wr =
      BuildWr(opcode, local, remote_addr, rkey, /*wr_id=*/0, /*flags=*/0);
  ++outstanding_;
  if (is_read) {
    ++reads_outstanding_;
  }
  Append(wr);
  *tail_out = wr;
  return true;
}

void SendQueue::SealBatch(BatchAwaiter* batch, ibv_send_wr* tail) {
  if (broken_ || tail == nullptr) {
    batch->Complete(IBV_WC_WR_FLUSH_ERR);
    return;
  }
  tail->send_flags |= IBV_SEND_SIGNALED;
  tail->wr_id = MakeWrId(batch, kTagBatchEnd);
  ArmDoorbell();
}

bool SendQueue::PostMessage(uint64_t wr_id, LocalRegion message) {
  if (broken_ || outstanding_ >= depth_) {
    return false;
  }
  ibv_send_wr* wr = BuildWr(IBV_WR_SEND, message, /*remote_addr=*/0, /*rkey=*/0,
                            wr_id, IBV_SEND_SIGNALED);
  // Latency sensitive: post now unless a chain is pending (program order).
  if (chain_head_ != nullptr) {
    ++outstanding_;
    Append(wr);
    return true;
  }
  if (!PostNow(wr, wr)) {
    broken_ = true;
    FailParked(IBV_WC_WR_FLUSH_ERR);
    return false;
  }
  ++outstanding_;
  return true;
}

void SendQueue::ReleaseSlots(uint32_t n, uint32_t reads) {
  if (n > outstanding_) {
    LOG(DFATAL) << "Release more send slots than are in flight: n=" << n
                << " outstanding=" << outstanding_ << " depth=" << depth_
                << "; clamping, but the accounting is already wrong";
    n = outstanding_;
  }
  outstanding_ -= n;
  reads_outstanding_ -= reads < reads_outstanding_ ? reads : reads_outstanding_;

  if (broken_) {
    return;
  }

  // Reads first: their window is tighter, a freed slot is worth more.
  while (!read_parked_.empty() && reads_outstanding_ < read_depth_ &&
         outstanding_ < depth_) {
    ++outstanding_;
    ++reads_outstanding_;
    Submit(read_parked_.Pop());
  }
  while (!parked_.empty() && outstanding_ < depth_) {
    ++outstanding_;
    Submit(parked_.Pop());
  }

  // Nothing in flight means nothing will ever call this again, so a parked op
  // here waits forever. It is the shape every silent hang has taken.
  LOG_IF(DFATAL, outstanding_ == 0 && parked_count() != 0)
      << "Send queue stalled with nothing in flight: parked=" << parked_count()
      << " read_parked=" << read_parked_.size() << " depth=" << depth_
      << " read_depth=" << read_depth_;
}

void SendQueue::OnError() {
  broken_ = true;

  ibv_send_wr* pending = chain_head_;
  chain_head_ = nullptr;
  chain_tail_ = nullptr;
  if (pending != nullptr) {
    FailUnposted(pending, IBV_WC_WR_FLUSH_ERR);
  }
  FailParked(IBV_WC_WR_FLUSH_ERR);
}

void SendQueue::Flush() {
  if (chain_head_ == nullptr) {
    return;
  }
  ibv_send_wr* head = chain_head_;
  ibv_send_wr* tail = chain_tail_;
  chain_head_ = nullptr;
  chain_tail_ = nullptr;
  if (!PostNow(head, tail)) {
    broken_ = true;
    FailUnposted(head, IBV_WC_GENERAL_ERR);
    FailParked(IBV_WC_WR_FLUSH_ERR);
  }
}

ibv_send_wr* SendQueue::BuildWr(ibv_wr_opcode opcode, const LocalRegion& local,
                                uint64_t remote_addr, uint32_t rkey,
                                uint64_t wr_id, unsigned flags) {
  const uint32_t wr_index = ring_pos_;
  ring_pos_ = ring_pos_ + 1 == depth_ ? 0 : ring_pos_ + 1;

  ibv_sge* sge = &sges_[wr_index];
  sge->addr = reinterpret_cast<uint64_t>(local.addr);
  sge->length = local.len;
  sge->lkey = local.lkey;

  ibv_send_wr* wr = &wrs_[wr_index];
  std::memset(wr, 0, sizeof(*wr));
  wr->wr_id = wr_id;
  wr->sg_list = sge;
  wr->num_sge = local.len == 0 ? 0 : 1;
  wr->opcode = opcode;
  wr->send_flags = flags;
  // Inline spares the HCA a DMA fetch; SEND/WRITE only, below the threshold.
  if ((opcode == IBV_WR_SEND || opcode == IBV_WR_RDMA_WRITE) &&
      local.len <= max_inline_data_) {
    wr->send_flags |= IBV_SEND_INLINE;
  }
  if (opcode == IBV_WR_RDMA_WRITE || opcode == IBV_WR_RDMA_READ) {
    wr->wr.rdma.remote_addr = remote_addr;
    wr->wr.rdma.rkey = rkey;
  }
  return wr;
}

void SendQueue::Submit(OpAwaiter* op) {
  ibv_send_wr* wr = BuildWr(op->opcode_, op->local_, op->remote_addr_,
                            op->rkey_, MakeWrId(op, kTagOp), IBV_SEND_SIGNALED);

  // A depth-1 request must not wait for a doorbell.
  if (chain_head_ == nullptr) {
    if (!PostNow(wr, wr)) {
      broken_ = true;
      FailUnposted(wr, IBV_WC_GENERAL_ERR);
      FailParked(IBV_WC_WR_FLUSH_ERR);
    }
    return;
  }
  Append(wr);
}

void SendQueue::Append(ibv_send_wr* wr) {
  wr->next = nullptr;
  if (chain_tail_ != nullptr) {
    chain_tail_->next = wr;
  } else {
    chain_head_ = wr;
  }
  chain_tail_ = wr;
  ArmDoorbell();
}

// Not back-pressure: false means nothing here reached the device; the
// caller fails and reclaims what it was posting.
bool SendQueue::PostNow(ibv_send_wr* head, ibv_send_wr* tail) {
  tail->next = nullptr;
  ibv_send_wr* bad = nullptr;
  int rc = ibv_post_send(qp_, head, &bad);
  if (rc != 0) {
    LOG(ERROR) << "Fail to post send work request: " << std::strerror(rc);
    return false;
  }
  return true;
}

void SendQueue::ArmDoorbell() {
  DoorbellList* doorbells = ThisDoorbells();
  if (doorbell_linked_ || doorbells == nullptr) {
    return;
  }
  doorbell_linked_ = true;
  doorbell_next_ = doorbells->head;
  doorbells->head = this;
}

void SendQueue::FailUnposted(ibv_send_wr* wr, int32_t wc_status) {
  uint32_t reclaimed = 0;
  uint32_t reads = 0;
  while (wr != nullptr) {
    ibv_send_wr* next = wr->next;
    ++reclaimed;
    if (wr->opcode == IBV_WR_RDMA_READ) {
      ++reads;
    }
    if (wr->wr_id != 0) {
      void* owner = WrIdPtr(wr->wr_id);
      switch (WrIdTag(wr->wr_id)) {
        case kTagOp:
          static_cast<OpAwaiter*>(owner)->Complete(wc_status);
          break;
        case kTagBatchEnd:
          static_cast<BatchAwaiter*>(owner)->Complete(wc_status);
          break;
        default:
          // kTagSendBuffer never lands here: PostMessage owns its failure.
          LOG(DFATAL) << "Fail to reclaim an unposted work request: tag="
                      << WrIdTag(wr->wr_id);
          break;
      }
    }
    wr = next;
  }
  ReleaseSlots(reclaimed, reads);
}

void SendQueue::FailParked(int32_t wc_status) {
  const auto fail = [wc_status](OpAwaiter* op) { op->Complete(wc_status); };
  read_parked_.TakeAllAnd(fail);
  parked_.TakeAllAnd(fail);
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
