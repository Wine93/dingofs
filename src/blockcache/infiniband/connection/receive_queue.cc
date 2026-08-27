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

#include "blockcache/infiniband/connection/receive_queue.h"

#include <glog/logging.h>

#include <cstring>

#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/common/wr_id.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

void ReceiveQueue::Init(QueuePair* qp, ReceiveBufferPool* buffers) {
  qp_ = qp->get();
  buffers_ = buffers;
  const uint32_t n = buffers->buffer_count();
  wrs_.resize(n);
  sges_.resize(n);
  pending_.reserve(n);
}

void ReceiveQueue::PostAllWorkRequests() {
  const uint32_t n = buffers_->buffer_count();
  std::vector<ReceiveBuffer*> all(n);
  for (uint32_t i = 0; i < n; ++i) {
    all[i] = &buffers_->Get(i);
  }
  PostWorkRequests(all);
}

bool ReceiveQueue::Recycle(uint16_t index) {
  if (broken_) {
    return false;
  }
  pending_.push_back(&buffers_->Get(index));
  if (pending_.size() >= kReplenishBatch) {
    FlushReplenish();
    return false;
  }
  if (queued_for_flush_) {
    return false;
  }
  queued_for_flush_ = true;
  return true;
}

void ReceiveQueue::FlushReplenish() {
  queued_for_flush_ = false;
  if (pending_.empty() || broken_) {
    pending_.clear();
    return;
  }
  PostWorkRequests(pending_);
  pending_.clear();
}

// One chained ibv_post_recv for the whole batch: one doorbell, not n.
void ReceiveQueue::PostWorkRequests(
    const std::vector<ReceiveBuffer*>& buffers) {
  if (buffers.empty() || broken_) {
    return;
  }
  for (size_t i = 0; i < buffers.size(); ++i) {
    ReceiveBuffer* buffer = buffers[i];
    ibv_sge* sge = &sges_[buffer->index];
    sge->addr = reinterpret_cast<uint64_t>(buffer->data);
    sge->length = buffer->size;
    sge->lkey = buffers_->lkey();

    ibv_recv_wr* wr = &wrs_[buffer->index];
    wr->wr_id = MakeWrId(buffer, kTagReceiveBuffer);
    wr->sg_list = sge;
    wr->num_sge = 1;
    wr->next = i + 1 < buffers.size() ? &wrs_[buffers[i + 1]->index] : nullptr;
  }

  ibv_recv_wr* bad = nullptr;
  int rc = ibv_post_recv(qp_, &wrs_[buffers[0]->index], &bad);
  if (rc != 0) {
    LOG(ERROR) << "Fail to post recv work request: " << std::strerror(rc);
    broken_ = true;
    return;
  }
  inflights_ += static_cast<uint32_t>(buffers.size());
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
