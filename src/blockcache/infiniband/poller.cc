/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "blockcache/infiniband/poller.h"

#include <memory>

#include <glog/logging.h>

#include <utility>

#include "blockcache/core/reactor/reactor.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/common/wc_status.h"
#include "blockcache/infiniband/common/wr_id.h"
#include "blockcache/infiniband/connection/batch.h"
#include "blockcache/infiniband/connection/receive_queue.h"
#include "blockcache/infiniband/connection/send_buffer.h"
#include "blockcache/infiniband/connection/event_handler.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

InfinibandPoller::InfinibandPoller(CompletionQueue* cq, int channel_fd)
    : cq_(cq),
      channel_fd_(channel_fd),
      channel_event_(std::make_unique<ChannelEvent>(this)) {
  ThisReactor().RegisterPoller(this);
  BindThisDoorbells(&doorbells_);
}

InfinibandPoller::~InfinibandPoller() {
  UnbindThisDoorbells(&doorbells_);
  ThisReactor().UnregisterPoller(this);
}

bool InfinibandPoller::Poll() {
  int done = 0;
  if (DispatchStaged()) {
    done = 1;
  }

  for (;;) {
    int n = cq_->Poll(kWcBatch, staged_);
    if (n < 0) {
      LOG(FATAL) << "Fail to poll completion queue";
    }
    if (n == 0) {
      break;
    }
    staged_count_ = n;
    staged_pos_ = 0;
    DispatchStaged();
    done += n;
    if (n < kWcBatch || done >= kPollBudget) {
      break;
    }
  }

  FlushPending();
  return done > 0;
}

bool InfinibandPoller::PurePoll() {
  if (staged_pos_ < staged_count_) {
    return true;
  }
  int n = cq_->Poll(kWcBatch, staged_);
  if (n <= 0) {
    return false;
  }
  staged_count_ = n;
  staged_pos_ = 0;
  return true;
}

bool InfinibandPoller::TryEnterInterruptMode() {
  if (cq_->ReqNotify() != 0) {
    return false;
  }
  if (PurePoll()) {
    return false;
  }
  if (!channel_armed_) {
    ThisDispatcher().AddEvent(channel_fd_, channel_event_.get(),
                              EventMode::kPollOnce);
    channel_armed_ = true;
  }
  return true;
}

void InfinibandPoller::ExitInterruptMode() { cq_->DrainEvents(); }

Future<> InfinibandPoller::Disarm() {
  Promise<> promise;
  Future<> done = promise.GetFuture();
  disarm_promise_ = &promise;
  if (!ThisDispatcher().DeleteEvent(channel_event_.get())) {
    disarm_promise_ = nullptr;
    co_return;
  }
  co_await std::move(done);
}

void InfinibandPoller::ChannelEvent::OnReady() noexcept {
  owner_->channel_armed_ = false;
}

void InfinibandPoller::ChannelEvent::OnCancelled() noexcept {
  owner_->channel_armed_ = false;
  if (owner_->disarm_promise_ != nullptr) {
    Promise<>* promise = owner_->disarm_promise_;
    owner_->disarm_promise_ = nullptr;
    promise->SetValue();
  }
}

void InfinibandPoller::FlushPending() {
  doorbells_.FlushAll();
  if (!replenish_list_.empty()) {
    for (ReceiveQueue* recv : replenish_list_) {
      recv->FlushReplenish();
    }
    replenish_list_.clear();
  }
}

bool InfinibandPoller::DispatchStaged() {
  const bool had_work = staged_pos_ < staged_count_;
  while (staged_pos_ < staged_count_) {
    const ibv_wc& wc = staged_[staged_pos_++];
    if (staged_pos_ < staged_count_) {
      __builtin_prefetch(WrIdPtr(staged_[staged_pos_].wr_id));
    }
    Dispatch(wc);
  }
  staged_count_ = 0;
  staged_pos_ = 0;
  return had_work;
}

void InfinibandPoller::Dispatch(const ibv_wc& wc) {
  if (wc.wr_id == 0) {
    if (wc.status != IBV_WC_SUCCESS && error_wc_logged_++ < 8) {
      LOG(ERROR) << "Fail to complete rdma batch attachment: "
                 << WcStatus(wc.status, "complete work request").ToString();
    }
    return;
  }

  void* owner = WrIdPtr(wc.wr_id);
  switch (WrIdTag(wc.wr_id)) {
    case kTagOp: {
      auto* op = static_cast<OpAwaiter*>(owner);
      op->queue()->ReleaseSlots(1, op->IsRead() ? 1 : 0);
      op->Complete(wc.status);
      break;
    }
    case kTagBatchEnd: {
      auto* batch = static_cast<BatchAwaiter*>(owner);
      batch->queue()->ReleaseSlots(batch->covered_wrs(),
                                   batch->covered_reads());
      batch->Complete(wc.status);
      break;
    }
    case kTagReceiveBuffer: {
      auto* buffer = static_cast<ReceiveBuffer*>(owner);
      static_cast<EventHandler*>(buffer->owner)->OnMessageReceived(buffer, wc);
      break;
    }
    case kTagSendBuffer: {
      auto* buffer = static_cast<SendBuffer*>(owner);
      static_cast<EventHandler*>(buffer->owner)
          ->OnMessageSent(buffer, wc.status);
      break;
    }
    default:
      LOG(ERROR) << "Fail to dispatch completion: unknown tag="
                 << WrIdTag(wc.wr_id);
      break;
  }
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
