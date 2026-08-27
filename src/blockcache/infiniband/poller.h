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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_POLLER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_POLLER_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/dispatcher.h"
#include "blockcache/core/reactor/poller.h"
#include "blockcache/infiniband/connection/send_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class CompletionQueue;
class ReceiveQueue;

class InfinibandPoller final : public Poller {
 public:
  InfinibandPoller(CompletionQueue* cq, int channel_fd);
  ~InfinibandPoller() override;

  InfinibandPoller(const InfinibandPoller&) = delete;
  InfinibandPoller& operator=(const InfinibandPoller&) = delete;

  bool Poll() override;
  bool PurePoll() override;
  bool TryEnterInterruptMode() override;
  void ExitInterruptMode() override;

  void ScheduleReplenish(ReceiveQueue* recv) {
    replenish_list_.push_back(recv);
  }

  Future<> Disarm();

 private:
  class ChannelEvent final : public Event {
   public:
    explicit ChannelEvent(InfinibandPoller* owner) : owner_(owner) {}

    void OnReady() noexcept override;
    void OnCancelled() noexcept override;

   private:
    InfinibandPoller* owner_;
  };

  static constexpr int kWcBatch = 64;
  static constexpr int kPollBudget = 256;

  void FlushPending();
  bool DispatchStaged();
  void Dispatch(const ibv_wc& wc);

  CompletionQueue* cq_;
  int channel_fd_;
  DoorbellList doorbells_;
  std::vector<ReceiveQueue*> replenish_list_;
  std::unique_ptr<ChannelEvent> channel_event_;
  bool channel_armed_ = false;
  uint64_t error_wc_logged_ = 0;

  ibv_wc staged_[kWcBatch];
  int staged_count_ = 0;
  int staged_pos_ = 0;

  Promise<>* disarm_promise_ = nullptr;
};

using InfinibandPollerUPtr = std::unique_ptr<InfinibandPoller>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
