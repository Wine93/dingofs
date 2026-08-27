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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CREDITS_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CREDITS_H_

#include <coroutine>
#include <cstdint>
#include <memory>

#include "blockcache/core/reactor/reactor.h"
#include "blockcache/utils/containers/park_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// Flow control: 1 credit = one posted, unconsumed peer receive buffer.
// One credit is held in reserve exclusively for credit-grant messages.
class Credits {
 public:
  // co_await -> void; parks in arrival order.
  class Waiter {
   public:
    explicit Waiter(Credits* gate) : gate_(gate) {}

    bool await_ready() const noexcept { return gate_->available_ > 0; }

    template <TaskPromise P>
    void await_suspend(std::coroutine_handle<P> h) noexcept {
      task_ = &h.promise();
      gate_->parked_.Push(this);
    }

    void await_resume() noexcept {
      // Ready path takes the credit now; a parked one got it in Refill.
      if (!taken_ && !gate_->failed_) {
        --gate_->available_;
      }
    }

    Waiter* park_next = nullptr;

   private:
    friend class Credits;

    Credits* gate_;
    Task* task_ = nullptr;
    bool taken_ = false;
  };

  explicit Credits(uint32_t peer_recv_buffers)
      : available_(peer_recv_buffers > 0 ? peer_recv_buffers - 1 : 0),
        reserved_(peer_recv_buffers > 0 ? 1 : 0) {}

  Credits(const Credits&) = delete;
  Credits& operator=(const Credits&) = delete;

  Waiter Acquire() { return Waiter(this); }

  // Pings only: never parks.
  bool TryAcquire() {
    if (failed_ || available_ == 0) {
      return false;
    }
    --available_;
    return true;
  }

  // Grant frames only; never parks. A broken connection has no credit to
  // give, so callers check failed() themselves.
  bool TryTakeReserved() {
    if (failed_ || reserved_ == 0) {
      return false;
    }
    --reserved_;
    return true;
  }

  void Refill(uint32_t n) {
    if (n == 0 || failed_) {
      return;
    }
    if (reserved_ == 0) {
      reserved_ = 1;
      --n;
    }
    // Credits go straight to waiters so a late arrival cannot overtake them.
    while (n > 0) {
      Waiter* waiter = parked_.Pop();
      if (waiter == nullptr) {
        break;
      }
      waiter->taken_ = true;
      NotifyWaiter(waiter);
      --n;
    }
    available_ += n;
  }

  void FailAll() {
    failed_ = true;
    parked_.TakeAllAnd([this](Waiter* waiter) {
      waiter->taken_ = true;
      NotifyWaiter(waiter);
    });
  }

  bool failed() const { return failed_; }

 private:
  void NotifyWaiter(Waiter* waiter) { ThisReactor().Schedule(waiter->task_); }

  uint32_t available_;
  uint32_t reserved_;
  bool failed_ = false;
  ParkQueue<Waiter> parked_;
};

using CreditsUPtr = std::unique_ptr<Credits>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CREDITS_H_
