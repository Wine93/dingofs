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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SESSION_REGISTRY_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SESSION_REGISTRY_H_

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/infiniband/infiniband.h"
#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

inline uint64_t IdleTimeoutNs() {
  return uint64_t{FLAGS_rdma_idle_timeout_s} * 1'000'000'000;
}

// Owns every admitted session on its shard, and their whole afterlife: the
// idle sweep, the reaping, the shutdown. `S` is the concrete session type
// (ServerSession on a serving shard, ClientSession on a dialing one).
template <typename S>
class SessionRegistry {
 public:
  explicit SessionRegistry(Infiniband* infiniband) : infiniband_(infiniband) {}

  ~SessionRegistry() {
    CHECK(sessions_.empty())
        << "SessionRegistry destroyed before ShutdownAll()";
  }

  SessionRegistry(const SessionRegistry&) = delete;
  SessionRegistry& operator=(const SessionRegistry&) = delete;

  // Serving shards only; a dialing shard reaps on each Dial instead.
  void StartSweep() {
    sweep_timer_.SetCallback([this] { (void)Sweep(); });
    sweep_timer_.ArmPeriodic(std::chrono::nanoseconds(IdleTimeoutNs() / 4));
  }

  // Takes ownership only on success; on false `session` still holds it.
  bool Add(std::unique_ptr<S>* session) {
    if (closing_) {
      return false;
    }
    sessions_.push_back(std::move(*session));
    return true;
  }

  // Reclaims dead sessions and their work-request budget.
  Future<> Reap() {
    const size_t reaped = co_await ReapDead();
    infiniband_->UnreserveWrs(reaped);
  }

  // Order matters: shut down BEFORE closing the gate, else deadlock.
  // Loops are BY INDEX: Shutdown() suspends and Add() can still append.
  Future<> ShutdownAll() {
    closing_ = true;
    sweep_timer_.Cancel();
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (size_t i = 0; i < sessions_.size(); ++i) {
      co_await sessions_[i]->Shutdown();
    }
    co_await infiniband_->gate().Close();
    // NOLINTNEXTLINE(modernize-loop-convert) -- same reason.
    for (size_t i = 0; i < sessions_.size(); ++i) {
      co_await sessions_[i]->Shutdown();
    }
    sessions_.clear();
  }

  bool empty() const { return sessions_.empty(); }

 private:
  // Suspends, so `reaping_` stops a second reaper mid-walk.
  Future<size_t> ReapDead() {
    if (closing_ || reaping_) {
      co_return 0;
    }
    reaping_ = true;

    // By index: Shutdown() suspends and Add() can still append; a
    // range-for would walk a vector that reallocated underneath it.
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (size_t i = 0; i < sessions_.size(); ++i) {
      if (!sessions_[i]->connection().initiator() &&
          !sessions_[i]->connection().Alive()) {
        co_await sessions_[i]->Shutdown();
      }
    }

    const size_t reaped =
        std::erase_if(sessions_, [](const std::unique_ptr<S>& session) {
          return session->connection().shutdown_done();
        });
    reaping_ = false;
    co_return reaped;
  }

  // Synchronous, no teardown.
  void MarkIdleDead(uint64_t cutoff_ns) {
    for (const std::unique_ptr<S>& session : sessions_) {
      Connection& conn = session->connection();
      if (conn.Alive() && !conn.initiator() &&
          conn.last_heard_ns() < cutoff_ns) {
        session->OnError(ToStatus(ETIMEDOUT, "hear from the peer"));
      }
    }
  }

  Future<> Sweep() {
    Gate::Holder holder(infiniband_->gate());
    if (!holder.ok()) {
      co_return;
    }
    const uint64_t now = TimestampNs();
    const uint64_t timeout = IdleTimeoutNs();
    MarkIdleDead(now > timeout ? now - timeout : 0);
    co_await Reap();
  }

  Infiniband* infiniband_;
  Timer sweep_timer_;
  std::vector<std::unique_ptr<S>> sessions_;
  bool closing_ = false;
  bool reaping_ = false;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_SESSION_REGISTRY_H_
