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

/*
 * Project: DingoFS
 * Created Date: 2026-06-05
 * Author: AI
 */

#include "cache/infiniband/outstanding_controllers.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>
#include <unordered_set>
#include <vector>

#include "cache/infiniband/controller.h"

namespace dingofs {
namespace cache {
namespace infiniband {
namespace {

TEST(OutstandingControllersTest, ShardIndex) {
  // Monotonic ids spread round-robin across the power-of-2 shards.
  for (uint64_t id = 0; id < 256; ++id) {
    auto shard = OutstandingControllers::ShardIndex(id);
    EXPECT_LT(shard, OutstandingControllers::kShardCount);
    EXPECT_EQ(shard, id & (OutstandingControllers::kShardCount - 1));
  }
}

TEST(OutstandingControllersTest, AddAndTake) {
  OutstandingControllers outstanding;
  Controller cntl;

  {  // Take resolves the registered controller exactly once and erases it.
    Controller* taken = nullptr;
    int hits = 0;
    outstanding.Add(1, &cntl);
    outstanding.Take(1, [&](Controller* c) {
      taken = c;
      ++hits;
    });
    EXPECT_EQ(taken, &cntl);
    EXPECT_EQ(hits, 1);
  }

  {  // A second Take for the same id finds nothing.
    int hits = 0;
    outstanding.Take(1, [&](Controller*) { ++hits; });
    EXPECT_EQ(hits, 0);
  }
}

TEST(OutstandingControllersTest, TakeMissAfterRemove) {
  OutstandingControllers outstanding;
  Controller cntl;

  outstanding.Add(7, &cntl);
  outstanding.Remove(7);

  int hits = 0;
  outstanding.Take(7, [&](Controller*) { ++hits; });
  EXPECT_EQ(hits, 0);
}

TEST(OutstandingControllersTest, RemoveIsIdempotent) {
  OutstandingControllers outstanding;
  Controller cntl;

  outstanding.Remove(42);  // absent id: no-op
  outstanding.Add(42, &cntl);
  outstanding.Remove(42);
  outstanding.Remove(42);  // already gone: no-op

  int hits = 0;
  outstanding.Take(42, [&](Controller*) { ++hits; });
  EXPECT_EQ(hits, 0);
}

TEST(OutstandingControllersTest, ClearAll) {
  OutstandingControllers outstanding;
  constexpr int kCount = 100;
  std::vector<Controller> cntls(kCount);

  for (int i = 0; i < kCount; ++i) {
    outstanding.Add(i, &cntls[i]);
  }

  {  // ClearAll runs fn on every distinct outstanding controller.
    std::unordered_set<Controller*> cleared;
    outstanding.ClearAll([&](Controller* c) { cleared.insert(c); });
    EXPECT_EQ(cleared.size(), static_cast<size_t>(kCount));
  }

  {  // Every entry was dropped, so a later Take finds nothing.
    int hits = 0;
    for (int i = 0; i < kCount; ++i) {
      outstanding.Take(i, [&](Controller*) { ++hits; });
    }
    EXPECT_EQ(hits, 0);
  }

  {  // ClearAll on an empty registry runs fn zero times.
    int hits = 0;
    outstanding.ClearAll([&](Controller*) { ++hits; });
    EXPECT_EQ(hits, 0);
  }
}

TEST(OutstandingControllersTest, ConcurrentAddTakeRemove) {
  OutstandingControllers outstanding;
  Controller cntl;
  constexpr int kThreads = 16;
  constexpr int kPerThread = 5000;
  std::atomic<int> taken{0};

  // Each thread owns a disjoint id range. It Adds every id, then Takes the even
  // ones and Removes the odd ones, stressing the per-shard locks concurrently.
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      for (int i = 0; i < kPerThread; ++i) {
        uint64_t id = static_cast<uint64_t>(t) * kPerThread + i;
        outstanding.Add(id, &cntl);
        if (i % 2 == 0) {
          outstanding.Take(id, [&](Controller*) {
            taken.fetch_add(1, std::memory_order_relaxed);
          });
        } else {
          outstanding.Remove(id);
        }
      }
    });
  }
  for (auto& w : workers) {
    w.join();
  }

  EXPECT_EQ(taken.load(), kThreads * (kPerThread / 2));

  // Every id was either taken or removed: nothing should be left outstanding.
  int leftover = 0;
  outstanding.ClearAll([&](Controller*) { ++leftover; });
  EXPECT_EQ(leftover, 0);
}

}  // namespace
}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
