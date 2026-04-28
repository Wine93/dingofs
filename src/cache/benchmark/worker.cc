/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-06-04
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/worker.h"

#include <butil/time.h>
#include <glog/logging.h>

#include "cache/benchmark/option.h"

namespace dingofs {
namespace cache {

Worker::Worker(uint64_t idx, TaskFactorySPtr factory, CollectorSPtr collector)
    : idx_(idx),
      factory_(factory),
      collector_(collector),
      finished_(1) {}

void Worker::Start() {
  auto status = context_.Init(idx_);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to init worker context idx=" << idx_
               << ": " << status.ToString();
    collector_->Submit([status](Stat* stat, Stat* total) {
      stat->Add(0, 0, false);
      total->Add(0, 0, false);
    });
    finished_.signal();
    return;
  }

  ExecAllTasks();
  finished_.signal();
}

void Worker::Shutdown() { finished_.wait(); }

void Worker::ExecAllTasks() {
  BlockKeyIterator iter(idx_, FLAGS_blksize, FLAGS_blocks);
  const int64_t deadline_us =
      FLAGS_time_based
          ? butil::gettimeofday_us() + static_cast<int64_t>(FLAGS_runtime) *
                                        1000 * 1000
          : 0;

  do {
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      if (FLAGS_time_based && butil::gettimeofday_us() >= deadline_us) {
        return;
      }
      auto key = iter.Key();
      auto task = factory_->GenTask(key, &context_);
      ExecTask(task);

      VLOG(9) << "Execute task (key=" << key.Filename() << ").";
    }
  } while (FLAGS_time_based);
}

void Worker::ExecTask(Task task) {
  butil::Timer timer;

  timer.start();
  auto result = task();
  timer.stop();

  const bool ok = result.status.ok();
  const uint64_t bytes = ok ? result.bytes : 0;
  const uint64_t latency_us = timer.u_elapsed();
  collector_->Submit([bytes, latency_us, ok](Stat* stat, Stat* total) {
    stat->Add(bytes, latency_us, ok);
    total->Add(bytes, latency_us, ok);
  });

  if (!ok) {
    LOG(ERROR) << "Task failed: " << result.status.ToString();
  }
}

}  // namespace cache
}  // namespace dingofs
