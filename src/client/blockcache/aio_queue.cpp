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
 * Created Date: 2025-03-30
 * Author: Jingli Chen (Wine93)
 */

#include "client/blockcache/aio_queue.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstring>
#include <ctime>
#include <memory>
#include <thread>
#include <unordered_map>

#include "client/blockcache/error.h"
#include "client/blockcache/phase_timer.h"

namespace dingofs {
namespace client {
namespace blockcache {

ThrottleQueue::ThrottleQueue(uint32_t capacity)
    : size_(0), capacity_(capacity) {}

void ThrottleQueue::PushOne() {
  std::unique_lock<bthread::Mutex> lk(mutex_);
  while (size_ == capacity_) {
    cv_.wait(lk);
  }
  size_++;
}

void ThrottleQueue::PopOne() {
  std::unique_lock<bthread::Mutex> lk(mutex_);
  CHECK(size_ > 0);
  size_--;
  cv_.notify_one();
}

AioQueueImpl::AioQueueImpl(const std::shared_ptr<IoRing>& io_ring)
    : running_(false), io_ring_(io_ring), submit_queue_id_({0}) {}

bool AioQueueImpl::Init(uint32_t io_depth) {
  if (running_.exchange(true)) {  // already running
    return true;
  }

  queued_ = std::make_unique<ThrottleQueue>(io_depth);

  if (!io_ring_->Init(io_depth)) {
    LOG(ERROR) << "Init io ring failed.";
    return false;
  }

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&submit_queue_id_, &queue_options,
                                          BatchSubmit, this);
  if (rc != 0) {
    LOG(ERROR) << "Start aio submit execution queue failed: rc = " << rc;
    return false;
  }

  thread_ = std::thread(&AioQueueImpl::BackgroundWait, this);
  return true;
}

bool AioQueueImpl::Shutdown() {
  if (running_.exchange(false)) {
    thread_.join();
    bthread::execution_queue_stop(submit_queue_id_);
    int rc = bthread::execution_queue_join(submit_queue_id_);
    if (rc != 0) {
      LOG(ERROR) << "Join aio submit execution queue failed: rc = " << rc;
      return false;
    }
  }
  return true;
}

void AioQueueImpl::Submit(Aio* aio) {
  if (!CheckIO(aio)) {
    RunClosure(aio, Phase::kCheckIO, BCACHE_ERROR::INVALID_ARGUMENT);
    return;
  }
  queued_->PushOne();
  CHECK_EQ(0, bthread::execution_queue_execute(submit_queue_id_, aio));
}

bool AioQueueImpl::CheckIO(Aio* aio) { return aio->fd > 0 && aio->length > 0; }

int AioQueueImpl::BatchSubmit(void* meta, bthread::TaskIterator<Aio*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  // prepare io
  std::vector<Aio*> ready;
  AioQueueImpl* queue = static_cast<AioQueueImpl*>(meta);
  auto& io_ring = queue->io_ring_;
  for (; iter; iter++) {
    auto* aio = *iter;
    if (!io_ring->PrepareIo(aio)) {
      queue->RunClosure(aio, Phase::kPrepareIO, BCACHE_ERROR::INTERNAL_ERROR);
      continue;
    }
    ready.emplace_back(aio);
  }

  // submit io
  if (io_ring->SubmitIo()) {
    return 0;
  }

  for (auto* aio : ready) {
    queue->RunClosure(aio, Phase::kSumbitIO, BCACHE_ERROR::INTERNAL_ERROR);
  }
  return 0;
}

void AioQueueImpl::BackgroundWait() {
  std::vector<Aio*> completed;
  while (running_.load(std::memory_order_relaxed)) {
    bool succ = io_ring_->WaitIo(1000, &completed);
    if (!succ) {
      continue;
    }

    BCACHE_ERROR code;
    for (auto* aio : completed) {
      code = (aio->retcode == 0) ? BCACHE_ERROR::OK : BCACHE_ERROR::IO_ERROR;
      RunClosure(aio, Phase::kExecuteIO, code);
    }
  }
}

std::string AioQueueImpl::StrPhase(Phase phase) {
  static std::unordered_map<Phase, std::string> phases{
      {Phase::kCheckIO, "check IO"},   {Phase::kPrepareIO, "prepare IO"},
      {Phase::kSumbitIO, "submit IO"}, {Phase::kExecuteIO, "execute IO"},
      {Phase::kWaitIO, "wait IO"},
  };

  auto iter = phases.find(phase);
  if (iter == phases.end()) {
    return "unknown";
  }
  return iter->second;
}

void AioQueueImpl::RunClosure(Aio* aio, Phase phase, BCACHE_ERROR code) {
  if (code != BCACHE_ERROR::OK) {
    LOG(ERROR) << "Encounter an error for " << StrAio(aio) << " in "
               << StrPhase(phase) << ": " << StrErr(code);
  }

  queued_->PopOne();
  if (aio->done != nullptr) {
    aio->done->SetCode(code);
    aio->done->Run();
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
