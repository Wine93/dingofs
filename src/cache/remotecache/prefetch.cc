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
 * Created Date: 2025-11-27
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/prefetch.h"

#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>
#include <bthread/mutex.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <atomic>
#include <cstdio>
#include <memory>
#include <mutex>
#include <utility>

#include "cache/blockcache/cache_store.h"
#include "cache/remotecache/prefetcher.h"
#include "cache/utils/bthread.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

struct Handle {
  void* value;
  void (*deleter)(const std::string_view&, void* value);
  Handle* next_hash;
  Handle* next;
  Handle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;     // Whether entry is in the cache.
  uint32_t refs;     // References, including cache reference, if present.
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  char key_data[1];  // Beginning of key

  std::string_view key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return std::string_view(key_data, key_length);
  }
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRURemove(e);
    LRUAppend(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRURemove(e);
    LRUAppend(&lru_, e);
  }
}

void LRUCache::LRURemove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRUAppend(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const std::string_view& key, uint32_t hash) {
  std::lock_guard<BthreadMutex> l(mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  std::lock_guard<BthreadMutex> l(mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const std::string_view& key, uint32_t hash,
                                void* value, size_t charge,
                                void (*deleter)(const std::string_view& key,
                                                void* value)) {
  std::lock_guard<BthreadMutex> l(mutex_);

  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRUAppend(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }

  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRURemove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const std::string_view& key, uint32_t hash) {
  std::lock_guard<BthreadMutex> l(mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  std::lock_guard<BthreadMutex> l(mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

BlockStatSPtr BlockMap::GetOrCreateBlockStat(const BlockKey& key) {
  uint64_t slice_id = key.id;
  uint64_t block_index = key.index;
  auto& m = blocks_[block_index % kBlockNum];

  {
    bthread::RWLockRdGuard guard(rwlock_);
    auto* stat = m.seek(slice_id);
    if (stat != nullptr) {
      return *stat;
    }
  }

  bthread::RWLockWrGuard guard(rwlock_);
  auto* stat = m.seek(slice_id);
  if (stat != nullptr) {
    return *stat;
  }

  return *m.insert(slice_id, std::make_shared<BlockStat>());
}

class BlockPrefetcher::WaitingLobby {
 public:
  void AddWaiters(const std::vector<Waiter>& waiters) {
    std::lock_guard<bthread::Mutex> lk(mutex_);
    auto* waiters = waiters_.seek(waiter.key.ToString());
    if (waiters == nullptr) {
      waiters = new std::vector<Waiter>();
      waiters_.insert(waiter.key.ToString(), waiters);
    }

    waiters->emplace_back(waiter);
  }

  std::vector<Waiter>* TakeWaiters(const std::string& key) {
    std::lock_guard<bthread::Mutex> lk(mutex_);
    auto it = waiters_.find(key);
    if (it != waiters_.end()) {
      auto* waiters = it->second;
      waiters_.erase(it);
      return waiters;
    }

    return nullptr;
  }

 private:
  bthread::Mutex mutex_;
  butil::FlatMap<std::string, std::vector<Waiter>*> waiters_;
};

class BlockPrefetcher::OnSegmentFetched : public Closure {
 public:
  OnSegmentFetched(IOBuffer* buffer, off_t offset, size_t length)
      : buffer_(buffer), offset_(offset), length_(length) {}

  void CopySegment(IOBuffer* in) { in->AppendTo(buffer_, length_, offset_); }

  void Wait() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    while (!done_) {
      cv_.wait(lk);
    }
  }

  void Run() override {
    {
      std::lock_guard<bthread::Mutex> lk(mutex_);
      done_ = true;
    }
    cv_.notify_one();
  }

 private:
  IOBuffer* buffer_;
  off_t offset_;
  size_t length_;
  bool done_;
  bthread::Mutex mutex_;
  bthread::ConditionVariable cv_;
};

void BlockPrefetcher::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;

  // auto rc =
  //     bthread::execution_queue_start(&queue_id_, &options, HandleTask, this);
  // CHECK_EQ(rc, 0);
}

void BlockPrefetcher::Shutdown() {
  // bthread::execution_queue_stop(queue_id_);
  // bthread::execution_queue_join(queue_id_);
}

void BlockPrefetcher::SubmitTasks(const std::vector<Task>& tasks) {
  //
}

void BlockPrefetcher::AddWaiters(const std::vector<Waiter>& waiters) {
  waiting_lobby_->AddWaiters(
      waiters);  // 这里会不会出现加进去之前已经完成了？造成死等？
}

int BlockPrefetcher::HandleTasks(
    void* meta, bthread::TaskIterator<std::vector<Task>*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  std::vector<Waiter> waiters;
  std::vector<Task> tasks;
  auto* self = static_cast<BlockPrefetcher*>(meta);
  for (; iter; iter++) {
    auto* tasks = *iter;

    for (const auto& task : *tasks) {
      waiters.emplace_back(Waiter{
          .key = task.key,
          .segment_index = task.segment_index,
          .done = task.done,
      });
    }

    // add tasks to tasks
  }

  self->waiting_lobby_->AddWaiters(waiters);
  for (const auto& task : tasks) {
    self->DoPrefetch(task);
  }

  return 0;
}

void BlockPrefetcher::DoPrefetch(const Task& task) {
  auto& stat = task.stat;
  for (;;) {
    auto succ = stat->SetSegmentPrefetching(task.segment_index);
    if (succ) {
      break;
    }

    auto st = stat->GetSegmentStatus(task.segment_index);
    if (st.IsExist()) {
      return;
    } else if (st.IsPrfetching()) {
      //
    }
  }

  auto tid = RunInBthread([this, task]() {
    auto* buffer = new IOBuffer();
    auto status = remote_node_->Range(nullptr, task.key, task.offset,
                                      task.length, &buffer);
    if (status.ok()) {
      auto* handler = cache_->Insert();
      auto succ = stat->SetSegmentExist(task.segment_index);
    } else {
      LOG(WARNING) << "Prefetch block segment failed: " << status.ToString();
    }

    auto* waiters = waiting_lobby_->TakeWaiters(task.key.ToString());
    if (waiters != nullptr) {
      for (const auto& waiter : *waiters) {
        CHECK_NOTNULL(waiter.done);
        auto* closure = waiter.closure;
        closure->CopySegment(handle->Value());
        closure->status() = status;
        closure->Run();
      }
      delete waiters;
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

CacheRetriever::CacheRetriever()
    : block_map_(std::make_unique<SharedBlockMap>()),
      prefetcher_(std::make_unique<BlockPrefetcher>()) {}

// NOTE: must gurantee takes less 50us per 128KB request
Status CacheRetriever::Range(const BlockKey& key, off_t offset, size_t length,
                             size_t total_length, IOBuffer* buffer) {
  CHECK_GT(total_length, 0);

  SegmentHandler handler;
  std::vector<SegmentHandler> handlers;
  std::vector<BlockPrefetcher::Waiter> waiters;
  std::vector<BlockPrefetcher::Task> to_prefetch;
  const auto& stat = block_map_->GetBlockStat(key);

  auto lindex = SegmentIndex(offset);
  auto rindex = SegmentIndex(offset + length);
  auto tindex = SegmentIndex(offset + total_length);

  for (off_t index = lindex; index <= tindex; ++index) {
    auto st = stat->GetSegmentStatus(index);
    bool care = (index <= rindex);

    if (st.IsExist()) {
      if (care) {
        handler = CacheHandler();
        handlers.emplace_back(std::move(handler));
        VLOG(3) << "Pick cache segment (" << index << ")";
      }
    } else if (st.IsPrfetching()) {
      if (care) {
        auto waiter = BlockPrefetcher::Waiter();
        waiter.key = key;
        waiter.segment_index = index;
        waiter.closure = NewSegmentClosure(index, buffer);
        waiters.emplace_back(std::move(waiter));

        handlers.emplace_back(WaitHandler(waiter.closure));
      }
      //
    } else {  // not prefetching
      auto task = BlockPrefetcher::Task();
      task.key = key;
      task.segment_index = index;
      if (care) {
        task.closure = NewSegmentClosure(index, buffer);
        handlers.emplace_back(WaitHandler(task.closure));
      }

      to_prefetch.emplace_back(std::move(task));
    }
  }

  // not exist

  // Submit prefetch task
  prefetcher_->SubmitTasks(to_prefetch);

  // Execute handler one by one
  for (auto& handler : handlers) {
    auto status = handler.Execute();
    if (!status.ok()) {
      status = handler.Retry();
      if (!status.ok()) {
        return status;
      }
    }
  }

  return Status::OK();
}

BlockPrefetcher::OnSegmentFetched CacheRetriever::NewSegmentClosure(
    IOBuffer* buffer, off_t offset, size_t length) {
  return BlockPrefetcher::OnSegmentFetched(buffer, offset, length);
}

Status CacheHandler::Execute() {
  // 从 lrucache 里拿一下
  auto* handler = cache_->Find();
  if (handler == nullptr) {
    return Status::NotFound("segment not cached");
  }

  auto* buffer = static_cast<IOBuffer*>(handler->Value());
  out_->Append(buffer);

  handler->Release();
  return Status::OK();
}

Status CacheHandler::Retry() {
  auto status = storage_->Range();
  //
  return Status::OK();
}

Status WaitHandler::Execute() {
  CHECK_NOTNULL(task->done);

  done_->Wait();
  auto status = done_->status();
  delete closure_;

  // 在这里 wait
  // task->done->wait();
  // 一旦这个 prefetch 任务完成，先放进 lrucache，再将 buffer 回调给所有 waiter
  // 防止一旦 buffer 加进去就被淘汰了

  // return Status::OK();
}

Status WaitHandler::Retry() {
  // 这里再去 retry 下 storage
  auto status = storage_->Range(key, );
  return Status::OK();
}

// BlockPrefetcher::BlockPrefetcher() {}
};  // namespace cache
};  // namespace dingofs
