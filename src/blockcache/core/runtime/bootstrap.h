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

#ifndef DINGOFS_BLOCKCACHE_CORE_RUNTIME_BOOTSTRAP_H_
#define DINGOFS_BLOCKCACHE_CORE_RUNTIME_BOOTSTRAP_H_

namespace dingofs {
namespace blockcache {

// Brings the share-nothing runtime up for the whole process: reactors on
// pinned cores, one buffer pool per shard, then the worker pool that is the
// one way OFF a reactor. Stop order is the exact reverse, and getting either
// wrong is silent, so neither entry point spells it out any more.
//
// Both are for the thread that owns main(); the runtime is a process
// singleton (WorkerPool::Start already CHECKs there is only one).
void StartProcessRuntime();
void StopProcessRuntime();

bool ProcessRuntimeStarted();

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_CORE_RUNTIME_BOOTSTRAP_H_
