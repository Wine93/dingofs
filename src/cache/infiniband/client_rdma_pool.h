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
 * Created Date: 2026-05-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_RDMA_POOL_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_RDMA_POOL_H_

#include <gflags/gflags_declare.h>

#include <memory>
#include <mutex>

#include "cache/infiniband/memory.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DECLARE_string(dingofs_rdma_device);
DECLARE_uint32(dingofs_rdma_port_num);
DECLARE_uint32(rdma_client_pool_size);
DECLARE_uint32(rdma_client_pool_buffer_size);

// Process-wide, lazily initialized RDMA buffer pool for clients. All
// RemoteBlockCache callers (Range and Cache RPCs) acquire / release buffers
// from this single pool — they end up registered against the same PD as the
// client's QPs (because both go through the DeviceRegistry in rdma_memory.cc
// keyed by FLAGS_dingofs_rdma_device).
class ClientRDMAPool {
 public:
  static ClientRDMAPool* Instance();

  // Idempotent. First call opens the device, allocates the registered
  // buffer pool, and remembers the resulting status. Subsequent calls are
  // O(1) cache lookups.
  Status EnsureInitialized();

  // Returns nullptr if EnsureInitialized failed or the pool is exhausted.
  Buffer* Acquire();
  void Release(Buffer* buf);

 private:
  ClientRDMAPool() = default;

  std::once_flag once_;
  Status init_status_ = Status::Unknown("uninitialized");
  RDMAMemoryPoolUPtr pool_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_RDMA_POOL_H_
