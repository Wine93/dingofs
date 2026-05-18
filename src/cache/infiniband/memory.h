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
 * Created Date: 2026-04-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_MEMORY_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_MEMORY_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "cache/infiniband/infiniband.h"
#include "common/memory_pool.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class RDMAMemoryPool;
using RDMAMemoryPoolUPtr = std::unique_ptr<RDMAMemoryPool>;

struct Buffer {
  char* data{nullptr};
  uint32_t capacity{0};
  uint32_t length{0};
  uint32_t lkey{0};
  uint32_t rkey{0};
};

// RDMA-aware wrapper around MemoryPool: the inner pool owns the
// buffer storage and the lock-free freelist; this layer adds MR registration
// and exposes Chunk metadata (data/bytes/lkey/rkey) that posted work requests
// need.
class RDMAMemoryPool {
 public:
  RDMAMemoryPool(MemoryPoolUPtr pool, ibv_mr* mr, std::vector<Buffer> buffers);
  ~RDMAMemoryPool();

  static RDMAMemoryPoolUPtr Create(std::string& device_name, size_t buffer_size,
                                   size_t buffer_count);
  static RDMAMemoryPoolUPtr Create(ProtectDomain* protect_domain,
                                   size_t buffer_size, size_t buffer_count);

  // Returns nullptr if the pool is exhausted.
  Buffer* Require();
  void Release(Buffer* buffer);

 private:
  // Owned only when created via Create(device_name, ...); nullptr when the
  // protect domain is borrowed. Declared first so they outlive mr_/pool_.
  DeviceUPtr device_;
  ProtectDomainUPtr pd_;
  ibv_mr* mr_;
  std::vector<Buffer> buffers_;
  MemoryPoolUPtr pool_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_MEMORY_H_
