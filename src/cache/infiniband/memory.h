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

#include <bits/types/struct_iovec.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/rdma_memory.h"
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
  // io_uring fixed-buffer indexes for this region after the buffer's owning
  // pool is also registered with io_uring (via RDMAFixedBufferRegistry +
  // IOUring). -1 means "not registered with io_uring" — io_uring callers
  // fall back to non-fixed prep_{read,write}.
  int io_uring_read_buf_index{-1};
  int io_uring_write_buf_index{-1};
};

// EndPoint describing where to register memory (device + port). Kept here
// (rather than connection.h) so RDMAMemoryPool can take it without a
// circular include.
struct PoolEndPoint {
  std::string device_name;
  uint8_t port_num{1};
};

class RDMAMemoryPool {
 public:
  RDMAMemoryPool(RDMARegionUPtr region, MemoryPoolUPtr pool,
                 std::vector<Buffer> buffers);
  ~RDMAMemoryPool();

  // Connection-bound use (caller already owns the PD, e.g. via a QP).
  static RDMAMemoryPoolUPtr Create(ProtectDomain* protect_domain,
                                   size_t buffer_size, size_t buffer_count);

  // Free-standing use (server attachment pool, client global pool). Opens
  // (or reuses) the device's cached PD via GetOrAllocPD.
  static RDMAMemoryPoolUPtr Create(const PoolEndPoint& endpoint,
                                   size_t buffer_size, size_t buffer_count);

  // Returns nullptr if the pool is exhausted.
  Buffer* Require();
  void Release(Buffer* buffer);
  size_t IndexOf(Buffer* buffer) const;

  // Iovec view of the underlying contiguous registered region, one entry
  // per buffer. Used by IOUring to register the same memory for fixed-buffer
  // io_uring ops, so a single buffer can simultaneously back an RDMA
  // attachment and an io_uring_prep_{read,write}_fixed.
  std::vector<iovec> Fetch() const;

  // After this pool's iovecs have been registered with io_uring, assign each
  // Buffer's read/write-local fixed-buffer index. Idempotent if called more
  // than once with the same offsets.
  void AssignIoUringIndices(int read_index_offset, int write_index_offset);

 private:
  RDMARegionUPtr region_;
  MemoryPoolUPtr pool_;
  std::vector<Buffer> buffers_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_MEMORY_H_
