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

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_RDMA_MEMORY_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_RDMA_MEMORY_H_

#include <bits/types/struct_iovec.h>
#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cache/infiniband/infiniband.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class RDMAMemoryPool;

struct RDMARegion {
  ibv_mr* mr{nullptr};
  uint32_t lkey{0};
  uint32_t rkey{0};
  void* base{nullptr};
  size_t length{0};

  RDMARegion() = default;
  ~RDMARegion();

  RDMARegion(const RDMARegion&) = delete;
  RDMARegion& operator=(const RDMARegion&) = delete;
};

using RDMARegionUPtr = std::unique_ptr<RDMARegion>;

// Register a chunk of host memory with the RDMA device identified by
// `device_name`. Internally caches the opened Device/Port/ProtectDomain so
// repeated registrations on the same device share one PD — required for
// QP/Buffer lkey/rkey to be mutually valid.
//
// Default `access` covers the common case (local R/W + remote R/W).
Status RegisterRDMAMemory(const std::string& device_name, uint8_t port_num,
                          void* buffer, size_t length, int access,
                          RDMARegionUPtr* out);

// Same as above with a sane default access bitmap.
Status RegisterRDMAMemory(const std::string& device_name, uint8_t port_num,
                          void* buffer, size_t length, RDMARegionUPtr* out);

// Register against an already-allocated PD. Used by RDMAMemoryPool::Create
// where the caller already owns the PD (e.g. tied to a QP).
Status RegisterRDMAMemoryOnPD(ProtectDomain* pd, void* buffer, size_t length,
                              int access, RDMARegionUPtr* out);

// Returns the cached PD for the device (lazily opens it). Used by
// RDMAMemoryPool::Create(EndPoint, ...) and by Listener so that connections
// and free-standing buffers all share a single PD per device.
ProtectDomain* GetOrAllocPD(const std::string& device_name, uint8_t port_num);

// Borrowed pointers to the cached Device / Port for `device_name`. Returns
// nullptr if the device has not been opened yet — call GetOrAllocPD first.
Device* GetCachedDevice(const std::string& device_name);
Port* GetCachedPort(const std::string& device_name);

// Process-wide registry of RDMA buffer pools that should ALSO be registered
// as io_uring fixed buffers. RDMAMemoryPool::Create adds itself here; the
// LocalFileSystem queries the registry at startup to collect iovecs and
// pass them to io_uring_register_buffers. After registration the registry
// calls back into each pool to fill in per-Buffer io_uring_buf_index.
//
// Ordering: every RDMA pool must be created before LocalFileSystem::Start()
// runs (because io_uring_register_buffers is a one-shot per ring).
class RDMAFixedBufferRegistry {
 public:
  static RDMAFixedBufferRegistry& Instance();

  void Register(RDMAMemoryPool* pool);

  // Collect all registered pools' iovecs in registration order; concatenated
  // ranges are contiguous in the resulting list. Also stores a snapshot of
  // the pool order so a later call to FinalizeIndexAssignment(base_offset)
  // can assign each pool's contiguous index range.
  std::vector<iovec> CollectIovecs();

  // Should be called by IOUring after io_uring_register_buffers succeeds.
  // `base_offset` is the index of the first registered RDMA iovec inside
  // the io_uring fixed-buffer table (i.e. equal to the total count of
  // earlier write/read iovecs already in the table).
  void FinalizeIndexAssignment(int base_offset);

 private:
  RDMAFixedBufferRegistry() = default;
  // CollectIovecs takes a stable snapshot for FinalizeIndexAssignment to
  // walk; afterwards the snapshot is cleared.
  std::vector<RDMAMemoryPool*> snapshot_;
  std::vector<size_t> snapshot_counts_;
  std::vector<RDMAMemoryPool*> pools_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_RDMA_MEMORY_H_
