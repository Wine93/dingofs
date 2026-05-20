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

#include "cache/infiniband/memory.h"

#include <glog/logging.h>

#include <memory>
#include <utility>

#include "cache/infiniband/rdma_memory.h"

namespace dingofs {
namespace cache {
namespace infiniband {

namespace {

constexpr int kPoolAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                            IBV_ACCESS_REMOTE_READ;

RDMAMemoryPoolUPtr BuildPool(RDMARegionUPtr region, MemoryPoolUPtr backing,
                             size_t buffer_size, size_t buffer_count) {
  std::vector<Buffer> buffers;
  buffers.reserve(buffer_count);
  for (size_t i = 0; i < buffer_count; ++i) {
    Buffer buffer;
    buffer.data = backing->base() + (i * buffer_size);
    buffer.capacity = static_cast<uint32_t>(buffer_size);
    buffer.lkey = region->lkey;
    buffer.rkey = region->rkey;
    buffers.emplace_back(buffer);
  }

  LOG(INFO) << "Successfully create RDMAMemoryPool{buffer_size=" << buffer_size
            << " buffer_count=" << buffer_count << " lkey=" << region->lkey
            << " rkey=" << region->rkey << "}";

  auto pool = std::make_unique<RDMAMemoryPool>(
      std::move(region), std::move(backing), std::move(buffers));
  // The same physical memory will (later, in LocalFileSystem::Start) be
  // handed to io_uring_register_buffers so cache-disk IO can read/write
  // directly into RDMA-registered memory without bouncing through io_uring's
  // own BufferPool.
  RDMAFixedBufferRegistry::Instance().Register(pool.get());
  return pool;
}

}  // namespace

RDMAMemoryPool::RDMAMemoryPool(RDMARegionUPtr region, MemoryPoolUPtr pool,
                               std::vector<Buffer> buffers)
    : region_(std::move(region)),
      pool_(std::move(pool)),
      buffers_(std::move(buffers)) {}

RDMAMemoryPool::~RDMAMemoryPool() = default;

RDMAMemoryPoolUPtr RDMAMemoryPool::Create(ProtectDomain* protect_domain,
                                          size_t buffer_size,
                                          size_t buffer_count) {
  CHECK_NOTNULL(protect_domain);

  auto backing = MemoryPool::Create(buffer_size, buffer_count);
  if (backing == nullptr) {
    return nullptr;
  }

  RDMARegionUPtr region;
  size_t bytes = buffer_size * buffer_count;
  auto st = RegisterRDMAMemoryOnPD(protect_domain, backing->base(), bytes,
                                   kPoolAccess, &region);
  if (!st.ok()) {
    return nullptr;
  }

  return BuildPool(std::move(region), std::move(backing), buffer_size,
                   buffer_count);
}

RDMAMemoryPoolUPtr RDMAMemoryPool::Create(const PoolEndPoint& endpoint,
                                          size_t buffer_size,
                                          size_t buffer_count) {
  auto backing = MemoryPool::Create(buffer_size, buffer_count);
  if (backing == nullptr) {
    return nullptr;
  }

  RDMARegionUPtr region;
  size_t bytes = buffer_size * buffer_count;
  auto st = RegisterRDMAMemory(endpoint.device_name, endpoint.port_num,
                               backing->base(), bytes, kPoolAccess, &region);
  if (!st.ok()) {
    return nullptr;
  }

  return BuildPool(std::move(region), std::move(backing), buffer_size,
                   buffer_count);
}

Buffer* RDMAMemoryPool::Require() {
  char* buf = pool_->Require();
  if (buf == nullptr) {
    return nullptr;
  }
  return &buffers_[pool_->IndexOf(buf)];
}

void RDMAMemoryPool::Release(Buffer* buffer) {
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer, buffers_.data());
  DCHECK_LT(buffer, buffers_.data() + buffers_.size());
  pool_->Release(buffer->data);
}

size_t RDMAMemoryPool::IndexOf(Buffer* buffer) const {
  return pool_->IndexOf(buffer->data);
}

std::vector<iovec> RDMAMemoryPool::Fetch() const {
  std::vector<iovec> v;
  v.reserve(buffers_.size());
  for (const auto& b : buffers_) {
    iovec iov;
    iov.iov_base = b.data;
    iov.iov_len = b.capacity;
    v.push_back(iov);
  }
  return v;
}

void RDMAMemoryPool::AssignIoUringIndices(int index_offset) {
  for (size_t i = 0; i < buffers_.size(); ++i) {
    buffers_[i].io_uring_buf_index = index_offset + static_cast<int>(i);
  }
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
