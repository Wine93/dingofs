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

namespace dingofs {
namespace cache {
namespace infiniband {

RDMAMemoryPool::RDMAMemoryPool(dingofs::MemoryPoolUPtr pool, ibv_mr* mr,
                               std::vector<Buffer> buffers)
    : mr_(mr), buffers_(std::move(buffers)), pool_(std::move(pool)) {}

RDMAMemoryPool::~RDMAMemoryPool() {
  // Dereg MR before the inner pool destructs and frees the underlying memory.
  if (mr_ != nullptr) {
    int ret = ibv_dereg_mr(mr_);
    PCHECK(ret == 0) << "ibv_dereg_mr failed: ret=" << ret;
    mr_ = nullptr;
  }
}

RDMAMemoryPoolUPtr RDMAMemoryPool::Create(ProtectDomain* protect_domain,
                                          size_t buffer_size,
                                          size_t buffer_count) {
  CHECK_NOTNULL(protect_domain);

  auto pool = MemoryPool::Create(buffer_size, buffer_count);
  if (pool == nullptr) {
    return nullptr;
  }

  size_t bytes = buffer_size * buffer_count;
  ibv_mr* mr = ibv_reg_mr(protect_domain->GetIbPd(), pool->base(), bytes,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  if (mr == nullptr) {
    PLOG(ERROR) << "Fail to register memory region: bytes=" << bytes;
    return nullptr;
  }

  std::vector<Buffer> buffers;
  buffers.reserve(buffer_count);
  for (size_t i = 0; i < buffer_count; ++i) {
    buffers.push_back({pool->base() + (i * buffer_size),
                       static_cast<uint32_t>(buffer_size), mr->lkey, mr->rkey});
  }

  LOG(INFO) << "Successfully create RDMAMemoryPool{buffer_size=" << buffer_size
            << " buffer_count=" << buffer_count << " lkey=" << mr->lkey
            << " rkey=" << mr->rkey << "}";

  return std::unique_ptr<RDMAMemoryPool>(
      new RDMAMemoryPool(std::move(pool), mr, std::move(buffers)));
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

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
