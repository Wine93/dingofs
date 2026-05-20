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

#include "cache/infiniband/client_rdma_pool.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_string(rdma_device, "mlx5_0",
              "IB device used by the client-side RDMA buffer pool. Must match "
              "the device used by client QPs (Dialer::Create) so that lkey "
              "and QP share a PD.");
DEFINE_uint32(rdma_port_num, 1,
              "HCA port (1-based) used by the client-side RDMA buffer pool");
DEFINE_uint32(rdma_client_pool_size, 256,
              "Number of buffers in the client-side global RDMA pool. Bounds "
              "the in-flight Range/Cache RPC concurrency on the client.");
DEFINE_uint32(rdma_client_pool_buffer_size, 4 * 1024 * 1024,
              "Per-buffer size of the client-side global RDMA pool. Must be "
              ">= the largest block_size handled (default 4MB).");

ClientRDMAPool* ClientRDMAPool::Instance() {
  static ClientRDMAPool pool;
  return &pool;
}

Status ClientRDMAPool::EnsureInitialized() {
  std::call_once(once_, [this]() {
    PoolEndPoint ep{
        FLAGS_rdma_device,
        static_cast<uint8_t>(FLAGS_rdma_port_num),
    };
    pool_ = RDMAMemoryPool::Create(ep, FLAGS_rdma_client_pool_buffer_size,
                                   FLAGS_rdma_client_pool_size);
    if (pool_ == nullptr) {
      init_status_ =
          Status::Internal("create client RDMA pool failed");
      LOG(ERROR) << "Fail to initialize ClientRDMAPool: device="
                 << FLAGS_rdma_device;
      return;
    }
    init_status_ = Status::OK();
    LOG(INFO) << "ClientRDMAPool initialized: device=" << FLAGS_rdma_device
              << " buffer_size=" << FLAGS_rdma_client_pool_buffer_size
              << " count=" << FLAGS_rdma_client_pool_size;
  });
  return init_status_;
}

Buffer* ClientRDMAPool::Acquire() {
  if (!EnsureInitialized().ok()) {
    return nullptr;
  }
  return pool_->Require();
}

void ClientRDMAPool::Release(Buffer* buf) {
  CHECK_NOTNULL(pool_);
  pool_->Release(buf);
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
