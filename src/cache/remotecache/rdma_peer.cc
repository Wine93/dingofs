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

#include "cache/remotecache/rdma_peer.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "cache/infiniband/client_rdma_pool.h"
#include "cache/infiniband/connection.h"

namespace dingofs {
namespace cache {

RDMAPeer::RDMAPeer(const std::string& id, const std::string& ip, uint32_t port,
                   uint32_t weight)
    : id_(id),
      ip_(ip),
      port_(port),
      weight_(weight),
      health_checker_(std::make_unique<PeerHealthChecker>(ip, port)) {}

Status RDMAPeer::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  // Make sure the global pool's PD is initialized before we create the
  // client QP. Both go through the DeviceRegistry keyed by
  // FLAGS_dingofs_rdma_device, so this guarantees QP lkey/rkey match.
  auto st = infiniband::ClientRDMAPool::Instance()->EnsureInitialized();
  if (!st.ok()) {
    LOG(ERROR) << "RDMAPeer::Start: ClientRDMAPool init failed: "
               << st.ToString();
    return st;
  }

  infiniband::EndPoint ep{
      infiniband::FLAGS_dingofs_rdma_device,
      static_cast<uint8_t>(infiniband::FLAGS_dingofs_rdma_port_num),
  };
  client_ = infiniband::Client::Create(ep);
  if (client_ == nullptr) {
    return Status::Internal("create rdma client failed");
  }

  st = client_->Connect(EndPoint());
  if (!st.ok()) {
    LOG(ERROR) << "RDMAPeer::Start: connect failed: peer=" << EndPoint()
               << " status=" << st.ToString();
    return st;
  }

  health_checker_->Start();
  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "RDMAPeer started: id=" << id_ << " endpoint=" << EndPoint();
  return Status::OK();
}

void RDMAPeer::Shutdown() {
  running_.store(false, std::memory_order_relaxed);
  // The Client owns its session/connection; dropping the unique_ptr would
  // trigger destruction. Today the underlying infiniband::Client does not
  // expose an explicit Shutdown; releasing the pointer suffices.
  client_.reset();
  if (health_checker_) {
    health_checker_->Shutdown();
  }
}

}  // namespace cache
}  // namespace dingofs
