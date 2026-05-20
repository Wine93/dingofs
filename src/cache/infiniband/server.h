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
 * Created Date: 2026-04-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_SERVER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_SERVER_H_

#include <brpc/server.h>
#include <google/protobuf/service.h>

#include <memory>
#include <mutex>
#include <vector>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/messenger.h"
#include "cache/infiniband/server_session.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Listener {
 public:
  Listener();
  Status Listen(const EndPoint& ep);
  ConnectionUPtr Accept(const ConnMangmentMeta& remote_cm_meta);

  ProtectDomain* GetProtectDomain() const { return protect_domain_; }

 private:
  // Borrowed pointers — Device/Port/PD are owned by the process-wide
  // DeviceRegistry in rdma_memory.cc, so a single PD per device is shared by
  // the listener QPs and any RDMAMemoryPool registered for this device.
  Device* device_{nullptr};
  Port* port_{nullptr};
  ProtectDomain* protect_domain_{nullptr};
};

using ListenerUPtr = std::unique_ptr<Listener>;

class InfinibandServiceImpl final : public pb::infiniband::InfinibandService {
 public:
  InfinibandServiceImpl(Listener* listener, Messenger* messenger);
  ~InfinibandServiceImpl() override;

  void Sync(google::protobuf::RpcController* controller,
            const pb::infiniband::SyncRequest* request,
            pb::infiniband::SyncResponse* response,
            google::protobuf::Closure* done) override;
  Status ShutdownSessions();

 private:
  Listener* listener_;
  Messenger* messenger_;
  std::mutex sessions_mutex_;
  std::vector<ServerSessionUPtr> sessions_;
};

struct ServerOptions {
  brpc::Server* brpc_server{nullptr};
};

class Server {
 public:
  Server();
  Status Start(const EndPoint& ep, ServerOptions* options);
  Status Shutdown();

  ProtectDomain* GetProtectDomain() const {
    return listener_->GetProtectDomain();
  }

  // brpc-style: derive from a protobuf-generated Service (e.g.
  // pb::cache::BlockCacheService), implement the virtual methods, then add
  // it here. Caller retains ownership; the service must outlive the Server.
  Status AddService(::google::protobuf::Service* service) {
    return messenger_->AddService(service);
  }

 private:
  ListenerUPtr listener_;
  MessengerUPtr messenger_;
  std::unique_ptr<InfinibandServiceImpl> service_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_SERVER_H_
