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
#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <bthread/execution_queue_inl.h>

#include <memory>

#include "cache/common/closure.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/messenger.h"
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

  ProtectDomain* GetProtectDomain() const { return protect_domain_.get(); }

 private:
  DeviceUPtr device_;
  PortUPtr port_;
  ProtectDomainUPtr protect_domain_;
};

using ListenerUPtr = std::unique_ptr<Listener>;

class InfinibandServiceImpl final : public pb::infiniband::InfinibandService {
 public:
  InfinibandServiceImpl(Listener* listener, Messenger* messenger);

  void Sync(google::protobuf::RpcController* controller,
            const pb::infiniband::SyncRequest* request,
            pb::infiniband::SyncResponse* response,
            google::protobuf::Closure* done) override;

 private:
  Listener* listener_;
  Messenger* messenger_;
};

class RDMAClosure : public ::google::protobuf::Closure {
 public:
  void Wait() { countdown_.wait(); }
  void Run() override { countdown_.signal(); }

 private:
  bthread::CountdownEvent countdown_{1};
};

class ServerRDMASession : public EventHandler {
 public:
  ServerRDMASession(ConnectionUPtr conn, Messenger* messenger);
  void Start();
  void Shutdown();

  void HandleEvent() override;

  Status OnEstablished();
  void OnRequestReceived(const WorkCompletion& wc);
  Status ProcessRequest(Controller* cntl,
                        pb::infiniband::InfinibandResponse* ib_response);
  Status SendResponse(Controller* cntl,
                      pb::infiniband::InfinibandResponse* ib_response);
  void OnResponseSent(const WorkCompletion& wc);

 private:
  static int HandleWorkCompletion(void* meta,
                                  bthread::TaskIterator<CompletionBatch>& iter);

  Messenger* messenger_;
  ConnectionUPtr conn_;
  bthread::ExecutionQueueId<CompletionBatch> handle_wc_queue_id_;
};

struct ServerOptions {
  brpc::Server* brpc_server{nullptr};
};

class Server {
 public:
  Server();
  Status Start(const EndPoint& ep, ServerOptions* options);
  Status Shutdown();

  ProtectDomain* GetProtectDomain() const { return listener_->GetProtectDomain(); }

  template <typename F>
  void RegisterHandler(F&& handler) {
    messenger_->RegisterHandler(std::forward<F>(handler));
  }

 private:
  ListenerUPtr listener_;
  MessengerUPtr messenger_;
  std::unique_ptr<pb::infiniband::InfinibandService> service_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_SERVER_H_
