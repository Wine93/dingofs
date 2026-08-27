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

#include "blockcache/infiniband/server/listener.h"

#include <butil/memory/scope_guard.h>

#include <glog/logging.h>

#include <cerrno>
#include <memory>
#include <utility>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/infiniband.h"
#include "blockcache/infiniband/server/session.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

Listener::Listener(ServiceRegistry* services)
    : infiniband_(ThisInfiniband()),
      services_(services),
      sessions_(
          std::make_unique<SessionRegistry<ServerSession>>(infiniband_)) {
  CHECK(infiniband_ != nullptr) << "a listener is born on its serving shard";
  sessions_->StartSweep();
}

// The wire is only born ready: the queue pairs are created and pushed to RTS
// first, and only a wire that made it gets a session.
Future<Status> Listener::Accept(HandshakeMsg peer, HandshakeMsg* mine) {
  Gate::Holder holder(infiniband_->gate());
  if (!holder.ok()) {
    co_return ToStatus(ECANCELED, "accept a connection: context is stopping");
  }

  co_await sessions_->Reap();

  Status status = infiniband_->CheckPeer(peer);
  if (!status.ok()) {
    co_return status;
  }
  status = infiniband_->ReserveWrs();
  if (!status.ok()) {
    co_return status;
  }
  // The budget belongs to the session once admitted; every earlier exit
  // hands it back.
  bool admitted = false;
  BRPC_SCOPE_EXIT {
    if (!admitted) {
      infiniband_->UnreserveWrs();
    }
  };

  StatusOr<QueuePairGroup> group = infiniband_->CreateQueuePairGroup();
  if (!group.ok()) {
    co_return group.status();
  }
  infiniband_->FillHandshake(group.value(), mine);
  // Nothing is posted yet, so a group that falls short of RTS just dies.
  status = group.value().ModifyToReady({peer.qps, peer.num_qps});
  if (!status.ok()) {
    co_return status;
  }

  auto session = std::make_unique<ServerSession>(
      infiniband_, std::move(group).value(), peer, services_);
  status = session->Start();
  if (status.ok() && sessions_->Add(&session)) {
    admitted = true;
    co_return Status::OK();
  }

  co_await session->Shutdown();
  if (!status.ok()) {
    co_return status;
  }
  co_return ToStatus(ECANCELED, "complete a handshake: context stopping");
}

Future<> Listener::Shutdown() { return sessions_->ShutdownAll(); }

Future<> HandshakeService::Handshake(
    net::Controller* cntl, const pb::cache::v2::HandshakeRequest* request,
    pb::cache::v2::HandshakeResponse* response) {
  HandshakeMsg peer;
  if (!peer.FromPb(request->endpoint())) {
    cntl->SetFailed("the peer's handshake does not decode");
    co_return;
  }

  HandshakeMsg mine;
  const Status accepted = co_await listener_->Accept(peer, &mine);
  if (!accepted.ok()) {
    cntl->SetFailed(accepted.ToString());
    co_return;
  }
  mine.ToPb(response->mutable_endpoint());
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
