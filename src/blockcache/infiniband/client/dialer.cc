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

#include "blockcache/infiniband/client/dialer.h"

#include <butil/memory/scope_guard.h>

#include <glog/logging.h>

#include <cerrno>
#include <memory>
#include <utility>

#include "blockcache/infiniband/client/channel.h"
#include "blockcache/infiniband/client/client.h"
#include "blockcache/infiniband/client/session.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/infiniband.h"
#include "blockcache/utils/gate.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// Mirrors Listener::Accept: the queue pairs are created and pushed to RTS
// first, and only a wire that made it gets a session.
Future<StatusOr<net::Channel*>> Dialer::Dial(net::DialOption option) {
  Client* client = Client::This();
  if (client == nullptr) {
    co_return Status::Internal("no rdma client on this shard");
  }
  Infiniband* infiniband = client->context();

  Gate::Holder holder(infiniband->gate());
  if (!holder.ok()) {
    co_return ToStatus(ECANCELED, "open a connection: the context is stopping");
  }

  co_await client->sessions().Reap();

  Status status = infiniband->ReserveWrs();
  if (!status.ok()) {
    co_return status;
  }
  // The budget belongs to the session once admitted; every earlier exit
  // hands it back.
  bool admitted = false;
  BRPC_SCOPE_EXIT {
    if (!admitted) {
      infiniband->UnreserveWrs();
    }
  };

  StatusOr<QueuePairGroup> group = infiniband->CreateQueuePairGroup();
  if (!group.ok()) {
    co_return group.status();
  }

  HandshakeMsg mine;
  infiniband->FillHandshake(group.value(), &mine);

  pb::cache::v2::HandshakeRequest request;
  mine.ToPb(request.mutable_endpoint());
  request.mutable_context()->set_routing_key(option.routing_key);
  pb::cache::v2::HandshakeResponse response;

  Status ready =
      co_await handshake_client_->Handshake(option, request, &response);

  HandshakeMsg peer;
  if (ready.ok() && !peer.FromPb(response.endpoint())) {
    ready = ToStatus(EPROTO, "read the handshake reply");
  }
  if (ready.ok()) {
    ready = infiniband->CheckPeer(peer);
  }
  if (ready.ok()) {
    ready = group.value().ModifyToReady({peer.qps, peer.num_qps});
  }
  // Nothing is posted yet, so a group that falls short of RTS just dies.
  if (!ready.ok()) {
    co_return ready;
  }

  auto dialing = std::make_unique<ClientSession>(
      infiniband, std::move(group).value(), peer);
  ClientSession* dialed = dialing.get();
  Status opened = dialing->Start();
  if (!opened.ok() || !client->sessions().Add(&dialing)) {
    co_await dialed->Shutdown();
    if (!opened.ok()) {
      co_return opened;
    }
    co_return ToStatus(ECANCELED, "complete a handshake: context stopping");
  }
  admitted = true;

  dialed->connection().StartKeepalive(PingIntervalNs());

  if (option.expected_shard != UINT32_MAX &&
      peer.shard != option.expected_shard) {
    LOG_EVERY_N(WARNING, 100)
        << "The peer at " << option.server
        << " answered the handshake from shard " << peer.shard << ", not "
        << option.expected_shard << "; its shard count changed under us";
  }
  co_return static_cast<net::Channel*>(&dialed->channel());
}

Future<> Dialer::Close(net::Channel* channel) {
  Client* client = Client::This();
  CHECK(client != nullptr) << "Close after the rdma client shut down";
  co_await static_cast<Channel*>(channel)->session()->Shutdown();
  co_await client->sessions().Reap();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
