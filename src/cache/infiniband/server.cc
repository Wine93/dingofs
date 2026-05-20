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

#include "cache/infiniband/server.h"

#include <brpc/controller.h>
#include <bthread/countdown_event.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstring>
#include <memory>
#include <utility>

#include "cache/common/closure.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/messenger.h"
#include "cache/infiniband/protocol.h"
#include "cache/infiniband/server_session.h"
#include "cache/iutil/bthread.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

Listener::Listener()
    : device_(nullptr), port_(nullptr), protect_domain_(nullptr) {}

Status Listener::Listen(const EndPoint& ep) {
  device_ = Device::Open(ep.device_name);
  if (nullptr == device_) {
    LOG(ERROR) << "Fail to open device=" << ep.device_name;
    return Status::Internal("open device failed");
  }

  port_ = Port::Query(device_.get(), ep.port_num);
  if (nullptr == port_) {
    LOG(ERROR) << "Fail to query port=" << (int)ep.port_num
               << " of device=" << ep.device_name;
    return Status::Internal("query port failed");
  } else if (port_->GetPortState() != PortState::kActive) {
    LOG(ERROR) << "Port=" << (int)ep.port_num << " of device=" << ep.device_name
               << " is not active";
    return Status::Internal("port is not active");
  } else if (port_->GetLinkLayer() == LinkLayer::kUnspecified) {
    LOG(ERROR) << "Port=" << (int)ep.port_num << " of device=" << ep.device_name
               << " has unspecified link layer";
    return Status::Internal("unspecified link layer");
  }

  protect_domain_ = ProtectDomain::Alloc(device_.get());
  if (nullptr == protect_domain_) {
    LOG(ERROR) << "Fail to alloc protect domain of device=" << ep.device_name;
    return Status::Internal("alloc protect domain failed");
  }

  LOG(INFO) << "RDMA listener is listening on " << ep.device_name << ":"
            << static_cast<int>(ep.port_num);
  return Status::OK();
}

ConnectionUPtr Listener::Accept(const ConnMangmentMeta& remote_cm_meta) {
  LOG(INFO) << "Accepting RDMA connection: peer=" << remote_cm_meta;

  auto completion_queue = CompletionQueue::Create(device_.get());
  if (nullptr == completion_queue) {
    LOG(ERROR) << "Fail to create completion queue";
    return nullptr;
  }

  auto queue_pair =
      QueuePair::Create(device_.get(), port_.get(), protect_domain_.get(),
                        completion_queue.get());
  if (nullptr == queue_pair) {
    LOG(ERROR) << "Fail to create queue pair";
    return nullptr;
  }

  auto status = queue_pair->ModifyQpToInit();
  if (!status.ok()) {
    return nullptr;
  }

  status = queue_pair->ModifyQpToRtr(remote_cm_meta);
  if (!status.ok()) {
    return nullptr;
  }

  status = queue_pair->ModifyQpToRts();
  if (!status.ok()) {
    return nullptr;
  }

  return std::make_unique<Connection>(std::move(queue_pair),
                                      std::move(completion_queue));
}

InfinibandServiceImpl::InfinibandServiceImpl(Listener* listener,
                                             Messenger* messenger)
    : listener_(listener), messenger_(messenger) {
  CHECK_NOTNULL(listener_);
  CHECK_NOTNULL(messenger_);
}

void InfinibandServiceImpl::Sync(google::protobuf::RpcController* controller,
                                 const pb::infiniband::SyncRequest* request,
                                 pb::infiniband::SyncResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);

  const auto& peer = request->cm_meta();
  if (peer.gid().size() != sizeof(ibv_gid)) {
    LOG(ERROR) << "Invalid gid size=" << peer.gid().size()
               << " in sync request";
    cntl->SetFailed("invalid gid size in sync request");
    return;
  }

  ConnMangmentMeta remote_cm_meta;
  remote_cm_meta.qpn = peer.qpn();
  remote_cm_meta.lid = static_cast<uint16_t>(peer.lid());
  std::memcpy(&remote_cm_meta.gid, peer.gid().data(), sizeof(ibv_gid));
  remote_cm_meta.port_num = static_cast<uint8_t>(peer.port_num());
  remote_cm_meta.link_type = static_cast<LinkLayer>(peer.link_type());
  remote_cm_meta.mtu = static_cast<ibv_mtu>(peer.mtu());

  auto conn = listener_->Accept(remote_cm_meta);
  if (conn == nullptr) {
    LOG(ERROR) << "Fail to accept connection";
    cntl->SetFailed("accept connection failed");
    return;
  }

  auto* qp = conn->GetQueuePair();
  auto* port = qp->GetPort();
  ConnMangmentMeta local_cm_meta;
  local_cm_meta.qpn = qp->GetQpNum();
  local_cm_meta.lid = port->GetLid();
  local_cm_meta.gid = port->GetGid();
  local_cm_meta.port_num = port->GetPortNum();
  local_cm_meta.link_type = port->GetLinkLayer();
  local_cm_meta.mtu = port->GetActiveMtu();

  int fd = conn->GetFd();

  // Session takes ownership of the connection and lives until the connection
  // is torn down. TODO(wine93): wire session lifecycle to a server-side
  // registry so it can be reclaimed on shutdown / connection close.
  // FIXME: delete session when connection closed
  auto* session = new ServerSession(std::move(conn), messenger_);
  session->Start();

  auto status = session->OnEstablished();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to establish session: " << status.ToString();
    session->Shutdown();
    delete session;
    cntl->SetFailed("establish session failed: " + status.ToString());
    return;
  }

  status =
      GetGlobalEventDispatcher(fd).AddEvent(fd, EventType::kReadEvent, session);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to register event: " << status.ToString();
    session->Shutdown();
    delete session;
    cntl->SetFailed("register event failed: " + status.ToString());
    return;
  }

  auto* cm = response->mutable_cm_meta();
  cm->set_qpn(local_cm_meta.qpn);
  cm->set_lid(local_cm_meta.lid);
  cm->set_gid(&local_cm_meta.gid, sizeof(local_cm_meta.gid));
  cm->set_port_num(local_cm_meta.port_num);
  cm->set_link_type(static_cast<uint32_t>(local_cm_meta.link_type));
  cm->set_mtu(static_cast<uint32_t>(local_cm_meta.mtu));

  LOG(INFO) << "Accepted RDMA connection: peer=" << remote_cm_meta
            << " local=" << local_cm_meta << " fd=" << fd;
}

Server::Server()
    : listener_(std::make_unique<Listener>()),
      messenger_(std::make_unique<Messenger>()),
      service_(std::make_unique<InfinibandServiceImpl>(listener_.get(),
                                                       messenger_.get())) {}

Status Server::Start(const EndPoint& ep, ServerOptions* options) {
  auto* brpc_server = options->brpc_server;
  CHECK_NOTNULL(brpc_server);  // TODO

  auto status = listener_->Listen(ep);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start listener: " << status.ToString();
    return status;
  }

  int rc = brpc_server->AddService(service_.get(), brpc::SERVER_OWNS_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Fail to add InfinibandService to brpc server";
    return Status::Internal("add service failed");
  }

  return Status::OK();
}

Status Server::Shutdown() {
  //
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
