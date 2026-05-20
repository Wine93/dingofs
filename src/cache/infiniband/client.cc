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

#include "cache/infiniband/client.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/endpoint.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/rdma_memory.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

Dialer::Dialer(Device* device, Port* port, ProtectDomain* protect_domain)
    : device_(device), port_(port), protect_domain_(protect_domain) {}

DialerUPtr Dialer::Create(const EndPoint& ep) {
  auto* pd = GetOrAllocPD(ep.device_name, ep.port_num);
  if (pd == nullptr) {
    return nullptr;
  }
  auto* device = GetCachedDevice(ep.device_name);
  auto* port = GetCachedPort(ep.device_name);
  CHECK_NOTNULL(device);
  CHECK_NOTNULL(port);
  return std::make_unique<Dialer>(device, port, pd);
}

ConnectionUPtr Dialer::Dial(const std::string& address) {
  auto completion_queue = CompletionQueue::Create(device_);
  if (nullptr == completion_queue) {
    LOG(ERROR) << "Fail to create completion queue";
    return nullptr;
  }

  auto queue_pair = QueuePair::Create(device_, port_, protect_domain_,
                                      completion_queue.get());
  if (nullptr == queue_pair) {
    LOG(ERROR) << "Fail to create queue pair";
    return nullptr;
  }

  ConnMangmentMeta local_cm_meta, remote_cm_meta;
  local_cm_meta.qpn = queue_pair->GetQpNum();
  local_cm_meta.lid = port_->GetLid();
  local_cm_meta.gid = port_->GetGid();
  local_cm_meta.port_num = port_->GetPortNum();
  local_cm_meta.link_type = port_->GetLinkLayer();
  local_cm_meta.mtu = port_->GetActiveMtu();
  auto status = SyncConnMangmentMeta(address, local_cm_meta, &remote_cm_meta);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to sync connection managment meta: "
               << status.ToString();
    return nullptr;
  }

  status = queue_pair->ModifyQpToInit();
  if (status.ok()) {
    status = queue_pair->ModifyQpToRtr(remote_cm_meta);
    if (status.ok()) {
      status = queue_pair->ModifyQpToRts();
    }
  }

  if (!status.ok()) {
    return nullptr;
  }

  return std::make_unique<Connection>(std::move(queue_pair),
                                      std::move(completion_queue));
}

Status Dialer::SyncConnMangmentMeta(const std::string& address,
                                    const ConnMangmentMeta& local_cm_meta,
                                    ConnMangmentMeta* remote_cm_meta) {
  butil::EndPoint ep;
  if (butil::str2endpoint(address.c_str(), &ep) != 0) {
    LOG(ERROR) << "Fail to parse address=" << address;
    return Status::Internal("str2endpoint failed");
  }

  brpc::Channel channel;
  brpc::ChannelOptions options;
  options.connect_timeout_ms = 1000;
  options.timeout_ms = 3000;
  if (channel.Init(ep, &options) != 0) {
    LOG(ERROR) << "Fail to init channel for address=" << address;
    return Status::Internal("init channel failed");
  }

  pb::infiniband::SyncRequest request;
  auto* cm = request.mutable_cm_meta();
  cm->set_qpn(local_cm_meta.qpn);
  cm->set_lid(local_cm_meta.lid);
  cm->set_gid(&local_cm_meta.gid, sizeof(local_cm_meta.gid));
  cm->set_port_num(local_cm_meta.port_num);
  cm->set_link_type(static_cast<uint32_t>(local_cm_meta.link_type));
  cm->set_mtu(static_cast<uint32_t>(local_cm_meta.mtu));

  brpc::Controller cntl;
  pb::infiniband::SyncResponse response;
  pb::infiniband::InfinibandService_Stub stub(&channel);
  stub.Sync(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    return Status::NetError(cntl.ErrorCode(), cntl.ErrorText());
  }

  const auto& peer = response.cm_meta();
  if (peer.gid().size() != sizeof(remote_cm_meta->gid)) {
    return Status::Internal("invalid gid size in sync response");
  }
  remote_cm_meta->qpn = peer.qpn();
  remote_cm_meta->lid = static_cast<uint16_t>(peer.lid());
  std::memcpy(&remote_cm_meta->gid, peer.gid().data(), sizeof(ibv_gid));
  remote_cm_meta->port_num = static_cast<uint8_t>(peer.port_num());
  remote_cm_meta->link_type = static_cast<LinkLayer>(peer.link_type());
  remote_cm_meta->mtu = static_cast<ibv_mtu>(peer.mtu());
  return Status::OK();
}

Client::Client(DialerUPtr dialer)
    : dialer_(std::move(dialer)), session_(nullptr) {}

Client::~Client() {
  if (session_ != nullptr) {
    int fd = session_->Fd();
    auto status = GetGlobalEventDispatcher(fd).DelEvent(fd);
    if (!status.ok()) {
      LOG(WARNING) << "Fail to remove RDMA client session event: "
                   << status.ToString();
    }
    session_->Shutdown();
  }
}

ClientUPtr Client::Create(const EndPoint& ep) {
  auto dialer = Dialer::Create(ep);
  if (nullptr == dialer) {
    return nullptr;
  }
  return std::make_unique<Client>(std::move(dialer));
}

Status Client::Connect(const std::string& address) {
  auto conn = dialer_->Dial(address);
  if (nullptr == conn) {
    LOG(ERROR) << "Fail to connect peer=" << address;
    return Status::Internal("dial peer failed");
  }

  int fd = conn->GetFd();
  auto session = std::make_unique<ClientSession>(std::move(conn));
  auto status = session->OnEstablished();
  if (!status.ok()) {
    return status;
  }

  session->Start();

  status = GetGlobalEventDispatcher(fd).AddEvent(fd, EventType::kReadEvent,
                                                 session.get());
  if (!status.ok()) {
    LOG(ERROR) << "Fail to add event to dispatcher";
    session->Shutdown();
    return status;
  }
  session_ = std::move(session);
  return Status::OK();
}

Status Client::DoCall(Controller* cntl, std::string_view service_name,
                      std::string_view method_name,
                      const google::protobuf::Message& request,
                      google::protobuf::Message* response) {
  auto* session = session_.get();
  CHECK_NOTNULL(session);

  auto status = session->SendRequest(cntl, service_name, method_name, request);
  if (status.ok()) {
    cntl->WaitResponseReceived();
    status = cntl->WcStatus();
    if (status.ok()) {
      status = session->ProcessResponse(cntl, response);
    }
  } else {
    LOG(ERROR) << "Fail to send request: " << status.ToString();
  }
  return status;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
