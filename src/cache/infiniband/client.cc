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

#include <absl/strings/str_format.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <butil/endpoint.h>
#include <butil/iobuf.h>
#include <butil/memory/scope_guard.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstdint>
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
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

Dialer::Dialer(DeviceUPtr device, PortUPtr port,
               ProtectDomainUPtr protect_domain)
    : device_(std::move(device)),
      port_(std::move(port)),
      protect_domain_(std::move(protect_domain)) {}

DialerUPtr Dialer::Create(const EndPoint& ep) {
  auto device = Device::Open(ep.device_name);
  if (nullptr == device) {
    LOG(ERROR) << "Fail to open device=" << ep.device_name;
    return nullptr;
  }

  auto port = Port::Query(device.get(), ep.port_num);
  if (nullptr == port) {
    LOG(ERROR) << "Fail to query port=" << (int)ep.port_num
               << " of device=" << ep.device_name;
    return nullptr;
  } else if (port->GetPortState() != PortState::kActive) {
    LOG(ERROR) << "Port=" << (int)ep.port_num << " of device=" << ep.device_name
               << " is not active";
    return nullptr;
  } else if (port->GetLinkLayer() == LinkLayer::kUnspecified) {
    LOG(ERROR) << "Port=" << (int)ep.port_num << " of device=" << ep.device_name
               << " has unspecified link layer";
    return nullptr;
  }

  auto protect_domain = ProtectDomain::Alloc(device.get());
  if (nullptr == protect_domain) {
    LOG(ERROR) << "Fail to alloc protect domain of device=" << ep.device_name;
    return nullptr;
  }

  return std::make_unique<Dialer>(std::move(device), std::move(port),
                                  std::move(protect_domain));
}

ConnectionUPtr Dialer::Dial(const std::string& address) {
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

ClientRDMASession::ClientRDMASession(ConnectionUPtr conn)
    : conn_(std::move(conn)) {}

void ClientRDMASession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&handle_wc_queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";
}

void ClientRDMASession::Shutdown() {
  CHECK_EQ(0, bthread::execution_queue_stop(handle_wc_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(handle_wc_queue_id_));
}

void ClientRDMASession::HandleEvent() {
  bool ok = conn_->HandleCompletion([this](CompletionBatch batch) {
    CHECK_EQ(0, bthread::execution_queue_execute(handle_wc_queue_id_, batch));
  });

  if (!ok) {
    LOG(ERROR) << "Fail to handle completion";
  }
}

int ClientRDMASession::HandleWorkCompletion(
    void* meta, bthread::TaskIterator<CompletionBatch>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* session = static_cast<ClientRDMASession*>(meta);
  for (; iter; iter++) {
    auto batch = *iter;
    for (int i = 0; i < batch.n; i++) {
      auto wc = batch.entries[i];
      switch (wc.optype) {
        case OpType::kSend:  // request sent
          session->OnRequestSent(wc);
          break;

        case OpType::kRecv:  // response received
          session->OnResponseReceived(wc);
          break;

        default:
          CHECK(false) << "Unexpected work completion opcode";
          break;
      }
    }
  }

  return 0;
}

Status ClientRDMASession::OnEstablished() {
  auto* mem_pool = conn_->GetRecvMemPool();
  std::vector<RecvWorkRequest> entries;

  do {
    auto* buffer = mem_pool->Require();
    if (nullptr == buffer) {
      break;
    }

    RecvWorkRequest entry;
    entry.id = reinterpret_cast<uint64_t>(buffer);
    entry.addr = reinterpret_cast<uint64_t>(buffer->data);
    entry.length = buffer->capacity;
    entry.lkey = buffer->lkey;

    entries.emplace_back(entry);
  } while (true);

  auto status = conn_->PostRecvWorkRequests(entries);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to post receive work requests";
    return status;
  }

  LOG(INFO) << "Successfully post " << entries.size()
            << " receive work requests";
  return Status::OK();
}

Status ClientRDMASession::SendRequest(
    Controller* cntl, const google::protobuf::Message& request) {
  auto* mem_pool = conn_->GetSendMemPool();
  auto* buffer = mem_pool->Require();
  if (buffer == nullptr) {
    LOG(ERROR) << "Fail to require send buffer";
    return Status::Internal("require send buffer failed");
  }

  BRPC_SCOPE_EXIT { mem_pool->Release(buffer); };

  pb::infiniband::InfinibandRequest ib_request;
  auto status = Protocol::PackBody(request, &ib_request);
  if (!status.ok()) {
    return status;
  }

  status = Protocol::Serialize(ib_request, cntl->correlation_id, buffer->data,
                               buffer->capacity, &buffer->length);
  if (!status.ok()) {
    return status;
  }

  SendWorkRequest entry;
  entry.id = reinterpret_cast<uint64_t>(cntl);
  entry.optype = OpType::kSend;
  entry.signal = true;
  entry.addr = reinterpret_cast<uint64_t>(buffer->data);
  entry.length = buffer->length;
  entry.lkey = buffer->lkey;

  status = conn_->PostSendWorkRequest(entry);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to post send work request: " << status.ToString();
    return status;
  }

  cntl->request_sent.wait();
  return cntl->status;
}

void ClientRDMASession::OnRequestSent(const WorkCompletion& wc) {
  auto* cntl = reinterpret_cast<Controller*>(wc.id);
  cntl->request_sent.signal();
  cntl->status = wc.status;
}

void ClientRDMASession::OnResponseReceived(const WorkCompletion& wc) {
  auto* buffer = reinterpret_cast<Buffer*>(wc.id);
  CHECK_NOTNULL(buffer);
  buffer->length = wc.byte_len;

  uint64_t correlation_id;
  auto status = Protocol::PeekCorrelationId(buffer->data, buffer->length,
                                            &correlation_id);
  CHECK(status.ok());

  auto* cntl = reinterpret_cast<Controller*>(correlation_id);
  cntl->response_buffer = buffer;
  cntl->response_received.signal();
}

Status ClientRDMASession::ProcessResponse(Controller* cntl,
                                          google::protobuf::Message* response) {
  auto* buffer = cntl->response_buffer;
  BRPC_SCOPE_EXIT {
    auto* mem_pool = conn_->GetRecvMemPool();
    mem_pool->Release(buffer);
  };

  pb::infiniband::InfinibandResponse ib_response;
  auto status = Protocol::Parse(buffer->data, buffer->length, &ib_response);
  if (status.ok()) {
    status = Protocol::UnpackBody(ib_response, response);
  }
  return status;
}

Client::Client(DialerUPtr dialer)
    : dialer_(std::move(dialer)), session_(nullptr) {}

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
  session_ = std::make_unique<ClientRDMASession>(std::move(conn));
  auto status = session_->OnEstablished();
  if (!status.ok()) {
    return status;
  }

  session_->Start();

  status = GetGlobalEventDispatcher(fd).AddEvent(fd, EventType::kReadEvent,
                                                 session_.get());
  if (!status.ok()) {
    LOG(ERROR) << "Fail to add event to dispatcher";
    return status;
  }
  return Status::OK();
}

Status Client::DoCall(const google::protobuf::Message& request,
                      google::protobuf::Message* response) {
  auto* session = session_.get();
  CHECK_NOTNULL(session);

  auto* cntl = new Controller();
  auto status = session->SendRequest(cntl, request);
  if (status.ok()) {
    cntl->response_received.wait();
    status = session->ProcessResponse(cntl, response);
  }

  delete cntl;
  return status;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
