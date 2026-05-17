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
  auto* session = new ServerRDMASession(std::move(conn), messenger_);
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

ServerRDMASession::ServerRDMASession(ConnectionUPtr conn, Messenger* messenger)
    : conn_(std::move(conn)), messenger_(messenger) {}

void ServerRDMASession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&handle_wc_queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";
}

void ServerRDMASession::Shutdown() {
  CHECK_EQ(0, bthread::execution_queue_stop(handle_wc_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(handle_wc_queue_id_));
}

void ServerRDMASession::HandleEvent() {
  bool ok = conn_->HandleCompletion([this](CompletionBatch batch) {
    CHECK_EQ(0, bthread::execution_queue_execute(handle_wc_queue_id_, batch));
  });

  if (!ok) {
    LOG(ERROR) << "Fail to handle completion";
  }
}

int ServerRDMASession::HandleWorkCompletion(
    void* meta, bthread::TaskIterator<CompletionBatch>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* session = static_cast<ServerRDMASession*>(meta);
  for (; iter; iter++) {
    auto batch = *iter;
    for (int i = 0; i < batch.n; i++) {
      auto wc = batch.entries[i];
      switch (wc.optype) {
        case OpType::kRecv:  // request received
          session->OnRequestReceived(wc);
          break;

        case OpType::kSend:  // response sent
          session->OnResponseSent(wc);
          break;

        default:
          CHECK(false) << "Unexpected work completion opcode";
          break;
      }
    }
  }

  return 0;
}

Status ServerRDMASession::OnEstablished() {
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

void ServerRDMASession::OnRequestReceived(const WorkCompletion& wc) {
  auto* buffer = reinterpret_cast<Buffer*>(wc.id);
  CHECK_NOTNULL(buffer);
  buffer->length = wc.byte_len;

  auto tid = iutil::RunInBthread([this, buffer]() {
    auto* cntl = new Controller;
    cntl->request_buffer = buffer;
    pb::infiniband::InfinibandResponse response;
    auto status = ProcessRequest(cntl, &response);
    if (status.ok()) {
      status = SendResponse(cntl, &response);
    }

    if (!status.ok()) {
      LOG(ERROR) << "Fail to process request: " << status.ToString();
    }
    delete cntl;
  });
  if (tid != 0) {
    //
  }
}

Status ServerRDMASession::ProcessRequest(
    Controller* cntl, pb::infiniband::InfinibandResponse* ib_response) {
  // parse request
  auto* buffer = cntl->request_buffer;
  uint64_t correlation_id;
  auto status = Protocol::PeekCorrelationId(buffer->data, buffer->length,
                                            &cntl->correlation_id);
  if (!status.ok()) {
    return status;
  }

  pb::infiniband::InfinibandRequest ib_request;
  status = Protocol::Parse(buffer->data, buffer->length, &ib_request);
  if (!status.ok()) {
    // FIXME: repost receive work request
    LOG(ERROR) << "Fail to parse infiniband request";
    return status;
  }
  cntl->raddr = ib_request.meta().addr();
  cntl->rkey = ib_request.meta().lkey();

  // return receive work request
  RecvWorkRequest entry;
  entry.id = reinterpret_cast<uint64_t>(buffer);
  entry.addr = reinterpret_cast<uint64_t>(buffer->data);
  entry.length = buffer->capacity;
  entry.lkey = buffer->lkey;
  status = conn_->PostRecvWorkRequest(entry);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to post recvive work request";
    return status;
  }

  // dispatch handler to process request
  RDMAClosure done;
  status = messenger_->Dispatch(cntl, ib_request, ib_response, &done);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to dispatch request";
    return status;
  }
  done.Wait();
  return status;
}

Status ServerRDMASession::SendResponse(
    Controller* cntl, pb::infiniband::InfinibandResponse* ib_response) {
  std::vector<SendWorkRequest> entries;

  // part1: attchment
  auto* attachment = cntl->response_attachment;
  if (attachment != nullptr) {
    SendWorkRequest entry;
    entry.id = reinterpret_cast<uint64_t>(attachment);
    entry.optype = OpType::kWrite;
    entry.signal = false;
    entry.addr = reinterpret_cast<uint64_t>(attachment->data);
    entry.length = attachment->length;
    entry.lkey = attachment->lkey;
    entry.raddr = cntl->raddr;
    entry.rkey = cntl->rkey;

    entries.emplace_back(entry);
  }

  // part2: response
  auto* mem_pool = conn_->GetSendMemPool();
  auto* buffer = mem_pool->Require();
  cntl->request_buffer = buffer;
  if (buffer == nullptr) {
    return Status::Internal("require send buffer failed");
  }

  BRPC_SCOPE_EXIT { mem_pool->Release(buffer); };

  auto status =
      Protocol::Serialize(*ib_response, cntl->correlation_id, buffer->data,
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
  entries.emplace_back(entry);

  // post send work requests
  status = conn_->PostSendWorkRequests(entries);
  if (!status.ok()) {
    return status;
  }

  cntl->response_sent.wait();
  return cntl->status;
}

void ServerRDMASession::OnResponseSent(const WorkCompletion& wc) {
  auto* cntl = reinterpret_cast<Controller*>(wc.id);
  cntl->status = wc.status;
  cntl->response_sent.signal();
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
