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

#include "cache/infiniband/server_session.h"

#include <bthread/execution_queue.h>
#include <glog/logging.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "cache/common/closure.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/protocol.h"
#include "cache/iutil/bthread.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

ServerSession::ServerSession(ConnectionUPtr conn, Messenger* messenger)
    : conn_(std::move(conn)), messenger_(messenger) {}

void ServerSession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&handle_wc_queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";
}

void ServerSession::Shutdown() {
  CHECK_EQ(0, bthread::execution_queue_stop(handle_wc_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(handle_wc_queue_id_));
}

void ServerSession::HandleEvent() {
  bool succ = conn_->HandleCompletion([this](WorkCompletions wcs) {
    CHECK_EQ(0, bthread::execution_queue_execute(handle_wc_queue_id_, wcs));
  });
  CHECK(succ);
}

int ServerSession::HandleWorkCompletion(
    void* meta, bthread::TaskIterator<WorkCompletions>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* session = static_cast<ServerSession*>(meta);
  for (; iter; iter++) {
    for (const auto& wc : *iter) {
      if (wc.status.ok()) {
        session->OnSuccess(wc);
      } else {
        session->OnError(wc);
      }
    }
  }
  return 0;
}

void ServerSession::OnSuccess(const WorkCompletion& wc) {
  if (wc.opcode == OpCode::kRecv) {  // request received
    OnRequestReceived(wc);
  } else if (wc.opcode == OpCode::kSend) {  // response sent
    OnResponseSent(wc);
  } else {
    CHECK(false) << "Unexpected work completion opcode="
                 << static_cast<int>(wc.opcode);
  }
}

void ServerSession::OnError(const WorkCompletion& wc) {
  auto* id = reinterpret_cast<WorkRequestId*>(wc.id);
  if (id->opcode == OpCode::kRecv) {
    LOG(WARNING) << "";
  } else if (id->opcode == OpCode::kSend || id->opcode == OpCode::kRDMAWrite) {
    OnResponseSent(wc);
  } else {
    CHECK(false) << "Unexpected work request id opcode="
                 << static_cast<int>(wc.opcode);
  }
}

Status ServerSession::OnEstablished() {
  auto* mem_pool = conn_->GetRecvMemPool();
  std::vector<RecvWorkRequest> entries;

  do {
    auto* buffer = mem_pool->Require();
    if (nullptr == buffer) {
      break;
    }

    RecvWorkRequest entry;
    entry.id = reinterpret_cast<uint64_t>(&recvive_contexts_[buffer->index]);
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

void ServerSession::OnRequestReceived(const WorkCompletion& wc) {
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

class Closure : public ::google::protobuf::Closure {
 public:
  void Wait() { countdown_.wait(); }
  void Run() override { countdown_.signal(); }

 private:
  bthread::CountdownEvent countdown_{1};
};

Status ServerSession::ProcessRequest(
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

  // memory ctx
  auto& ctx = cntl->request_memory_context;
  ctx.addr = ib_request.memory_context().addr();
  ctx.length = ib_request.memory_context().length();
  ctx.rkey = ib_request.memory_context().rkey();

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
  Closure done;
  status = messenger_->Dispatch(cntl, ib_request, ib_response, &done);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to dispatch request";
    return status;
  }
  done.Wait();
  return status;
}

Status ServerSession::SendResponse(
    Controller* cntl, pb::infiniband::InfinibandResponse* ib_response) {
  std::vector<SendWorkRequest> entries;

  // part1: attchment
  auto* attachment = cntl->response_attachment;
  if (attachment != nullptr) {
    SendWorkRequest entry;
    entry.id = reinterpret_cast<uint64_t>(attachment);
    entry.optype = OpCode::kRDMAWrite;
    entry.signaled = false;
    entry.addr = reinterpret_cast<uint64_t>(attachment->data);
    entry.length = attachment->length;
    entry.lkey = attachment->lkey;
    entry.raddr = cntl->request_memory_context.addr;
    entry.rkey = cntl->request_memory_context.rkey;

    // FIXME: check cntl->request_memory_context.length >= attchment->length

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
  entry.opcode = OpCode::kSend;
  entry.signaled = true;
  entry.addr = reinterpret_cast<uint64_t>(buffer->data);
  entry.length = buffer->length;
  entry.lkey = buffer->lkey;
  entries.emplace_back(entry);

  // post send work requests
  status = conn_->PostSendWorkRequests(entries);
  if (!status.ok()) {
    return status;
  }

  cntl->response_sent.wait();  // We will release buffer on response sent
  return cntl->status;
}

void ServerSession::OnResponseSent(const SendWorkRequestCtx& ctx) {
  auto* cntl = reinterpret_cast<Controller*>(wc.id);
  cntl->status = wc.status;
  cntl->response_sent.signal();
  // LOG(INFO) << "<<< response sent";
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
