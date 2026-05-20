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

#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <butil/memory/scope_guard.h>
#include <glog/logging.h>

#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/protocol.h"
#include "cache/iutil/bthread.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {
namespace {

// Blocking closure used to wait for a Service handler's done->Run(). Distinct
// from cache::Closure (which is a status holder for callback-style flows) —
// this one provides a bthread CountdownEvent for synchronous dispatch.
class BlockingClosure : public ::google::protobuf::Closure {
 public:
  void Wait() { event_.wait(); }
  void Run() override { event_.signal(); }

 private:
  bthread::CountdownEvent event_{1};
};

}  // namespace

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
  conn_->HandleCompletion([this](WorkCompletions wcs) {
    CHECK_EQ(0, bthread::execution_queue_execute(handle_wc_queue_id_, wcs));
  });
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
  } else if (wc.opcode == OpCode::kRDMARead) {  // handler-driven RDMA_READ
    auto* cntl = reinterpret_cast<Controller*>(wc.id);
    cntl->NotifyRdmaReadDone(wc.status);
  } else {
    CHECK(false) << "Unexpected work completion opcode="
                 << static_cast<int>(wc.opcode);
  }
}

void ServerSession::OnError(const WorkCompletion& wc) {
  if (wc.opcode == OpCode::kRecv) {
    LOG(WARNING) << "Recv work completion in error state";
  } else if (wc.opcode == OpCode::kSend || wc.opcode == OpCode::kRDMAWrite) {
    OnResponseSent(wc);
  } else if (wc.opcode == OpCode::kRDMARead) {
    auto* cntl = reinterpret_cast<Controller*>(wc.id);
    cntl->NotifyRdmaReadDone(wc.status);
  } else {
    CHECK(false) << "Unexpected work completion opcode="
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

void ServerSession::OnRequestReceived(const WorkCompletion& wc) {
  auto* buffer = reinterpret_cast<Buffer*>(wc.id);
  CHECK_NOTNULL(buffer);
  buffer->length = wc.byte_len;

  auto tid = iutil::RunInBthread([this, buffer]() {
    auto* cntl = new Controller;
    cntl->SetRequestBuffer(buffer);
    cntl->SetConnection(conn_.get());
    pb::infiniband::ResponseMeta resp_meta;
    ::google::protobuf::Message* resp_body = nullptr;
    auto status = ProcessRequest(cntl, &resp_meta, &resp_body);
    if (status.ok()) {
      status = SendResponse(cntl, &resp_meta, resp_body);
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

Status ServerSession::ProcessRequest(
    Controller* cntl, pb::infiniband::ResponseMeta* resp_meta,
    ::google::protobuf::Message** resp_body) {
  auto* buffer = cntl->RequestBuffer();

  // Parse [Header][RequestMeta][data] from the recv buffer.
  pb::infiniband::RequestMeta req_meta;
  std::string_view data;
  uint64_t correlation_id = 0;
  auto status = Protocol::ParseFrame(buffer, &correlation_id, &req_meta, &data);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to parse infiniband frame";
    RecvWorkRequest entry;
    entry.id = reinterpret_cast<uint64_t>(buffer);
    entry.addr = reinterpret_cast<uint64_t>(buffer->data);
    entry.length = buffer->capacity;
    entry.lkey = buffer->lkey;
    auto repost_status = conn_->PostRecvWorkRequest(entry);
    if (!repost_status.ok()) {
      LOG(ERROR) << "Fail to repost receive work request: "
                 << repost_status.ToString();
    }
    return status;
  }
  cntl->SetCorrelationId(correlation_id);

  // RDMA memory context (where the server may RDMA_WRITE the attachment).
  MemoryContext ctx;
  ctx.addr = req_meta.memory_context().addr();
  ctx.length = req_meta.memory_context().length();
  ctx.rkey = req_meta.memory_context().rkey();
  cntl->SetRequestMemoryContext(ctx);

  // Dispatch via brpc-style Service::CallMethod. The Dispatch path allocates
  // request/response on cntl->Arena() — no per-RPC operator new/delete.
  BlockingClosure done;
  status = messenger_->Dispatch(cntl, req_meta, data.data(), data.size(),
                                resp_meta, resp_body, &done);
  RecvWorkRequest entry;
  entry.id = reinterpret_cast<uint64_t>(buffer);
  entry.addr = reinterpret_cast<uint64_t>(buffer->data);
  entry.length = buffer->capacity;
  entry.lkey = buffer->lkey;
  auto repost_status = conn_->PostRecvWorkRequest(entry);
  if (!repost_status.ok()) {
    LOG(ERROR) << "Fail to repost receive work request: "
               << repost_status.ToString();
    return repost_status;
  }
  if (!status.ok()) {
    LOG(ERROR) << "Fail to dispatch request: " << status.ToString();
    if (resp_meta->error_code() != 0) {
      return Status::OK();
    }
    return status;
  }
  done.Wait();
  return status;
}

Status ServerSession::SendResponse(
    Controller* cntl, pb::infiniband::ResponseMeta* resp_meta,
    const ::google::protobuf::Message* resp_body) {
  std::vector<SendWorkRequest> entries;

  // part1: attchment
  auto* attachment = cntl->ResponseAttachment();
  if (attachment != nullptr) {
    const auto& mc = cntl->RequestMemoryContext();
    if (attachment->length > mc.length) {
      resp_meta->set_error_code(-1);
      resp_meta->set_error_message("response attachment exceeds remote buffer");
    } else {
      SendWorkRequest entry;
      entry.id = reinterpret_cast<uint64_t>(cntl);
      entry.opcode = OpCode::kRDMAWrite;
      entry.signaled = false;
      entry.addr = reinterpret_cast<uint64_t>(attachment->data);
      entry.length = attachment->length;
      entry.lkey = attachment->lkey;
      entry.raddr = mc.addr;
      entry.rkey = mc.rkey;

      // Carry the attachment's effective length to the client via the response
      // meta — there is no transport-level signal for the bytes RDMA_WRITTEN.
      resp_meta->mutable_memory_context()->set_attachment_length(
          attachment->length);

      entries.emplace_back(entry);
    }
  }

  // part2: response [Header][ResponseMeta][response body]
  auto* mem_pool = conn_->GetSendMemPool();
  auto* buffer = mem_pool->Require();
  if (buffer == nullptr) {
    return Status::Internal("require send buffer failed");
  }

  BRPC_SCOPE_EXIT { mem_pool->Release(buffer); };

  auto status = Protocol::SerializeFrame(buffer, cntl->CorrelationId(),
                                         *resp_meta, resp_body);
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

  cntl->WaitResponseSent();  // We will release buffer on response sent
  return cntl->WcStatus();
}

void ServerSession::OnResponseSent(const WorkCompletion& wc) {
  auto* cntl = reinterpret_cast<Controller*>(wc.id);
  cntl->NotifyResponseSent(wc.status);
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
