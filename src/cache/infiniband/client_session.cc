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
 * Created Date: 2026-04-29
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/client_session.h"

#include <bthread/execution_queue.h>
#include <butil/memory/scope_guard.h>
#include <butil/time.h>
#include <glog/logging.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "cache/common/closure.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/protocol.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

ClientSession::ClientSession(ConnectionUPtr conn)
    : conn_(std::move(conn)), handle_wc_queue_id_({0}) {}

void ClientSession::Start() {
  bthread::ExecutionQueueOptions options;
  options.use_pthread = true;
  CHECK_EQ(0, bthread::execution_queue_start(&handle_wc_queue_id_, &options,
                                             HandleWorkCompletion, this))
      << "Fail to start ExecutionQueue for handle work completion";
}

void ClientSession::Shutdown() {
  CHECK_EQ(0, bthread::execution_queue_stop(handle_wc_queue_id_));
  CHECK_EQ(0, bthread::execution_queue_join(handle_wc_queue_id_));
}

void ClientSession::HandleEvent() {
  // Skip the per-WC handoff to a dedicated ExecutionQueue consumer: the
  // callbacks here (OnRequestSent / OnResponseReceived) are signal-only and
  // do not block, so running them inline on the epoll thread removes one
  // cross-thread wakeup per WC.
  conn_->HandleCompletion([this](WorkCompletions wcs) {
    for (const auto& wc : wcs) {
      if (wc.status.ok()) {
        OnSuccess(wc);
      } else {
        OnError(wc);
      }
    }
  });
}

int ClientSession::HandleWorkCompletion(
    void* meta, bthread::TaskIterator<WorkCompletions>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* session = static_cast<ClientSession*>(meta);
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

void ClientSession::OnSuccess(const WorkCompletion& wc) {
  if (wc.opcode == OpCode::kSend) {
    OnRequestSent(wc);
  } else if (wc.opcode == OpCode::kRecv) {
    OnResponseReceived(wc);
  } else {
    LOG(WARNING) << "Unexpected work completion opcode="
                 << static_cast<int>(wc.opcode);
  }
}

void ClientSession::OnError(const WorkCompletion& wc) {
  LOG(ERROR) << "Work completion in error state: opcode="
             << static_cast<int>(wc.opcode) << " wc.id=" << wc.id
             << " status=" << wc.status.ToString();
}

Status ClientSession::OnEstablished() {
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

Status ClientSession::SendRequest(Controller* cntl,
                                  std::string_view service_name,
                                  std::string_view method_name,
                                  const google::protobuf::Message& request) {
  auto* mem_pool = conn_->GetSendMemPool();
  auto* buffer = mem_pool->Require();
  if (buffer == nullptr) {
    LOG(ERROR) << "Fail to require send buffer";
    return Status::Internal("require send buffer failed");
  }

  BRPC_SCOPE_EXIT { mem_pool->Release(buffer); };

  // Build RequestMeta on the Controller's Arena to avoid a per-RPC heap
  // allocation for the wrapper protobuf.
  auto* meta = ::google::protobuf::Arena::CreateMessage<
      pb::infiniband::RequestMeta>(cntl->Arena());
  meta->set_service_name(std::string(service_name));
  meta->set_method_name(std::string(method_name));
  const auto& ctx = cntl->RequestMemoryContext();
  auto* mc = meta->mutable_memory_context();
  mc->set_addr(ctx.addr);
  mc->set_length(ctx.length);
  mc->set_rkey(ctx.rkey);

  auto status =
      Protocol::SerializeFrame(buffer, cntl->CorrelationId(), *meta, request);
  if (!status.ok()) {
    return status;
  }

  // timer.stop();

  // LOG(INFO) << "<<< Serialize send request cost "
  //           << absl::StrFormat("%.6lf", timer.u_elapsed(0) / 1e6)
  //           << " seconds.";

  // timer.start();

  SendWorkRequest entry;
  entry.id = reinterpret_cast<uint64_t>(cntl);
  entry.opcode = OpCode::kSend;
  entry.signaled = true;
  entry.addr = reinterpret_cast<uint64_t>(buffer->data);
  entry.length = buffer->length;
  entry.lkey = buffer->lkey;

  status = conn_->PostSendWorkRequest(entry);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to post send work request: " << status.ToString();
    return status;
  }

  // timer.stop();

  // LOG(INFO) << "<<< Post send work request cost "
  //           << absl::StrFormat("%.6lf", timer.u_elapsed(0) / 1e6)
  //           << " seconds.";

  // timer.start();
  cntl->WaitRequestSent();
  // timer.stop();

  // LOG(INFO) << "<<< Wait request sent success cost "
  //           << absl::StrFormat("%.6lf", timer.u_elapsed(0) / 1e6)
  //           << " seconds.";

  return cntl->WcStatus();
}

void ClientSession::OnRequestSent(const WorkCompletion& wc) {
  // LOG(INFO) << "<<< OnRequestSent()";

  butil::Timer timer;
  timer.start();

  auto* cntl = reinterpret_cast<Controller*>(wc.id);
  cntl->SetWcStatus(wc.status);
  cntl->SignalRequestSent();

  timer.stop();
  // LOG(INFO) << "<<< Notify request sent cost "
  //           << absl::StrFormat("%.6lf", timer.u_elapsed() * 1.0 / 1e6)
  //           << " seconds.";
}

void ClientSession::OnResponseReceived(const WorkCompletion& wc) {
  auto* buffer = reinterpret_cast<Buffer*>(wc.id);
  CHECK_NOTNULL(buffer);
  buffer->length = wc.byte_len;

  uint64_t correlation_id;
  auto status = Protocol::PeekCorrelationId(buffer, &correlation_id);
  CHECK(status.ok());

  auto* cntl = reinterpret_cast<Controller*>(correlation_id);
  cntl->SetResponseBuffer(buffer);
  cntl->SignalResponseReceived();
}

Status ClientSession::ProcessResponse(Controller* cntl,
                                      google::protobuf::Message* response) {
  auto* buffer = cntl->ResponseBuffer();
  BRPC_SCOPE_EXIT {
    RecvWorkRequest entry;
    entry.id = reinterpret_cast<uint64_t>(buffer);
    entry.addr = reinterpret_cast<uint64_t>(buffer->data);
    entry.length = buffer->capacity;
    entry.lkey = buffer->lkey;

    CHECK(conn_->PostRecvWorkRequest(entry).ok());
  };

  pb::infiniband::ResponseMeta resp_meta;
  std::string_view data;
  uint64_t correlation_id = 0;
  auto status =
      Protocol::ParseFrame(buffer, &correlation_id, &resp_meta, &data);
  if (!status.ok()) {
    return status;
  }
  if (resp_meta.error_code() != 0) {
    LOG(ERROR) << "RDMA RPC failed on server: code=" << resp_meta.error_code()
               << " msg=" << resp_meta.error_message();
    return Status::Internal(resp_meta.error_message());
  }
  cntl->SetResponseAttachmentLength(resp_meta.memory_context().attachment_length());
  if (!response->ParseFromArray(data.data(), static_cast<int>(data.size()))) {
    LOG(ERROR) << "Fail to parse response body";
    return Status::InvalidParam("parse response body failed");
  }
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
