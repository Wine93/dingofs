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
  bool succ = conn_->HandleCompletion([this](WorkCompletions wcs) {
    CHECK_EQ(0, bthread::execution_queue_execute(handle_wc_queue_id_, wcs));
  });
  CHECK(succ);
}

int ClientSession::HandleWorkCompletion(
    void* meta, bthread::TaskIterator<WorkCompletions>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* session = static_cast<ClientSession*>(meta);
  for (; iter; iter++) {
    for (const auto& wc : *iter) {
      //
    }
    // auto& batch = *iter;
    //  for (const auto& wc : *batch.wcs) {
    //    switch (wc.optype) {
    //      case OpType::kSend:  // request sent
    //        session->OnRequestSent(wc);
    //        break;

    //    case OpType::kRecv:  // response received
    //      session->OnResponseReceived(wc);
    //      break;

    //    default:
    //      CHECK(false) << "Unexpected work completion opcode";
    //      break;
    //  }
    //}
  }

  return 0;
}

void ClientSession::OnSuccess(const WorkCompletion& wc) {
  if (wc.opcode == OpCode::kSend) {
    OnRequestSent(wc);
  } else if (wc.opcode == OpCode::kRecv) {
    OnResponseReceived(wc);
  } else {
    //
  }
}

void ClientSession::OnError(const WorkCompletion& wc) {
  //
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
    // entry.id = WorkRequestCtx(OpCode::kRecv, buffer).Id();
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
                                  const google::protobuf::Message& request) {
  // butil::Timer timer;
  // timer.start();

  auto* mem_pool = conn_->GetSendMemPool();
  auto* buffer = mem_pool->Require();
  if (buffer == nullptr) {
    LOG(ERROR) << "Fail to require send buffer";
    return Status::Internal("require send buffer failed");
  }

  // timer.stop();

  // LOG(INFO) << "<<< Require send buffer cost "
  //           << absl::StrFormat("%.6lf", timer.u_elapsed(0) / 1e6)
  //           << " seconds.";

  BRPC_SCOPE_EXIT { mem_pool->Release(buffer); };

  // timer.start();
  pb::infiniband::InfinibandRequest ib_request;
  auto& ctx = cntl->request_memory_context;
  ib_request.mutable_memory_context()->set_addr(ctx.addr);
  ib_request.mutable_memory_context()->set_length(ctx.length);
  ib_request.mutable_memory_context()->set_rkey(ctx.rkey);

  auto status = Protocol::PackBody(request, &ib_request);
  if (!status.ok()) {
    return status;
  }

  status = Protocol::Serialize(ib_request, cntl->correlation_id, buffer->data,
                               buffer->capacity, &buffer->length);
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

  // timer.stop();

  // LOG(INFO) << "<<< Post send work request cost "
  //           << absl::StrFormat("%.6lf", timer.u_elapsed(0) / 1e6)
  //           << " seconds.";

  // timer.start();
  cntl->request_sent.wait();
  // timer.stop();

  // LOG(INFO) << "<<< Wait request sent success cost "
  //           << absl::StrFormat("%.6lf", timer.u_elapsed(0) / 1e6)
  //           << " seconds.";

  return cntl->status;
}

void ClientSession::OnRequestSent(const WorkCompletion& wc) {
  // LOG(INFO) << "<<< OnRequestSent()";

  butil::Timer timer;
  timer.start();

  auto* cntl = reinterpret_cast<Controller*>(wc.id);
  cntl->status = wc.status;
  cntl->request_sent.signal();

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
  auto status = Protocol::PeekCorrelationId(buffer->data, buffer->length,
                                            &correlation_id);
  CHECK(status.ok());

  auto* cntl = reinterpret_cast<Controller*>(correlation_id);
  cntl->response_buffer = buffer;
  cntl->response_received.signal();
}

Status ClientSession::ProcessResponse(Controller* cntl,
                                      google::protobuf::Message* response) {
  auto* buffer = cntl->response_buffer;
  BRPC_SCOPE_EXIT {
    RecvWorkRequest entry;
    entry.id = reinterpret_cast<uint64_t>(buffer);
    entry.addr = reinterpret_cast<uint64_t>(buffer->data);
    entry.length = buffer->capacity;
    entry.lkey = buffer->lkey;

    CHECK(conn_->PostRecvWorkRequest(entry).ok());
  };

  pb::infiniband::InfinibandResponse ib_response;
  auto status = Protocol::Parse(buffer->data, buffer->length, &ib_response);
  if (status.ok()) {
    status = Protocol::UnpackBody(ib_response, response);
  }
  return status;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
