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
 * Created Date: 2026-05-14
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_

#include <bthread/countdown_event.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/service.h>

#include <cstdint>
#include <functional>
#include <string>
#include <utility>

#include "cache/infiniband/memory.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Connection;

struct MemoryContext {
  uint64_t addr{0};
  uint32_t length{0};
  uint32_t rkey{0};
};

// Per-RPC state shared between the transport and the user-defined Service
// method. Inherits from google::protobuf::RpcController so it can be passed
// through Service::CallMethod and downcast inside the method implementation
// (mirrors brpc::Controller).
//
// One Controller per RPC: created at the entry to an RPC, destroyed when
// the RPC is fully retired (response sent on the server, response parsed on
// the client). All synchronization primitives are one-shot.
class Controller : public ::google::protobuf::RpcController {
 public:
  Controller() = default;
  ~Controller() override {
    if (on_destroy_) on_destroy_();
  }

  // ---- google::protobuf::RpcController interface ----
  void Reset() override {
    failed_ = false;
    error_text_.clear();
  }
  bool Failed() const override { return failed_; }
  std::string ErrorText() const override { return error_text_; }
  void StartCancel() override {}
  void SetFailed(const std::string& reason) override {
    failed_ = true;
    error_text_ = reason;
  }
  bool IsCanceled() const override { return false; }
  void NotifyOnCancel(::google::protobuf::Closure* /*callback*/) override {}

  // ---- Correlation id (transport-assigned, used to match request/response).
  uint64_t CorrelationId() const { return correlation_id_; }
  void SetCorrelationId(uint64_t id) { correlation_id_ = id; }

  // ---- Transport-level status (last work-completion result, etc.) ----
  const dingofs::Status& WcStatus() const { return wc_status_; }
  void SetWcStatus(const dingofs::Status& s) { wc_status_ = s; }

  // ---- Arena: backing allocator for protobuf objects allocated on the
  // dispatch path. The Arena lives as long as this Controller; objects
  // allocated via Arena::CreateMessage<T>(cntl->Arena()) are reclaimed in
  // bulk when the Controller is destroyed.
  ::google::protobuf::Arena* Arena() { return &arena_; }

  // ---- Request side ----
  Buffer* RequestBuffer() const { return request_buffer_; }
  void SetRequestBuffer(Buffer* buffer) { request_buffer_ = buffer; }

  // RDMA memory context the client advertises: the address/length/rkey of a
  // remote region the server may RDMA_WRITE the attachment into.
  const MemoryContext& RequestMemoryContext() const {
    return request_memory_context_;
  }
  void SetRequestMemoryContext(const MemoryContext& ctx) {
    request_memory_context_ = ctx;
  }

  void WaitRequestSent() { request_sent_.wait(); }
  void SignalRequestSent() { request_sent_.signal(); }

  // ---- Connection back-pointer (server side; set when the request is
  // dispatched). The service handler uses this for handler-driven RDMA ops
  // such as ReadRequestAttachment().
  Connection* GetConnection() const { return conn_; }
  void SetConnection(Connection* conn) { conn_ = conn; }

  // Synchronously pull the client's advertised request payload via
  // RDMA_READ into `dst`. `dst` must be registered against the same PD as
  // this controller's connection. Blocks the calling bthread until the
  // work completion arrives. Must be called from inside a Service handler
  // (i.e. after the transport has populated RequestMemoryContext).
  Status ReadRequestAttachment(Buffer* dst, size_t length);

  void SignalRdmaReadDone(const dingofs::Status& s) {
    wc_status_ = s;
    rdma_read_done_.signal();
  }

  // ---- Response side ----
  Buffer* ResponseBuffer() const { return response_buffer_; }
  void SetResponseBuffer(Buffer* buffer) { response_buffer_ = buffer; }

  // Optional out-of-band payload the server may set from inside its service
  // method. The transport will RDMA_WRITE these bytes into the client's
  // RequestMemoryContext region before posting the response SEND.
  Buffer* ResponseAttachment() const { return response_attachment_; }
  void SetResponseAttachment(Buffer* buffer) {
    response_attachment_ = buffer;
  }

  // Client-side: number of bytes the server reports it RDMA_WRITTEN into
  // the client's advertised region. Populated by ClientSession from
  // ResponseMeta after the response SEND completes.
  uint32_t ResponseAttachmentLength() const {
    return response_attachment_length_;
  }
  void SetResponseAttachmentLength(uint32_t n) {
    response_attachment_length_ = n;
  }

  void WaitResponseSent() { response_sent_.wait(); }
  void SignalResponseSent() { response_sent_.signal(); }

  void WaitResponseReceived() { response_received_.wait(); }
  void SignalResponseReceived() { response_received_.signal(); }

  // ---- Cleanup hook ----
  // A closure to run inside ~Controller(). Useful for returning per-RPC
  // resources (e.g. attachment buffers) to a pool once the transport is
  // guaranteed done with them.
  void SetOnDestroy(std::function<void()> fn) {
    on_destroy_ = std::move(fn);
  }

 private:
  uint64_t correlation_id_{0};
  dingofs::Status wc_status_{dingofs::Status::Unknown("unknown")};
  ::google::protobuf::Arena arena_;

  Buffer* request_buffer_{nullptr};
  bthread::CountdownEvent request_sent_{1};
  MemoryContext request_memory_context_;
  Connection* conn_{nullptr};
  bthread::CountdownEvent rdma_read_done_{1};

  Buffer* response_buffer_{nullptr};
  Buffer* response_attachment_{nullptr};
  uint32_t response_attachment_length_{0};
  bthread::CountdownEvent response_sent_{1};
  bthread::CountdownEvent response_received_{1};

  std::function<void()> on_destroy_;

  bool failed_{false};
  std::string error_text_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_
