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

#ifndef DINGOFS_BLOCKCACHE_NET_BRPC_BRIDGE_H_
#define DINGOFS_BLOCKCACHE_NET_BRPC_BRIDGE_H_

#include <glog/logging.h>

#include <cstdint>
#include <span>
#include <string>
#include <utility>

#include "blockcache/common/brpc_headers.h"
#include "blockcache/common/route.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/runtime/shard_inbox.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/net/brpc_server.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {
namespace net {

// brpc resolves the verb itself, so a request arrives already parsed on a
// bthread. All that is left is to reach the shard that owns its key, run the
// coroutine there, and answer -- which is what everything below does.

struct BrpcCall;

// Carries a finished response attachment back to the shard that owns it.
struct AttachmentDoneWork : InboxWork {
  BrpcCall* call = nullptr;
};

// The untyped half of one call in flight: the hop, the answer, the attachment
// lifetime. The verb it invokes on the shard is the typed half below.
struct BrpcCall : InboxWork {
  BrpcCall(BrpcServer* s, ::brpc::Controller* c, google::protobuf::Closure* d)
      : server(s), cntl(c), done(d) {
    attachment_work.call = this;
    attachment_work.run = &BrpcCall::OnAttachmentReleasedOnShard;
  }

  virtual ~BrpcCall() {
    CHECK(!attachment_started || attachment_released)
        << "a response attachment was abandoned before brpc was done with it";
  }
  BrpcCall(const BrpcCall&) = delete;
  BrpcCall& operator=(const BrpcCall&) = delete;

  // Hands the answer to brpc; idempotent.
  void RunDone() {
    if (done == nullptr) {
      return;
    }
    google::protobuf::Closure* closure = done;
    // Everything below belongs to brpc and dies with the closure.
    done = nullptr;
    cntl = nullptr;
    if (server->reply_on_bthread()) {
      bthread_t tid;
      if (bthread_start_background(&tid, nullptr, &BrpcCall::RunDoneOnBthread,
                                   closure) == 0) {
        return;
      }
      LOG(ERROR) << "Fail to start a bthread for a reply; replying inline";
    }
    closure->Run();
  }

  static void* RunDoneOnBthread(void* arg) {
    static_cast<google::protobuf::Closure*>(arg)->Run();
    return nullptr;
  }

  void Retire() {
    server->CallFinished();
    delete this;
  }

  void Finish() {
    RunDone();
    Retire();
  }

  // The whole error path in one move: fail the call, answer, retire.
  void FailNow(int code, const std::string& reason) {
    if (cntl != nullptr) {
      cntl->SetFailed(code, "%s", reason.c_str());
    }
    Finish();
  }

  // Answers with `attachment` still borrowed; resolves once brpc is done
  // reading from it, which is what keeps the caller's frame -- and the buffer
  // it holds -- alive until then.
  Future<Status> ReplyWithAttachment(BufferView attachment) {
    attachment_started = true;
    Future<Status> released = attachment_done.GetFuture();
    // The buffer becomes the attachment: no copy; `meta` 0 is right for tcp.
    BrpcCall* self = this;
    cntl->response_attachment().append_user_data_with_meta(
        attachment.data, attachment.size,
        [self](void*) { self->OnAttachmentReleased(); }, 0);
    RunDone();
    return released;
  }

  // Runs on an unpredictable thread; the buffer is shard-owned, so hop.
  void OnAttachmentReleased() {
    if (IsOnShard(shard)) {
      Release();
      return;
    }
    if (!PostTo(shard, &attachment_work)) {
      LOG(ERROR) << "Fail to release a response attachment to shard " << shard
                 << ": it is stopping";
    }
  }

  void Release() {
    attachment_released = true;
    attachment_done.SetValue(Status::OK());
  }

  static void OnAttachmentReleasedOnShard(InboxWork* base) {
    static_cast<AttachmentDoneWork*>(base)->call->Release();
  }

  BrpcServer* server;
  ::brpc::Controller* cntl;
  google::protobuf::Closure* done;
  unsigned shard = 0;
  AttachmentDoneWork attachment_work;
  Promise<Status> attachment_done;
  bool attachment_started = false;
  bool attachment_released = false;
};

// A verb whose request carries no context has no key to shard by -- Ping and
// GetNodeInfo answer the same from any core.
template <typename Req>
uint64_t RoutingKeyOf(const Req& request) {
  if constexpr (requires { request.context().routing_key(); }) {
    return request.context().routing_key();
  } else {
    return 0;
  }
}

// The typed half: the verb, and the coroutine that runs it on the shard.
template <typename S, typename Req, typename Resp>
struct TypedBrpcCall final : BrpcCall {
  using Method = Future<> (S::*)(Controller*, const Req*, Resp*);

  TypedBrpcCall(BrpcServer* srv, ::brpc::Controller* c,
                google::protobuf::Closure* d, S* s, Method m, const Req* req,
                Resp* resp)
      : BrpcCall(srv, c, d),
        service(s),
        method(m),
        request(req),
        response(resp) {
    run = &TypedBrpcCall::OnShard;
  }

  static void OnShard(InboxWork* base) {
    (void)static_cast<TypedBrpcCall*>(base)->Serve();
  }

  // What one request does, once it is on the shard that owns its key.
  Future<Status> Serve() {
    Controller shard_cntl;
    const butil::IOBuf& attached = cntl->request_attachment();
    const auto attached_size = static_cast<uint32_t>(attached.size());
    if (attached_size > kMaxAttachmentBytes) {
      FailNow(EINVAL, "the request attachment is too large");
      co_return Status::OK();
    }
    if (attached_size > 0) {
      // Slab-backed: 4 KiB-aligned, so O_DIRECT can land straight in it. The
      // one copy on this path -- brpc's blocks are unaligned and unregistered.
      Buffer buffer = Buffer::Alloc(attached_size);
      if (buffer.empty()) {
        FailNow(ENOMEM, "no buffer for the request attachment");
        co_return Status::OK();
      }
      attached.copy_to(buffer.view().data, attached_size);
      shard_cntl.request_attachment() = std::move(buffer);
    }
    shard_cntl.set_request_attachment_size(attached_size);

    co_await (service->*method)(&shard_cntl, request, response);

    if (shard_cntl.Failed()) {
      FailNow(shard_cntl.ErrorCode(), shard_cntl.ErrorText());
      co_return Status::OK();
    }
    if (!shard_cntl.has_response_body()) {
      Finish();
      co_return Status::OK();
    }
    // The attachment stays alive: this frame is suspended until brpc is done.
    co_await ReplyWithAttachment(shard_cntl.response_view());
    Retire();
    co_return Status::OK();
  }

  S* service;
  Method method;
  const Req* request;
  Resp* response;
};

// The receiving bthread's whole job: pick the shard, hand the call over.
template <typename S, typename Req, typename Resp>
void BridgeToShard(BrpcServer* server, std::span<S* const> services,
                   Future<> (S::*method)(Controller*, const Req*, Resp*),
                   ::brpc::Controller* cntl, const Req* request, Resp* response,
                   google::protobuf::Closure* done) {
  const unsigned shard =
      ShardOf(RoutingKeyOf(*request), static_cast<unsigned>(services.size()));
  auto* call = new TypedBrpcCall<S, Req, Resp>(
      server, cntl, done, services[shard], method, request, response);
  call->shard = shard;
  server->CallStarted();
  if (PostTo(shard, call)) {
    return;
  }
  // Only reachable if the runtime went down first; answer, don't hang peer.
  LOG(ERROR) << "Fail to post an rpc to a shard: the runtime is stopping";
  call->FailNow(EHOSTDOWN, "the runtime is stopping");
}

}  // namespace net
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_BRPC_BRIDGE_H_
