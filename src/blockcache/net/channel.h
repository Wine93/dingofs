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

#ifndef DINGOFS_BLOCKCACHE_NET_CHANNEL_H_
#define DINGOFS_BLOCKCACHE_NET_CHANNEL_H_

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cerrno>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {
namespace net {

// Each transport reads the half of the verb it dispatches by and leaves the
// other alone. `send` is a LIST because a writer accumulates into fixed-size
// pages, and gathering them first would be a full-attachment memcpy on every
// write; `recv` is one range because a reader needs somewhere contiguous to
// land. Every buffer here, and the array of views itself, stays borrowed
// until the returned future resolves.
struct Call {
  const google::protobuf::MethodDescriptor* method = nullptr;
  Opcode opcode = kOpUnspecified;
  const google::protobuf::Message* request = nullptr;
  google::protobuf::Message* response = nullptr;
  BufferViews send;  // request attachment
  BufferView recv;   // where a response attachment lands
};

// One resolved verb, typed to its messages. A transport dispatches by
// whichever half it understands: the opcode, or the descriptor.
template <typename Req, typename Resp>
struct MethodRef {
  Opcode opcode = kOpUnspecified;
  const google::protobuf::MethodDescriptor* method = nullptr;
};

// The caller's side of one open wire, dealt out by a Dialer and returned to
// it. Belongs to its creating shard.
class Channel {
 public:
  virtual ~Channel() = default;
  Channel() = default;

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  // `Call` the type needs qualifying in class scope: the method template
  // below shadows it.
  virtual Future<Status> CallMethod(net::Call call) = 0;

  virtual bool Alive() const { return true; }

  template <typename Req, typename Resp>
  Future<Status> Call(MethodRef<Req, Resp> ref, const Req* request,
                      Resp* response, Controller* cntl) {
    // One way, not both; owned and borrowed read the same here.
    const BufferViews to_server = cntl->request_ranges();
    const BufferView to_client = cntl->response_view();
    if (!to_server.empty() && !to_client.empty()) {
      cntl->SetFailed(EINVAL, "both attachments are set on one call");
      co_return Status::Internal(
          "a call carries a attachment one way, not both");
    }

    const Status status = co_await CallMethod(net::Call{
        .method = ref.method,
        .opcode = ref.opcode,
        .request = request,
        .response = response,
        .send = to_server,
        .recv = to_client,
    });
    if (!status.ok()) {
      cntl->SetFailed(EIO, status.ToString());
    }
    co_return status;
  }
};

}  // namespace net
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_CHANNEL_H_
