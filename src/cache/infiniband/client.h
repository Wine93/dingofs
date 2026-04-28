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

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_H_

#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <google/protobuf/message.h>

#include <memory>
#include <type_traits>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/memory.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Dialer;
class ClientRDMASession;
class Client;

using DialerUPtr = std::unique_ptr<Dialer>;
using ClientRDMASessionUPtr = std::unique_ptr<ClientRDMASession>;
using ClientUPtr = std::unique_ptr<Client>;

class Dialer {
 public:
  Dialer(DeviceUPtr device, PortUPtr port, ProtectDomainUPtr protect_domain);
  static DialerUPtr Create(const EndPoint& ep);

  ConnectionUPtr Dial(const std::string& address);

 private:
  Status SyncConnMangmentMeta(const std::string& address,
                              const ConnMangmentMeta& local_cm_meta,
                              ConnMangmentMeta* remote_cm_meta);

  DeviceUPtr device_;
  PortUPtr port_;
  ProtectDomainUPtr protect_domain_;
};

class ClientRDMASession : public EventHandler {
 public:
  explicit ClientRDMASession(ConnectionUPtr conn);
  void Start();
  void Shutdown();

  void HandleEvent() override;

  Status OnEstablished();
  Status SendRequest(Controller* cntl,
                     const google::protobuf::Message& request);
  void OnRequestSent(const WorkCompletion& wc);
  void OnResponseReceived(const WorkCompletion& wc);
  Status ProcessResponse(Controller* cntl, google::protobuf::Message* response);

 private:
  static int HandleWorkCompletion(void* meta,
                                  bthread::TaskIterator<CompletionBatch>& iter);

  ConnectionUPtr conn_;
  bthread::ExecutionQueueId<CompletionBatch> handle_wc_queue_id_;
};

class Client {
 public:
  explicit Client(DialerUPtr dialer);
  static ClientUPtr Create(const EndPoint& ep);

  Status Connect(const std::string& address);

  template <typename Req, typename Resp>
  Status Call(const Req& request, Resp* response) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Req>,
                  "Req must be a protobuf Message type");
    static_assert(std::is_base_of_v<google::protobuf::Message, Resp>,
                  "Resp must be a protobuf Message type");
    return DoCall(request, response);
  }

 private:
  Status DoCall(const google::protobuf::Message& request,
                google::protobuf::Message* response);

  DialerUPtr dialer_;
  ClientRDMASessionUPtr session_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_H_
