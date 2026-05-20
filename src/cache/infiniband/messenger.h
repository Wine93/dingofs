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
 * Created Date: 2026-05-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_MESSENGER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_MESSENGER_H_

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include "cache/infiniband/controller.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

// brpc-style service dispatcher.
//
// Users derive from a protobuf-generated Service (e.g.
// pb::cache::BlockCacheService) and register it via Server::AddService(),
// which forwards to Messenger::AddService. Dispatch looks up the target
// service by name in the request metadata, then calls Service::CallMethod —
// the same machinery brpc uses.
//
// Request/response protobuf objects are allocated on Controller::arena so
// the dispatch path has zero per-request operator new/delete.
class Messenger {
 public:
  Messenger() = default;

  // Register a Service. Multiple services with distinct full names are
  // allowed. Caller retains ownership of `service`.
  Status AddService(::google::protobuf::Service* service);

  // Called by ServerSession after parsing the wire frame:
  // - cntl:      transport state (request_memory_context already filled)
  // - meta:      parsed RequestMeta (carries service_name, method_name)
  // - data:      pointer into the recv buffer for the request body
  // - data_len:  bytes available at `data`
  // - resp_meta: output, populated with error_code/message on failure
  // - resp_body: output, set to the response Message* on success (lives on
  //              cntl->Arena(); nullptr if the call failed before invocation)
  // - done:      invoked after the service method's Closure fires
  Status Dispatch(Controller* cntl,
                  const pb::infiniband::RequestMeta& meta, const char* data,
                  size_t data_len, pb::infiniband::ResponseMeta* resp_meta,
                  ::google::protobuf::Message** resp_body,
                  ::google::protobuf::Closure* done);

 private:
  // Keyed by service->GetDescriptor()->full_name().
  std::unordered_map<std::string, ::google::protobuf::Service*> services_;
};

using MessengerUPtr = std::unique_ptr<Messenger>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_MESSENGER_H_
