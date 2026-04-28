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

#include "cache/infiniband/messenger.h"

#include <glog/logging.h>
#include <google/protobuf/descriptor.h>

#include "cache/infiniband/protocol.h"
#include "cache/iutil/bthread.h"

namespace dingofs {
namespace cache {
namespace infiniband {

namespace {

// 在 container_desc 的 body oneof 里查找类型为 type_desc 的字段描述符。
const google::protobuf::FieldDescriptor* LookupOneofField(
    const google::protobuf::Descriptor* container_desc,
    const google::protobuf::Descriptor* type_desc) {
  const auto* oneof_desc = container_desc->FindOneofByName("body");
  if (oneof_desc == nullptr) {
    return nullptr;
  }
  for (int i = 0; i < oneof_desc->field_count(); ++i) {
    const auto* field = oneof_desc->field(i);
    if (field->message_type() == type_desc) {
      return field;
    }
  }
  return nullptr;
}

}  // namespace

void Messenger::DoRegister(const google::protobuf::Descriptor* req_type_desc,
                           const google::protobuf::Descriptor* resp_type_desc,
                           ErasedHandler handler) {
  const auto* req_field = LookupOneofField(
      pb::infiniband::InfinibandRequest::descriptor(), req_type_desc);
  CHECK(req_field != nullptr)
      << "Type " << req_type_desc->full_name() << " is not a member of "
      << pb::infiniband::InfinibandRequest::descriptor()->full_name()
      << ".body oneof. Add it in proto first.";

  const auto* resp_field = LookupOneofField(
      pb::infiniband::InfinibandResponse::descriptor(), resp_type_desc);
  CHECK(resp_field != nullptr)
      << "Type " << resp_type_desc->full_name() << " is not a member of "
      << pb::infiniband::InfinibandResponse::descriptor()->full_name()
      << ".body oneof. Add it in proto first.";

  auto body_case = static_cast<pb::infiniband::InfinibandRequest::BodyCase>(
      req_field->number());

  handlers_[body_case] = [req_field, resp_field, h = std::move(handler)](
                             Controller* cntl,
                             const pb::infiniband::InfinibandRequest& req,
                             pb::infiniband::InfinibandResponse* resp,
                             google::protobuf::Closure* done) {
    const auto* req_refl = pb::infiniband::InfinibandRequest::GetReflection();
    const auto& req_body = req_refl->GetMessage(req, req_field);

    const auto* resp_refl = pb::infiniband::InfinibandResponse::GetReflection();
    auto* resp_body = resp_refl->MutableMessage(resp, resp_field);

    h(cntl, &req_body, resp_body, done);
  };
}

Status Messenger::Dispatch(Controller* cntl,
                           const pb::infiniband::InfinibandRequest& request,
                           pb::infiniband::InfinibandResponse* response,
                           google::protobuf::Closure* done) {
  auto it = handlers_.find(request.body_case());
  if (it == handlers_.end()) {
    LOG(WARNING) << "No handler for request body_case=" << request.body_case()
                 << ", request_id=" << request.request_id();
    // if (done != nullptr) {
    //   done->Run();
    // }
    return Status::InvalidParam("");
  }

  it->second(cntl, request, response, done);
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
