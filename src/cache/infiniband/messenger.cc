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

namespace dingofs {
namespace cache {
namespace infiniband {

Status Messenger::AddService(::google::protobuf::Service* service) {
  CHECK_NOTNULL(service);
  const std::string name = service->GetDescriptor()->full_name();
  auto [_, inserted] = services_.emplace(name, service);
  if (!inserted) {
    LOG(ERROR) << "Service already registered: " << name;
    return Status::Exist("service already registered: " + name);
  }
  LOG(INFO) << "Registered service: " << name;
  return Status::OK();
}

Status Messenger::Dispatch(Controller* cntl,
                           const pb::infiniband::RequestMeta& meta,
                           const char* data, size_t data_len,
                           pb::infiniband::ResponseMeta* resp_meta,
                           ::google::protobuf::Message** resp_body,
                           ::google::protobuf::Closure* done) {
  *resp_body = nullptr;

  auto it = services_.find(meta.service_name());
  if (it == services_.end()) {
    const std::string err = "service not found: " + meta.service_name();
    LOG(WARNING) << err;
    resp_meta->set_error_code(-1);
    resp_meta->set_error_message(err);
    done->Run();
    return Status::NotFound(err);
  }

  auto* service = it->second;
  const auto* method =
      service->GetDescriptor()->FindMethodByName(meta.method_name());
  if (method == nullptr) {
    const std::string err = "method not found: " + meta.service_name() +
                            "." + meta.method_name();
    LOG(WARNING) << err;
    resp_meta->set_error_code(-1);
    resp_meta->set_error_message(err);
    done->Run();
    return Status::NotFound(err);
  }

  // Allocate request/response on the Controller's Arena — they live as long
  // as cntl does and are reclaimed in bulk on cntl destruction. No per-RPC
  // operator new/delete on the dispatch path.
  auto* request = service->GetRequestPrototype(method).New(cntl->Arena());
  auto* response = service->GetResponsePrototype(method).New(cntl->Arena());

  if (!request->ParseFromArray(data, static_cast<int>(data_len))) {
    const std::string err = "fail to parse request body for " +
                            meta.service_name() + "." + meta.method_name();
    LOG(ERROR) << err;
    resp_meta->set_error_code(-1);
    resp_meta->set_error_message(err);
    done->Run();
    return Status::InvalidParam(err);
  }

  *resp_body = response;
  service->CallMethod(method, cntl, request, response, done);
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
