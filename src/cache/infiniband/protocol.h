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
 * Created Date: 2026-05-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_

#include <google/protobuf/message.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Protocol {
 public:
  static constexpr size_t kHeaderSize = 16;

  static Status PeekCorrelationId(const char* buffer, size_t size,
                                  uint64_t* correlation_id);

  static Status Parse(const char* buffer, size_t size,
                      pb::infiniband::InfinibandRequest* request);
  static Status Parse(const char* buffer, size_t size,
                      pb::infiniband::InfinibandResponse* response);

  static Status Serialize(uint64_t correlation_id,
                          const pb::infiniband::InfinibandRequest& request,
                          char* buffer, size_t size, uint32_t* nwritten);
  static Status Serialize(const pb::infiniband::InfinibandResponse& response,
                          uint64_t correlation_id, char* buffer, size_t size,
                          uint32_t* nwritten);

  static Status PackBody(const google::protobuf::Message& message,
                         google::protobuf::Message* ib_message);

  static Status UnpackBody(const google::protobuf::Message& ib_message,
                           google::protobuf::Message* message);
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_
