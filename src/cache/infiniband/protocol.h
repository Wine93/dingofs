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

// Wire framing for InfiniBand RDMA SEND/RECV messages.
//
// On the wire each message is a fixed 8-byte header followed by a protobuf
// payload of the size declared by the header.
class Protocol {
 public:
  // Bytes of framing overhead Serialize() adds on top of the protobuf payload.
  static constexpr size_t kHeaderSize = 16;

  // Parse one framed message from [buffer, buffer + size). On success the
  // protobuf message is populated.
  static Status Parse(const char* buffer, size_t size,
                      pb::infiniband::InfinibandRequest* request);
  static Status Parse(const char* buffer, size_t size,
                      pb::infiniband::InfinibandResponse* response);

  // Serialize one framed message into [buffer, buffer + size). 'correlation_id'
  // is written into the header so the receiver can dispatch the completion
  // without parsing the protobuf payload. On success *written receives the
  // total bytes written (header + payload).
  static Status Serialize(const pb::infiniband::InfinibandRequest& request,
                          uint64_t correlation_id, char* buffer, size_t size,
                          uint32_t* written);
  static Status Serialize(const pb::infiniband::InfinibandResponse& response,
                          uint64_t correlation_id, char* buffer, size_t size,
                          uint32_t* written);

  // Peeks at a framed message's correlation_id without parsing the payload.
  // Returns InvalidParam if the header is missing/malformed.
  static Status PeekCorrelationId(const char* buffer, size_t size,
                                  uint64_t* correlation_id);

  // Pack a concrete sub-message into the `body` oneof of a Request /
  // Response envelope via protobuf reflection. The matching branch is
  // chosen by `body.GetDescriptor()`.
  // Returns Internal if no oneof branch matches.
  static Status PackBody(const google::protobuf::Message& body,
                         google::protobuf::Message* envelope);

  // Unpack the currently-set `body` oneof branch of `envelope` into `*body`.
  // Returns Internal if the oneof is unset or the set branch's type doesn't
  // match `body->GetDescriptor()`.
  static Status UnpackBody(const google::protobuf::Message& envelope,
                           google::protobuf::Message* body);
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_
