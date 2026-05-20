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

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "cache/infiniband/memory.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

// Wire format (per frame):
//   +-----------------------------------------------------+
//   | Header (24 B, fixed)                                |
//   |   uint32 magic           = 'RDMA' (0x52444D41)      |
//   |   uint32 meta_len        bytes of metadata section  |
//   |   uint32 data_len        bytes of data section      |
//   |   uint64 correlation_id                             |
//   +-----------------------------------------------------+
//   | Metadata (meta_len bytes)                           |
//   |   RequestMeta or ResponseMeta protobuf              |
//   +-----------------------------------------------------+
//   | Data (data_len bytes)                               |
//   |   business Req/Resp protobuf serialized raw         |
//   +-----------------------------------------------------+
class Protocol {
 public:
  static constexpr size_t kHeaderSize = 24;

  // Read the correlation_id from the header without parsing the rest of the
  // frame. Inputs `buffer->data` + `buffer->length`.
  static Status PeekCorrelationId(const Buffer* buffer,
                                  uint64_t* correlation_id);

  // Serialize a request/response frame into `buffer`. On success
  // `buffer->length` is updated to the bytes written; the buffer's
  // registered MR (lkey/rkey) is untouched.
  static Status SerializeFrame(Buffer* buffer, uint64_t correlation_id,
                               const pb::infiniband::RequestMeta& meta,
                               const google::protobuf::Message& data);
  // `data` may be nullptr — used when meta carries an error and no body is
  // available; the data section is then zero length.
  static Status SerializeFrame(Buffer* buffer, uint64_t correlation_id,
                               const pb::infiniband::ResponseMeta& meta,
                               const google::protobuf::Message* data);

  // Parse a received frame from `buffer->data` (using `buffer->length` as the
  // received byte count). On success `meta` is populated and `data` is a
  // view into `buffer->data` — valid for the buffer's lifetime.
  static Status ParseFrame(const Buffer* buffer, uint64_t* correlation_id,
                           pb::infiniband::RequestMeta* meta,
                           std::string_view* data);
  static Status ParseFrame(const Buffer* buffer, uint64_t* correlation_id,
                           pb::infiniband::ResponseMeta* meta,
                           std::string_view* data);
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_PROTOCOL_H_
