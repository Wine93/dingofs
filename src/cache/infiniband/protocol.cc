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

#include "cache/infiniband/protocol.h"

#include <glog/logging.h>

#include <cstdint>
#include <cstring>

#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

namespace {

struct MessageHeader {
  uint32_t magic;
  uint32_t meta_len;
  uint32_t data_len;
  uint64_t correlation_id;
};

static_assert(sizeof(MessageHeader) == Protocol::kHeaderSize,
              "MessageHeader size must match Protocol::kHeaderSize");

// 'R' 'D' 'M' 'A' — identifies frames belonging to this RDMA protocol.
constexpr uint32_t kMagic = 0x52444D41;

Status ValidateHeader(const Buffer* buffer, const MessageHeader** out) {
  CHECK_NOTNULL(buffer);
  if (buffer->length < sizeof(MessageHeader)) {
    LOG(ERROR) << "Message too small: length=" << buffer->length;
    return Status::InvalidParam("message too small");
  }
  const auto* header = reinterpret_cast<const MessageHeader*>(buffer->data);
  if (header->magic != kMagic) {
    LOG(ERROR) << "Invalid magic: " << std::hex << header->magic;
    return Status::InvalidParam("invalid magic");
  }
  const size_t need = sizeof(MessageHeader) +
                      static_cast<size_t>(header->meta_len) +
                      static_cast<size_t>(header->data_len);
  if (buffer->length < need) {
    LOG(WARNING) << "Incomplete frame: need=" << need
                 << " available=" << buffer->length
                 << " meta_len=" << header->meta_len
                 << " data_len=" << header->data_len;
    return Status::InvalidParam("incomplete frame");
  }
  *out = header;
  return Status::OK();
}

template <typename Meta>
Status SerializeFrameImpl(Buffer* buffer, uint64_t correlation_id,
                          const Meta& meta,
                          const google::protobuf::Message* data) {
  CHECK_NOTNULL(buffer);
  const size_t meta_len = meta.ByteSizeLong();
  const size_t data_len = (data == nullptr) ? 0 : data->ByteSizeLong();
  const size_t total = sizeof(MessageHeader) + meta_len + data_len;
  if (buffer->capacity < total) {
    LOG(ERROR) << "Buffer too small: need=" << total
               << " capacity=" << buffer->capacity;
    return Status::InvalidParam("buffer too small");
  }

  auto* header = reinterpret_cast<MessageHeader*>(buffer->data);
  header->magic = kMagic;
  header->meta_len = static_cast<uint32_t>(meta_len);
  header->data_len = static_cast<uint32_t>(data_len);
  header->correlation_id = correlation_id;

  char* meta_ptr = buffer->data + sizeof(MessageHeader);
  if (!meta.SerializeToArray(meta_ptr, static_cast<int>(meta_len))) {
    LOG(ERROR) << "Fail to serialize metadata";
    return Status::Internal("serialize metadata failed");
  }
  if (data != nullptr && data_len > 0) {
    char* data_ptr = meta_ptr + meta_len;
    if (!data->SerializeToArray(data_ptr, static_cast<int>(data_len))) {
      LOG(ERROR) << "Fail to serialize data";
      return Status::Internal("serialize data failed");
    }
  }
  buffer->length = static_cast<uint32_t>(total);
  return Status::OK();
}

template <typename Meta>
Status ParseFrameImpl(const Buffer* buffer, uint64_t* correlation_id,
                      Meta* meta, std::string_view* data) {
  const MessageHeader* header = nullptr;
  auto s = ValidateHeader(buffer, &header);
  if (!s.ok()) {
    return s;
  }
  *correlation_id = header->correlation_id;
  const char* meta_ptr = buffer->data + sizeof(MessageHeader);
  if (!meta->ParseFromArray(meta_ptr, static_cast<int>(header->meta_len))) {
    LOG(ERROR) << "Fail to parse metadata";
    return Status::InvalidParam("parse metadata failed");
  }
  *data = std::string_view(meta_ptr + header->meta_len, header->data_len);
  return Status::OK();
}

}  // namespace

Status Protocol::PeekCorrelationId(const Buffer* buffer,
                                   uint64_t* correlation_id) {
  const MessageHeader* header = nullptr;
  auto s = ValidateHeader(buffer, &header);
  if (!s.ok()) {
    return s;
  }
  *correlation_id = header->correlation_id;
  return Status::OK();
}

Status Protocol::SerializeFrame(Buffer* buffer, uint64_t correlation_id,
                                const pb::infiniband::RequestMeta& meta,
                                const google::protobuf::Message& data) {
  return SerializeFrameImpl(buffer, correlation_id, meta, &data);
}

Status Protocol::SerializeFrame(Buffer* buffer, uint64_t correlation_id,
                                const pb::infiniband::ResponseMeta& meta,
                                const google::protobuf::Message* data) {
  return SerializeFrameImpl(buffer, correlation_id, meta, data);
}

Status Protocol::ParseFrame(const Buffer* buffer, uint64_t* correlation_id,
                            pb::infiniband::RequestMeta* meta,
                            std::string_view* data) {
  return ParseFrameImpl(buffer, correlation_id, meta, data);
}

Status Protocol::ParseFrame(const Buffer* buffer, uint64_t* correlation_id,
                            pb::infiniband::ResponseMeta* meta,
                            std::string_view* data) {
  return ParseFrameImpl(buffer, correlation_id, meta, data);
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
