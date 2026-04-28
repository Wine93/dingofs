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
#include <google/protobuf/descriptor.h>

#include <cstdint>

#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

namespace {

struct MessageHeader {
  uint32_t magic;
  uint32_t payload_len;
  uint64_t correlation_id;
};
static_assert(sizeof(MessageHeader) == Protocol::kHeaderSize,
              "MessageHeader size must match Protocol::kHeaderSize");

// 'R' 'D' 'M' 'A' — identifies frames belonging to this RDMA protocol.
constexpr uint32_t kMagic = 0x52444D41;

// Validates the header at the start of [buffer, buffer + size) and returns
// a pointer to it. The full framed message (header + payload) must fit.
Status ValidateHeader(const char* buffer, size_t size,
                      const MessageHeader** out) {
  if (size < sizeof(MessageHeader)) {
    LOG(ERROR) << "Message too small: size=" << size;
    return Status::InvalidParam("message too small");
  }
  const auto* header = reinterpret_cast<const MessageHeader*>(buffer);
  if (header->magic != kMagic) {
    LOG(ERROR) << "Invalid magic: " << std::hex << header->magic;
    return Status::InvalidParam("invalid magic");
  }
  if (size < sizeof(MessageHeader) + header->payload_len) {
    LOG(WARNING) << "Incomplete payload: payload_len=" << header->payload_len
                 << " available=" << (size - sizeof(MessageHeader));
    return Status::InvalidParam("incomplete payload");
  }
  *out = header;
  return Status::OK();
}

template <typename Message>
Status ParseFramed(const char* buffer, size_t size, Message* msg) {
  const MessageHeader* header = nullptr;
  auto s = ValidateHeader(buffer, size, &header);
  if (!s.ok()) {
    return s;
  }
  if (!msg->ParseFromArray(buffer + sizeof(MessageHeader),
                           static_cast<int>(header->payload_len))) {
    LOG(ERROR) << "Fail to parse protobuf message";
    return Status::InvalidParam("parse protobuf failed");
  }
  return Status::OK();
}

template <typename Message>
Status SerializeFramed(const Message& msg, uint64_t correlation_id,
                       char* buffer, size_t size, uint32_t* written) {
  const size_t payload_len = msg.ByteSizeLong();
  const size_t total = sizeof(MessageHeader) + payload_len;
  if (size < total) {
    LOG(ERROR) << "Buffer too small: need=" << total << " size=" << size;
    return Status::InvalidParam("buffer too small");
  }
  auto* header = reinterpret_cast<MessageHeader*>(buffer);
  header->magic = kMagic;
  header->payload_len = static_cast<uint32_t>(payload_len);
  header->correlation_id = correlation_id;
  if (!msg.SerializeToArray(buffer + sizeof(MessageHeader),
                            static_cast<int>(payload_len))) {
    LOG(ERROR) << "Fail to serialize protobuf message";
    return Status::Internal("serialize protobuf failed");
  }
  *written = total;
  return Status::OK();
}

}  // namespace

Status Protocol::Parse(const char* buffer, size_t size,
                       pb::infiniband::InfinibandRequest* request) {
  return ParseFramed(buffer, size, request);
}

Status Protocol::Parse(const char* buffer, size_t size,
                       pb::infiniband::InfinibandResponse* response) {
  return ParseFramed(buffer, size, response);
}

Status Protocol::Serialize(const pb::infiniband::InfinibandRequest& request,
                           uint64_t correlation_id, char* buffer, size_t size,
                           uint32_t* written) {
  return SerializeFramed(request, correlation_id, buffer, size, written);
}

Status Protocol::Serialize(const pb::infiniband::InfinibandResponse& response,
                           uint64_t correlation_id, char* buffer, size_t size,
                           uint32_t* written) {
  return SerializeFramed(response, correlation_id, buffer, size, written);
}

Status Protocol::PeekCorrelationId(const char* buffer, size_t size,
                                   uint64_t* correlation_id) {
  const MessageHeader* header = nullptr;
  auto s = ValidateHeader(buffer, size, &header);
  if (!s.ok()) {
    return s;
  }
  *correlation_id = header->correlation_id;
  return Status::OK();
}

namespace {

const google::protobuf::OneofDescriptor* GetBodyOneof(
    const google::protobuf::Message& envelope) {
  return envelope.GetDescriptor()->FindOneofByName("body");
}

}  // namespace

Status Protocol::PackBody(const google::protobuf::Message& body,
                          google::protobuf::Message* envelope) {
  const auto* oneof = GetBodyOneof(*envelope);
  if (nullptr == oneof) {
    return Status::Internal("envelope has no oneof field named 'body'");
  }

  const auto* body_desc = body.GetDescriptor();
  for (int i = 0; i < oneof->field_count(); ++i) {
    const auto* field = oneof->field(i);
    if (field->message_type() == body_desc) {
      envelope->GetReflection()
          ->MutableMessage(envelope, field)
          ->CopyFrom(body);
      return Status::OK();
    }
  }
  return Status::Internal("no matching body oneof branch for type=" +
                          body_desc->full_name());
}

Status Protocol::UnpackBody(const google::protobuf::Message& envelope,
                            google::protobuf::Message* body) {
  const auto* oneof = GetBodyOneof(envelope);
  if (nullptr == oneof) {
    return Status::Internal("envelope has no oneof field named 'body'");
  }

  const auto* refl = envelope.GetReflection();
  const auto* field = refl->GetOneofFieldDescriptor(envelope, oneof);
  if (nullptr == field) {
    return Status::Internal("envelope body oneof is not set");
  }
  if (field->message_type() != body->GetDescriptor()) {
    return Status::Internal("envelope body type mismatch, want=" +
                            body->GetDescriptor()->full_name() +
                            ", got=" + field->message_type()->full_name());
  }
  body->CopyFrom(refl->GetMessage(envelope, field));
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
