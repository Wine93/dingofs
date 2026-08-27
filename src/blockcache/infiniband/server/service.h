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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVICE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVICE_H_

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/net/codec.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/proto.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// One instance per shard; methods are coroutines (no `done`). Just the
// opcode -> method registry: a Method is the typed core of one call --
// decode, invoke, encode -- and everything around it (the attachment read,
// the reply) is ServerSession's linear pipeline.
class Service {
 public:
  using Method = std::function<Future<ReplyCode>(
      net::Controller* cntl, std::string_view request, std::string* response)>;

  Service() = default;
  virtual ~Service() = default;

  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  virtual void VerifyBound() const {}

  bool Has(Opcode opcode) const {
    return opcode < methods_.size() && methods_[opcode] != nullptr;
  }
  const Method& MethodOf(Opcode opcode) const { return methods_[opcode]; }
  size_t method_limit() const { return methods_.size(); }

 protected:
  template <typename S, typename Req, typename Resp>
  void AddMethod(Opcode opcode,
                 Future<> (S::*method)(net::Controller*, const Req*, Resp*)) {
    auto* self = static_cast<S*>(this);
    Bind(opcode, [self, method](net::Controller* cntl, std::string_view request,
                                std::string* response) {
      return Call<S, Req, Resp>(self, method, cntl, request, response);
    });
  }

 private:
  template <typename S, typename Req, typename Resp, typename M>
  static Future<ReplyCode> Call(S* self, M method, net::Controller* cntl,
                                std::string_view request,
                                std::string* response) {
    Req req;
    if (!net::Decode(request, &req)) {
      co_return kReplyBadRequest;
    }
    Resp resp;
    // A call the method could not serve at all says so on the controller.
    co_await (self->*method)(cntl, &req, &resp);
    if (cntl->Failed()) {
      co_return kReplyHandlerError;
    }
    if (!resp.SerializeToString(response)) {
      co_return kReplyHandlerError;
    }
    co_return kReplyOk;
  }

  void Bind(Opcode opcode, Method method) {
    if (methods_.size() <= opcode) {
      methods_.resize(size_t{opcode} + 1);
    }
    methods_[opcode] = std::move(method);
  }

  std::vector<Method> methods_;
};

using ServiceUPtr = std::unique_ptr<Service>;

class ProtoService : public Service {
 public:
  void VerifyBound() const override { bound_.CheckAll(descriptor_); }

 protected:
  explicit ProtoService(const google::protobuf::ServiceDescriptor* descriptor)
      : descriptor_(descriptor) {
    net::CheckContract(descriptor);
  }

  template <typename S, typename Req, typename Resp>
  void AddMethod(std::string_view name,
                 Future<> (S::*method)(net::Controller*, const Req*, Resp*)) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Req> &&
                      std::is_base_of_v<google::protobuf::Message, Resp>,
                  "a contract method speaks the contract's messages");
    const google::protobuf::MethodDescriptor* m =
        net::MethodNamed(descriptor_, name);
    net::CheckMethodTypes(m, Req::descriptor(), Resp::descriptor());
    Service::AddMethod(net::OpcodeOf(m), method);
    bound_.Mark(m);
  }

 private:
  const google::protobuf::ServiceDescriptor* descriptor_;
  net::MethodSet bound_;
};

// One per shard: the opcode -> method map every ServerSession serves from.
// Non-owning: the verbs belong to the application.
class ServiceRegistry final {
 public:
  ServiceRegistry() = default;

  ServiceRegistry(const ServiceRegistry&) = delete;
  ServiceRegistry& operator=(const ServiceRegistry&) = delete;

  void Add(Service* service);

  const Service::Method* Find(Opcode opcode) const {
    if (opcode >= methods_.size() || methods_[opcode] == nullptr) {
      return nullptr;
    }
    return &methods_[opcode];
  }

 private:
  std::vector<Service::Method> methods_;  // opcode -> method, dense
};

using ServiceRegistryUPtr = std::unique_ptr<ServiceRegistry>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_SERVER_SERVICE_H_
