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

#include <butil/iobuf.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Messenger {
 public:
  Messenger() = default;

  // 类型安全的注册接口，Req/Resp 从 lambda 签名自动推导，无需显式模板参数。
  // 在 proto 里给 Request.body 和 Response.body oneof
  // 各加一个分支后，业务侧直接：
  //   messenger.RegisterHandler(
  //       [](Controller* cntl,
  //          const pb::cache::PutRequest* req,
  //          pb::cache::PutResponse* resp,
  //          google::protobuf::Closure* done) { ... });
  // 即可，messenger.h 不用改。
  //
  // 实现说明：通过 DeduceHandlerArgs 从 lambda::operator() 的成员指针类型
  // 反推 Req、Resp；真正的 descriptor 查找、注册逻辑在 messenger.cpp 的
  // DoRegister。
  template <typename F>
  void RegisterHandler(F&& handler) {
    using Args = decltype(DeduceHandlerArgs(&std::decay_t<F>::operator()));
    using Req = typename Args::first_type;
    using Resp = typename Args::second_type;

    static_assert(std::is_base_of_v<google::protobuf::Message, Req>,
                  "Req must be a protobuf Message type");
    static_assert(std::is_base_of_v<google::protobuf::Message, Resp>,
                  "Resp must be a protobuf Message type");

    DoRegister(
        Req::descriptor(), Resp::descriptor(),
        [h = std::forward<F>(handler)](
            Controller* cntl, const google::protobuf::Message* req,
            google::protobuf::Message* resp, google::protobuf::Closure* done) {
          h(cntl, static_cast<const Req*>(req), static_cast<Resp*>(resp), done);
        });
  }

  // Connection 完成 buffer 解析、构造好 Controller / Response / Closure 后调。
  // - cntl:     调用方分配，已填好 request_attachment
  // - response: 调用方分配的空 Response，handler 内填充
  // - done:     handler 处理完后调用 done->Run() 触发 send-back
  Status Dispatch(Controller* cntl,
                  const pb::infiniband::InfinibandRequest& request,
                  pb::infiniband::InfinibandResponse* response,
                  google::protobuf::Closure* done);

 private:
  // 仅用于在 RegisterHandler 里从 lambda 的 operator() 反推 (Req, Resp)。
  // const / 非 const 两个版本分别匹配普通 lambda 和 mutable lambda。
  // 仅声明、不需要定义 —— 用在 decltype 上下文里推导返回类型。
  template <typename C, typename Req, typename Resp>
  static auto DeduceHandlerArgs(void (C::*)(Controller*, const Req*, Resp*,
                                            google::protobuf::Closure*) const)
      -> std::pair<Req, Resp>;
  template <typename C, typename Req, typename Resp>
  static auto DeduceHandlerArgs(void (C::*)(Controller*, const Req*, Resp*,
                                            google::protobuf::Closure*))
      -> std::pair<Req, Resp>;

  using ErasedHandler = std::function<void(
      Controller*, const google::protobuf::Message*, google::protobuf::Message*,
      google::protobuf::Closure*)>;

  // 非模板，定义在 messenger.cpp。所有 protobuf reflection 相关的逻辑都在这里。
  void DoRegister(const google::protobuf::Descriptor* req_type_desc,
                  const google::protobuf::Descriptor* resp_type_desc,
                  ErasedHandler handler);

  using DispatchEntry = std::function<void(
      Controller*, const pb::infiniband::InfinibandRequest&,
      pb::infiniband::InfinibandResponse*, google::protobuf::Closure*)>;

  std::unordered_map<pb::infiniband::InfinibandRequest::BodyCase, DispatchEntry>
      handlers_;
};

using MessengerUPtr = std::unique_ptr<Messenger>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_MESSENGER_H_
