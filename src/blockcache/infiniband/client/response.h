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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_RESPONSE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_RESPONSE_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// code() is the HANDLER's verdict, not the call's Status. The payload still
// lives in the receive slot, which this owns and hands back when it dies.
class Response {
 public:
  using ReleaseFn = void (*)(void* owner, uint64_t token);

  Response(net::ReplyCode code, std::string_view payload,
           ReleaseFn release = nullptr, void* owner = nullptr,
           uint64_t token = 0)
      : payload_(payload),
        release_(release),
        owner_(owner),
        token_(token),
        code_(code) {}

  ~Response() { Release(); }

  Response(const Response&) = delete;
  Response& operator=(const Response&) = delete;

  Response(Response&& o) noexcept
      : payload_(o.payload_),
        release_(std::exchange(o.release_, nullptr)),
        owner_(o.owner_),
        token_(o.token_),
        code_(o.code_) {}

  Response& operator=(Response&& o) noexcept {
    if (this != &o) {
      Release();
      payload_ = o.payload_;
      release_ = std::exchange(o.release_, nullptr);
      owner_ = o.owner_;
      token_ = o.token_;
      code_ = o.code_;
    }
    return *this;
  }

  net::ReplyCode code() const { return code_; }
  bool accepted() const {
    return code_ == net::kReplyOk;
  }  // the handler said yes
  std::string_view payload() const { return payload_; }

 private:
  void Release() {
    if (release_ != nullptr) {
      release_(owner_, token_);
      release_ = nullptr;
    }
  }

  std::string_view payload_;
  ReleaseFn release_ = nullptr;
  void* owner_ = nullptr;
  uint64_t token_ = 0;
  net::ReplyCode code_ = 0;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_RESPONSE_H_
