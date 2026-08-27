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

#ifndef DINGOFS_BLOCKCACHE_NET_CONTROLLER_H_
#define DINGOFS_BLOCKCACHE_NET_CONTROLLER_H_

#include <cstdint>
#include <string>
#include <utility>

#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/reactor/coroutine.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {
namespace net {

class Controller {
 public:
  Controller() = default;

  Controller(const Controller&) = delete;
  Controller& operator=(const Controller&) = delete;

  Buffer& request_attachment() { return request_body_; }
  const Buffer& request_attachment() const { return request_body_; }
  Buffer& response_attachment() { return response_body_; }
  const Buffer& response_attachment() const { return response_body_; }

  // Borrowed memory must be registered and live until the future resolves,
  // the array of views included. List vs range: see Call in net/channel.h.
  void set_borrowed_request(BufferViews borrowed) {
    borrowed_request_ = borrowed;
  }
  void set_borrowed_response(BufferView borrowed) {
    borrowed_response_ = borrowed;
  }
  // Non-const: an owned attachment is published as a one-element list.
  BufferViews request_ranges() {
    if (request_body_.empty()) {
      return borrowed_request_;
    }
    owned_request_ = request_body_.view();
    return {&owned_request_, 1};
  }
  BufferView response_view() const {
    return response_body_.empty() ? borrowed_response_ : response_body_.view();
  }
  bool has_response_body() const { return !response_view().empty(); }

  // Bytes the caller advertised, 0 when none; the bridge that fetched sets it.
  uint32_t request_attachment_size() const { return request_attachment_size_; }
  void set_request_attachment_size(uint32_t size) {
    request_attachment_size_ = size;
  }

  void SetFailed(int error_code, std::string reason) {
    error_code_ = error_code;
    error_text_ = std::move(reason);
  }
  void SetFailed(std::string reason) { SetFailed(EINVAL, std::move(reason)); }
  bool Failed() const { return error_code_ != 0; }
  int ErrorCode() const { return error_code_; }
  const std::string& ErrorText() const { return error_text_; }

 private:
  Buffer request_body_;
  Buffer response_body_;
  BufferViews borrowed_request_;
  BufferView owned_request_;  // backs the one-element list above
  BufferView borrowed_response_;

  std::string error_text_;
  int error_code_ = 0;
  uint32_t request_attachment_size_ = 0;
};

}  // namespace net
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_CONTROLLER_H_
