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

#ifndef DINGOFS_BLOCKCACHE_NET_CODEC_H_
#define DINGOFS_BLOCKCACHE_NET_CODEC_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace dingofs {
namespace blockcache {
namespace net {

// By template parameter rather than MessageLite: a generated message is
// final, so a caller holding the concrete type keeps its direct calls.

template <typename T>
bool Decode(std::string_view in, T* out) {
  return out->ParseFromArray(in.data(), static_cast<int>(in.size()));
}

// In-place encoded message; view() points into it -- must outlive the send.
class Encoded {
 public:
  template <typename T>
  explicit Encoded(const T& value) {
    const size_t n = value.ByteSizeLong();
    char* dst = inlined_;
    if (n > sizeof(inlined_)) {
      spilled_.resize(n);
      dst = spilled_.data();
    }
    value.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t*>(dst));
    view_ = {dst, n};
  }

  Encoded(const Encoded&) = delete;
  Encoded& operator=(const Encoded&) = delete;

  std::string_view view() const { return view_; }

 private:
  char inlined_[256];
  std::string spilled_;
  std::string_view view_;
};

}  // namespace net
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_CODEC_H_
