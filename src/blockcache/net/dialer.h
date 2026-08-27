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

#ifndef DINGOFS_BLOCKCACHE_NET_DIALER_H_
#define DINGOFS_BLOCKCACHE_NET_DIALER_H_

#include <cstdint>
#include <string>

#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"

namespace dingofs {
namespace blockcache {
namespace net {

class Channel;

// Timeouts are server-wide and read by the transport that opens the wire.
struct DialOption {
  std::string server;
  std::string tag;
  uint64_t routing_key = 0;
  uint32_t expected_shard = UINT32_MAX;
};

class Dialer {
 public:
  virtual ~Dialer() = default;
  Dialer() = default;

  Dialer(const Dialer&) = delete;
  Dialer& operator=(const Dialer&) = delete;

  virtual Future<StatusOr<Channel*>> Dial(DialOption option) = 0;

  virtual Future<> Close(Channel* channel) = 0;
};

}  // namespace net
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_DIALER_H_
