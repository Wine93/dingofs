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
 * Created Date: 2026-05-14
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_

#include <bthread/countdown_event.h>
#include <bthread/mutex.h>

#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "cache/infiniband/memory.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

struct Controller {
  uint64_t correlation_id{0};
  Status status{Status::Unknown("unknown")};

  // request
  Buffer* request_buffer{nullptr};  // => request_buffer
  uint64_t raddr{0};
  uint32_t rkey{0};
  bthread::CountdownEvent request_sent{1};

  // response
  Buffer* response_buffer{nullptr};
  Buffer* response_attachment{nullptr};
  bthread::CountdownEvent response_sent{1};
  bthread::CountdownEvent response_received{1};
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CONTROLLER_H_
