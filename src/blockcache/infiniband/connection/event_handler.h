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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_EVENT_HANDLER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_EVENT_HANDLER_H_

#include <infiniband/verbs.h>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/connection/receive_buffer.h"
#include "blockcache/infiniband/connection/send_buffer.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class EventHandler {
 public:
  virtual ~EventHandler() = default;

  EventHandler(const EventHandler&) = delete;
  EventHandler& operator=(const EventHandler&) = delete;

  virtual void OnMessageReceived(ReceiveBuffer* buffer, const ibv_wc& wc) = 0;
  virtual void OnMessageSent(SendBuffer* buffer, int retcode) = 0;
  virtual void OnError(Status status) = 0;

 protected:
  EventHandler() = default;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_EVENT_HANDLER_H_
