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
 * Created Date: 2026-04-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_SESSION_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_SESSION_H_

#include <bthread/execution_queue.h>
#include <google/protobuf/message.h>

#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/event.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class ClientSession : public EventHandler {
 public:
  explicit ClientSession(ConnectionUPtr conn);
  void Start();
  void Shutdown();

  Status OnEstablished();
  void HandleEvent() override;
  int Fd() const { return conn_->GetFd(); }

  // Build a 3-section frame [Header][RequestMeta][request body] and post it.
  Status SendRequest(Controller* cntl, std::string_view service_name,
                     std::string_view method_name,
                     const google::protobuf::Message& request);
  Status ProcessResponse(Controller* cntl, google::protobuf::Message* response);

 private:
  static int HandleWorkCompletion(void* meta,
                                  bthread::TaskIterator<WorkCompletions>& iter);
  void OnSuccess(const WorkCompletion& wc);
  void OnError(const WorkCompletion& wc);

  void OnRequestSent(const WorkCompletion& wc);
  void OnResponseReceived(const WorkCompletion& wc);
  void RepostRecv(Buffer* buffer);

  void RegisterInflight(Controller* cntl);
  void UnregisterInflight(uint64_t correlation_id);
  Controller* FindInflight(uint64_t correlation_id);
  void FailInflights(const Status& status);

  ConnectionUPtr conn_;
  bthread::ExecutionQueueId<WorkCompletions> handle_wc_queue_id_;
  std::mutex inflight_mutex_;
  std::unordered_map<uint64_t, Controller*> inflights_;
};

using ClientSessionUPtr = std::unique_ptr<ClientSession>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_SESSION_H_
