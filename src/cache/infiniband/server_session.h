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
 * Created Date: 2026-04-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_SERVER_SESSION_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_SERVER_SESSION_H_

#include <bthread/countdown_event.h>
#include <bthread/execution_queue.h>
#include <google/protobuf/service.h>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/event.h"
#include "cache/infiniband/messenger.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class ServerSession : public EventHandler {
 public:
  ServerSession(ConnectionUPtr conn, Messenger* messenger);
  void Start();
  void Shutdown();

  void HandleEvent() override;
  Status OnEstablished();

 private:
  static int HandleWorkCompletion(void* meta,
                                  bthread::TaskIterator<WorkCompletions>& iter);
  void OnSuccess(const WorkCompletion& wc);
  void OnError(const WorkCompletion& wc);

  void OnRequestReceived(const WorkCompletion& wc);
  Status ProcessRequest(Controller* cntl,
                        pb::infiniband::InfinibandResponse* ib_response);
  Status SendResponse(Controller* cntl,
                      pb::infiniband::InfinibandResponse* ib_response);
  void OnResponseSent(const WorkCompletion& wc);

  Messenger* messenger_;
  ConnectionUPtr conn_;
  std::vector<WorkRequestId> ids_;
  bthread::ExecutionQueueId<WorkCompletions> handle_wc_queue_id_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_SERVER_SESSION_H_
