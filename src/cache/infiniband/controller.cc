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
 * Created Date: 2026-05-20
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/controller.h"

#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cerrno>

#include "cache/infiniband/connection.h"

namespace dingofs {
namespace cache {
namespace infiniband {

DEFINE_uint32(rdma_rpc_timeout_ms, 30000,
              "Timeout for RDMA RPC transport waits in milliseconds");

Status Controller::Wait(bthread::CountdownEvent* event, uint32_t timeout_ms,
                        const char* timeout_message) {
  int rc = 0;
  if (timeout_ms == 0) {
    rc = event->wait();
  } else {
    rc = event->timed_wait(butil::milliseconds_from_now(timeout_ms));
  }

  if (rc == 0) {
    return Status::OK();
  } else if (rc == ETIMEDOUT) {
    return Status::Timeout(timeout_message);
  }
  return Status::Internal("wait rdma event failed");
}

Status Controller::ReadRequestAttachment(Buffer* dst, size_t length) {
  CHECK_NOTNULL(conn_);
  CHECK_NOTNULL(dst);
  if (length > dst->capacity) {
    return Status::InvalidParam("request attachment exceeds buffer capacity");
  }

  SendWorkRequest wr;
  wr.id = reinterpret_cast<uint64_t>(this);
  wr.opcode = OpCode::kRDMARead;
  wr.signaled = true;
  wr.addr = reinterpret_cast<uint64_t>(dst->data);
  wr.length = static_cast<uint32_t>(length);
  wr.lkey = dst->lkey;
  wr.raddr = request_memory_context_.addr;
  wr.rkey = request_memory_context_.rkey;

  auto st = conn_->PostSendWorkRequest(wr);
  if (!st.ok()) {
    return st;
  }
  auto wait_status =
      Wait(&rdma_read_done_, 0, "wait rdma read completion timeout");
  if (!wait_status.ok()) {
    return wait_status;
  }
  return wc_status_;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
