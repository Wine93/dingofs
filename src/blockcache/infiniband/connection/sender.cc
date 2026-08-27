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

#include "blockcache/infiniband/connection/sender.h"

#include <glog/logging.h>

#include <cerrno>
#include <ios>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/common/wr_id.h"
#include "blockcache/infiniband/connection/batch.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

Status MsgSender::Send(SendBuffer* buffer, size_t len) {
  const LocalRegion sge{.addr = buffer->data,
                        .len = static_cast<uint32_t>(len),
                        .lkey = buffers_->lkey()};
  if (!qps_->GetMsgQueue()->PostMessage(MakeWrId(buffer, kTagSendBuffer),
                                        sge)) {
    buffers_->Release(buffer);
    return ToStatus(ENOBUFS, "post a frame");
  }
  return Status::OK();
}

void MsgSender::OnSent(SendBuffer* buffer) {
  qps_->GetMsgQueue()->ReleaseSlots(1, 0);
  buffers_->Release(buffer);
}

Future<Status> BulkSender::Move(BufferView buffer,
                                std::span<const RemoteRegion> regions,
                                bool read) {
  StatusOr<uint32_t> lkey = Check(buffer, regions);
  if (!lkey.ok()) {
    co_return lkey.status();
  }

  char* at = static_cast<char*>(buffer.data);
  uint32_t left = buffer.size;
  OpBatch batch = OpBatch(qps_->NextBulkQueue());
  for (size_t i = 0; i < regions.size() && left != 0; ++i) {
    const RemoteRegion& region = regions[i];
    const uint32_t n = region.len < left ? region.len : left;
    const LocalRegion local{.addr = at, .len = n, .lkey = lkey.value()};
    const Status added = co_await AddRegion(&batch, read, local, region);
    if (!added.ok()) {
      co_return added;
    }
    at += n;
    left -= n;
  }
  const Status status = co_await batch.Submit();
  if (!status.ok()) {
    // A bare remote-access error is unactionable; log both ends' addresses.
    LOG_EVERY_N(ERROR, 100)
        << "Fail to " << (read ? "read" : "write")
        << " an attachment: local=" << buffer.data << "+" << buffer.size
        << " lkey=" << lkey.value() << " remote=0x" << std::hex
        << regions[0].addr << std::dec << "+" << regions[0].len
        << " rkey=" << regions[0].rkey << " regions=" << regions.size()
        << ": " << status.ToString();
  }
  co_return status;
}

StatusOr<uint32_t> BulkSender::Check(
    BufferView buffer, std::span<const RemoteRegion> regions) const {
  if (buffer.empty()) {
    return ToStatus(EINVAL, "move an attachment: the buffer is empty");
  }
  // Unregistered memory would fail on the HCA.
  StatusOr<uint32_t> lkey = registry_->GetLKey(buffer.data, buffer.size);
  if (!lkey.ok()) {
    return ToStatus(EINVAL, "move an attachment: the buffer is not registered");
  }
  if (regions.empty()) {
    return ToStatus(EINVAL,
                    "move an attachment: the peer advertised no region");
  }
  if (buffer.size > GetLength(regions)) {
    return ToStatus(EMSGSIZE,
                    "move an attachment: it exceeds what the peer advertised");
  }
  return lkey;
}

Future<Status> BulkSender::AddRegion(OpBatch* batch, bool read,
                                     LocalRegion local, RemoteRegion region) {
  for (;;) {
    bool added = false;
    if (read) {
      added = batch->AddRead(local, region);
    } else {
      added = batch->AddWrite(local, region);
    }
    if (added) {
      co_return Status::OK();
    }

    // Full send queue: retry THIS region rather than advancing past it.
    if (batch->count() != 0) {
      // Awaiting what is built releases the slots it holds.
      const Status submitted = co_await batch->Submit();
      if (!submitted.ok()) {
        co_return submitted;
      }
      *batch = OpBatch(qps_->NextBulkQueue());
      continue;
    }
    // An empty batch is ready immediately, so retrying it would spin this
    // shard without yielding -- and the poller that frees the slots runs
    // on this very shard, so the queue could never drain: a livelock, not
    // a stall. The single-op path parks and truly suspends.
    const ibv_wr_opcode opcode = read ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE;
    co_return co_await OpAwaiter(qps_->NextBulkQueue(), opcode, local,
                                 region.addr, region.rkey);
  }
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
