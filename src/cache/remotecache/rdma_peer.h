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

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_PEER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_PEER_H_

#include <bthread/countdown_event.h>
#include <bthread/mutex.h>
#include <butil/memory/scope_guard.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cache/common/error.h"
#include "cache/infiniband/client.h"
#include "cache/infiniband/client_rdma_pool.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "cache/remotecache/peer_health_checker.h"
#include "cache/remotecache/request.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

// Counterpart to Peer for the RDMA transport. Holds an infiniband::Client
// (a single QP for now — full duplex, deep queues), a shared health checker,
// and uses the process-wide ClientRDMAPool for request/response buffers.
//
// SendRequest is a template mirroring Peer::SendRequest's signature so that
// Peer can delegate to it without changing call sites in Upstream.
class RDMAPeer {
 public:
  RDMAPeer(const std::string& id, const std::string& ip, uint32_t port,
           uint32_t weight);
  Status Start();
  void Shutdown();

  template <typename T, typename U>
  Response<U> SendRequest(const Request<T>& request);

  bool IsHealthy() { return health_checker_->IsHealthy(); }
  std::string Id() const { return id_; }
  std::string IP() const { return ip_; }
  uint32_t Port() const { return port_; }
  uint32_t Weight() const { return weight_; }

 private:
  std::string EndPoint() const { return ip_ + ":" + std::to_string(port_); }

  std::atomic<bool> running_{false};
  std::string id_;
  std::string ip_;
  uint32_t port_;
  uint32_t weight_;
  infiniband::ClientUPtr client_;
  PeerHealthCheckerUPtr health_checker_;
};

using RDMAPeerUPtr = std::unique_ptr<RDMAPeer>;

template <typename T, typename U>
Response<U> RDMAPeer::SendRequest(const Request<T>& request) {
  Response<U> response;
  BRPC_SCOPE_EXIT {
    auto& s = response.status;
    if (s.ok()) {
      health_checker_->IOSuccess();
    } else if (!s.IsNotFound()) {
      health_checker_->IOError();
    }
  };

  if (!running_.load(std::memory_order_relaxed)) {
    response.status = Status::Internal("rdma peer not running");
    return response;
  }

  auto* pool = infiniband::ClientRDMAPool::Instance();
  infiniband::Buffer* buf = nullptr;
  bool pool_buffer = false;
  bool direct_response_sink = false;
  uint64_t advertised_addr = 0;
  uint32_t advertised_length = 0;
  uint32_t advertised_rkey = 0;

  auto advertise_registered_iobuf = [&](const IOBuffer* io) {
    if (io == nullptr || io->Size() == 0) {
      return false;
    }
    auto iovs = io->Fetch();
    if (iovs.size() != 1) {
      return false;
    }
    infiniband::RegisteredMemoryRegion region;
    if (!infiniband::FindRegisteredRDMAMemory(iovs[0].iov_base,
                                              iovs[0].iov_len, &region)) {
      return false;
    }
    advertised_addr = reinterpret_cast<uint64_t>(iovs[0].iov_base);
    advertised_length = static_cast<uint32_t>(iovs[0].iov_len);
    advertised_rkey = region.rkey;
    return true;
  };

  if (request.body != nullptr) {
    if (!advertise_registered_iobuf(request.body)) {
      buf = pool->Acquire();
      if (buf == nullptr) {
        response.status =
            Status::Internal("client rdma pool exhausted or uninitialized");
        return response;
      }
      pool_buffer = true;
      size_t body_size = request.body->Size();
      if (body_size > buf->capacity) {
        pool->Release(buf);
        response.status =
            Status::Internal("request body exceeds rdma buffer capacity");
        return response;
      }
      const_cast<IOBuffer*>(request.body)->CopyTo(buf->data, body_size);
      buf->length = static_cast<uint32_t>(body_size);
      advertised_addr = reinterpret_cast<uint64_t>(buf->data);
      advertised_length = buf->length;
      advertised_rkey = buf->rkey;
    }
  } else if (request.response_body != nullptr &&
             advertise_registered_iobuf(request.response_body)) {
    direct_response_sink = true;
  } else if (request.method == "Range") {
    buf = pool->Acquire();
    if (buf == nullptr) {
      response.status =
          Status::Internal("client rdma pool exhausted or uninitialized");
      return response;
    }
    pool_buffer = true;
    buf->length = buf->capacity;  // full advertised region for Range writes
    advertised_addr = reinterpret_cast<uint64_t>(buf->data);
    advertised_length = buf->length;
    advertised_rkey = buf->rkey;
  }

  infiniband::Controller cntl;
  cntl.SetRequestMemoryContext({
      .addr = advertised_addr,
      .length = advertised_length,
      .rkey = advertised_rkey,
  });

  auto st = client_->template Call<T, U>(
      &cntl, "dingofs.pb.cache.BlockCacheService", request.method, request.raw,
      &response.raw);

  if (!st.ok()) {
    if (pool_buffer) {
      pool->Release(buf);
    }
    LOG(ERROR) << "RDMAPeer::SendRequest failed: " << st.ToString()
               << " endpoint=" << EndPoint();
    response.status = st;
    return response;
  }

  response.status = ToStatus(response.raw.status());
  if (!response.status.ok()) {
    if (pool_buffer) {
      pool->Release(buf);
    }
    return response;
  }

  // For Range responses the server has RDMA_WRITTEN the block payload into
  // buf->data already (see ServerSession::SendResponse + Controller
  // ResponseAttachment). The exact number of bytes written is reported via
  // cntl.ResponseAttachmentLength (set from ResponseMeta on the wire). Hand
  // the same buffer to the caller as an IOBuffer whose deleter returns it to
  // the pool — zero-copy from wire to FUSE.
  uint32_t att_len = cntl.ResponseAttachmentLength();
  if (direct_response_sink) {
    DCHECK_GT(att_len, 0u);
  } else if (att_len > 0) {
    CHECK(pool_buffer);
    butil::IOBuf io;
    io.append_user_data(buf->data, att_len,
                        [buf](void*) {
                          infiniband::ClientRDMAPool::Instance()->Release(buf);
                        });
    response.body = IOBuffer(std::move(io));
  } else {
    if (pool_buffer) {
      pool->Release(buf);
    }
  }
  return response;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_PEER_H_
