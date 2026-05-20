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

#include "cache/cachegroup/rdma_service.h"

#include <brpc/closure_guard.h>
#include <glog/logging.h>

#include "cache/common/block_key_helper.h"
#include "cache/common/context.h"
#include "cache/common/error.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

RDMABlockCacheServiceImpl::RDMABlockCacheServiceImpl(
    CacheNodeSPtr node, infiniband::RDMAMemoryPool* attachment_pool)
    : node_(CHECK_NOTNULL(node)),
      attachment_pool_(CHECK_NOTNULL(attachment_pool)) {}

void RDMABlockCacheServiceImpl::Put(
    google::protobuf::RpcController* controller,
    const pb::cache::PutRequest* request, pb::cache::PutResponse* response,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<infiniband::Controller*>(controller);
  auto ctx = NewContext();

  auto* buf = attachment_pool_->Require();
  if (buf == nullptr) {
    LOG(ERROR) << "RDMA Put: attachment pool exhausted";
    response->set_status(pb::cache::BlockCacheErrFailure);
    return;
  }
  cntl->SetOnDestroy(
      [pool = attachment_pool_, buf]() { pool->Release(buf); });

  auto status = cntl->ReadRequestAttachment(buf, request->block_size());
  if (!status.ok()) {
    LOG(ERROR) << "RDMA Put: RDMA_READ failed: " << status.ToString();
    response->set_status(ToPBErr(status));
    return;
  }

  IOBuffer wrapped;
  wrapped.AppendUserData(buf->data, request->block_size(), [](void*) {});
  BlockContext block_ctx = FromContextPB(request->block_ctx());
  status = node_->Put(ctx, block_ctx, Block(std::move(wrapped)));
  response->set_status(ToPBErr(status));
}

void RDMABlockCacheServiceImpl::Range(
    google::protobuf::RpcController* controller,
    const pb::cache::RangeRequest* request, pb::cache::RangeResponse* response,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<infiniband::Controller*>(controller);
  auto ctx = NewContext();

  auto* buf = attachment_pool_->Require();
  if (buf == nullptr) {
    LOG(ERROR) << "RDMA Range: attachment pool exhausted";
    response->set_status(pb::cache::BlockCacheErrFailure);
    return;
  }
  cntl->SetOnDestroy(
      [pool = attachment_pool_, buf]() { pool->Release(buf); });

  if (request->length() > buf->capacity) {
    LOG(ERROR) << "RDMA Range: request length=" << request->length()
               << " exceeds buffer capacity=" << buf->capacity;
    response->set_status(pb::cache::BlockCacheErrFailure);
    return;
  }

  size_t out_length = 0;
  BlockContext block_ctx = FromContextPB(request->block_ctx());
  auto status = node_->Range(ctx, block_ctx, request->offset(),
                             request->length(), buf->data, buf->capacity,
                             &out_length, request->block_size(),
                             buf->io_uring_buf_index);
  if (status.ok()) {
    buf->length = static_cast<uint32_t>(out_length);
    cntl->SetResponseAttachment(buf);
  }
  response->set_status(ToPBErr(status));
  response->set_cache_hit(ctx->GetCacheHit());
}

void RDMABlockCacheServiceImpl::Cache(
    google::protobuf::RpcController* controller,
    const pb::cache::CacheRequest* request, pb::cache::CacheResponse* response,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<infiniband::Controller*>(controller);
  auto ctx = NewContext();

  auto* buf = attachment_pool_->Require();
  if (buf == nullptr) {
    LOG(ERROR) << "RDMA Cache: attachment pool exhausted";
    response->set_status(pb::cache::BlockCacheErrFailure);
    return;
  }

  auto status = cntl->ReadRequestAttachment(buf, request->block_size());
  if (!status.ok()) {
    LOG(ERROR) << "RDMA Cache: RDMA_READ failed: " << status.ToString();
    attachment_pool_->Release(buf);
    response->set_status(ToPBErr(status));
    return;
  }

  // Ownership of `buf` transfers into AsyncCache via the on_buffer_released
  // hook; we do NOT set cntl->SetOnDestroy for the attachment here. Passing
  // buf->io_uring_buf_index lets the disk write go straight from this RDMA
  // buffer via io_uring_prep_write_fixed — no bounce through the disk pool.
  BlockContext block_ctx = FromContextPB(request->block_ctx());
  status = node_->AsyncCache(ctx, block_ctx, buf->data, request->block_size(),
                             [pool = attachment_pool_, buf]() {
                               pool->Release(buf);
                             },
                             buf->io_uring_buf_index);
  response->set_status(ToPBErr(status));
}

void RDMABlockCacheServiceImpl::Prefetch(
    google::protobuf::RpcController* controller,
    const pb::cache::PrefetchRequest* request,
    pb::cache::PrefetchResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto ctx = NewContext();
  BlockContext block_ctx = FromContextPB(request->block_ctx());
  auto status =
      node_->AsyncPrefetch(ctx, block_ctx, request->block_size());
  response->set_status(ToPBErr(status));
}

void RDMABlockCacheServiceImpl::Ping(
    google::protobuf::RpcController* /*controller*/,
    const pb::cache::PingRequest* /*request*/,
    pb::cache::PingResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
}

}  // namespace cache
}  // namespace dingofs
