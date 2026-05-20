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

#include "cache/common/block_handle_helper.h"
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

  auto* buf = attachment_pool_->Require();
  if (buf == nullptr) {
    LOG(ERROR) << "RDMA Put: attachment pool exhausted";
    response->set_status(pb::cache::BlockCacheErrFailure);
    return;
  }
  cntl->SetOnDestroy(
      [pool = attachment_pool_, buf]() { pool->Release(buf); });

  if (request->block_size() > buf->capacity) {
    LOG(ERROR) << "RDMA Put: request block_size=" << request->block_size()
               << " exceeds buffer capacity=" << buf->capacity;
    response->set_status(pb::cache::BlockCacheErrInvalidParam);
    return;
  }

  auto status = cntl->ReadRequestAttachment(buf, request->block_size());
  if (!status.ok()) {
    LOG(ERROR) << "RDMA Put: RDMA_READ failed: " << status.ToString();
    response->set_status(ToPBErr(status));
    return;
  }

  // Wrap the RDMA-registered region as an IOBuffer (zero-copy hand-off);
  // the controller's on_destroy hook owns the pool release.
  IOBuffer block;
  block.AppendUserData(buf->data, request->block_size(), [](void*) {});
  PutOption option;
  option.source_buffer_prepared = true;
  option.source_buffer = buf->data;
  option.source_buffer_capacity = buf->capacity;
  option.source_buffer_index = buf->io_uring_write_buf_index;
  status = node_->Put(FromHandlePB(request->handle()), std::move(block),
                      option);
  response->set_status(ToPBErr(status));
}

void RDMABlockCacheServiceImpl::Range(
    google::protobuf::RpcController* controller,
    const pb::cache::RangeRequest* request, pb::cache::RangeResponse* response,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<infiniband::Controller*>(controller);

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

  IOBuffer buffer;
  bool cache_hit = false;
  RangeOption option;
  option.retrieve_storage =
      request->has_retrieve_storage() ? request->retrieve_storage() : true;
  option.buffer_prepared = true;
  option.prepared_buffer = buf->data;
  option.prepared_buffer_capacity = buf->capacity;
  option.prepared_buffer_index = buf->io_uring_read_buf_index;
  auto status = node_->Range(FromHandlePB(request->handle()), request->offset(),
                             request->length(), &buffer, request->block_size(),
                             &cache_hit, option);
  if (status.ok()) {
    buf->length = static_cast<uint32_t>(buffer.Size());
    cntl->SetResponseAttachment(buf);
  }
  response->set_status(ToPBErr(status));
  response->set_cache_hit(cache_hit);
}

void RDMABlockCacheServiceImpl::Cache(
    google::protobuf::RpcController* controller,
    const pb::cache::CacheRequest* request, pb::cache::CacheResponse* response,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<infiniband::Controller*>(controller);

  auto* buf = attachment_pool_->Require();
  if (buf == nullptr) {
    LOG(ERROR) << "RDMA Cache: attachment pool exhausted";
    response->set_status(pb::cache::BlockCacheErrFailure);
    return;
  }

  if (request->block_size() > buf->capacity) {
    LOG(ERROR) << "RDMA Cache: request block_size=" << request->block_size()
               << " exceeds buffer capacity=" << buf->capacity;
    attachment_pool_->Release(buf);
    response->set_status(pb::cache::BlockCacheErrInvalidParam);
    return;
  }

  auto status = cntl->ReadRequestAttachment(buf, request->block_size());
  if (!status.ok()) {
    LOG(ERROR) << "RDMA Cache: RDMA_READ failed: " << status.ToString();
    attachment_pool_->Release(buf);
    response->set_status(ToPBErr(status));
    return;
  }

  // Hand ownership of `buf` to the IOBuffer's deleter — it returns the
  // attachment to the pool when the async cache write finally drops the
  // last reference to the user-data region. No on_destroy hook on cntl
  // because cntl can be retired before the disk write completes.
  IOBuffer block;
  auto* pool = attachment_pool_;
  block.AppendUserData(buf->data, request->block_size(),
                       [pool, buf](void*) { pool->Release(buf); });
  CacheOption option;
  option.source_buffer_prepared = true;
  option.source_buffer = buf->data;
  option.source_buffer_capacity = buf->capacity;
  option.source_buffer_index = buf->io_uring_write_buf_index;
  status = node_->AsyncCache(FromHandlePB(request->handle()),
                             std::move(block), option);
  response->set_status(ToPBErr(status));
}

void RDMABlockCacheServiceImpl::Prefetch(
    google::protobuf::RpcController* /*controller*/,
    const pb::cache::PrefetchRequest* request,
    pb::cache::PrefetchResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto status = node_->AsyncPrefetch(FromHandlePB(request->handle()),
                                     request->block_size());
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
