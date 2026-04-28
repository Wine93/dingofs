/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-01-08
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <butil/memory/aligned_memory.h>

#include <utility>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/block_handle_helper.h"
#include "cache/common/error.h"
#include "cache/common/slab_pool.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

template <typename T, typename U>
struct ServiceClosure : public google::protobuf::Closure {
  ServiceClosure(google::protobuf::Closure* done, const T* request, U* response,
                 Status& status)
      : done(done), request(request), response(response), status(status) {}

  ~ServiceClosure() override = default;

  void Run() override {
    std::unique_ptr<ServiceClosure<T, U>> self_guard(this);

    if (response->status() != pb::cache::BlockCacheOk) {
      LOG(ERROR) << "Fail to process rpc request="
                 << request->ShortDebugString()
                 << ", response=" << response->ShortDebugString();
    }
    done->Run();
  }

  google::protobuf::Closure* done;
  const T* request;
  U* response;
  Status& status;
};

BlockCacheServiceImpl::BlockCacheServiceImpl(ServiceType service_type,
                                             CacheNode* node)
    : service_type_(service_type), node_(CHECK_NOTNULL(node)) {}

IOBuffer BlockCacheServiceImpl::GetRequestAttachment(
    google::protobuf::RpcController* controller) {
  if (service_type_ == ServiceType::kBRPC) {
    auto* cntl = static_cast<brpc::Controller*>(controller);
    return IOBuffer(cntl->request_attachment().movable());
  } else if (service_type_ == ServiceType::kRDMA) {
    auto* cntl = static_cast<infiniband::Controller*>(controller);
  }

  CHECK(false) << "Unknown service type";
}

void BlockCacheServiceImpl::Put(google::protobuf::RpcController* controller,
                                const pb::cache::PutRequest* request,
                                pb::cache::PutResponse* response,
                                google::protobuf::Closure* done) {
  Status status;
  auto* srv_done = new ServiceClosure(done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  BlockHandle handle = FromHandlePB(request->handle());
  IOBuffer block = GetRequestAttachment(controller);
  status = node_->Put(std::move(handle), std::move(block));
  response->set_status(ToPBErr(status));
}

IOBuffer BlockCacheServiceImpl::AllocIOBuffer(size_t size) {
  auto& slab_pool = GetGlobalSendSlabPool();

  IOBuffer buffer;
  buffer.AppendUserData(, size_t size, std::function<void(void*)> deleter);
}

void BlockCacheServiceImpl::Range(google::protobuf::RpcController* controller,
                                  const pb::cache::RangeRequest* request,
                                  pb::cache::RangeResponse* response,
                                  google::protobuf::Closure* done) {
  Status status;
  auto* srv_done = new ServiceClosure(done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  BlockHandle handle = FromHandlePB(request->handle());
  // 这么要考虑前后对齐, 需要对分配点内存
  auto buffer = AllocIOBuffer(request->offset(), request->length());
  status = node_->Range(handle, request->offset(), request->length(), &buffer,
                        request->block_size(), &cache_hit);

  // auto* cntl = static_cast<brpc::Controller*>(controller);

  // IOBuffer buffer;
  // bool cache_hit = false;
  // if (status.ok()) {
  //   cntl->response_attachment() = buffer.IOBuf().movable();
  // }
  // response->set_status(ToPBErr(status));
  // response->set_cache_hit(cache_hit);
}

// void BlockCacheServiceImpl::Range(google::protobuf::RpcController*
// controller,
//                                   const pb::cache::RangeRequest* request,
//                                   pb::cache::RangeResponse* response,
//                                   google::protobuf::Closure* done) {
//   Status status;
//   auto* cntl = static_cast<brpc::Controller*>(controller);
//   auto* srv_done = new ServiceClosure(done, request, response, status);
//   brpc::ClosureGuard done_guard(srv_done);
//
//   IOBuffer buffer;
//   bool cache_hit = false;
//   BlockHandle handle = FromHandlePB(request->handle());
//   status = node_->Range(handle, request->offset(), request->length(),
//   &buffer,
//                         request->block_size(), &cache_hit);
//   if (status.ok()) {
//     cntl->response_attachment() = buffer.IOBuf().movable();
//   }
//   response->set_status(ToPBErr(status));
//   response->set_cache_hit(cache_hit);
// }

void BlockCacheServiceImpl::Cache(google::protobuf::RpcController* controller,
                                  const pb::cache::CacheRequest* request,
                                  pb::cache::CacheResponse* response,
                                  google::protobuf::Closure* done) {
  Status status;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto* srv_done = new ServiceClosure(done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  IOBuffer buffer = IOBuffer(cntl->request_attachment().movable());
  status = CheckBodySize(request->block_size(), buffer.Size());
  if (status.ok()) {
    BlockHandle handle = FromHandlePB(request->handle());
    status = node_->AsyncCache(std::move(handle), std::move(buffer));
  }
  response->set_status(ToPBErr(status));
}

void BlockCacheServiceImpl::Prefetch(
    google::protobuf::RpcController* controller,
    const pb::cache::PrefetchRequest* request,
    pb::cache::PrefetchResponse* response, google::protobuf::Closure* done) {
  Status status;
  auto* cntl = static_cast<brpc::Controller*>(controller);
  auto* srv_done = new ServiceClosure(done, request, response, status);
  brpc::ClosureGuard done_guard(srv_done);

  BlockHandle handle = FromHandlePB(request->handle());
  status = node_->AsyncPrefetch(handle, request->block_size());
  response->set_status(ToPBErr(status));
}

void BlockCacheServiceImpl::Ping(
    google::protobuf::RpcController* /*controller*/,
    const pb::cache::PingRequest* /*request*/,
    pb::cache::PingResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  // do nothing, just reply
}

}  // namespace cache
}  // namespace dingofs
