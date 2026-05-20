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

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_RDMA_SERVICE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_RDMA_SERVICE_H_

#include "cache/cachegroup/node.h"
#include "cache/infiniband/memory.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

// BlockCacheService implementation backed by the RDMA transport. Registered
// into infiniband::Messenger (NOT into a brpc::Server). The brpc-based
// BlockCacheServiceImpl in service.{h,cc} continues to serve TCP traffic.
//
// Range path: zero-copy via cntl->SetResponseAttachment(); transport does
//   RDMA_WRITE of the attachment buffer into the client's advertised region.
// Cache path: zero-copy via cntl->ReadRequestAttachment(); transport does
//   RDMA_READ from the client's advertised region into a server-side
//   attachment buffer.
class RDMABlockCacheServiceImpl final : public pb::cache::BlockCacheService {
 public:
  RDMABlockCacheServiceImpl(CacheNodeSPtr node,
                            infiniband::RDMAMemoryPool* attachment_pool);

  void Put(google::protobuf::RpcController* controller,
           const pb::cache::PutRequest* request,
           pb::cache::PutResponse* response,
           google::protobuf::Closure* done) override;

  void Range(google::protobuf::RpcController* controller,
             const pb::cache::RangeRequest* request,
             pb::cache::RangeResponse* response,
             google::protobuf::Closure* done) override;

  void Cache(google::protobuf::RpcController* controller,
             const pb::cache::CacheRequest* request,
             pb::cache::CacheResponse* response,
             google::protobuf::Closure* done) override;

  void Prefetch(google::protobuf::RpcController* controller,
                const pb::cache::PrefetchRequest* request,
                pb::cache::PrefetchResponse* response,
                google::protobuf::Closure* done) override;

  void Ping(google::protobuf::RpcController* controller,
            const pb::cache::PingRequest* request,
            pb::cache::PingResponse* response,
            google::protobuf::Closure* done) override;

 private:
  CacheNodeSPtr node_;
  infiniband::RDMAMemoryPool* attachment_pool_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_RDMA_SERVICE_H_
