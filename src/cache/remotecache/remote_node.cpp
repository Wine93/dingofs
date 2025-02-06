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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_node.h"

#include <butil/endpoint.h>

#include <memory>

#include "cache/common/errno.h"
#include "dingofs/blockcache.pb.h"
#include "dingofs/cachegroup.pb.h"

namespace dingofs {
namespace cache {
namespace remotecache {

using pb::cache::blockcache::BlockCacheErrCode;

RemoteNodeImpl::RemoteNodeImpl(
    const pb::mds::cachegroup::CacheGroupMember& member)
    : member_(member), channel_(std::make_unique<brpc::Channel>()) {}

bool RemoteNodeImpl::Init() {
  std::string listen_ip = member_.ip();
  uint32_t listen_port = member_.port();

  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "str2endpoint(" << listen_ip << "," << listen_port
               << ") failed, rc = " << rc;
    return false;
  }

  rc = channel_->Init(ep, nullptr);
  if (rc != 0) {
    LOG(INFO) << "Init channel for " << listen_ip << ":" << listen_port
              << " failed, rc = " << rc;
    return false;
  }

  LOG(INFO) << "Create channel for " << listen_ip << ":" << listen_port
            << " success.";
  return true;
}

Errno RemoteNodeImpl::Range(const BlockKey& block_key, uint64_t block_size,
                            uint64_t offset, uint64_t length,
                            butil::IOBuf* buffer) {
  brpc::Controller cntl;
  pb::cache::blockcache::RangeRequest request;
  pb::cache::blockcache::RangeResponse response;

  *request.mutable_block_key() = block_key.ToPb();
  request.set_block_size(block_size);
  request.set_offset(offset);
  request.set_length(length);

  cntl.set_timeout_ms(3000);  // FIXME
  pb::cache::blockcache::BlockCacheService_Stub stub(channel_.get());
  stub.Range(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << cntl.ErrorText();
    return Errno::IO_ERROR;
  }

  auto status = response.status();
  if (status == BlockCacheErrCode::BlockCacheOk) {
    *buffer = cntl.response_attachment();
    return Errno::OK;
  }
  return Errno::IO_ERROR;
}

}  // namespace remotecache
}  // namespace cache
}  // namespace dingofs
