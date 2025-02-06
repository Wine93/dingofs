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
 * Created Date: 2025-03-18
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/s3_client_pool.h"

#include <glog/logging.h>

#include <memory>
#include <ostream>

#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "dingofs/mds.pb.h"
#include "stub/metric/metric.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace common {

using pb::mds::FsInfo;

using cache::common::S3ClientOptions;
using pb::mds::FSStatusCode;
using utils::ReadLockGuard;
using utils::WriteLockGuard;

S3ClientPoolImpl::S3ClientPoolImpl(std::shared_ptr<MdsClient> mds_client)
    : mds_client_(mds_client) {}

Errno S3ClientPoolImpl::Get(uint32_t fs_id,
                            std::shared_ptr<S3Client>& s3_client) {
  auto rc = DoGet(fs_id, s3_client);
  if (rc == Errno::OK) {
    return rc;
  }

  if (NewS3Client(fs_id, s3_client)) {
    DoInsert(fs_id, s3_client);
    return Errno::OK;
  }
  return Errno::NOT_FOUND;
}

Errno S3ClientPoolImpl::DoGet(uint32_t fs_id,
                              std::shared_ptr<S3Client>& s3_client) {
  ReadLockGuard lk(rwlock_);
  auto iter = clients_.find(fs_id);
  if (iter != clients_.end()) {
    s3_client = iter->second;
    return Errno::OK;
  }
  return Errno::NOT_FOUND;
}

void S3ClientPoolImpl::DoInsert(uint32_t fs_id,
                                std::shared_ptr<S3Client> s3_client) {
  WriteLockGuard lk(rwlock_);
  clients_.emplace(fs_id, s3_client);
}

bool S3ClientPoolImpl::NewS3Client(uint32_t fs_id,
                                   std::shared_ptr<S3Client>& s3_client) {
  pb::mds::FsInfo fs_info;
  FSStatusCode code = mds_client_->GetFsInfo(fs_id, &fs_info);
  if (code != FSStatusCode::OK) {
    LOG(ERROR) << "Get filesystem information failed: fs_id = " << fs_id
               << ", rc = " << FSStatusCode_Name(code);
    return false;
  } else if (!fs_info.detail().has_s3info()) {
    LOG(ERROR) << "The filesystem missing s3 information: fs_id = " << fs_id;
    return false;
  }

  S3ClientOptions options;
  auto s3_info = fs_info.detail().s3info();
  options.ak = s3_info.ak();
  options.sk = s3_info.sk();
  options.endpoint = s3_info.endpoint();
  options.bucket_name = s3_info.bucketname();
  s3_client = std::make_shared<S3ClientImpl>();
  s3_client->Init();
  return true;
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
