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
 * Created Date: 2025-03-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_S3CLIENT_POOL_H_
#define DINGOFS_SRC_CACHE_COMMON_S3CLIENT_POOL_H_

#include <memory>
#include <unordered_map>

#include "cache/common/s3_client.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace common {

using stub::rpcclient::MdsClient;
using utils::RWLock;

class S3ClientPool {
 public:
  virtual ~S3ClientPool() = default;

  virtual Errno Get(uint32_t fs_id, std::shared_ptr<S3Client>& s3_client) = 0;
};

class S3ClientPoolImpl : public S3ClientPool {
 public:
  static S3ClientPoolImpl& GetInstance() {
    static S3ClientPoolImpl instance;
    return instance;
  }

  void Init(std::shared_ptr<MdsClient> mds_client);

  Errno Get(uint32_t fs_id, std::shared_ptr<S3Client>& s3_client) override;

 private:
  Errno DoGet(uint32_t fs_id, std::shared_ptr<S3Client>& s3_client);

  void DoInsert(uint32_t fs_id, std::shared_ptr<S3Client> s3_client);

  bool NewS3Client(uint32_t fs_id, std::shared_ptr<S3Client>& s3_client);

 private:
  RWLock rwlock_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unordered_map<uint32_t, std::shared_ptr<S3Client>> clients_;
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_S3CLIENT_POOL_H_
