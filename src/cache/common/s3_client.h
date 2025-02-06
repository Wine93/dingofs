/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-25
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_S3_CLIENT_H_
#define DINGOFS_SRC_CACHE_COMMON_S3_CLIENT_H_

#include <butil/iobuf.h>

#include <functional>
#include <string>

#include "aws/s3_adapter.h"
#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "cache/common/io_buffer.h"

namespace dingofs {
namespace cache {
namespace common {

class S3Client {
 public:
  using GetObjectAsyncContext = aws::GetObjectAsyncContext;
  using PutObjectAsyncContext = aws::PutObjectAsyncContext;
  // retry if callback return true
  using RetryCallback = std::function<bool(int code)>;

 public:
  virtual ~S3Client() = default;

  virtual void Init() = 0;

  virtual void Destroy() = 0;

  virtual Errno Put(const std::string& key, const char* buffer,
                    size_t length) = 0;

  virtual Errno Put(const std::string& key, const butil::IOBuf& buffer) = 0;

  virtual Errno Range(const std::string& key, uint64_t offset, uint64_t length,
                      butil::IOBuf* buffer) = 0;

  virtual void AsyncPut(const std::string& key, const char* buffer,
                        size_t length, RetryCallback callback) = 0;

  virtual void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) = 0;

  virtual void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) = 0;
};

class S3ClientImpl : public S3Client {
 public:
  ~S3ClientImpl() override = default;

  explicit S3ClientImpl(S3ClientOptions options);

  void Init() override;

  void Destroy() override;

  Errno Put(const std::string& key, const char* buffer, size_t length) override;

  Errno Range(const std::string& key, uint64_t offset, uint64_t length,
              butil::IOBuf* buffer) override;

  void AsyncPut(const std::string& key, const char* buffer, size_t length,
                RetryCallback retry) override;

  void AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) override;

  void AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) override;

 private:
  static Aws::String S3Key(const std::string& key);

 private:
  S3ClientOptions options_;
  std::unique_ptr<aws::S3Adapter> client_;
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_S3_CLIENT_H_
