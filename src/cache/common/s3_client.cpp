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

#include "cache/common/s3_client.h"

#include <butil/iobuf.h>

#include <ostream>

#include "stub/metric/metric.h"

namespace dingofs {
namespace cache {
namespace common {

using aws::GetObjectAsyncContext;
using aws::PutObjectAsyncContext;
using aws::S3AdapterOption;
using stub::metric::MetricGuard;
using stub::metric::S3Metric;

#define METRIC_GUARD(metric)             \
  auto start = butil::cpuwide_time_us(); \
  MetricGuard guard(&rc, &S3Metric::GetInstance().metric, length, start);

S3ClientImpl::S3ClientImpl(S3ClientOptions options)
    : options_(options), client_(std::make_unique<aws::S3Adapter>()) {}

void S3ClientImpl::Init() {
  aws::S3AdapterOption option;
  option.ak = options_.ak;
  option.sk = options_.sk;
  option.s3Address = options_.endpoint;
  option.bucketName = options_.bucket_name;
  client_->Init(option);
  // TODO: loglevel
}

void S3ClientImpl::Destroy() { client_->Deinit(); }

Errno S3ClientImpl::Put(const std::string& key, const char* buffer,
                        size_t length) {
  int rc;
  METRIC_GUARD(write_s3);
  rc = client_->PutObject(S3Key(key), buffer, length);
  if (rc < 0) {
    LOG(ERROR) << "Put object(" << key << ") failed, retCode=" << rc;
    return Errno::IO_ERROR;
  }
  return Errno::OK;
}

Errno S3ClientImpl::Range(const std::string& key, uint64_t offset,
                          uint64_t length, butil::IOBuf* buffer) {
  int rc;
  METRIC_GUARD(read_s3);
  char* data = new char[length];
  buffer->append_user_data(data, length, [](void* addr) {});

  rc = client_->GetObject(S3Key(key), data, offset, length);
  if (rc < 0) {
    if (!client_->ObjectExist(S3Key(key))) {  // TODO: more efficient
      LOG(WARNING) << "Object(" << key << ") not found.";
      return Errno::NOT_FOUND;
    }
    LOG(ERROR) << "Get object(" << key << ") failed, retCode=" << rc;
    return Errno::IO_ERROR;
  }
  return Errno::OK;
}

void S3ClientImpl::AsyncPut(const std::string& key, const char* buffer,
                            size_t length, RetryCallback retry) {
  auto context = std::make_shared<PutObjectAsyncContext>();
  context->key = key;
  context->buffer = buffer;
  context->bufferSize = length;
  context->startTime = butil::cpuwide_time_us();
  context->cb = [&,
                 retry](const std::shared_ptr<PutObjectAsyncContext>& context) {
    MetricGuard guard(&context->retCode, &S3Metric::GetInstance().write_s3,
                      context->bufferSize, context->startTime);
    if (retry(context->retCode)) {  // retry
      client_->PutObjectAsync(context);
    }
  };
  client_->PutObjectAsync(context);
}

void S3ClientImpl::AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) {
  client_->PutObjectAsync(context);
}

void S3ClientImpl::AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) {
  client_->GetObjectAsync(context);
}

Aws::String S3ClientImpl::S3Key(const std::string& key) {
  return Aws::String(key.c_str(), key.size());
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
