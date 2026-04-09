// SPDX-License-Identifier: Apache-2.0

#ifndef DINGOFS_INTEGRATION_LMCACHE_NOOP_BLOCK_ACCESSER_H_
#define DINGOFS_INTEGRATION_LMCACHE_NOOP_BLOCK_ACCESSER_H_

#include "common/blockaccess/block_accesser.h"

namespace dingofs {

// NoopBlockAccesser satisfies StorageClient's constructor requirement.
// In KVCache scenarios we only use Cache()/Range()/IsCached() which
// never invoke BlockAccesser methods.
class NoopBlockAccesser : public blockaccess::BlockAccesser {
 public:
  Status Init() override { return Status::OK(); }
  Status Destroy() override { return Status::OK(); }
  bool ContainerExist() override { return true; }

  Status Put(const std::string& /*key*/,
             const std::string& /*data*/) override {
    return Status::NotSupport("NoopBlockAccesser");
  }

  Status Put(const std::string& /*key*/, const char* /*buffer*/,
             size_t /*length*/) override {
    return Status::NotSupport("NoopBlockAccesser");
  }

  void AsyncPut(
      std::shared_ptr<blockaccess::PutObjectAsyncContext> ctx) override {
    if (ctx && ctx->cb) {
      ctx->status = Status::NotSupport("NoopBlockAccesser");
      ctx->cb(ctx);
    }
  }

  Status Get(const std::string& /*key*/, std::string* /*data*/) override {
    return Status::NotSupport("NoopBlockAccesser");
  }

  void AsyncGet(
      std::shared_ptr<blockaccess::GetObjectAsyncContext> ctx) override {
    if (ctx && ctx->cb) {
      ctx->status = Status::NotSupport("NoopBlockAccesser");
      ctx->cb(ctx);
    }
  }

  Status Range(const std::string& /*key*/, off_t /*offset*/, size_t /*length*/,
               char* /*buffer*/) override {
    return Status::NotSupport("NoopBlockAccesser");
  }

  bool BlockExist(const std::string& /*key*/) override { return false; }

  Status Delete(const std::string& /*key*/) override {
    return Status::NotSupport("NoopBlockAccesser");
  }

  Status BatchDelete(const std::list<std::string>& /*keys*/) override {
    return Status::NotSupport("NoopBlockAccesser");
  }
};

}  // namespace dingofs

#endif  // DINGOFS_INTEGRATION_LMCACHE_NOOP_BLOCK_ACCESSER_H_
