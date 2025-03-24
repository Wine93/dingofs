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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_

#include <butil/iobuf.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/hash/ketama_con_hash.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache.h"
#include "cache/blockcache/disk_cache_watcher.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using base::hash::ConHash;
using cache::common::DiskCacheOptions;
using UploadFunc = CacheStore::UploadFunc;

class DiskCacheGroup : public CacheStore {
 public:
  ~DiskCacheGroup() override = default;

  explicit DiskCacheGroup(std::vector<DiskCacheOptions> options);

  Errno Init(UploadFunc uploader) override;

  Errno Shutdown() override;

  Errno Stage(const BlockKey& key, const Block& block,
              BlockContext ctx) override;

  Errno RemoveStage(const BlockKey& key, BlockContext ctx) override;

  Errno Cache(const BlockKey& key, const Block& block) override;

  Errno Load(const BlockKey& key,
             std::shared_ptr<BlockReader>& reader) override;

  Errno Load(const BlockKey& key, uint64_t offset, uint64_t length,
             butil::IOBuf* buffer) override;

  bool IsCached(const BlockKey& key) override;

  std::string Id() override;

 private:
  std::vector<uint64_t> CalcWeights(std::vector<DiskCacheOptions> options);

  std::shared_ptr<DiskCache> GetStore(const BlockKey& key);

 private:
  std::vector<DiskCacheOptions> options_;
  std::unique_ptr<ConHash> chash_;
  std::unordered_map<std::string, std::shared_ptr<DiskCache>> stores_;
  std::unique_ptr<DiskCacheWatcher> watcher_;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_DISK_CACHE_GROUP_H_
