// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "connector_base.h"
#include "exists_cache.h"

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "common/blockaccess/block_accesser.h"

namespace dingofs {
namespace connector {

struct DingoFSConfig {
  uint32_t fs_id = 1;
  uint64_t ino = 1;
  std::string cache_dir;
  uint32_t cache_size_mb = 102400;
  size_t exists_cache_capacity = 100000;
  int num_workers = 4;
};

// Connection object -- one instance per worker thread.
// Holds non-owning pointers to process-level singletons.
struct DingoFSConnection {
  cache::BlockCache* block_cache;
  ExistsCache* exists_cache;
  DingoFSConfig config;
};

class DingoFSConnector
    : public lmcache::connector::ConnectorBase<DingoFSConnection> {
 public:
  // Expanded-parameter constructor for pybind11
  DingoFSConnector(const std::string& cache_dir,
                   uint32_t fs_id = 1,
                   uint64_t ino = 1,
                   uint32_t cache_size_mb = 102400,
                   size_t exists_cache_capacity = 100000,
                   int num_workers = 4);

  ~DingoFSConnector() override;

 protected:
  DingoFSConnection create_connection() override;

  void do_single_set(DingoFSConnection& conn, const std::string& key,
                     const void* buf, size_t len,
                     size_t chunk_size) override;

  void do_single_get(DingoFSConnection& conn, const std::string& key,
                     void* buf, size_t len,
                     size_t chunk_size) override;

  bool do_single_exists(DingoFSConnection& conn,
                        const std::string& key) override;

  void shutdown_connections() override;

 private:
  cache::BlockKey MapKey(const std::string& key, uint16_t shard_id) const;
  uint32_t ExtractChunkHash(const std::string& key) const;
  uint16_t CalcNumShards(size_t data_size) const;

  // Synchronous wrappers around bthread async APIs
  Status SyncCache(const cache::BlockKey& block_key,
                   const cache::Block& block);
  Status SyncRange(const cache::BlockKey& block_key,
                   size_t offset, size_t length, IOBuffer* io_buf);

  DingoFSConfig config_;
  std::unique_ptr<cache::BlockCache> block_cache_;
  std::unique_ptr<blockaccess::BlockAccesser> block_accesser_;
  std::unique_ptr<ExistsCache> exists_cache_;

  static constexpr size_t kMaxBlockSize = 4 * 1024 * 1024;
};

}  // namespace connector
}  // namespace dingofs
