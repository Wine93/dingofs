#ifndef DINGOFS_SRC_CACHE_BENCH_BLOCK_READER_H_
#define DINGOFS_SRC_CACHE_BENCH_BLOCK_READER_H_

#include "cache/storage/aio/aio.h"
#include "cache/utils/buffer_pool.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class BlockReader {
 public:
  BlockReader();

  Status Init();

  Status Read(const std::string& filepath, int stripe_size);

 private:
  BufferPoolSPtr buffer_pool_;
  IORingSPtr io_ring_;
  AioQueueUPtr aio_queue_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCH_BLOCK_READER_H_
