
#include "cache/bench/block_reader.h"

#include <absl/strings/str_format.h>
#include <butil/time.h>
#include <fcntl.h>

#include <cstddef>
#include <memory>

#include "cache/common/const.h"
#include "cache/storage/aio/aio_queue.h"
#include "cache/storage/aio/linux_io_uring.h"
#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "cache/utils/posix.h"
#include "common/const.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

BlockReader::BlockReader()
    : buffer_pool_(std::make_shared<BufferPool>(FLAGS_ioring_iodepth * 4 * kMiB,
                                                Helper::GetIOAlignedBlockSize(),
                                                4 * kMiB)),
      io_ring_(std::make_shared<LinuxIOUring>(FLAGS_ioring_iodepth,
                                              buffer_pool_->RawBuffer())),
      aio_queue_(std::make_unique<AioQueueImpl>(io_ring_)) {}

Status BlockReader::Init() {
  auto status = io_ring_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start io ring failed: " << status.ToString();
    return status;
  }

  status = status = aio_queue_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start aio queue failed: " << status.ToString();
    return status;
  }

  LOG(INFO) << "Init block reader success: iodepth = " << FLAGS_ioring_iodepth;
  return Status::OK();
}

/*
Status BlockReader::Read(const std::string& filepath) {
  int fd;
  auto status = Posix::Open(filepath, O_RDONLY | O_DIRECT, &fd);
  if (!status.ok()) {
    return status;
  }

  butil::Timer timer;

  timer.start();

  IOBuffer buffer;
  Aio aio(NewContext(), fd, 0, 4 * kMiB, &buffer, true);
  aio_queue_->Submit(&aio);

  aio.Wait();
  CHECK(aio.status().ok());

  timer.stop();

  LOG(INFO) << "Read block success, cost " << timer.u_elapsed(1.0) / 1e6
            << " seconds.";
  return Status::OK();
}
*/

Status BlockReader::Read(const std::string& filepath, int stripe_size) {
  int fd;
  auto status = Posix::Open(filepath, O_RDONLY | O_DIRECT, &fd);
  if (!status.ok()) {
    return status;
  }

  std::vector<Aio*> aios;
  std::vector<IOBuffer*> io_buffers;

  // int stripe_count = 1 * kMiB / stripe_size;
  int stripe_count = 128 * kKiB / stripe_size;
  for (int i = 0; i < stripe_count; i++) {
    auto* buffer = new IOBuffer();
    auto* aio =
        new Aio(NewContext(), fd, i * stripe_size, stripe_size, buffer, true);

    io_buffers.push_back(buffer);
    aios.push_back(aio);
  }

  butil::Timer timer;
  timer.start();

  for (auto* aio : aios) {
    aio_queue_->Submit(aio);
  }

  for (auto* aio : aios) {
    aio->Wait();
  }

  timer.stop();

  LOG(INFO) << "Read block success, strip_size=" << stripe_size
            << ", stripe_count=" << stripe_count << ", total cost "
            << absl::StrFormat("%.6lf", timer.u_elapsed(1.0) / 1e6)
            << " seconds.";
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs

int main(int argc, char* argv[]) {
  CHECK_EQ(argc, 3);

  auto stripe_size = atoi(argv[2]);

  dingofs::cache::BlockReader reader;
  CHECK(reader.Init().ok());
  CHECK(reader.Read(argv[1], stripe_size * dingofs::kKiB).ok());

  return 0;
}
