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
 * Created Date: 2025-05-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/local_filesystem.h"

#include <butil/memory/aligned_memory.h>
#include <butil/memory/scope_guard.h>
#include <fcntl.h>
#include <fmt/format.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <string>

#include "cache/blockcache/aio.h"
#include "cache/blockcache/aio_queue.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_health_checker.h"
#include "cache/common/macro.h"
#include "cache/infiniband/rdma_memory.h"
#include "cache/iutil/buffer_pool.h"
#include "cache/iutil/file_util.h"
#include "cache/iutil/inflight_tracker.h"
#include "common/const.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_bool(fix_buffer, true, "whether to use fixed buffer for aio");

LocalFileSystem::LocalFileSystem(DiskCacheLayoutSPtr layout)
    : running_(false),
      layout_(layout),
      write_buffer_pool_(std::make_unique<BufferPool>(4 * kMiB, FLAGS_iodepth,
                                                      kAlignedIOBlockSize)),
      read_buffer_pool_(std::make_unique<BufferPool>(4 * kMiB, FLAGS_iodepth,
                                                     kAlignedIOBlockSize)),
      inflight_(FLAGS_iodepth),
      health_checker_(std::make_unique<DiskHealthChecker>(layout)) {}

Status LocalFileSystem::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "LocalFileSystem already started";
    return Status::OK();
  }

  LOG(INFO) << "LocalFileSystem is starting...";

  auto fixed_write_buffers = write_buffer_pool_->Fetch();
  auto fixed_read_buffers = read_buffer_pool_->Fetch();
  const int rdma_write_index_base = static_cast<int>(fixed_write_buffers.size());
  const int rdma_read_index_base = static_cast<int>(fixed_read_buffers.size());
  auto rdma_buffers =
      infiniband::RDMAFixedBufferRegistry::Instance().CollectIovecs();
  fixed_write_buffers.insert(fixed_write_buffers.end(), rdma_buffers.begin(),
                             rdma_buffers.end());
  fixed_read_buffers.insert(fixed_read_buffers.end(),
                            rdma_buffers.begin(),
                            rdma_buffers.end());

  aio_queue_ =
      std::make_unique<AioQueue>(fixed_write_buffers, fixed_read_buffers);
  auto status = aio_queue_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start AioQueue";
    return status;
  }
  if (!rdma_buffers.empty()) {
    infiniband::RDMAFixedBufferRegistry::Instance().FinalizeIndexAssignment(
        rdma_read_index_base, rdma_write_index_base);
    LOG(INFO) << "Registered " << rdma_buffers.size()
              << " RDMA buffers as io_uring fixed read/write buffers";
  }

  health_checker_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "LocalFileSystem started";
  return Status::OK();
}

Status LocalFileSystem::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "LocalFileSystem already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "LocalFileSystem is shutting down...";

  health_checker_->Shutdown();

  auto status = aio_queue_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown AioQueue";
    return status;
  }
  aio_queue_.reset();

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "LocalFilesystem is down";
  return Status::OK();
}

Status LocalFileSystem::WriteFile(const std::string& path,
                                  const IOBuffer* buffer) {
  DCHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  auto tmppath = TempFilepath(path);
  status = iutil::MkDirs(iutil::ParentDir(tmppath));
  if (!status.ok() && !status.IsExist()) {
    LOG(ERROR) << "Fail to mkdirs `" << iutil::ParentDir(tmppath) << "'";
    return status;
  }

  int fd;
  status = iutil::OpenFile(tmppath, O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT,
                           0644, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to open file=`" << tmppath << "'";
    return status;
  }

  BRPC_SCOPE_EXIT {
    iutil::Close(fd);
    if (!status.ok()) {
      iutil::Unlink(tmppath);
    }
  };

  size_t aligned_length = AlignLength(buffer->Size());
  if (buffer->Size() != aligned_length) {
    status = iutil::Fallocate(fd, 0, 0, aligned_length);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to fallocate file=`" << path << "'";
      return status;
    }
  }

  IOBuffer tbuffer;
  int buf_index = AllocateAlignedMemory(&tbuffer, aligned_length, false);
  buffer->CopyTo(tbuffer.Fetch1());
  status = AioWrite(fd, tbuffer.Fetch1(), aligned_length, buf_index);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to write file'`" << tmppath << "'";
    return status;
  }

  status = iutil::Rename(tmppath, path);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to rename file from `" << tmppath << "' to `" << path
               << "'";
    return status;
  }
  return status;
}

Status LocalFileSystem::ReadFile(const std::string& path, off_t offset,
                                 size_t length, IOBuffer* buffer) {
  CHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  int fd;
  status = iutil::OpenFile(path, O_RDONLY | O_DIRECT, &fd);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to open file";
    return status;
  }

  BRPC_SCOPE_EXIT { iutil::Close(fd); };

  off_t aligned_offset = AlignOffset(offset);
  size_t aligned_length = AlignLength(length + offset - aligned_offset);
  // TODO: 这里的内存需要根据 option 提供 2 个路径，一个是普通的内存池，一个是
  // RDMA 注册的内存池
  int buf_index = AllocateAlignedMemory(buffer, aligned_length, true);
  status =
      AioRead(fd, aligned_offset, aligned_length, buffer->Fetch1(), buf_index);
  if (status.ok()) {
    if (aligned_offset != offset) {
      buffer->PopFront(offset - aligned_offset);
    }
    if (aligned_length != length) {
      buffer->PopBack(aligned_offset + aligned_length - (offset + length));
    }
  } else {
    LOG(ERROR) << "Fail to read file=`" << path << "'";
  }

  return status;
}

Status LocalFileSystem::WriteFile(const std::string& path, const char* data,
                                  size_t length, int buf_index) {
  DCHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  size_t aligned_length = AlignLength(length);
  if (aligned_length != length) {
    // Caller-provided RDMA buffers are sized to block_size (4MB), which is
    // already aligned. If a future caller passes a non-aligned length they'd
    // need to either pre-zero past `length` or expose an aligned capacity.
    return Status::InvalidParam(
        "WriteFile zero-copy path requires aligned length");
  }

  auto tmppath = TempFilepath(path);
  status = iutil::MkDirs(iutil::ParentDir(tmppath));
  if (!status.ok() && !status.IsExist()) {
    return status;
  }

  int fd;
  status = iutil::OpenFile(tmppath, O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT,
                           0644, &fd);
  if (!status.ok()) {
    return status;
  }
  BRPC_SCOPE_EXIT {
    iutil::Close(fd);
    if (!status.ok()) {
      iutil::Unlink(tmppath);
    }
  };

  status = AioWrite(fd, const_cast<char*>(data), aligned_length, buf_index);
  if (!status.ok() && buf_index >= 0) {
    LOG(WARNING) << "Fixed-buffer write failed, retry with non-fixed io_uring: "
                 << status.ToString() << ", path=" << path
                 << ", buf_index=" << buf_index;
    status = AioWrite(fd, const_cast<char*>(data), aligned_length, -1);
  }
  if (!status.ok()) {
    return status;
  }

  return iutil::Rename(tmppath, path);
}

Status LocalFileSystem::ReadFile(const std::string& path, off_t offset,
                                 size_t length, char* data,
                                 size_t data_capacity, int buf_index) {
  DCHECK_RUNNING("LocalFilesystem");

  if (!health_checker_->IsHealthy()) {
    return Status::CacheUnhealthy("disk is unhealthy");
  }

  Status status;
  BRPC_SCOPE_EXIT {
    if (status.ok()) {
      health_checker_->IOSuccess();
    } else {
      health_checker_->IOError();
    }
  };

  off_t aligned_offset = AlignOffset(offset);
  size_t aligned_length = AlignLength(length + offset - aligned_offset);
  if (aligned_length > data_capacity) {
    return Status::InvalidParam("read buffer too small for aligned range");
  }

  int fd;
  status = iutil::OpenFile(path, O_RDONLY | O_DIRECT, &fd);
  if (!status.ok()) {
    return status;
  }
  BRPC_SCOPE_EXIT { iutil::Close(fd); };

  status = AioRead(fd, aligned_offset, aligned_length, data, buf_index);
  if (!status.ok() && buf_index >= 0) {
    LOG(WARNING) << "Fixed-buffer read failed, retry with non-fixed io_uring: "
                 << status.ToString() << ", path=" << path
                 << ", buf_index=" << buf_index;
    status = AioRead(fd, aligned_offset, aligned_length, data, -1);
  }
  if (!status.ok()) {
    return status;
  }

  if (aligned_offset != offset) {
    std::memmove(data, data + (offset - aligned_offset), length);
  }
  return status;
}

// The inflight for aio which use fixed buffer is controlled by buffer pool,
// others need to be tracked here.
struct InflightAioGuard {
  InflightAioGuard(int fd, iutil::InflightTracker* inflight)
      : fd(fd), inflight(inflight) {
    if (!FLAGS_fix_buffer) {
      CHECK(inflight->Add(std::to_string(fd)).ok());
    }
  }

  ~InflightAioGuard() {
    if (!FLAGS_fix_buffer) {
      inflight->Remove(std::to_string(fd));
    }
  }

  int fd;
  iutil::InflightTracker* inflight;
};

Status LocalFileSystem::AioWrite(int fd, char* buffer, size_t length,
                                 int buf_index) {
  InflightAioGuard guard(fd, &inflight_);

  auto aio = Aio(fd, 0, length, buffer, buf_index, false);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.Result().status;
}

Status LocalFileSystem::AioRead(int fd, off_t offset, size_t length,
                                char* buffer, int buf_index) {
  InflightAioGuard guard(fd, &inflight_);

  auto aio = Aio(fd, offset, length, buffer, buf_index, true);
  aio_queue_->Submit(&aio);
  aio.Wait();
  return aio.Result().status;
}

off_t LocalFileSystem::AlignOffset(off_t offset) {
  auto alignment = kAlignedIOBlockSize;
  if (!IsAligned(offset, alignment)) {
    offset = offset - (offset % alignment);
  }
  return offset;
}

size_t LocalFileSystem::AlignLength(size_t length) {
  auto alignment = kAlignedIOBlockSize;
  if (!IsAligned(length, alignment)) {
    length = (length + alignment - 1) & ~(alignment - 1);
  }
  return length;
}

int LocalFileSystem::AllocateAlignedMemory(IOBuffer* buffer,
                                           size_t aligned_length,
                                           bool for_read) {
  if (!FLAGS_fix_buffer) {
    char* data =
        (char*)butil::AlignedAlloc(aligned_length, kAlignedIOBlockSize);
    buffer->AppendUserData(data, aligned_length, butil::AlignedFree);
    return -1;
  }

  // Use fixed buffer. IOUring::FixedBuffers::GetIndex applies the write/read
  // offset internally, so callers pass an index local to the selected pool.
  if (for_read) {
    char* data = read_buffer_pool_->Alloc();
    buffer->AppendUserData(data, aligned_length, [this](void* ptr) {
      read_buffer_pool_->Free((char*)ptr);
    });
    return read_buffer_pool_->Index(data);
  }

  char* data = write_buffer_pool_->Alloc();
  buffer->AppendUserData(data, aligned_length, [this](void* ptr) {
    write_buffer_pool_->Free((char*)ptr);
  });
  return write_buffer_pool_->Index(data);
}

}  // namespace cache
}  // namespace dingofs
