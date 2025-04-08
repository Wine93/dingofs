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
 * Created Date: 2025-04-09
 * Author: Jingli Chen (Wine93)
 */

#include "client/blockcache/aio_usrbio.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <glog/logging.h>
#include <hf3fs_usrbio.h>
#include <liburing.h>
#include <sys/epoll.h>

#include <cstring>

#include "client/blockcache/aio.h"
#include "client/blockcache/disk_cache_metric.h"

namespace dingofs {
namespace client {
namespace blockcache {

using absl::StrFormat;

template <typename... Args>
static std::string Errorf(const char* format, const Args&... args) {
  std::ostringstream message;
  message << StrFormat(format, args...) << ": " << ::strerror(errno);
  return message.str();
}

namespace {}  // namespace

Usrbio::Usrbio(const std::string& mountpoint, bool for_read)
    : running_(false),
      mountpoint_(mountpoint),
      for_read_(for_read),
      cqes_(nullptr) {}

bool Usrbio::Init(uint32_t io_depth) {
  if (running_.exchange(true)) {  // already running
    return true;
  }

  io_depth_ = io_depth;
  cqes_ = new Cqe[io_depth];
  return IorCreate(&ior_, for_read_);
}

void Usrbio::Shutdown() {
  if (running_.exchange(false)) {
    IorDestroy(&ior_);
    delete[] cqes_;
  }
}

bool Usrbio::PrepareIo(Aio* aio) {
  CHECK_EQ(aio->aio_type == AioType::kRead, for_read_);
  bool succ;
  aio->aux_info = new Iov;
  auto defer = ::absl::MakeCleanup([&] {
    if (!succ) {
      IovDestory((Iov*)aio->aux_info);
    }
  });

  succ = IovCreate((Iov*)aio->aux_info, aio->length);
  if (succ) {
    succ = PrepIo(aio);
  }
  return succ;
}

bool Usrbio::SubmitIo() { return hf3fs_submit_ios(&ior_); }

bool Usrbio::WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) {
  CHECK_NOTNULL(aios);
  aios->clear();

  int n = WaitForIos(&ior_, timeout_ms, cqes_);
  for (int i = 0; i < n; i++) {
    auto* aio = (struct Aio*)(cqes_[i].userdata);
    aio->retcode = (cqes_[i].result >= 0) ? 0 : -1;
    IovDestory((Iov*)aio->aux_info);  // TODO: destory by async
  }
  return true;
}

bool Usrbio::IorCreate(Ior* ior, bool for_read) {
  int rc = hf3fs_iorcreate4(ior, mountpoint_.c_str(), io_depth_, for_read,
                            io_depth_, 0, -1, 0);
  if (rc != 0) {
    LOG(ERROR) << Errorf("hf3fs_iorcreate4(%s,%s)", mountpoint_,
                         (for_read ? "r" : "w"));
    return false;
  }
  return true;
}

void Usrbio::IorDestroy(Ior* ior) { hf3fs_iordestroy(ior); }

bool Usrbio::IovCreate(Iov* iov, size_t size) {
  auto rc = hf3fs_iovcreate(iov, mountpoint_.c_str(), size, 0, -1);
  if (rc != 0) {
    LOG(ERROR) << Errorf("hf3fs_iovcreate(%s,%d)", mountpoint_, size);
    return false;
  }
  return true;
}

void Usrbio::IovDestory(Iov* iov) { hf3fs_iovdestroy(iov); }

bool Usrbio::PrepIo(Aio* aio) {
  Iov* iov = (Iov*)aio->aux_info;
  int rc = hf3fs_prep_io(&ior_, iov, for_read_, iov->base, aio->fd, aio->offset,
                         aio->length, aio);
  if (rc < 0) {
    LOG(ERROR) << Errorf("hf3fs_prep_io(%d,%d,%d)", aio->fd, aio->offset,
                         aio->length);
    return false;
  }
  return true;
}

bool Usrbio::SubmitIos(Ior* ior) {
  int rc = hf3fs_submit_ios(ior);
  if (rc != 0) {
    LOG(ERROR) << Errorf("hf3fs_submit_ios()");
    return false;
  }
  return true;
}

static timespec MakeTimeout(uint32_t timeout_ms) {
  timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
  return ts;
}

int Usrbio::WaitForIos(Ior* ior, uint32_t timeout_ms, Cqe* cqes) const {
  auto timeout = MakeTimeout(timeout_ms);
  return hf3fs_wait_for_ios(ior, cqes, io_depth_, 1, &timeout);
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
