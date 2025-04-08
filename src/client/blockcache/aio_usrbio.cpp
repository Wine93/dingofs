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

#include <absl/strings/str_format.h>
#include <glog/logging.h>
#include <hf3fs_usrbio.h>
#include <liburing.h>
#include <sys/epoll.h>

#include <cstring>

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
USRBIO::USRBIO(const std::string& mountpoint)
    : running_(false), mountpoint_(mountpoint) {}

bool USRBIO::Init(uint32_t io_depth) {
  if (running_.exchange(true)) {  // already running
    return true;
  }

  io_depth_ = io_depth;
  return IORCreate(&ior_r_, true) && IORCreate(&ior_w_, false);
}

void USRBIO::Shutdown() {
  if (running_.exchange(false)) {
    IORDestroy(&ior_r_);
    IORDestroy(&ior_w_);
  }
}

bool USRBIO::PrepareIO(Aio* aio) {
  aio->aux_info = new struct hf3fs_iov;
  if (!IOVCreate((struct hf3fs_iov*)aio->aux_info, aio->length)) {
    return false;
  } else if (!IOPrepare(aio, aio->aio_type == AioType::kRead)) {
    return false;
  }
  return true;
}

bool USRBIO::SubmitIO() { /*return hf3fs_submit_ios(&queue->io_uring_);*/
  return true;
}

bool USRBIO::WaitIO(uint32_t /*timeout_ms*/, std::vector<Aio*>* completed) {
  CHECK_NOTNULL(completed);
  completed->clear();

  /*
    static hf3fs_cqe r_cqes[512], w_cqes[512];
    int n = IORWait(&ior_w_, timeout_ms, completed_entries);
    for (int i = 0; i < n; i++) {
      auto* aio = (struct Aio*)(completed_entries[i].userdata);
      aio->succ = completed_entries[i].result >= 0;
      aios->emplace_back(aio);
    }
  */
  return true;
}

bool USRBIO::IORCreate(hf3fs_ior* ior, bool for_read) {
  int rc = hf3fs_iorcreate4(ior, mountpoint_.c_str(), io_depth_, for_read,
                            io_depth_, 0, -1, 0);
  if (rc != 0) {
    LOG(ERROR) << Errorf("hf3fs_iorcreate4(%s,%s)", mountpoint_,
                         (for_read ? "r" : "w"));
    return false;
  }
  return true;
}

void USRBIO::IORDestroy(struct hf3fs_ior* ior) { hf3fs_iordestroy(ior); }

bool USRBIO::IOVCreate(struct hf3fs_iov* iov, size_t size) {
  auto rc = hf3fs_iovcreate(iov, mountpoint_.c_str(), size, 0, -1);
  if (rc != 0) {
    LOG(ERROR) << Errorf("hf3fs_iovcreate(%s,%d)", mountpoint_, size);
    return false;
  }
  return true;
}

void USRBIO::IOVDestory(struct hf3fs_iov* iov) { hf3fs_iovdestroy(iov); }

bool USRBIO::IOPrepare(Aio* aio, bool for_read) {
  struct hf3fs_ior* ior = for_read ? &ior_r_ : &ior_w_;
  hf3fs_iov* iov = (hf3fs_iov*)aio->aux_info;
  int rc = hf3fs_prep_io(ior, iov, for_read, iov->base, aio->fd, aio->offset,
                         aio->length, aio);
  if (rc < 0) {
    LOG(ERROR) << Errorf("hf3fs_prep_io(%d,%d,%d)", aio->fd, aio->offset,
                         aio->length);
    return false;
  }
  return true;
}

bool USRBIO::IOSubmit(struct hf3fs_ior* ior) {
  int rc = hf3fs_submit_ios(ior);
  if (rc != 0) {
    LOG(ERROR) << Errorf("hf3fs_submit_ios()");
    return false;
  }
  return true;
}

int USRBIO::IOWait(struct hf3fs_ior* ior, uint32_t timeout_ms,
                   struct hf3fs_cqe* cqes) {
  timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
  return hf3fs_wait_for_ios(ior, cqes, io_depth_, 1, &ts);
}

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs
