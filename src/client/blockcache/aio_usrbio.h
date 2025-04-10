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

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_USRBIO_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_USRBIO_H_

#include <butil/iobuf.h>
#include <hf3fs_usrbio.h>
#include <liburing.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <cstddef>
#include <cstdint>

#include "client/blockcache/aio.h"

namespace dingofs {
namespace client {
namespace blockcache {

// User Space Ring Based IO, not thread-safe
class USRBIO : public IORing {
  struct Ring {};

 public:
  explicit USRBIO(const std::string& mountpoint);

  bool Init(uint32_t io_depth) override;

  void Shutdown() override;

  bool PrepareIO(Aio* aio) override;

  bool SubmitIO() override;

  bool WaitIO(uint32_t timeout_ms, std::vector<Aio*>* completed) override;

 private:
  bool IORCreate(struct hf3fs_ior* ior, bool for_read);
  static void IORDestroy(struct hf3fs_ior* ior);

  bool IOVCreate(struct hf3fs_iov* iov, size_t size);
  static void IOVDestory(struct hf3fs_iov* iov);

  bool IOPrepare(Aio* aio, bool for_read);
  static bool IOSubmit(struct hf3fs_ior* ior);
  int IOWait(struct hf3fs_ior* ior, uint32_t timeout_ms,
             struct hf3fs_cqe* cqes);

 private:
  std::atomic<bool> running_;
  std::string mountpoint_;
  uint32_t io_depth_;
  hf3fs_ior ior_r_;
  hf3fs_ior ior_w_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_USRBIO_H_
