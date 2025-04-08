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
class Usrbio : public IoRing {
  using Ior = struct hf3fs_ior;
  using Iov = struct hf3fs_iov;
  using Cqe = struct hf3fs_cqe;

 public:
  explicit Usrbio(const std::string& mountpoint, bool for_read);

  bool Init(uint32_t io_depth) override;

  void Shutdown() override;

  bool PrepareIo(Aio* aio) override;

  bool SubmitIo() override;

  bool WaitIo(uint32_t timeout_ms, std::vector<Aio*>* aios) override;

 private:
  // Wrapper for 3fs usrbio interface
  bool IorCreate(Ior* ior, bool for_read);

  static void IorDestroy(Ior* ior);

  bool IovCreate(Iov* iov, size_t size);

  static void IovDestory(Iov* iov);

  bool PrepIo(Aio* aio);

  static bool SubmitIos(Ior* ior);

  int WaitForIos(Ior* ior, uint32_t timeout_ms, Cqe* cqes) const;

 private:
  std::atomic<bool> running_;
  std::string mountpoint_;
  bool for_read_;
  uint32_t io_depth_;
  Ior ior_;
  Cqe* cqes_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_AIO_USRBIO_H_
