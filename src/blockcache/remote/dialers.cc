/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "blockcache/remote/dialers.h"

#include <glog/logging.h>

#include <cstdint>
#include <memory>

#include "blockcache/common/flag_decls.h"
#include "blockcache/infiniband/client/client.h"
#include "blockcache/infiniband/client/dialer.h"
#include "blockcache/remote/brpc_dialer.h"
#include "blockcache/remote/brpc_handshake_client.h"

namespace dingofs {
namespace blockcache {

namespace {

struct ShardDialers {
  std::unique_ptr<BrpcDialer> brpc;
  std::unique_ptr<BrpcHandshakeClient> handshake_client;
  std::unique_ptr<infiniband::Dialer> rdma;
  int refs = 0;
};

thread_local ShardDialers tls_dialers;

}  // namespace

Future<Status> Dialers::InitOnThisShard() {
  if (++tls_dialers.refs > 1) {
    co_return Status::OK();
  }

  tls_dialers.brpc = std::make_unique<BrpcDialer>();

  if (FLAGS_remote_rdma) {
    const Status status =
        co_await infiniband::Client::InitOnThisShard(FLAGS_remote_rdma_device);
    if (!status.ok()) {
      co_return status;
    }
    tls_dialers.handshake_client =
        std::make_unique<BrpcHandshakeClient>(tls_dialers.brpc.get());
    tls_dialers.rdma = std::make_unique<infiniband::Dialer>(
        tls_dialers.handshake_client.get());
  }
  co_return Status::OK();
}

Future<> Dialers::ShutdownOnThisShard() {
  if (--tls_dialers.refs > 0) {
    co_return;
  }

  if (tls_dialers.rdma != nullptr) {
    tls_dialers.rdma.reset();
    tls_dialers.handshake_client.reset();
    co_await infiniband::Client::ShutdownOnThisShard();
  }
  tls_dialers.brpc.reset();
}

net::Dialer* Dialers::OfThisShard(bool use_rdma) {
  net::Dialer* dialer = use_rdma
                            ? static_cast<net::Dialer*>(tls_dialers.rdma.get())
                            : static_cast<net::Dialer*>(tls_dialers.brpc.get());
  CHECK(dialer != nullptr) << "dialers are not initialized on this shard";
  return dialer;
}

}  // namespace blockcache
}  // namespace dingofs
