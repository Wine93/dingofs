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

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/service.h>

#include <cstdint>

#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/server.h"
#include "dingofs/blockcache.pb.h"

DEFINE_string(device, "mlx5_0", "IB device name (e.g. mlx5_0)");
DEFINE_int32(port_num, 1, "IB HCA port, 1-based");
DEFINE_int32(brpc_port, 8888,
             "TCP port for the connection-management brpc service");
DEFINE_int32(attachment_pool_size, 64,
             "Number of attachment buffers — must be >= max concurrent "
             "in-flight RPCs across all clients");
DEFINE_int32(attachment_buffer_size, 4194304, "Attachment buffer size in bytes");

namespace {

using ::dingofs::cache::infiniband::Controller;
using ::dingofs::cache::infiniband::RDMAMemoryPool;

// Demo Service: implements BlockCacheService::Range as an echo over RDMA.
// The client encodes a 64-bit value in RangeRequest.offset and exposes a
// remote RDMA buffer; the server writes back `offset + 1` into a per-RPC
// staging buffer and lets the transport RDMA_WRITE it to the client.
//
// Each Range invocation acquires its own buffer from `pool_` so concurrent
// RPCs do not clobber each other's staged value. The buffer is returned to
// the pool via Controller::on_destroy, which fires after the response (and
// the RDMA_WRITE) have been sent.
class BlockCacheServiceImpl
    : public ::dingofs::pb::cache::BlockCacheService {
 public:
  explicit BlockCacheServiceImpl(RDMAMemoryPool* pool) : pool_(pool) {}

  void Range(::google::protobuf::RpcController* controller,
             const ::dingofs::pb::cache::RangeRequest* request,
             ::dingofs::pb::cache::RangeResponse* response,
             ::google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = static_cast<Controller*>(controller);

    auto* buf = pool_->Require();
    CHECK(buf != nullptr) << "attachment pool exhausted; bump "
                             "--attachment_pool_size";

    uint64_t out = request->offset() + 1;
    *reinterpret_cast<uint64_t*>(buf->data) = out;
    *reinterpret_cast<uint64_t*>(buf->data + buf->capacity - sizeof(uint64_t)) =
        out;
    buf->length = buf->capacity;
    cntl->SetResponseAttachment(buf);

    // Return the buffer to the pool when the Controller is destroyed — by
    // that point the transport has already issued the RDMA_WRITE.
    auto* pool = pool_;
    cntl->SetOnDestroy([pool, buf]() { pool->Release(buf); });

    response->set_status(::dingofs::pb::cache::BlockCacheOk);
    response->set_cache_hit(true);
  }

 private:
  RDMAMemoryPool* pool_;
};

}  // namespace

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  using namespace dingofs::cache::infiniband;  // NOLINT

  brpc::Server brpc_server;
  Server rdma_server;

  EndPoint ep;
  ep.device_name = FLAGS_device;
  ep.port_num = static_cast<uint8_t>(FLAGS_port_num);

  ServerOptions options;
  options.brpc_server = &brpc_server;

  // Start rdma_server first so the listener's PD is allocated. The PD must
  // exist before we create any RDMA memory pool that gets advertised via
  // wr.sgl.lkey on this connection — otherwise WRITE WRs from this side
  // would fail with LOC_PROT_ERR (lkey doesn't belong to the QP's PD).
  auto status = rdma_server.Start(ep, &options);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start RDMA server: " << status.ToString();
    return 1;
  }

  // Allocate from the *same* PD that backs server's QPs.
  auto mem_pool = RDMAMemoryPool::Create(rdma_server.GetProtectDomain(),
                                         FLAGS_attachment_buffer_size,
                                         FLAGS_attachment_pool_size);
  CHECK_NOTNULL(mem_pool);

  // brpc-style: derive from the generic Service, override the method(s),
  // hand it to the server. No oneof, no lambda glue.
  BlockCacheServiceImpl service(mem_pool.get());
  status = rdma_server.AddService(&service);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to add service: " << status.ToString();
    return 1;
  }

  brpc::ServerOptions brpc_options;
  if (brpc_server.Start(FLAGS_brpc_port, &brpc_options) != 0) {
    LOG(ERROR) << "Fail to start brpc server on port=" << FLAGS_brpc_port;
    return 1;
  }

  LOG(INFO) << "RDMA server is up: device=" << FLAGS_device
            << " port_num=" << FLAGS_port_num
            << " brpc_port=" << FLAGS_brpc_port
            << " (waiting for incoming connections...)";

  brpc_server.RunUntilAskedToQuit();
  return 0;
}
