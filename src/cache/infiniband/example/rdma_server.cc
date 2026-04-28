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

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/service.h>

#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/server.h"
#include "dingofs/infiniband.pb.h"

DEFINE_string(device, "mlx5_0", "IB device name (e.g. mlx5_0)");
DEFINE_int32(port_num, 1, "IB HCA port, 1-based");
DEFINE_int32(brpc_port, 8888,
             "TCP port for the connection-management brpc service");

int main(int argc, char** argv) {
  // Perf-friendly logging defaults:
  // - INFO -> buffered log file under FLAGS_log_dir (default /tmp),
  //   flushed every FLAGS_logbufsecs. No syscall per LOG on hot paths.
  // - WARNING+ -> file (immediate) AND stderr (so failures are visible live).
  // Override on the command line as needed, e.g.:
  //   --logtostderr=true   force everything back to stderr (debugging)
  //   --log_dir=...        change log directory
  //   --logbufsecs=N       change flush cadence (default 1s)
  //   --v=N                enable VLOG(<=N) lines
  // FLAGS_logtostderr = false;
  // FLAGS_alsologtostderr = false;
  // FLAGS_stderrthreshold = google::WARNING;
  // FLAGS_logbuflevel = google::INFO;
  // FLAGS_logbufsecs = 1;
  // FLAGS_max_log_size = 256;

  google::ParseCommandLineFlags(&argc, &argv, true);
  // google::InitGoogleLogging(argv[0]);

  using namespace dingofs::cache::infiniband;  // NOLINT
  namespace pb_ib = dingofs::pb::infiniband;

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
  auto mem_pool =
      RDMAMemoryPool::Create(rdma_server.GetProtectDomain(), 4194304, 2);
  CHECK_NOTNULL(mem_pool);
  auto* buffer = mem_pool->Require();
  CHECK_NOTNULL(buffer);

  // Register RangeRequest handler: echo back num + 1. Registered after Start
  // (PD/listener ready) and before brpc_server.Start (no client can connect
  // yet), so no handler-less request can slip through.
  rdma_server.RegisterHandler(
      [buffer](Controller* cntl, const pb_ib::RangeRequest* req,
               pb_ib::RangeResponse* resp, google::protobuf::Closure* done) {
        uint64_t in = req->num();
        uint64_t out = in + 1;
        resp->set_num(out);
        *reinterpret_cast<uint64_t*>(buffer->data) = out;
        *reinterpret_cast<uint64_t*>(buffer->data + buffer->capacity -
                                     sizeof(uint64_t)) = out;
        buffer->length = buffer->capacity;

        cntl->response_attachment = buffer;

        done->Run();
      });

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
