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
#include "cache/infiniband/server.h"
#include "dingofs/infiniband.pb.h"

DEFINE_string(device, "mlx5_0", "IB device name (e.g. mlx5_0)");
DEFINE_int32(port_num, 1, "IB HCA port, 1-based");
DEFINE_int32(brpc_port, 8888,
             "TCP port for the connection-management brpc service");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  using namespace dingofs::cache::infiniband;  // NOLINT
  namespace pb_ib = dingofs::pb::infiniband;

  brpc::Server brpc_server;
  Server rdma_server;

  // Register RangeRequest handler: echo back num + 1.
  rdma_server.RegisterHandler(
      [](Controller* /*cntl*/, const pb_ib::RangeRequest* req,
         pb_ib::RangeResponse* resp, google::protobuf::Closure* done) {
        uint64_t in = req->num();
        uint64_t out = in + 1;
        resp->set_num(out);
        LOG(INFO) << "Handle RangeRequest: num=" << in << " -> " << out;
        done->Run();
      });

  EndPoint ep;
  ep.device_name = FLAGS_device;
  ep.port_num = static_cast<uint8_t>(FLAGS_port_num);

  ServerOptions options;
  options.brpc_server = &brpc_server;

  auto status = rdma_server.Start(ep, &options);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start RDMA server: " << status.ToString();
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
