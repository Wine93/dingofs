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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include "cache/infiniband/client.h"
#include "dingofs/infiniband.pb.h"

DEFINE_string(device, "mlx5_0", "IB device name (e.g. mlx5_0)");
DEFINE_int32(port_num, 1, "IB HCA port, 1-based");
DEFINE_string(server_address, "127.0.0.1:8888",
              "RDMA server brpc address (host:port)");
DEFINE_int32(rounds, 1, "Number of connect rounds; -1 means infinite");
DEFINE_int32(interval_ms, 1000, "Interval between rounds in milliseconds");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  using namespace dingofs::cache::infiniband;  // NOLINT

  EndPoint ep;
  ep.device_name = FLAGS_device;
  ep.port_num = static_cast<uint8_t>(FLAGS_port_num);

  auto client = Client::Create(ep);

  int success = 0;
  int fail = 0;
  for (int i = 0; FLAGS_rounds < 0 || i < FLAGS_rounds; ++i) {
    if (client == nullptr) {
      LOG(ERROR) << "Round[" << i << "] Fail to create RDMA client";
      ++fail;
    } else {
      auto status = client->Connect(FLAGS_server_address);
      if (status.ok()) {
        ++success;
        LOG(INFO) << "Round[" << i
                  << "] Connect succeeded: server=" << FLAGS_server_address;
      } else {
        ++fail;
        LOG(ERROR) << "Round[" << i
                   << "] Fail to connect: server=" << FLAGS_server_address
                   << " err=" << status.ToString();
      }
    }

    bool more = (FLAGS_rounds < 0) || (i + 1 < FLAGS_rounds);
    if (more && FLAGS_interval_ms > 0) {
      ::usleep(static_cast<useconds_t>(FLAGS_interval_ms) * 1000);
    }
  }

  LOG(INFO) << "Done: success=" << success << " fail=" << fail;

  dingofs::pb::infiniband::RangeRequest request;
  dingofs::pb::infiniband::RangeResponse response;
  auto status = client->Call(request, &response);

  return fail == 0 ? 0 : 1;
}
