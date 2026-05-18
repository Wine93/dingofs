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

//
// brpc/TCP counterpart of rdma_server.cc — used for end-to-end latency
// comparison against the RDMA path. Handles RangeBench.Range: returns
// num+1 plus a configurable-size response attachment whose first and last
// 8 bytes are set to `out`, mirroring the RDMA example's WRITE payload.
//
// The attachment is shipped via brpc's response_attachment (IOBuf side
// channel), which is the closest fair comparison to RDMA's "small SEND
// control + bulk WRITE" pattern: no protobuf serialization overhead for
// the bulk bytes, only the small RangeBenchResponse is protobuf-encoded.
//

#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/iobuf.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdint>

#include "dingofs/range_bench.pb.h"

DEFINE_int32(port, 8889, "Server TCP port");
DEFINE_int32(attachment_bytes, 4194304,
             "Response attachment size in bytes (default: 4MB)");

namespace pb_rb = dingofs::pb::range_bench;

class RangeBenchServiceImpl : public pb_rb::RangeBenchService {
 public:
  explicit RangeBenchServiceImpl(size_t bytes) : bytes_(bytes) {}

  void Range(google::protobuf::RpcController* controller,
             const pb_rb::RangeBenchRequest* request,
             pb_rb::RangeBenchResponse* response,
             google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = static_cast<brpc::Controller*>(controller);

    uint64_t in = request->num();
    uint64_t out = in + 1;
    response->set_num(out);

    // Per-request 4MB buffer. Head/tail markers mirror the RDMA example so
    // the client can do the same correctness check. Handed to brpc via
    // append_user_data — brpc owns the buffer until the bytes have been
    // flushed onto the wire, and then calls the deleter. No intermediate
    // memcpy compared to IOBuf::append.
    // char* buf = new char[bytes_];
    // static char buf[4194304];
    //*reinterpret_cast<uint64_t*>(buf) = out;
    //*reinterpret_cast<uint64_t*>(buf + bytes_ - sizeof(uint64_t)) = out;
    // cntl->response_attachment().append_user_data(buf, bytes_, [](void* p) {
    //  // do nothing
    //});
  }

 private:
  size_t bytes_;
};

int main(int argc, char** argv) {
  // Same perf-friendly logging defaults as rdma_server.cc.
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;
  FLAGS_stderrthreshold = google::WARNING;
  FLAGS_logbuflevel = google::INFO;
  FLAGS_logbufsecs = 1;
  FLAGS_max_log_size = 256;

  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  brpc::Server server;
  RangeBenchServiceImpl service(FLAGS_attachment_bytes);
  if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add RangeBenchService";
    return 1;
  }

  brpc::ServerOptions options;
  if (server.Start(FLAGS_port, &options) != 0) {
    LOG(ERROR) << "Fail to start brpc server on port=" << FLAGS_port;
    return 1;
  }

  LOG(INFO) << "brpc server up: port=" << FLAGS_port
            << " attachment_bytes=" << FLAGS_attachment_bytes
            << " (waiting for incoming RPCs...)";

  server.RunUntilAskedToQuit();
  return 0;
}
