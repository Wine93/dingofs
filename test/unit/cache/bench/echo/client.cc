// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// A client sending requests to server every 1 second.

#include <brpc/channel.h>
#include <bthread/countdown_event.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <gflags/gflags.h>

#include "cache/utils/bthread.h"
#include "dingofs/echo.pb.h"

DEFINE_string(attachment, "", "Carry this along with requests");
DEFINE_string(protocol, "baidu_std",
              "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "",
              "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8000", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 1000, "Milliseconds between consecutive requests");
DEFINE_bool(enable_checksum, false, "Enable checksum or not");

static int SyncEcho() {
  brpc::Channel channel;
  brpc::ChannelOptions options;
  options.protocol = FLAGS_protocol;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms /*milliseconds*/;
  options.max_retry = FLAGS_max_retry;
  if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(),
                   &options) != 0) {
    LOG(ERROR) << "Fail to initialize channel";
    return -1;
  }

  example::EchoService_Stub stub(&channel);

  butil::Timer timer;
  timer.start();
  bthread::CountdownEvent countdown(10000);

  //
  for (int i = 0; i < 10000; i++) {
    example::EchoRequest request;
    example::EchoResponse response;
    brpc::Controller cntl;

    request.set_message("hello world");

    // Because `done'(last parameter) is NULL, this function waits until
    // the response comes back or error occurs(including timedout).
    stub.Echo(&cntl, &request, &response, NULL);
    if (!cntl.Failed()) {
      LOG(INFO) << "Received response from " << cntl.remote_side() << " to "
                << cntl.local_side() << ": " << response.message()
                << " (attached=" << cntl.response_attachment() << ")"
                << " latency=" << cntl.latency_us() << "us";
    } else {
      LOG(WARNING) << cntl.ErrorText();
    }

    countdown.signal(1);
  }

  countdown.wait();
  timer.stop();
  LOG(INFO) << "Sync echo cost: " << timer.u_elapsed(0) / 1e6;

  return 0;
}

static int BthreadEcho() {
  brpc::Channel channel;
  brpc::ChannelOptions options;
  options.protocol = FLAGS_protocol;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms /*milliseconds*/;
  options.max_retry = FLAGS_max_retry;
  if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(),
                   &options) != 0) {
    LOG(ERROR) << "Fail to initialize channel";
    return -1;
  }

  example::EchoService_Stub stub(&channel);

  butil::Timer timer;
  timer.start();
  bthread::CountdownEvent countdown(10000);

  //
  for (int i = 0; i < 10000; i++) {
    auto* sub_timer = new butil::Timer;
    sub_timer->start();

    dingofs::cache::RunInBthread([&, sub_timer]() {
      sub_timer->stop();

      LOG(INFO) << "Start bthread cost: " << sub_timer->u_elapsed(0) / 1e6;

      example::EchoRequest request;
      example::EchoResponse response;
      brpc::Controller cntl;

      request.set_message("hello world");

      // Because `done'(last parameter) is NULL, this function waits until
      // the response comes back or error occurs(including timedout).
      stub.Echo(&cntl, &request, &response, NULL);
      if (!cntl.Failed()) {
        LOG(INFO) << "Received response from " << cntl.remote_side() << " to "
                  << cntl.local_side() << ": " << response.message()
                  << " (attached=" << cntl.response_attachment() << ")"
                  << " latency=" << cntl.latency_us() << "us";
      } else {
        LOG(WARNING) << cntl.ErrorText();
      }

      countdown.signal(1);
    });
  }

  countdown.wait();
  timer.stop();
  LOG(INFO) << "Bthread echo cost: " << timer.u_elapsed(0) / 1e6;

  return 0;
}

static bthread::CountdownEvent countdown(10000);

static void HandleEchoResponse(brpc::Controller* cntl,
                               example::EchoResponse* response) {
  // std::unique_ptr makes sure cntl/response will be deleted before returning.
  std::unique_ptr<brpc::Controller> cntl_guard(cntl);
  std::unique_ptr<example::EchoResponse> response_guard(response);

  if (cntl->Failed()) {
    LOG(WARNING) << "Fail to send EchoRequest, " << cntl->ErrorText();
    return;
  }
  LOG(INFO) << "Received response from " << cntl->remote_side() << ": "
            << response->message()
            << " (attached=" << cntl->response_attachment() << ")"
            << " latency=" << cntl->latency_us() << "us";

  countdown.signal(1);
}

static int AsyncEcho() {
  brpc::Channel channel;
  brpc::ChannelOptions options;
  options.protocol = FLAGS_protocol;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms /*milliseconds*/;
  options.max_retry = FLAGS_max_retry;
  if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(),
                   &options) != 0) {
    LOG(ERROR) << "Fail to initialize channel";
    return -1;
  }

  example::EchoService_Stub stub(&channel);

  butil::Timer timer;
  timer.start();

  //
  for (int i = 0; i < 10000; i++) {
    example::EchoRequest request;
    example::EchoResponse* response = new example::EchoResponse();
    brpc::Controller* cntl = new brpc::Controller();

    request.set_message("hello world");

    google::protobuf::Closure* done =
        brpc::NewCallback(&HandleEchoResponse, cntl, response);
    stub.Echo(cntl, &request, response, done);
  }

  countdown.wait();
  timer.stop();
  LOG(INFO) << "Async echo cost: " << timer.u_elapsed(0) / 1e6;

  return 0;
}

int main(int argc, char* argv[]) {
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  // return SyncEcho();
  BthreadEcho();
  SyncEcho();
  AsyncEcho();
  return 0;
}

// int main(int argc, char* argv[]) {
//   // Parse gflags. We recommend you to use gflags as well.
//   GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
//
//   // A Channel represents a communication line to a Server. Notice that
//   // Channel is thread-safe and can be shared by all threads in your program.
//   brpc::Channel channel;
//
//   // Initialize the channel, NULL means using default options.
//   brpc::ChannelOptions options;
//   options.protocol = FLAGS_protocol;
//   options.connection_type = FLAGS_connection_type;
//   options.timeout_ms = FLAGS_timeout_ms /*milliseconds*/;
//   options.max_retry = FLAGS_max_retry;
//   if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(),
//                    &options) != 0) {
//     LOG(ERROR) << "Fail to initialize channel";
//     return -1;
//   }
//
//   // Normally, you should not call a Channel directly, but instead construct
//   // a stub Service wrapping it. stub can be shared by all threads as well.
//   example::EchoService_Stub stub(&channel);
//
//   // Send a request and wait for the response every 1 second.
//   int log_id = 0;
//   while (!brpc::IsAskedToQuit()) {
//     // We will receive response synchronously, safe to put variables
//     // on stack.
//     example::EchoRequest request;
//     example::EchoResponse response;
//     brpc::Controller cntl;
//
//     request.set_message("hello world");
//
//     cntl.set_log_id(log_id++);  // set by user
//     // Set attachment which is wired to network directly instead of
//     // being serialized into protobuf messages.
//     cntl.request_attachment().append(FLAGS_attachment);
//
//     // Use checksum, only support CRC32C now.
//     if (FLAGS_enable_checksum) {
//       // cntl.set_request_checksum_type(brpc::CHECKSUM_TYPE_CRC32C);
//     }
//
//     // Because `done'(last parameter) is NULL, this function waits until
//     // the response comes back or error occurs(including timedout).
//     stub.Echo(&cntl, &request, &response, NULL);
//     if (!cntl.Failed()) {
//       LOG(INFO) << "Received response from " << cntl.remote_side() << " to "
//                 << cntl.local_side() << ": " << response.message()
//                 << " (attached=" << cntl.response_attachment() << ")"
//                 << " latency=" << cntl.latency_us() << "us";
//     } else {
//       LOG(WARNING) << cntl.ErrorText();
//     }
//     usleep(FLAGS_interval_ms * 1000L);
//   }
//
//   LOG(INFO) << "EchoClient is going to quit";
//   return 0;
// }
//