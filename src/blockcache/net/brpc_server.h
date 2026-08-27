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

#ifndef DINGOFS_BLOCKCACHE_NET_BRPC_SERVER_H_
#define DINGOFS_BLOCKCACHE_NET_BRPC_SERVER_H_

#include <google/protobuf/service.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"

namespace brpc {
class Server;
}

namespace dingofs {
namespace blockcache {
namespace net {

// Spins out calls in flight; completions land on bthreads, nowhere to park.
inline void DrainInflight(const std::atomic<int64_t>& inflight) {
  while (inflight.load(std::memory_order_acquire) > 0) {
    std::this_thread::yield();
  }
}

// A plain brpc server: it dispatches by method name itself, and every
// registered service bridges its calls onto the owning shard.
class BrpcServer {
 public:
  // Where this server listens. Everything else brpc needs -- admission
  // control, idle reaping, where `done` runs -- comes from --brpc_*.
  struct Option {
    std::string listen_ip = "0.0.0.0";
    uint16_t listen_port = 0;  // 0 lets the kernel choose
  };

  explicit BrpcServer(Option option);
  ~BrpcServer();
  BrpcServer(const BrpcServer&) = delete;
  BrpcServer& operator=(const BrpcServer&) = delete;

  // Added before Start, owned here, and registered in the order added.
  void AddService(std::unique_ptr<google::protobuf::Service> service);

  Status Start();

  // Drains requests already handed to a shard; returns before runtime stops.
  void Shutdown();

  // In-flight accounting, kept by the request path.
  void CallStarted() { inflight_.fetch_add(1, std::memory_order_relaxed); }
  void CallFinished() { inflight_.fetch_sub(1, std::memory_order_release); }

  static bool reply_on_bthread();

 private:
  Option option_;

  std::unique_ptr<::brpc::Server> server_;
  std::vector<std::unique_ptr<google::protobuf::Service>> services_;
  bool started_ = false;
  std::atomic<int64_t> inflight_{0};
};

}  // namespace net
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_BRPC_SERVER_H_
