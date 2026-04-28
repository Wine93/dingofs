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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/server.h"

#include <atomic>
#include <memory>

#include "cache/cachegroup/rdma_service.h"
#include "cache/cachegroup/service.h"
#include "cache/common/use_rdma_flag.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/memory.h"
#include "cache/infiniband/server.h"
#include "cache/iutil/string_util.h"
#include "common/options/cache.h"
#include "fmt/format.h"

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace cache {

DEFINE_string(listen_ip, "", "ip address to listen on for this cache node");
DEFINE_validator(listen_ip, iutil::StringValidator);

DEFINE_uint32(listen_port, 9300, "port to listen on for this cache node");

DEFINE_string(rdma_device, "mlx5_0",
              "IB device the cache RDMA server listens on (matches "
              "Listener::Listen)");
DEFINE_uint32(rdma_port, 1,
              "HCA port (1-based) the cache RDMA server listens on");

DEFINE_uint32(rdma_server_attachment_buffer_size, 4 * 1024 * 1024,
              "Per-buffer size of the server-side RDMA attachment pool. Must "
              "be >= the largest block_size handled.");
DEFINE_uint32(rdma_server_attachment_pool_size, 256,
              "Number of buffers in the server-side RDMA attachment pool. "
              "Bounds the in-flight RDMA Range/Cache concurrency on the "
              "server.");
DEFINE_bool(use_rdma, false,
            "Enable Infiniband/RDMA transport for cache RPCs. When true, the "
            "cache server starts an RDMA service alongside brpc, and the "
            "client routes Range/Cache RPCs through the RDMA path. Server and "
            "client must agree on the value for the RDMA path to be used.");

static void PrintReadyInfo(const std::string& addr) {
  std::cout << "\n";
  std::cout << "dingo-cache is listening on " << addr;
  std::cout << "\n";
  std::cout.flush();
}

// DEFINE_bool(wide_access, true, "whether to enable wide access listen
// address");

Server::Server()
    : running_(false),
      node_(std::make_unique<CacheNode>()),
      brpc_service_(std::make_unique<BlockCacheServiceImpl>(ServiceType::kBRPC,
                                                            node_.get())),
      rdma_service_(std::make_unique<BlockCacheServiceImpl>(ServiceType::kRDMA,
                                                            node_.get())),
      brpc_server_(std::make_unique<brpc::Server>()),
      rdma_server_(std::make_unique<infiniband::Server>()) {}

Status Server::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Server already started";
    return Status::OK();
  }

  LOG(INFO) << "Server is starting...";

  InstallSignal();

  if (FLAGS_use_rdma) {
    auto status = StartRDMAServer();
    if (!status.ok()) {
      LOG(ERROR) << "Fail to start RDMA server: " << status.ToString();
      return status;
    }
  }

  std::string listen_ip = FLAGS_wide_access ? "0.0.0.0" : FLAGS_listen_ip;
  auto status = StartRpcServer(FLAGS_listen_ip, FLAGS_listen_port);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start rpc server at " << FLAGS_listen_ip << ":"
               << FLAGS_listen_port;
    return status;
  }

  status = node_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start CacheNode";
    return status;
  }

  // log_clean_manager_ =
  //     std::make_unique<utils::LogCleanManager>(::FLAGS_log_dir);
  // status = log_clean_manager_->Start();
  // if (!status.ok()) {
  //   LOG(ERROR) << "start log clean manager fail, status: " <<
  //   status.ToString(); return status;
  // }

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Cache node server is up, address=" << listen_ip << ":"
            << FLAGS_listen_port;

  // PrintReadyInfo(fmt::format("{}:{}", listen_ip, FLAGS_listen_port));

  // Run until asked to quit
  brpc::FLAGS_graceful_quit_on_sigterm = true;
  brpc_server_->RunUntilAskedToQuit();

  return Status::OK();
}

Status Server::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Server already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "Server is shutting down...";

  brpc::AskToQuit();
  auto status = node_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown CacheNode";
    return status;
  }

  if (rdma_server_ != nullptr) {
    auto rs = rdma_server_->Shutdown();
    if (!rs.ok()) {
      LOG(ERROR) << "Fail to shutdown RDMA server: " << rs.ToString();
      // Continue — log clean manager still needs to stop.
    }
  }

  // status = log_clean_manager_->Stop();
  // if (!status.ok()) {
  //   LOG(ERROR) << "Stop log clean manager failed, status: "
  //              << status.ToString();
  //   return status;
  // }

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Server is shutdown";
  return status;
}

void Server::InstallSignal() { CHECK(SIG_ERR != signal(SIGPIPE, SIG_IGN)); }

Status Server::StartRdmaServer() {
  infiniband::EndPoint endpoint{
      FLAGS_rdma_device,
      static_cast<uint8_t>(FLAGS_rdma_port),
  };
  infiniband::ServerOptions options{.brpc_server = brpc_server_.get()};
  auto status = rdma_server_->Start(endpoint, &options);
  if (!status.ok()) {
    return status;
  }

  status = rdma_server_->AddService(rdma_service_.get());
  if (!status.ok()) {
    return status;
  }

  // LOG(INFO) << "RDMA cache service is registered on device="
  //           << FLAGS_rdma_listen_device
  //           << " port=" << static_cast<int>(FLAGS_rdma_listen_port_num);
  return Status::OK();
}

Status Server::StartBrpcServer(const std::string& listen_ip,
                               uint32_t listen_port) {
  butil::EndPoint ep;
  int rc = butil::str2endpoint(listen_ip.c_str(), listen_port, &ep);
  if (rc != 0) {
    LOG(ERROR) << "Fail to str2endpoint(" << listen_ip << "," << listen_port
               << ")";
    return Status::Internal("str2endpoint() failed");
  }

  rc = brpc_server_->AddService(brpc_service_.get(),
                                brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Fail to add BlockCacheService to brpc server";
    return Status::Internal("add service failed");
  }

  brpc::ServerOptions options;
  options.ignore_eovercrowded = true;
  rc = brpc_server_->Start(ep, &options);
  if (rc != 0) {
    LOG(ERROR) << "Fail to start brpc server";
    return Status::Internal("start brpc server failed");
  }
  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
