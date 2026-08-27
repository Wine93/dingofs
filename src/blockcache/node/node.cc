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

#include "blockcache/node/node.h"

#include <butil/memory/scope_guard.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/node/native_service.h"

namespace dingofs {
namespace blockcache {

DEFINE_string(id, "", "cache node id");
DEFINE_validator(id, [](const char* /*name*/, const std::string& value) {
  return !value.empty();
});
DEFINE_string(listen_ip, "", "ip to listen on");
DEFINE_validator(listen_ip, [](const char* /*name*/, const std::string& value) {
  return !value.empty();
});
DEFINE_uint32(listen_port, 9300, "port to listen on");
DEFINE_string(bind_ip, "0.0.0.0", "ip to bind");
DEFINE_bool(rdma, false, "enable rdma transport");
DEFINE_string(rdma_device, "", "rdma device");
DEFINE_bool(daemonize, false, "run in background");

namespace {

std::atomic<bool> g_asked_to_quit{false};

void OnQuitSignal(int /*signo*/) {
  g_asked_to_quit.store(true, std::memory_order_relaxed);
}

}  // namespace

CacheNode::CacheNode() : CacheNode(std::make_unique<MDSClientImpl>()) {}

CacheNode::CacheNode(MDSClientUPtr mds_client)
    : mds_client_(std::move(mds_client)),
      block_cache_(std::make_unique<ShardedLocalCache>(mds_client_.get())),
      membership_(std::make_unique<GroupMembership>(mds_client_.get())),
      heartbeat_(std::make_unique<Heartbeat>(mds_client_.get())) {
  FLAGS_cache_dir_uuid = FLAGS_id;
}

CacheNode::~CacheNode() { Shutdown(); }

Status CacheNode::Start() {
  CHECK(!running_) << "CacheNode started twice";
  CHECK(ShardCount() > 0) << "the runtime must be up before CacheNode::Start";

  LOG(INFO) << "CacheNode is starting...";

  running_ = true;
  Status status;
  BRPC_SCOPE_EXIT {
    if (!status.ok()) {
      Shutdown();
    }
  };

  status = block_cache_->Start();
  if (!status.ok()) {
    return status;
  }

  status = StartServers();
  if (!status.ok()) {
    return status;
  }

  status = membership_->Start();
  if (!status.ok()) {
    return status;
  }

  heartbeat_->Start();

  LOG(INFO) << "Successfully start CacheNode{id=" << FLAGS_id
            << " shards=" << ShardCount()
            << " rdma=" << (FLAGS_rdma ? "on" : "off")
            << " listen_port=" << FLAGS_listen_port << "}";
  return Status::OK();
}

void CacheNode::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "CacheNode is shutting down...";

  heartbeat_->Shutdown();
  membership_->Shutdown();
  ShutdownServers();
  block_cache_->Shutdown();

  running_ = false;
  LOG(INFO) << "Successfully shutdown CacheNode";
}

// External thread only, and once per process: the handler and its flag are
// process-wide, as signals are.
void CacheNode::RunUntilAskedToQuit() {
  g_asked_to_quit.store(false, std::memory_order_relaxed);
  (void)std::signal(SIGINT, OnQuitSignal);
  (void)std::signal(SIGTERM, OnQuitSignal);

  while (!g_asked_to_quit.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  (void)std::signal(SIGINT, SIG_DFL);
  (void)std::signal(SIGTERM, SIG_DFL);

  LOG(INFO) << "Asked to quit, stopping the node";
}

Status CacheNode::StartServers() {
  Status status = BuildServices();
  if (!status.ok()) {
    return status;
  }

  net::BrpcServer::Option option;
  option.listen_ip = FLAGS_bind_ip;
  option.listen_port = static_cast<uint16_t>(FLAGS_listen_port);
  brpc_server_ = std::make_unique<net::BrpcServer>(option);
  brpc_server_->AddService(std::make_unique<CacheServiceNative>(
      brpc_server_.get(), cache_services_));

  // rdma starts BEFORE brpc: its serving slots must exist before any wire
  // can deliver a handshake (which it registers on the brpc server itself),
  // and it drains last on the reverse Shutdown.
  if (FLAGS_rdma) {
    infiniband::ServerOption server_option;
    server_option.device_name = FLAGS_rdma_device;
    server_option.brpc_server = brpc_server_.get();
    rdma_server_ =
        std::make_unique<infiniband::Server>(std::move(server_option));
    rdma_server_->AddService(std::vector<infiniband::Service*>(
        cache_services_.begin(), cache_services_.end()));
    status = rdma_server_->Start();
    if (!status.ok()) {
      LOG(ERROR) << "Fail to start the rdma server: " << status.ToString();
      return status;
    }
  }

  status = brpc_server_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start the brpc server: " << status.ToString();
  }
  return status;
}

// brpc first (stop accepting, drain what was handed to the shards), rdma
// second, and only THEN the verbs: else in-flight requests use-after-free.
void CacheNode::ShutdownServers() {
  if (brpc_server_ != nullptr) {
    brpc_server_->Shutdown();
  }
  if (rdma_server_ != nullptr) {
    rdma_server_->Shutdown();
  }
  DropServices();
}

Status CacheNode::BuildServices() {
  const unsigned shards = ShardCount();
  services_.resize(shards);
  cache_services_.assign(shards, nullptr);

  return RunOnAllAndWait([this](unsigned shard) -> Future<Status> {
    auto verbs = std::make_unique<CacheService>(block_cache_.get());
    cache_services_[shard] = verbs.get();
    services_[shard] = std::move(verbs);
    return MakeReadyFuture<Status>(Status::OK());
  });
}

void CacheNode::DropServices() {
  if (!services_.empty()) {
    RunOnAllAndWait([this](unsigned shard) -> Future<> {
      services_[shard].reset();
      return MakeReadyFuture<>();
    });
  }
  services_.clear();
  cache_services_.clear();
}

}  // namespace blockcache
}  // namespace dingofs
