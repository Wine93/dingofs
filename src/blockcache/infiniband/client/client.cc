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

#include "blockcache/infiniband/client/client.h"

#include <glog/logging.h>

#include <memory>
#include <utility>

#include "blockcache/core/runtime/smp.h"
#include "blockcache/core/runtime/worker_pool.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

namespace {

thread_local ClientUPtr tls_client;
thread_local unsigned tls_client_refs = 0;

}  // namespace

Future<Status> Client::InitOnThisShard(std::string device_name) {
  if (tls_client != nullptr) {
    ++tls_client_refs;
    co_return Status::OK();
  }
  StatusOr<ClientUPtr> made = Client::Create(std::move(device_name));
  if (!made.ok()) {
    co_return made.status();
  }
  tls_client = std::move(made).value();
  tls_client_refs = 1;
  co_return Status::OK();
}

Future<> Client::ShutdownOnThisShard() {
  if (tls_client == nullptr || --tls_client_refs != 0) {
    co_return;
  }
  co_await tls_client->Shutdown();
  tls_client.reset();
}

Status Client::RegisterOnAllShards(void* base, size_t bytes) {
  if (base == nullptr || bytes == 0) {
    return Status::OK();
  }

  return RunOnAllAndWait([base, bytes](unsigned) -> Future<Status> {
    Client* client = Client::This();
    if (client == nullptr) {
      co_return Status::OK();
    }
    Infiniband* infiniband = client->context();

    ibv_pd* pd = infiniband->device().pd();
    StatusOr<MemoryRegion> mr = co_await GetGlobalWorkers()->Submit(
        [pd, base, bytes] { return MemoryRegion::Register(pd, base, bytes); });
    if (!mr.ok()) {
      co_return mr.status();
    }
    infiniband->memory_registry().Add(std::move(mr).value());
    co_return Status::OK();
  });
}

Client* Client::This() { return tls_client.get(); }

Client::~Client() {
  LOG_IF(WARNING, context_ != nullptr) << "Client destroyed without Shutdown()";
}

StatusOr<std::unique_ptr<Client>> Client::Create(std::string device_name) {
  ClientUPtr self(new Client());

  StatusOr<InfinibandUPtr> made = Infiniband::Create(std::move(device_name));
  if (!made.ok()) {
    return made.status();
  }
  self->context_ = std::move(made).value();
  self->sessions_ =
      std::make_unique<SessionRegistry<ClientSession>>(self->context_.get());
  BindThisInfiniband(self->context_.get());
  return self;
}

Future<> Client::Shutdown() {
  if (context_ != nullptr) {
    co_await sessions_->ShutdownAll();
    sessions_.reset();
    co_await context_->Shutdown();
    UnbindThisInfiniband(context_.get());
    context_.reset();
  }
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
