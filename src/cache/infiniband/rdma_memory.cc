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

/*
 * Project: DingoFS
 * Created Date: 2026-05-20
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/rdma_memory.h"

#include <glog/logging.h>

#include <mutex>
#include <unordered_map>

#include "cache/infiniband/memory.h"

namespace dingofs {
namespace cache {
namespace infiniband {

RDMARegion::~RDMARegion() {
  if (mr != nullptr) {
    PCHECK(ibv_dereg_mr(mr) == 0) << "Fail to dereg memory region";
    mr = nullptr;
  }
}

namespace {

struct DeviceEntry {
  DeviceUPtr device;
  PortUPtr port;
  ProtectDomainUPtr pd;
};

class DeviceRegistry {
 public:
  static DeviceRegistry& Instance() {
    static DeviceRegistry r;
    return r;
  }

  // Returns the cached entry for `device_name`, opening it on first call.
  // Returns nullptr on failure (logs the cause).
  const DeviceEntry* GetOrAlloc(const std::string& device_name,
                                uint8_t port_num) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = by_name_.find(device_name);
    if (it != by_name_.end()) {
      return &it->second;
    }

    DeviceEntry entry;
    entry.device = Device::Open(device_name);
    if (entry.device == nullptr) {
      LOG(ERROR) << "Fail to open RDMA device: " << device_name;
      return nullptr;
    }
    entry.port = Port::Query(entry.device.get(), port_num);
    if (entry.port == nullptr) {
      LOG(ERROR) << "Fail to query port: device=" << device_name
                 << " port_num=" << static_cast<int>(port_num);
      return nullptr;
    }
    if (entry.port->GetPortState() != PortState::kActive) {
      LOG(ERROR) << "Port=" << static_cast<int>(port_num)
                 << " of device=" << device_name << " is not active";
      return nullptr;
    }
    if (entry.port->GetLinkLayer() == LinkLayer::kUnspecified) {
      LOG(ERROR) << "Port=" << static_cast<int>(port_num)
                 << " of device=" << device_name
                 << " has unspecified link layer";
      return nullptr;
    }
    entry.pd = ProtectDomain::Alloc(entry.device.get());
    if (entry.pd == nullptr) {
      LOG(ERROR) << "Fail to alloc PD: device=" << device_name;
      return nullptr;
    }

    auto [iter, _] = by_name_.emplace(device_name, std::move(entry));
    LOG(INFO) << "Opened RDMA device and cached PD: device=" << device_name
              << " port_num=" << static_cast<int>(port_num);
    return &iter->second;
  }

  const DeviceEntry* Lookup(const std::string& device_name) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = by_name_.find(device_name);
    return it == by_name_.end() ? nullptr : &it->second;
  }

 private:
  std::mutex mu_;
  std::unordered_map<std::string, DeviceEntry> by_name_;
};

constexpr int kDefaultAccess = IBV_ACCESS_LOCAL_WRITE |
                               IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_READ;

}  // namespace

ProtectDomain* GetOrAllocPD(const std::string& device_name, uint8_t port_num) {
  const auto* entry = DeviceRegistry::Instance().GetOrAlloc(device_name, port_num);
  return entry ? entry->pd.get() : nullptr;
}

Device* GetCachedDevice(const std::string& device_name) {
  const auto* entry = DeviceRegistry::Instance().Lookup(device_name);
  return entry ? entry->device.get() : nullptr;
}

Port* GetCachedPort(const std::string& device_name) {
  const auto* entry = DeviceRegistry::Instance().Lookup(device_name);
  return entry ? entry->port.get() : nullptr;
}

Status RegisterRDMAMemoryOnPD(ProtectDomain* pd, void* buffer, size_t length,
                              int access, RDMARegionUPtr* out) {
  CHECK_NOTNULL(pd);
  CHECK_NOTNULL(buffer);
  CHECK_NOTNULL(out);
  CHECK_GT(length, 0u);

  ibv_mr* mr = ibv_reg_mr(pd->GetIbPd(), buffer, length, access);
  if (mr == nullptr) {
    PLOG(ERROR) << "Fail to register memory region: buffer=" << buffer
                << " length=" << length;
    return Status::Internal("ibv_reg_mr failed");
  }

  auto region = std::make_unique<RDMARegion>();
  region->mr = mr;
  region->lkey = mr->lkey;
  region->rkey = mr->rkey;
  region->base = buffer;
  region->length = length;
  *out = std::move(region);
  return Status::OK();
}

Status RegisterRDMAMemory(const std::string& device_name, uint8_t port_num,
                          void* buffer, size_t length, int access,
                          RDMARegionUPtr* out) {
  auto* pd = GetOrAllocPD(device_name, port_num);
  if (pd == nullptr) {
    return Status::Internal("open RDMA device failed");
  }
  return RegisterRDMAMemoryOnPD(pd, buffer, length, access, out);
}

Status RegisterRDMAMemory(const std::string& device_name, uint8_t port_num,
                          void* buffer, size_t length, RDMARegionUPtr* out) {
  return RegisterRDMAMemory(device_name, port_num, buffer, length,
                            kDefaultAccess, out);
}

RDMAFixedBufferRegistry& RDMAFixedBufferRegistry::Instance() {
  static RDMAFixedBufferRegistry r;
  return r;
}

void RDMAFixedBufferRegistry::Register(RDMAMemoryPool* pool) {
  CHECK_NOTNULL(pool);
  pools_.push_back(pool);
}

std::vector<iovec> RDMAFixedBufferRegistry::CollectIovecs() {
  std::vector<iovec> out;
  snapshot_.clear();
  snapshot_counts_.clear();
  for (auto* pool : pools_) {
    auto iovs = pool->Fetch();
    snapshot_.push_back(pool);
    snapshot_counts_.push_back(iovs.size());
    out.insert(out.end(), iovs.begin(), iovs.end());
  }
  return out;
}

void RDMAFixedBufferRegistry::FinalizeIndexAssignment(int base_offset) {
  int cursor = base_offset;
  for (size_t i = 0; i < snapshot_.size(); ++i) {
    snapshot_[i]->AssignIoUringIndices(cursor);
    cursor += static_cast<int>(snapshot_counts_[i]);
  }
  snapshot_.clear();
  snapshot_counts_.clear();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
