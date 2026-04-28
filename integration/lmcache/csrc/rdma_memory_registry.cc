// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "rdma_memory_registry.h"

#include <sstream>
#include <stdexcept>
#include <utility>

#include <glog/logging.h>

#include "cache/infiniband/rdma_memory.h"
#include "native_engine.h"

namespace dingofs {
namespace integration {
namespace lmcache {

namespace {

std::string RegionString(const MemoryRegion& region) {
  std::ostringstream os;
  os << "addr=0x" << std::hex << region.addr << std::dec
     << " length=" << region.length;
  return os.str();
}

}  // namespace

std::unique_ptr<RdmaMemoryRegistry> RdmaMemoryRegistry::Create(
    const std::vector<MemoryRegion>& regions) {
  if (regions.empty()) {
    return nullptr;
  }

  std::unique_ptr<RdmaMemoryRegistry> registry(new RdmaMemoryRegistry());
  registry->RegisterRegions(regions);
  return registry;
}

bool RdmaMemoryRegistry::Covers(const void* data, std::size_t length) const {
  if (data == nullptr || length == 0) {
    return false;
  }

  const auto start = reinterpret_cast<std::uintptr_t>(data);
  const auto end = start + length;
  if (end < start) {
    return false;
  }

  for (const auto& region : registered_regions_) {
    const auto region_start = region.addr;
    const auto region_end = region.addr + region.length;
    if (region_end < region_start) {
      continue;
    }
    if (start >= region_start && end <= region_end) {
      return true;
    }
  }
  return false;
}

void RdmaMemoryRegistry::RegisterRegions(
    const std::vector<MemoryRegion>& regions) {
  registered_regions_.reserve(regions.size());
  owned_regions_.reserve(regions.size());
  device_name_ = cache::infiniband::FLAGS_dingofs_rdma_device;

  for (const auto& region : regions) {
    if (region.addr == 0 || region.length == 0) {
      throw std::invalid_argument(
          "DingoFS LMCache RDMA: invalid memory region: " +
          RegionString(region));
    }

    cache::infiniband::RDMARegionUPtr rdma_region;
    auto status = cache::infiniband::RegisterRDMAMemory(
        device_name_, reinterpret_cast<void*>(region.addr), region.length,
        &rdma_region);
    if (!status.ok()) {
      throw std::runtime_error(
          "DingoFS LMCache RDMA: RegisterRDMAMemory failed for " +
          RegionString(region) + ": " + status.ToString());
    }

    registered_regions_.push_back(RegisteredMemoryRegion{
        region.addr, region.length, rdma_region->lkey, rdma_region->rkey});
    owned_regions_.push_back(std::move(rdma_region));
  }

  LOG(INFO) << "DingoFS LMCache RDMA: registered "
            << registered_regions_.size() << " memory region(s) on device "
            << device_name_;
}

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs
