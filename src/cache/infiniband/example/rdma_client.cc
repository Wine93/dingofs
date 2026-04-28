/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <absl/strings/str_format.h>
#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "cache/infiniband/client.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/memory.h"
#include "dingofs/blockcache.pb.h"

DEFINE_string(device, "mlx5_0", "IB device name (e.g. mlx5_0)");
DEFINE_int32(port_num, 1, "IB HCA port, 1-based");
DEFINE_string(server_address, "127.0.0.1:8888",
              "RDMA server brpc address (host:port)");
DEFINE_string(op, "rdma_write",
              "Operation: ping|rdma_read|rdma_write|mixed|invalid_service|"
              "invalid_method|remote_too_small");
DEFINE_uint64(payload_size, 4096,
              "Payload bytes for RDMA_READ/RDMA_WRITE operations");
DEFINE_int32(rounds, 1, "Measured rounds per thread");
DEFINE_int32(warmup_rounds, 0, "Warmup rounds per thread, excluded from stats");
DEFINE_int32(threads, 1, "Number of concurrent worker threads issuing RPCs");
DEFINE_string(verify, "full", "Verification mode: full|markers|none");
DEFINE_bool(json_result, false,
            "Print one machine-readable JSON result line to stdout");
DEFINE_bool(log_per_round, false, "Log each measured round result");

namespace {

using ::dingofs::Status;
using ::dingofs::cache::infiniband::Client;
using ::dingofs::cache::infiniband::Controller;
using ::dingofs::cache::infiniband::EndPoint;
using ::dingofs::cache::infiniband::Infiniband;
using ::dingofs::cache::infiniband::RDMABuffer;
using ::dingofs::cache::infiniband::RDMABufferPool;
using ::dingofs::cache::infiniband::RDMABufferPoolUPtr;
namespace pb_cache = ::dingofs::pb::cache;

constexpr const char* kServiceName = "dingofs.pb.cache.BlockCacheService";
constexpr const char* kInvalidServiceName =
    "dingofs.pb.cache.MissingBlockCacheService";
constexpr const char* kInvalidMethodName = "MissingMethod";

enum class Op {
  kPing,
  kRDMARead,
  kRDMAWrite,
  kMixed,
  kInvalidService,
  kInvalidMethod,
  kRemoteTooSmall,
};

enum class VerifyMode {
  kFull,
  kMarkers,
  kNone,
};

struct RoundResult {
  bool ok{false};
  uint64_t latency_us{0};
  uint64_t bytes{0};
  std::string error;
};

struct WorkerStats {
  std::vector<uint64_t> latencies_us;
  uint64_t success{0};
  uint64_t fail{0};
  uint64_t bytes{0};
  uint64_t warmup_fail{0};
};

struct Summary {
  uint64_t total_rpcs{0};
  uint64_t success{0};
  uint64_t fail{0};
  uint64_t warmup_fail{0};
  uint64_t bytes{0};
  double wall_s{0};
  double qps{0};
  double mib_s{0};
  uint64_t min_us{0};
  double mean_us{0};
  uint64_t p50_us{0};
  uint64_t p90_us{0};
  uint64_t p99_us{0};
  uint64_t max_us{0};
};

struct PreparedWorkerCalls {
  Controller ping_cntl;
  pb_cache::PingRequest ping_request;
  pb_cache::PingResponse ping_response;

  Controller read_cntl;
  pb_cache::PutRequest read_request;
  pb_cache::PutResponse read_response;

  Controller write_cntl;
  pb_cache::RangeRequest write_request;
  pb_cache::RangeResponse write_response;

  Controller too_small_cntl;
  pb_cache::RangeRequest too_small_request;
  pb_cache::RangeResponse too_small_response;

  Controller dispatch_cntl;
  pb_cache::PingRequest dispatch_request;
  pb_cache::PingResponse dispatch_response;
};

Op ParseOp(const std::string& value) {
  if (value == "ping") return Op::kPing;
  if (value == "rdma_read") return Op::kRDMARead;
  if (value == "rdma_write") return Op::kRDMAWrite;
  if (value == "mixed") return Op::kMixed;
  if (value == "invalid_service") return Op::kInvalidService;
  if (value == "invalid_method") return Op::kInvalidMethod;
  if (value == "remote_too_small") return Op::kRemoteTooSmall;
  LOG(FATAL) << "Unsupported --op=" << value;
  return Op::kPing;
}

VerifyMode ParseVerifyMode(const std::string& value) {
  if (value == "full") return VerifyMode::kFull;
  if (value == "markers") return VerifyMode::kMarkers;
  if (value == "none") return VerifyMode::kNone;
  LOG(FATAL) << "Unsupported --verify=" << value;
  return VerifyMode::kFull;
}

const char* OpName(Op op) {
  switch (op) {
    case Op::kPing:
      return "ping";
    case Op::kRDMARead:
      return "rdma_read";
    case Op::kRDMAWrite:
      return "rdma_write";
    case Op::kMixed:
      return "mixed";
    case Op::kInvalidService:
      return "invalid_service";
    case Op::kInvalidMethod:
      return "invalid_method";
    case Op::kRemoteTooSmall:
      return "remote_too_small";
  }
  return "unknown";
}

uint64_t MakeSeed(int thread_id, int round, Op op) {
  const uint64_t op_bits = static_cast<uint64_t>(op) << 56;
  const uint64_t tid_bits = static_cast<uint64_t>(thread_id & 0xFFFF) << 40;
  return op_bits ^ tid_bits ^ static_cast<uint64_t>(round + 1);
}

void FillPattern(char* data, size_t size, uint64_t seed) {
  size_t pos = 0;
  while (pos + sizeof(seed) <= size) {
    std::memcpy(data + pos, &seed, sizeof(seed));
    pos += sizeof(seed);
  }
  if (pos < size) {
    std::memcpy(data + pos, &seed, size - pos);
  }
}

bool CheckByte(const char* data, size_t pos, uint64_t seed) {
  char pattern[sizeof(seed)];
  std::memcpy(pattern, &seed, sizeof(seed));
  return data[pos] == pattern[pos % sizeof(seed)];
}

bool VerifyPattern(const char* data, size_t size, uint64_t seed,
                   VerifyMode mode) {
  if (mode == VerifyMode::kNone || size == 0) {
    return true;
  }

  if (mode == VerifyMode::kMarkers) {
    return CheckByte(data, 0, seed) && CheckByte(data, size / 2, seed) &&
           CheckByte(data, size - 1, seed);
  }

  char expected[sizeof(seed)];
  std::memcpy(expected, &seed, sizeof(seed));
  size_t pos = 0;
  while (pos + sizeof(seed) <= size) {
    if (std::memcmp(data + pos, expected, sizeof(seed)) != 0) {
      return false;
    }
    pos += sizeof(seed);
  }
  return pos == size || std::memcmp(data + pos, expected, size - pos) == 0;
}

std::string ControllerError(const Controller& cntl) {
  if (!cntl.ErrorText().empty()) {
    return cntl.ErrorText();
  }
  return absl::StrFormat("rdma rpc failed: error_code=%d", cntl.ErrorCode());
}

RDMABufferPoolUPtr CreateBufferPool(const EndPoint& ep, size_t buffer_size,
                                    size_t buffer_count) {
  Infiniband::Context context;
  auto status = Infiniband::Init(ep.device_name, ep.port_num, &context);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to initialize infiniband context: "
               << status.ToString();
    return nullptr;
  }
  return RDMABufferPool::Create(context.protect_domain, buffer_size,
                                buffer_count);
}

void SetWriteRegion(Controller* cntl, const RDMABuffer* buffer,
                    uint32_t advertised_length) {
  auto& region = cntl->write_region();
  region.set_addr(reinterpret_cast<uint64_t>(buffer->data));
  region.set_length(advertised_length);
  region.set_rkey(buffer->rkey);
}

void AddReadRegion(Controller* cntl, const RDMABuffer* buffer,
                   uint32_t attachment_size) {
  auto* region = cntl->read_regions().Add();
  region->set_addr(reinterpret_cast<uint64_t>(buffer->data));
  region->set_length(attachment_size);
  region->set_rkey(buffer->rkey);
}

void SetBlockHandle(pb_cache::BlockHandle* handle, uint64_t seed,
                    uint64_t payload_size) {
  auto* key = handle->mutable_block_key();
  key->set_id(seed);
  key->set_index(0);
  key->set_size(static_cast<uint32_t>(payload_size));
}

bool UsePreparedCalls(VerifyMode verify_mode) {
  return verify_mode == VerifyMode::kNone;
}

void PrepareWorkerCalls(PreparedWorkerCalls* calls, RDMABuffer* buffer,
                        size_t payload_size, int thread_id) {
  buffer->length = static_cast<uint32_t>(payload_size);

  const uint64_t read_seed = MakeSeed(thread_id, 0, Op::kRDMARead);
  SetBlockHandle(calls->read_request.mutable_handle(), read_seed, payload_size);
  calls->read_request.set_block_size(payload_size);
  AddReadRegion(&calls->read_cntl, buffer, static_cast<uint32_t>(payload_size));

  const uint64_t write_seed = MakeSeed(thread_id, 0, Op::kRDMAWrite);
  SetBlockHandle(calls->write_request.mutable_handle(), write_seed,
                 payload_size);
  calls->write_request.set_block_size(payload_size);
  calls->write_request.set_offset(write_seed);
  calls->write_request.set_length(payload_size);
  SetWriteRegion(&calls->write_cntl, buffer,
                 static_cast<uint32_t>(payload_size));

  SetBlockHandle(calls->too_small_request.mutable_handle(), write_seed,
                 payload_size);
  calls->too_small_request.set_block_size(payload_size);
  calls->too_small_request.set_offset(write_seed);
  calls->too_small_request.set_length(payload_size);
  const uint32_t advertised_length =
      static_cast<uint32_t>(payload_size == 0 ? 0 : payload_size - 1);
  SetWriteRegion(&calls->too_small_cntl, buffer, advertised_length);
}

RoundResult CallPing(Client* client) {
  butil::Timer timer;
  timer.start();

  Controller cntl;
  pb_cache::PingRequest request;
  pb_cache::PingResponse response;
  client->Call(&cntl, kServiceName, "Ping", request, &response);

  timer.stop();
  const bool ok = !cntl.Failed();
  return RoundResult{ok, static_cast<uint64_t>(timer.u_elapsed()), 0,
                     ok ? "" : ControllerError(cntl)};
}

RoundResult CallPreparedPing(Client* client, PreparedWorkerCalls* calls) {
  butil::Timer timer;
  timer.start();
  client->Call(&calls->ping_cntl, kServiceName, "Ping", calls->ping_request,
               &calls->ping_response);
  timer.stop();

  const auto latency_us = static_cast<uint64_t>(timer.u_elapsed());
  if (calls->ping_cntl.Failed()) {
    return RoundResult{false, latency_us, 0, ControllerError(calls->ping_cntl)};
  }
  return RoundResult{true, latency_us, 0, ""};
}

RoundResult CallRDMARead(Client* client, RDMABuffer* buffer,
                         size_t payload_size, uint64_t seed,
                         VerifyMode verify_mode) {
  FillPattern(buffer->data, payload_size, seed);
  buffer->length = static_cast<uint32_t>(payload_size);

  pb_cache::PutRequest request;
  SetBlockHandle(request.mutable_handle(), seed, payload_size);
  request.set_block_size(payload_size);
  pb_cache::PutResponse response;

  butil::Timer timer;
  timer.start();

  Controller cntl;
  AddReadRegion(&cntl, buffer, static_cast<uint32_t>(payload_size));
  client->Call(&cntl, kServiceName, "Put", request, &response);

  timer.stop();
  if (cntl.Failed()) {
    return RoundResult{false, static_cast<uint64_t>(timer.u_elapsed()), 0,
                       ControllerError(cntl)};
  }
  if (response.status() != pb_cache::BlockCacheOk) {
    return RoundResult{false, static_cast<uint64_t>(timer.u_elapsed()), 0,
                       "server rejected RDMA_READ payload"};
  }
  if (!VerifyPattern(buffer->data, payload_size, seed, verify_mode)) {
    return RoundResult{false, static_cast<uint64_t>(timer.u_elapsed()), 0,
                       "local source payload changed unexpectedly"};
  }
  return RoundResult{true, static_cast<uint64_t>(timer.u_elapsed()),
                     payload_size, ""};
}

RoundResult CallPreparedRDMARead(Client* client, PreparedWorkerCalls* calls,
                                 size_t payload_size) {
  butil::Timer timer;
  timer.start();
  client->Call(&calls->read_cntl, kServiceName, "Put", calls->read_request,
               &calls->read_response);
  timer.stop();

  const auto latency_us = static_cast<uint64_t>(timer.u_elapsed());
  if (calls->read_cntl.Failed()) {
    return RoundResult{false, latency_us, 0, ControllerError(calls->read_cntl)};
  }
  if (calls->read_response.status() != pb_cache::BlockCacheOk) {
    return RoundResult{false, latency_us, 0,
                       "server rejected RDMA_READ payload"};
  }
  return RoundResult{true, latency_us, payload_size, ""};
}

RoundResult CallRDMAWrite(Client* client, RDMABuffer* buffer,
                          size_t payload_size, uint64_t seed,
                          VerifyMode verify_mode, bool advertise_too_small) {
  std::memset(buffer->data, 0, payload_size);
  buffer->length = static_cast<uint32_t>(payload_size);

  pb_cache::RangeRequest request;
  SetBlockHandle(request.mutable_handle(), seed, payload_size);
  request.set_block_size(payload_size);
  request.set_offset(seed);
  request.set_length(payload_size);
  pb_cache::RangeResponse response;

  const uint32_t advertised_length =
      advertise_too_small
          ? static_cast<uint32_t>(payload_size == 0 ? 0 : payload_size - 1)
          : static_cast<uint32_t>(payload_size);

  butil::Timer timer;
  timer.start();

  Controller cntl;
  SetWriteRegion(&cntl, buffer, advertised_length);
  client->Call(&cntl, kServiceName, "Range", request, &response);

  timer.stop();
  const auto latency_us = static_cast<uint64_t>(timer.u_elapsed());
  if (advertise_too_small) {
    if (cntl.Failed()) {
      return RoundResult{true, latency_us, 0, ""};
    }
    return RoundResult{false, latency_us, 0,
                       "remote_too_small unexpectedly succeeded"};
  }
  if (cntl.Failed()) {
    return RoundResult{false, latency_us, 0, ControllerError(cntl)};
  }
  if (response.status() != pb_cache::BlockCacheOk) {
    return RoundResult{false, latency_us, 0,
                       "server rejected RDMA_WRITE request"};
  }
  if (cntl.response_attachment_size() != payload_size) {
    return RoundResult{
        false, latency_us, 0,
        absl::StrFormat("attachment length mismatch: got=%zu "
                        "expected=%zu",
                        cntl.response_attachment_size(), payload_size)};
  }
  if (!VerifyPattern(buffer->data, payload_size, seed, verify_mode)) {
    return RoundResult{false, latency_us, 0,
                       "RDMA_WRITE payload verification failed"};
  }
  return RoundResult{true, latency_us, payload_size, ""};
}

RoundResult CallPreparedRDMAWrite(Client* client, PreparedWorkerCalls* calls,
                                  size_t payload_size,
                                  bool advertise_too_small) {
  auto* cntl =
      advertise_too_small ? &calls->too_small_cntl : &calls->write_cntl;
  const auto& request =
      advertise_too_small ? calls->too_small_request : calls->write_request;
  auto* response =
      advertise_too_small ? &calls->too_small_response : &calls->write_response;

  butil::Timer timer;
  timer.start();
  client->Call(cntl, kServiceName, "Range", request, response);
  timer.stop();

  const auto latency_us = static_cast<uint64_t>(timer.u_elapsed());
  if (advertise_too_small) {
    if (cntl->Failed()) {
      return RoundResult{true, latency_us, 0, ""};
    }
    return RoundResult{false, latency_us, 0,
                       "remote_too_small unexpectedly succeeded"};
  }
  if (cntl->Failed()) {
    return RoundResult{false, latency_us, 0, ControllerError(*cntl)};
  }
  if (response->status() != pb_cache::BlockCacheOk) {
    return RoundResult{false, latency_us, 0,
                       "server rejected RDMA_WRITE request"};
  }
  if (cntl->response_attachment_size() != payload_size) {
    return RoundResult{
        false, latency_us, 0,
        absl::StrFormat("attachment length mismatch: got=%zu "
                        "expected=%zu",
                        cntl->response_attachment_size(), payload_size)};
  }
  return RoundResult{true, latency_us, payload_size, ""};
}

RoundResult CallExpectedDispatchFailure(Client* client, Op op) {
  butil::Timer timer;
  timer.start();

  Controller cntl;
  pb_cache::PingRequest request;
  pb_cache::PingResponse response;
  if (op == Op::kInvalidService) {
    client->Call(&cntl, kInvalidServiceName, "Ping", request, &response);
  } else {
    client->Call(&cntl, kServiceName, kInvalidMethodName, request, &response);
  }

  timer.stop();
  const auto latency_us = static_cast<uint64_t>(timer.u_elapsed());
  if (cntl.Failed()) {
    return RoundResult{true, latency_us, 0, ""};
  }
  return RoundResult{false, latency_us, 0,
                     "dispatch failure unexpectedly succeeded"};
}

RoundResult CallPreparedExpectedDispatchFailure(Client* client,
                                                PreparedWorkerCalls* calls,
                                                Op op) {
  butil::Timer timer;
  timer.start();
  if (op == Op::kInvalidService) {
    client->Call(&calls->dispatch_cntl, kInvalidServiceName, "Ping",
                 calls->dispatch_request, &calls->dispatch_response);
  } else {
    client->Call(&calls->dispatch_cntl, kServiceName, kInvalidMethodName,
                 calls->dispatch_request, &calls->dispatch_response);
  }
  timer.stop();

  const auto latency_us = static_cast<uint64_t>(timer.u_elapsed());
  if (calls->dispatch_cntl.Failed()) {
    return RoundResult{true, latency_us, 0, ""};
  }
  return RoundResult{false, latency_us, 0,
                     "dispatch failure unexpectedly succeeded"};
}

RoundResult RunOne(Client* client, RDMABuffer* buffer, Op op, int thread_id,
                   int round, size_t payload_size, VerifyMode verify_mode,
                   PreparedWorkerCalls* prepared_calls) {
  Op actual = op;
  if (op == Op::kMixed) {
    switch (round % 3) {
      case 0:
        actual = Op::kPing;
        break;
      case 1:
        actual = Op::kRDMARead;
        break;
      default:
        actual = Op::kRDMAWrite;
        break;
    }
  }

  if (prepared_calls != nullptr) {
    switch (actual) {
      case Op::kPing:
        return CallPreparedPing(client, prepared_calls);
      case Op::kRDMARead:
        return CallPreparedRDMARead(client, prepared_calls, payload_size);
      case Op::kRDMAWrite:
        return CallPreparedRDMAWrite(client, prepared_calls, payload_size,
                                     false);
      case Op::kInvalidService:
      case Op::kInvalidMethod:
        return CallPreparedExpectedDispatchFailure(client, prepared_calls,
                                                   actual);
      case Op::kRemoteTooSmall:
        return CallPreparedRDMAWrite(client, prepared_calls, payload_size,
                                     true);
      case Op::kMixed:
        break;
    }
  }

  const uint64_t seed = MakeSeed(thread_id, round, actual);
  switch (actual) {
    case Op::kPing:
      return CallPing(client);
    case Op::kRDMARead:
      return CallRDMARead(client, buffer, payload_size, seed, verify_mode);
    case Op::kRDMAWrite:
      return CallRDMAWrite(client, buffer, payload_size, seed, verify_mode,
                           false);
    case Op::kInvalidService:
    case Op::kInvalidMethod:
      return CallExpectedDispatchFailure(client, actual);
    case Op::kRemoteTooSmall:
      return CallRDMAWrite(client, buffer, payload_size, seed, verify_mode,
                           true);
    case Op::kMixed:
      break;
  }
  return RoundResult{false, 0, 0, "unreachable mixed op"};
}

Summary BuildSummary(const std::vector<WorkerStats>& per_thread,
                     double wall_s) {
  Summary summary;
  std::vector<uint64_t> latencies;
  for (const auto& stats : per_thread) {
    latencies.insert(latencies.end(), stats.latencies_us.begin(),
                     stats.latencies_us.end());
    summary.success += stats.success;
    summary.fail += stats.fail;
    summary.warmup_fail += stats.warmup_fail;
    summary.bytes += stats.bytes;
  }

  summary.total_rpcs = latencies.size();
  summary.wall_s = wall_s;
  summary.fail += summary.warmup_fail;
  summary.qps =
      wall_s > 0 ? static_cast<double>(summary.total_rpcs) / wall_s : 0;
  summary.mib_s = wall_s > 0 ? static_cast<double>(summary.bytes) /
                                   (1024.0 * 1024.0) / wall_s
                             : 0;

  if (latencies.empty()) {
    return summary;
  }

  std::sort(latencies.begin(), latencies.end());
  summary.min_us = latencies.front();
  summary.max_us = latencies.back();
  const size_t n = latencies.size();
  summary.p50_us = latencies[std::min(n - 1, n * 50 / 100)];
  summary.p90_us = latencies[std::min(n - 1, n * 90 / 100)];
  summary.p99_us = latencies[std::min(n - 1, n * 99 / 100)];
  const auto sum =
      std::accumulate(latencies.begin(), latencies.end(), uint64_t{0});
  summary.mean_us = static_cast<double>(sum) / static_cast<double>(n);
  return summary;
}

void PrintSummary(const Summary& summary, Op op, VerifyMode verify_mode) {
  const char* verify_name =
      verify_mode == VerifyMode::kFull
          ? "full"
          : (verify_mode == VerifyMode::kMarkers ? "markers" : "none");

  LOG(INFO) << "==================== RDMA Summary ====================";
  LOG(INFO) << "op=" << OpName(op) << " device=" << FLAGS_device
            << " server=" << FLAGS_server_address
            << " payload_size=" << FLAGS_payload_size
            << " verify=" << verify_name << " threads=" << FLAGS_threads
            << " warmup_rounds=" << FLAGS_warmup_rounds
            << " measured_rounds=" << FLAGS_rounds;
  LOG(INFO) << "total_rpcs=" << summary.total_rpcs
            << " success=" << summary.success << " fail=" << summary.fail
            << " warmup_fail=" << summary.warmup_fail
            << " bytes=" << summary.bytes;
  LOG(INFO) << "wall=" << absl::StrFormat("%.6fs", summary.wall_s)
            << " qps=" << absl::StrFormat("%.2f", summary.qps)
            << " throughput=" << absl::StrFormat("%.2f MiB/s", summary.mib_s);
  LOG(INFO) << "latency_us: min=" << summary.min_us
            << " mean=" << absl::StrFormat("%.2f", summary.mean_us)
            << " p50=" << summary.p50_us << " p90=" << summary.p90_us
            << " p99=" << summary.p99_us << " max=" << summary.max_us;
  LOG(INFO) << "======================================================";

  if (FLAGS_json_result) {
    std::cout << "{"
              << "\"op\":\"" << OpName(op) << "\","
              << "\"server\":\"" << FLAGS_server_address << "\","
              << "\"device\":\"" << FLAGS_device << "\","
              << "\"payload_size\":" << FLAGS_payload_size << ","
              << "\"threads\":" << FLAGS_threads << ","
              << "\"warmup_rounds\":" << FLAGS_warmup_rounds << ","
              << "\"rounds\":" << FLAGS_rounds << ","
              << "\"total_rpcs\":" << summary.total_rpcs << ","
              << "\"success\":" << summary.success << ","
              << "\"fail\":" << summary.fail << ","
              << "\"warmup_fail\":" << summary.warmup_fail << ","
              << "\"bytes\":" << summary.bytes << ","
              << "\"wall_s\":" << absl::StrFormat("%.6f", summary.wall_s) << ","
              << "\"qps\":" << absl::StrFormat("%.2f", summary.qps) << ","
              << "\"mib_s\":" << absl::StrFormat("%.2f", summary.mib_s) << ","
              << "\"min_us\":" << summary.min_us << ","
              << "\"mean_us\":" << absl::StrFormat("%.2f", summary.mean_us)
              << ","
              << "\"p50_us\":" << summary.p50_us << ","
              << "\"p90_us\":" << summary.p90_us << ","
              << "\"p99_us\":" << summary.p99_us << ","
              << "\"max_us\":" << summary.max_us << "}" << std::endl;
  }
}

}  // namespace

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_threads <= 0 || FLAGS_rounds < 0 || FLAGS_warmup_rounds < 0) {
    LOG(ERROR) << "--threads must be > 0 and --rounds/--warmup_rounds must be "
                  ">= 0";
    return 1;
  }
  if (FLAGS_payload_size > std::numeric_limits<uint32_t>::max()) {
    LOG(ERROR) << "--payload_size is too large for current RDMA protocol";
    return 1;
  }

  const Op op = ParseOp(FLAGS_op);
  const VerifyMode verify_mode = ParseVerifyMode(FLAGS_verify);
  const size_t payload_size = static_cast<size_t>(FLAGS_payload_size);

  EndPoint ep;
  ep.device_name = FLAGS_device;
  ep.port_num = static_cast<uint8_t>(FLAGS_port_num);

  auto client = Client::Create(ep);
  if (client == nullptr) {
    LOG(ERROR) << "Fail to create RDMA client";
    return 1;
  }

  auto status = client->Connect(FLAGS_server_address);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to connect: server=" << FLAGS_server_address
               << " err=" << status.ToString();
    return 1;
  }
  LOG(INFO) << "Connected to server=" << FLAGS_server_address;

  const size_t buffer_size = std::max<size_t>(8, payload_size);
  RDMABufferPoolUPtr mem_pool =
      CreateBufferPool(ep, buffer_size, static_cast<size_t>(FLAGS_threads));
  CHECK_NOTNULL(mem_pool);

  std::vector<std::thread> workers;
  std::vector<WorkerStats> per_thread(FLAGS_threads);
  std::atomic<int> ready{0};
  std::atomic<bool> start{false};

  auto worker = [&](int tid) {
    auto* buffer = mem_pool->Alloc();
    CHECK_NOTNULL(buffer);
    PreparedWorkerCalls prepared_calls;
    PreparedWorkerCalls* prepared =
        UsePreparedCalls(verify_mode) ? &prepared_calls : nullptr;
    if (prepared != nullptr) {
      PrepareWorkerCalls(prepared, buffer, payload_size, tid);
    }

    for (int i = 0; i < FLAGS_warmup_rounds; ++i) {
      auto result = RunOne(client.get(), buffer, op, tid, i, payload_size,
                           verify_mode, prepared);
      if (!result.ok) {
        ++per_thread[tid].warmup_fail;
        LOG(ERROR) << "warmup failed: tid=" << tid << " round=" << i
                   << " op=" << FLAGS_op << " err=" << result.error;
      }
    }

    ready.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    per_thread[tid].latencies_us.reserve(FLAGS_rounds);
    for (int i = 0; i < FLAGS_rounds; ++i) {
      auto result = RunOne(client.get(), buffer, op, tid, i, payload_size,
                           verify_mode, prepared);
      per_thread[tid].latencies_us.push_back(result.latency_us);
      if (result.ok) {
        ++per_thread[tid].success;
        per_thread[tid].bytes += result.bytes;
      } else {
        ++per_thread[tid].fail;
        LOG(ERROR) << "round failed: tid=" << tid << " round=" << i
                   << " op=" << FLAGS_op << " err=" << result.error;
      }
      if (FLAGS_log_per_round) {
        LOG(INFO) << "round: tid=" << tid << " round=" << i
                  << " ok=" << result.ok << " latency_us=" << result.latency_us
                  << " bytes=" << result.bytes;
      }
    }

    mem_pool->Free(buffer);
  };

  workers.reserve(FLAGS_threads);
  for (int i = 0; i < FLAGS_threads; ++i) {
    workers.emplace_back(worker, i);
  }

  while (ready.load(std::memory_order_acquire) < FLAGS_threads) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  butil::Timer wall;
  wall.start();
  start.store(true, std::memory_order_release);

  for (auto& worker_thread : workers) {
    worker_thread.join();
  }
  wall.stop();

  const double wall_s = wall.u_elapsed() / 1e6;
  const auto summary = BuildSummary(per_thread, wall_s);
  PrintSummary(summary, op, verify_mode);

  return summary.fail == 0 ? 0 : 1;
}
