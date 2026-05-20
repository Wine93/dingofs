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

#include <absl/strings/str_format.h>
#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "cache/infiniband/client.h"
#include "cache/infiniband/controller.h"
#include "dingofs/blockcache.pb.h"

DEFINE_string(device, "mlx5_0", "IB device name (e.g. mlx5_0)");
DEFINE_int32(port_num, 1, "IB HCA port, 1-based");
DEFINE_string(server_address, "127.0.0.1:8888",
              "RDMA server brpc address (host:port)");
DEFINE_int32(rounds, 1,
             "Number of RangeRequest rounds per thread; -1 means infinite");
DEFINE_int32(threads, 1, "Number of concurrent worker threads issuing RPCs");
DEFINE_bool(log_per_round, true,
            "Log per-round result. Disable for cleaner latency stats.");

namespace {
constexpr const char* kServiceName = "dingofs.pb.cache.BlockCacheService";
constexpr const char* kMethodName = "Range";
}  // namespace

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  using namespace dingofs::cache::infiniband;  // NOLINT
  namespace pb_cache = dingofs::pb::cache;

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

  // One buffer per worker thread — the server RDMA_WRITEs the attachment into
  // the region whose addr/rkey each Controller advertises, so threads cannot
  // share a buffer (concurrent writes would clobber each other).
  const int pool_size = std::max(2, FLAGS_threads);
  auto mem_pool = RDMAMemoryPool::Create(client->GetProtectDomain(), 4194304,
                                         pool_size);
  CHECK_NOTNULL(mem_pool);

  auto worker = [&](int tid, std::vector<uint64_t>* latencies_us,
                    int* success_out, int* fail_out) {
    auto* buffer = mem_pool->Require();
    CHECK(buffer != nullptr) << "tid=" << tid << " (pool exhausted)";

    std::mt19937_64 rng(std::random_device{}() ^ static_cast<uint64_t>(tid));
    std::uniform_int_distribution<uint64_t> dist(0, (1ULL << 32) - 1);

    int success = 0;
    int fail = 0;
    latencies_us->reserve(FLAGS_rounds > 0 ? FLAGS_rounds : 1024);

    for (int i = 0; FLAGS_rounds < 0 || i < FLAGS_rounds; ++i) {
      butil::Timer timer;
      timer.start();

      uint64_t num = dist(rng);
      pb_cache::RangeRequest request;
      request.set_offset(num);
      request.set_length(1);
      pb_cache::RangeResponse response;

      auto* cntl = new Controller;
      cntl->SetCorrelationId(reinterpret_cast<uint64_t>(cntl));
      cntl->SetRequestMemoryContext(
          MemoryContext{reinterpret_cast<uint64_t>(buffer->data),
                        buffer->capacity, buffer->rkey});

      auto st =
          client->Call(cntl, kServiceName, kMethodName, request, &response);
      uint64_t echoed = *reinterpret_cast<uint64_t*>(buffer->data);

      timer.stop();
      const uint64_t cost_us = timer.u_elapsed(0);
      latencies_us->push_back(cost_us);

      if (!st.ok()) {
        ++fail;
        LOG(ERROR) << "tid=" << tid << " Round[" << i << "] Call failed:"
                   << " req.offset=" << num << " err=" << st.ToString();
      } else if (echoed != num + 1) {
        ++fail;
        LOG(ERROR) << "tid=" << tid << " Round[" << i << "] Mismatch:"
                   << " req.offset=" << num << " attachment[0]=" << echoed
                   << " expected=" << (num + 1);
      } else {
        ++success;
        if (FLAGS_log_per_round) {
          LOG(INFO) << "tid=" << tid << " Round[" << i
                    << "] OK: req.offset=" << num
                    << " resp.cache_hit=" << response.cache_hit()
                    << ", cost "
                    << absl::StrFormat("%.6lf", cost_us / 1e6) << " seconds.";
        }
      }

      delete cntl;
    }

    *success_out = success;
    *fail_out = fail;
  };

  std::vector<std::thread> workers;
  std::vector<std::vector<uint64_t>> per_thread_lat(FLAGS_threads);
  std::vector<int> per_thread_success(FLAGS_threads, 0);
  std::vector<int> per_thread_fail(FLAGS_threads, 0);

  butil::Timer wall;
  wall.start();
  workers.reserve(FLAGS_threads);
  for (int t = 0; t < FLAGS_threads; ++t) {
    workers.emplace_back(worker, t, &per_thread_lat[t],
                         &per_thread_success[t], &per_thread_fail[t]);
  }
  for (auto& th : workers) th.join();
  wall.stop();

  // Aggregate.
  std::vector<uint64_t> all_latencies;
  int total_success = 0, total_fail = 0;
  for (int t = 0; t < FLAGS_threads; ++t) {
    all_latencies.insert(all_latencies.end(), per_thread_lat[t].begin(),
                         per_thread_lat[t].end());
    total_success += per_thread_success[t];
    total_fail += per_thread_fail[t];
  }
  std::sort(all_latencies.begin(), all_latencies.end());

  uint64_t min_us = 0, p50 = 0, p90 = 0, p99 = 0, max_us = 0;
  double mean_us = 0;
  if (!all_latencies.empty()) {
    const size_t n = all_latencies.size();
    min_us = all_latencies.front();
    max_us = all_latencies.back();
    p50 = all_latencies[n * 50 / 100];
    p90 = all_latencies[n * 90 / 100];
    p99 = all_latencies[std::min(n - 1, n * 99 / 100)];
    uint64_t sum = 0;
    for (auto v : all_latencies) sum += v;
    mean_us = static_cast<double>(sum) / static_cast<double>(n);
  }

  const double wall_s = wall.u_elapsed() / 1e6;
  const double qps =
      wall_s > 0 ? static_cast<double>(total_success + total_fail) / wall_s : 0;

  LOG(INFO) << "==================== Summary ====================";
  LOG(INFO) << "threads=" << FLAGS_threads << " rounds_per_thread="
            << FLAGS_rounds << " total_rpcs=" << all_latencies.size()
            << " success=" << total_success << " fail=" << total_fail;
  LOG(INFO) << "wall=" << absl::StrFormat("%.3fs", wall_s)
            << " throughput=" << absl::StrFormat("%.0f rpc/s", qps);
  LOG(INFO) << "latency(us): min=" << min_us << " mean="
            << absl::StrFormat("%.0f", mean_us) << " P50=" << p50
            << " P90=" << p90 << " P99=" << p99 << " max=" << max_us;
  LOG(INFO) << "================================================";

  return total_fail == 0 ? 0 : 1;
}
