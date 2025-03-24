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
 * Created Date: 2025-03-12
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/config.h"

namespace dingofs {
namespace cache {
namespace common {

S3ClientOptions::S3ClientOptions() = default;

DiskStateOptions::DiskStateOptions() : tick_duration_s(60) {}

DiskCacheOptions::DiskCacheOptions()
    : cache_dir("/var/run/dingofs"), cache_size_mb(100) {}

BlockCacheOptions::BlockCacheOptions()
    : cache_store("disk"),
      stage(true),
      stage_throttle_enable(false),
      stage_throttle_bandwidth_mb(4096),
      flush_file_workers(10),
      flush_file_queue_size(100),
      flush_slice_workers(10),
      flush_slice_queue_size(100),
      upload_stage_workers(10),
      upload_stage_queue_size(100) {}

CacheGroupNodeOptions::CacheGroupNodeOptions()
    : listen_ip("0.0.0.0"),
      listen_port(9301),
      group_weight(100),
      max_range_size_kb(128),
      metadata_filepath("/var/run/dingofs/cache_group_node.meta"),
      mds_client(nullptr) {}

RemoteBlockCacheOptions::RemoteBlockCacheOptions()
    : load_members_interval_ms(3000), mds_client(nullptr) {}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
