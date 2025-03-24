/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/errno.h"

#include <ostream>
#include <unordered_map>

#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace common {

using pb::cache::blockcache::BlockCacheErrIOError;
using pb::cache::blockcache::BlockCacheErrNotFound;
using pb::cache::blockcache::BlockCacheErrUnknown;
using pb::cache::blockcache::BlockCacheOk;

struct Error {
  Errno code;
  BlockCacheErrCode pb_code;
  std::string description;
};

static const std::vector<Error> kErrors = {
    {Errno::OK, BlockCacheOk, "OK"},
    {Errno::INVALID_ARGUMENT, BlockCacheErrUnknown, "invalid argument"},
    {Errno::NOT_FOUND, BlockCacheErrNotFound, "not found"},
    {Errno::EXISTS, BlockCacheErrUnknown, "already exists"},
    {Errno::NOT_DIRECTORY, BlockCacheErrUnknown, "not a directory"},
    {Errno::FILE_TOO_LARGE, BlockCacheErrUnknown, "file is too large"},
    {Errno::END_OF_FILE, BlockCacheErrUnknown, "end of file"},
    {Errno::IO_ERROR, BlockCacheErrIOError, "IO error"},
    {Errno::ABORT, BlockCacheErrUnknown, "abort"},
    {Errno::CACHE_DOWN, BlockCacheErrUnknown, "cache is down"},
    {Errno::CACHE_UNHEALTHY, BlockCacheErrUnknown, "cache is unhealthy"},
    {Errno::CACHE_FULL, BlockCacheErrUnknown, "cache is full"},
    {Errno::NOT_SUPPORTED, BlockCacheErrUnknown, "not supported"},
};

static std::unordered_map<Errno, Error> k_codes;
static std::unordered_map<BlockCacheErrCode, Error> k_pb_codes;

static void BuildOnce() {
  static bool inited = false;
  if (!inited) {
    return;
  }

  for (const auto& err : kErrors) {
    k_codes[err.code] = err;
    k_pb_codes[err.pb_code] = err;
  }
  inited = true;
}

std::string StrErr(Errno code) {
  BuildOnce();
  auto it = k_codes.find(code);
  if (it != k_codes.end()) {
    return it->second.description;
  }
  return "unknown";
}

BlockCacheErrCode PbErr(Errno code) {
  BuildOnce();
  auto it = k_codes.find(code);
  if (it != k_codes.end()) {
    return it->second.pb_code;
  }
  return BlockCacheOk;
}

Errno ToErrno(BlockCacheErrCode pb_code) {
  BuildOnce();
  auto it = k_pb_codes.find(pb_code);
  if (it != k_pb_codes.end()) {
    return it->second.code;
  }
  return Errno::OK;
}

std::ostream& operator<<(std::ostream& os, Errno code) {
  if (code == Errno::OK) {
    os << "success";
  } else {
    os << "failed [" << StrErr(code) << "]";
  }
  return os;
}

}  // namespace common
}  // namespace cache
}  // namespace dingofs
