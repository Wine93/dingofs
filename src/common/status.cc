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

#include "common/status.h"

#include <cstdio>
#include <string>

#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {

std::unique_ptr<const char[]> Status::CopyState(const char* s) {
  const size_t cch = std::strlen(s) + 1;  // +1 for the null terminator
  char* rv = new char[cch];
  std::strncpy(rv, s, cch);
  return std::unique_ptr<const char[]>(rv);
}

Status::Status(Code code, int32_t p_errno, const StringSlice& msg,
               const StringSlice& msg2)
    : code_(code), errno_(p_errno) {
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);

  char* const result = new char[size + 1];  // +1 for null terminator
  memcpy(result, msg.data(), len1);

  if (len2) {
    result[len1] = ':';
    result[len1 + 1] = ' ';
    memcpy(result + len1 + 2, msg2.data(), len2);
  }
  result[size] = '\0';  // null terminator for C style string
  state_.reset(result);
}

Status::Status(pb::error::Errno code, const StringSlice& msg,
               const StringSlice& msg2)
    : Status(PBErrnoToCode(code), static_cast<int32_t>(code), msg, msg2) {}

Status::Code Status::PBErrnoToCode(pb::error::Errno code) {
  switch (code) {
    case pb::error::OK:
      return kOk;
    case pb::error::EINTERNAL:
      return kInternal;
    case pb::error::ECACHE_UNKNOWN:
      return kUnknown;
    case pb::error::EEXISTED:
      return kExist;
    case pb::error::ECACHE_NOT_EXIST:
      return kNotExist;
    case pb::error::ECACHE_NO_SPACE:
      return kNoSpace;
    case pb::error::ECACHE_BAD_FD:
      return kBadFd;
    case pb::error::EILLEGAL_PARAMTETER:
      return kInvalidParam;
    case pb::error::ENO_PERMISSION:
      return kNoPermission;
    case pb::error::ENOT_EMPTY:
      return kNotEmpty;
    case pb::error::ECACHE_NO_FLUSH:
      return kNoFlush;
    case pb::error::ENOT_SUPPORT:
      return kNotSupport;
    case pb::error::ECACHE_NAME_TOO_LONG:
      return kNameTooLong;
    case pb::error::ECACHE_MOUNTPOINT_EXIST:
      return kMountPointExist;
    case pb::error::ECACHE_MOUNT_FAILED:
      return kMountFailed;
    case pb::error::EOUT_OF_RANGE:
      return kOutOfRange;
    case pb::error::ENO_DATA:
      return kNoData;
    case pb::error::ECACHE_IO_ERROR:
      return kIoError;
    case pb::error::ECACHE_STALE:
      return kStale;
    case pb::error::ECACHE_NO_SYS:
      return kNoSys;
    case pb::error::ECACHE_NO_PERMITTED:
      return kNoPermitted;
    case pb::error::ECACHE_NET_ERROR:
      return kNetError;
    case pb::error::ETIMEOUT:
      return kTimeout;
    case pb::error::ENOT_FOUND:
      return kNotFound;
    case pb::error::ECACHE_NOT_DIRECTORY:
      return kNotDirectory;
    case pb::error::ECACHE_FILE_TOO_LARGE:
      return kFileTooLarge;
    case pb::error::ECACHE_END_OF_FILE:
      return kEndOfFile;
    case pb::error::ECACHE_ABORT:
      return kAbort;
    case pb::error::ECACHE_DOWN:
      return kCacheDown;
    case pb::error::ECACHE_UNHEALTHY:
      return kCacheUnhealthy;
    case pb::error::ECACHE_FULL:
      return kCacheFull;
    case pb::error::ECACHE_STOP:
      return kStop;
    case pb::error::ECACHE_NOT_FIT:
      return kNotFit;
    case pb::error::ECACHE_OUT_OF_MEMORY:
      return kOutOfMemory;
    default:
      // An errcode without a cache classification (e.g. an MDS-only code):
      // keep the exact code in errno_ and fall back to a coarse Internal.
      return kInternal;
  }
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    std::string type;
    switch (code_) {
      case kOk:
        type = "OK";
        break;
      case kInternal:
        type = "Internal";
        break;
      case kUnknown:
        type = "Unknown";
        break;
      case kExist:
        type = "Exist";
        break;
      case kNotExist:
        type = "NotExist";
        break;
      case kNoSpace:
        type = "NoSpace";
        break;
      case kBadFd:
        type = "BadFd";
        break;
      case kInvalidParam:
        type = "InvalidParam";
        break;
      case kNoPermission:
        type = "NoPermission";
        break;
      case kNotEmpty:
        type = "NotEmpty";
        break;
      case kNoFlush:
        type = "NoFlush";
        break;
      case kNotSupport:
        type = "NotSupport";
        break;
      case kNameTooLong:
        type = "NameTooLong";
        break;
      case kMountPointExist:
        type = "MountPointExist";
        break;
      case kMountFailed:
        type = "MountFailed";
        break;
      case kOutOfRange:
        type = "OutOfRange";
        break;
      case kNoData:
        type = "NoData";
        break;
      case kIoError:
        type = "IoError";
        break;
      case kStale:
        type = "Stale";
        break;
      case kNoSys:
        type = "NoSys";
        break;
      case kNoPermitted:
        type = "NoPermitted";
        break;
      case kNetError:
        type = "NetError";
        break;
      case kNotFound:
        type = "NotFound";
        break;
      case kNotDirectory:
        type = "NotDirectory";
        break;
      case kFileTooLarge:
        type = "FileTooLarge";
        break;
      case kEndOfFile:
        type = "EndOfFile";
        break;
      case kAbort:
        type = "Abort";
        break;
      case kCacheDown:
        type = "CacheDown";
        break;
      case kCacheUnhealthy:
        type = "CacheUnhealthy";
        break;
      case kCacheFull:
        type = "CacheFull";
        break;
      case kStop:
        type = "Stop";
        break;
      case kNotFit:
        type = "NotFit";
        break;
      case kTimeout:
        type = "Timeout";
        break;
      case kOutOfMemory:
        type = "OutOfMemory";
        break;
      default:
        type = std::to_string(static_cast<int>(code_));
        LOG(ERROR) << fmt::format("Unknown code({}):", type);
    }

    std::string result(type);
    if (errno_ != kNone) {
      result.append(fmt::format(" (errno:{}) ", errno_));
    }

    if (state_ != nullptr) {
      result.append(": ");
      result.append(state_.get());
    }

    return result;
  }
}

}  // namespace dingofs
