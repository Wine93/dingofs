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

#ifndef DINGOFS_COMMON_STATUS_H_
#define DINGOFS_COMMON_STATUS_H_

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/string_slice.h"
#include "dingofs/error.pb.h"

namespace dingofs {

/// @brief Return the given status if it is not @c OK.
#define DINGOFS_RETURN_NOT_OK(s)       \
  do {                                 \
    const ::dingofs::Status& _s = (s); \
    if (!_s.IsOK()) return _s;         \
  } while (0)

#undef DECLARE_ERROR_STATUS

// CODE is the coarse, cache-local classification (an enum Code value); PB_ERRNO
// is the dingofs.pb.error.Errno that this classification maps to on the wire.
// The one-argument form fills errno_ with PB_ERRNO; the two-argument form lets
// the caller carry a more precise upstream errno (e.g. relayed from MDS).
#define DECLARE_ERROR_STATUS(NAME, CODE, PB_ERRNO)              \
  static Status NAME(const StringSlice& msg,                    \
                     const StringSlice& msg2 = StringSlice()) { \
    return Status(CODE, (PB_ERRNO), msg, msg2);                 \
  };                                                            \
  static Status NAME(int32_t p_errno, const StringSlice& msg,   \
                     const StringSlice& msg2 = StringSlice()) { \
    return Status(CODE, p_errno, msg, msg2);                    \
  }                                                             \
  bool Is##NAME() const { return code_ == (CODE); }

class Status {
 private:
  // A coarse, cache-local classification that drives ToSysErrNo() and the
  // Is##Name() predicates. It never crosses the wire; the precise
  // dingofs.pb.error.Errno travels in errno_ (filled by the named constructors
  // and round-tripped by StatusToPB / PBToStatus in cache/common/error.cc).
  enum Code : uint8_t {
    kOk = 0,
    kInternal = 1,
    kUnknown = 2,
    kExist = 3,
    kNotExist = 4,
    kNoSpace = 5,
    kBadFd = 6,
    kInvalidParam = 7,
    kNoPermission = 8,
    kNotEmpty = 9,
    kNoFlush = 10,
    kNotSupport = 11,
    kNameTooLong = 12,
    kMountPointExist = 13,
    kMountFailed = 14,
    kOutOfRange = 15,
    kNoData = 16,
    kIoError = 17,
    kStale = 18,
    kNoSys = 19,
    kNoPermitted = 20,
    kNetError = 21,
    kNotFound = 22,
    kNotDirectory = 23,
    kFileTooLarge = 24,
    kEndOfFile = 25,
    kAbort = 26,
    kCacheDown = 27,
    kCacheUnhealthy = 28,
    kCacheFull = 29,
    kStop = 30,
    kNotFit = 31,
    kTimeout = 32,
    kOutOfMemory = 33,
  };
  static const int32_t kNone = 0;

 public:
  // Create a success status.
  Status() noexcept : code_(kOk), errno_(kNone), state_(nullptr) {}

  // Create a status directly from a dingofs.pb.error.Errno. This is the primary
  // way to raise a precise error: the exact code is kept in errno_ (so the peer
  // learns exactly what happened, regardless of where it originated) while the
  // coarse Code is derived from it for Is##Name() / ToSysErrNo().
  Status(pb::error::Errno code, const StringSlice& msg,
         const StringSlice& msg2 = StringSlice());

  ~Status() = default;

  Status(const Status& rhs);

  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept;

  Status& operator=(Status&& rhs) noexcept;

  bool operator==(const Status& rhs) const { return code_ == rhs.code_; }

  bool operator!=(const Status& rhs) const { return code_ != rhs.code_; }

  bool ok() const { return code_ == kOk; }  // NOLINT

  static Status OK() { return Status(); }

  DECLARE_ERROR_STATUS(OK, kOk, pb::error::OK);
  DECLARE_ERROR_STATUS(Internal, kInternal, pb::error::EINTERNAL);
  DECLARE_ERROR_STATUS(Unknown, kUnknown, pb::error::ECACHE_UNKNOWN);
  DECLARE_ERROR_STATUS(Exist, kExist, pb::error::EEXISTED);
  DECLARE_ERROR_STATUS(NotExist, kNotExist, pb::error::ECACHE_NOT_EXIST);
  DECLARE_ERROR_STATUS(NoSpace, kNoSpace, pb::error::ECACHE_NO_SPACE);
  DECLARE_ERROR_STATUS(BadFd, kBadFd, pb::error::ECACHE_BAD_FD);
  DECLARE_ERROR_STATUS(InvalidParam, kInvalidParam,
                       pb::error::EILLEGAL_PARAMTETER);
  DECLARE_ERROR_STATUS(NoPermission, kNoPermission, pb::error::ENO_PERMISSION);
  DECLARE_ERROR_STATUS(NotEmpty, kNotEmpty, pb::error::ENOT_EMPTY);
  DECLARE_ERROR_STATUS(NoFlush, kNoFlush, pb::error::ECACHE_NO_FLUSH);
  DECLARE_ERROR_STATUS(NotSupport, kNotSupport, pb::error::ENOT_SUPPORT);
  DECLARE_ERROR_STATUS(NameTooLong, kNameTooLong,
                       pb::error::ECACHE_NAME_TOO_LONG);
  DECLARE_ERROR_STATUS(MountPointExist, kMountPointExist,
                       pb::error::ECACHE_MOUNTPOINT_EXIST);
  DECLARE_ERROR_STATUS(MountFailed, kMountFailed,
                       pb::error::ECACHE_MOUNT_FAILED);
  DECLARE_ERROR_STATUS(OutOfRange, kOutOfRange, pb::error::EOUT_OF_RANGE);
  DECLARE_ERROR_STATUS(NoData, kNoData, pb::error::ENO_DATA);
  DECLARE_ERROR_STATUS(IoError, kIoError, pb::error::ECACHE_IO_ERROR);
  DECLARE_ERROR_STATUS(Stale, kStale, pb::error::ECACHE_STALE);
  DECLARE_ERROR_STATUS(NoSys, kNoSys, pb::error::ECACHE_NO_SYS);
  DECLARE_ERROR_STATUS(NoPermitted, kNoPermitted,
                       pb::error::ECACHE_NO_PERMITTED);
  DECLARE_ERROR_STATUS(NetError, kNetError, pb::error::ECACHE_NET_ERROR);
  DECLARE_ERROR_STATUS(Timeout, kTimeout, pb::error::ETIMEOUT);
  DECLARE_ERROR_STATUS(NotFound, kNotFound, pb::error::ENOT_FOUND);
  DECLARE_ERROR_STATUS(NotDirectory, kNotDirectory,
                       pb::error::ECACHE_NOT_DIRECTORY);
  DECLARE_ERROR_STATUS(FileTooLarge, kFileTooLarge,
                       pb::error::ECACHE_FILE_TOO_LARGE);
  DECLARE_ERROR_STATUS(EndOfFile, kEndOfFile, pb::error::ECACHE_END_OF_FILE);
  DECLARE_ERROR_STATUS(Abort, kAbort, pb::error::ECACHE_ABORT);
  DECLARE_ERROR_STATUS(CacheDown, kCacheDown, pb::error::ECACHE_DOWN);
  DECLARE_ERROR_STATUS(CacheUnhealthy, kCacheUnhealthy,
                       pb::error::ECACHE_UNHEALTHY);
  DECLARE_ERROR_STATUS(CacheFull, kCacheFull, pb::error::ECACHE_FULL);
  DECLARE_ERROR_STATUS(Stop, kStop, pb::error::ECACHE_STOP);
  DECLARE_ERROR_STATUS(NotFit, kNotFit, pb::error::ECACHE_NOT_FIT);
  DECLARE_ERROR_STATUS(OutOfMemory, kOutOfMemory,
                       pb::error::ECACHE_OUT_OF_MEMORY);

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  // The precise dingofs.pb.error.Errno carried by this status. This is what
  // crosses the wire (see StatusToPB / PBToStatus) and what callers match
  // against pb::error::Errno values (e.g. MDSClient retry classification).
  int32_t Errno() const { return errno_; }

  int ToSysErrNo() const {
    switch (code_) {
      case kOk:
        return 0;
      case kInternal:
        return EIO;
      case kUnknown:
        return EIO;
      case kExist:
        return EEXIST;
      case kNotExist:
        return ENOENT;
      case kNoSpace:
        return ENOSPC;
      case kBadFd:
        return EBADF;
      case kInvalidParam:
        return EINVAL;
      case kNoPermission:
        return EACCES;
      case kNotEmpty:
        return ENOTEMPTY;
      case kNoFlush:
        return EIO;
      case kNotSupport:
        return EOPNOTSUPP;
      case kNameTooLong:
        return ENAMETOOLONG;
      case kMountPointExist:
        return EIO;
      case kMountFailed:
        return EIO;
      case kOutOfRange:
        return ERANGE;
      case kNoData:
        return ENODATA;
      case kIoError:
        return EIO;
      case kStale:
        return ESTALE;
      case kNoSys:
        return ENOSYS;
      case kNoPermitted:
        return EPERM;
      case kNetError:
        return EIO;
      case kNotFound:
        return ENOENT;
      case kStop:
        return EIO;
      case kNotFit:
        return EIO;
      case kTimeout:
        return ETIMEDOUT;
      case kOutOfMemory:
        return ENOMEM;
      default:
        return EIO;
    }
  }

 private:
  Status(Code code, int32_t p_errno, const StringSlice& msg,
         const StringSlice& msg2);

  // Derive the coarse Code from a wire dingofs.pb.error.Errno. The single
  // source of truth for the errcode -> Code mapping; the inverse (Code -> a
  // canonical errcode) lives in the DECLARE_ERROR_STATUS list above.
  static Code PBErrnoToCode(pb::error::Errno code);

  static std::unique_ptr<const char[]> CopyState(const char* s);

  Code code_;
  int32_t errno_;
  // A nullptr state_ (which is at least the case for OK) means the extra
  // message is empty.
  std::unique_ptr<const char[]> state_;
};

inline Status::Status(const Status& rhs)
    : code_(rhs.code_), errno_(rhs.errno_) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_.get());
}

inline Status& Status::operator=(const Status& rhs) {
  if (this != &rhs) {
    code_ = rhs.code_;
    errno_ = rhs.errno_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_.get());
  }
  return *this;
}

inline Status::Status(Status&& rhs) noexcept : Status() {
  *this = std::move(rhs);
}

inline Status& Status::operator=(Status&& rhs) noexcept {
  if (this != &rhs) {
    code_ = rhs.code_;
    errno_ = rhs.errno_;
    state_ = std::move(rhs.state_);
  }
  return *this;
}

#undef DECLARE_ERROR_STATUS

}  // namespace dingofs

#endif  // DINGOFS_COMMON_STATUS_H_
