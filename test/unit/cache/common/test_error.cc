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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "cache/common/error.h"

namespace dingofs {
namespace cache {

class ErrorTest : public ::testing::Test {};

// Each named constructor fills errno_ with its dingofs.pb.error.Errno, and
// StatusToPB writes that errno_ to the wire. This pins down the
// classification -> wire-code mapping for every status the cache can produce.
TEST_F(ErrorTest, StatusToPB) {
  std::vector<std::pair<Status, pb::error::Errno>> cases = {
      {Status::OK(), pb::error::OK},
      {Status::Internal("m"), pb::error::EINTERNAL},
      {Status::Unknown("m"), pb::error::ECACHE_UNKNOWN},
      {Status::Exist("m"), pb::error::EEXISTED},
      {Status::NotExist("m"), pb::error::ECACHE_NOT_EXIST},
      {Status::NoSpace("m"), pb::error::ECACHE_NO_SPACE},
      {Status::BadFd("m"), pb::error::ECACHE_BAD_FD},
      {Status::InvalidParam("m"), pb::error::EILLEGAL_PARAMTETER},
      {Status::NoPermission("m"), pb::error::ENO_PERMISSION},
      {Status::NotEmpty("m"), pb::error::ENOT_EMPTY},
      {Status::NoFlush("m"), pb::error::ECACHE_NO_FLUSH},
      {Status::NotSupport("m"), pb::error::ENOT_SUPPORT},
      {Status::NameTooLong("m"), pb::error::ECACHE_NAME_TOO_LONG},
      {Status::MountPointExist("m"), pb::error::ECACHE_MOUNTPOINT_EXIST},
      {Status::MountFailed("m"), pb::error::ECACHE_MOUNT_FAILED},
      {Status::OutOfRange("m"), pb::error::EOUT_OF_RANGE},
      {Status::NoData("m"), pb::error::ENO_DATA},
      {Status::IoError("m"), pb::error::ECACHE_IO_ERROR},
      {Status::Stale("m"), pb::error::ECACHE_STALE},
      {Status::NoSys("m"), pb::error::ECACHE_NO_SYS},
      {Status::NoPermitted("m"), pb::error::ECACHE_NO_PERMITTED},
      {Status::NetError("m"), pb::error::ECACHE_NET_ERROR},
      {Status::NotFound("m"), pb::error::ENOT_FOUND},
      {Status::NotDirectory("m"), pb::error::ECACHE_NOT_DIRECTORY},
      {Status::FileTooLarge("m"), pb::error::ECACHE_FILE_TOO_LARGE},
      {Status::EndOfFile("m"), pb::error::ECACHE_END_OF_FILE},
      {Status::Abort("m"), pb::error::ECACHE_ABORT},
      {Status::CacheDown("m"), pb::error::ECACHE_DOWN},
      {Status::CacheUnhealthy("m"), pb::error::ECACHE_UNHEALTHY},
      {Status::CacheFull("m"), pb::error::ECACHE_FULL},
      {Status::Stop("m"), pb::error::ECACHE_STOP},
      {Status::NotFit("m"), pb::error::ECACHE_NOT_FIT},
      {Status::Timeout("m"), pb::error::ETIMEOUT},
      {Status::OutOfMemory("m"), pb::error::ECACHE_OUT_OF_MEMORY},
  };

  for (const auto& [status, want] : cases) {
    pb::error::Error pb;
    StatusToPB(status, &pb);
    EXPECT_EQ(pb.errcode(), want) << "status=" << status.ToString();
  }
}

TEST_F(ErrorTest, PBToStatus) {
  auto from = [](pb::error::Errno code) {
    pb::error::Error pb;
    pb.set_errcode(code);
    return PBToStatus(pb);
  };

  EXPECT_TRUE(from(pb::error::OK).ok());
  EXPECT_TRUE(from(pb::error::EINTERNAL).IsInternal());
  EXPECT_TRUE(from(pb::error::ENOT_FOUND).IsNotFound());
  EXPECT_TRUE(from(pb::error::ECACHE_IO_ERROR).IsIoError());
  EXPECT_TRUE(from(pb::error::ECACHE_DOWN).IsCacheDown());

  // A code with no Status equivalent reconstructs gracefully: not ok, falls
  // back to a coarse Internal, but the raw wire code is preserved in errno_.
  Status unknown = from(static_cast<pb::error::Errno>(99999));
  EXPECT_FALSE(unknown.ok());
  EXPECT_TRUE(unknown.IsInternal());
  EXPECT_EQ(unknown.Errno(), 99999);
}

// The primary way to raise a precise error: Status(pb::error::Errno, msg). The
// exact code lands in errno_, the coarse Code is derived so Is##Name() /
// ToSysErrNo() keep working.
TEST_F(ErrorTest, ConstructFromPBErrno) {
  Status io(pb::error::ECACHE_IO_ERROR, "disk read failed");
  EXPECT_FALSE(io.ok());
  EXPECT_TRUE(io.IsIoError());
  EXPECT_EQ(io.Errno(), pb::error::ECACHE_IO_ERROR);
  EXPECT_NE(io.ToString().find("disk read failed"), std::string::npos);

  // A code mapping to a shared (non-cache) classification still works.
  Status nf(pb::error::ENOT_FOUND, "block missing");
  EXPECT_TRUE(nf.IsNotFound());
  EXPECT_EQ(nf.Errno(), pb::error::ENOT_FOUND);

  // An errcode with no cache classification: exact code preserved in errno_,
  // coarse Code falls back to Internal.
  Status other(pb::error::EBUSYING, "busy");
  EXPECT_TRUE(other.IsInternal());
  EXPECT_EQ(other.Errno(), pb::error::EBUSYING);

  // The generic ctor and the named shortcut agree for the same code.
  EXPECT_TRUE(Status(pb::error::EINTERNAL, "x").IsInternal());
}

TEST_F(ErrorTest, ErrmsgPassthrough) {
  pb::error::Error pb;
  StatusToPB(Status::IoError("disk read failed"), &pb);
  EXPECT_NE(pb.errmsg().find("disk read failed"), std::string::npos);

  Status status = PBToStatus(pb);
  EXPECT_TRUE(status.IsIoError());
  EXPECT_NE(status.ToString().find("disk read failed"), std::string::npos);
}

// Every Status code must survive a Status -> Error -> Status round-trip.
TEST_F(ErrorTest, RoundTripPreservesAllCodes) {
  std::vector<Status> all = {
      Status::OK(),
      Status::Internal("m"),
      Status::Unknown("m"),
      Status::Exist("m"),
      Status::NotExist("m"),
      Status::NoSpace("m"),
      Status::BadFd("m"),
      Status::InvalidParam("m"),
      Status::NoPermission("m"),
      Status::NotEmpty("m"),
      Status::NoFlush("m"),
      Status::NotSupport("m"),
      Status::NameTooLong("m"),
      Status::MountPointExist("m"),
      Status::MountFailed("m"),
      Status::OutOfRange("m"),
      Status::NoData("m"),
      Status::IoError("m"),
      Status::Stale("m"),
      Status::NoSys("m"),
      Status::NoPermitted("m"),
      Status::NetError("m"),
      Status::NotFound("m"),
      Status::NotDirectory("m"),
      Status::FileTooLarge("m"),
      Status::EndOfFile("m"),
      Status::Abort("m"),
      Status::CacheDown("m"),
      Status::CacheUnhealthy("m"),
      Status::CacheFull("m"),
      Status::Stop("m"),
      Status::NotFit("m"),
      Status::Timeout("m"),
      Status::OutOfMemory("m"),
  };

  for (const auto& original : all) {
    pb::error::Error pb;
    StatusToPB(original, &pb);
    Status converted = PBToStatus(pb);
    EXPECT_TRUE(original == converted)
        << "original=" << original.ToString()
        << ", converted=" << converted.ToString();
    // The precise wire code survives too, not just the coarse classification.
    EXPECT_EQ(original.Errno(), converted.Errno())
        << "original=" << original.ToString()
        << ", converted=" << converted.ToString();
  }
}

}  // namespace cache
}  // namespace dingofs
