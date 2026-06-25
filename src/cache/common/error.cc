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
 * Created Date: 2026-01-19
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/error.h"

namespace dingofs {
namespace cache {

// A Status already carries the precise dingofs.pb.error.Errno in errno_ (see
// common/status.h), so serialization is just a copy of Errno() plus the
// message; the coarse Status::Code never touches the wire.
void StatusToPB(const Status& status, pb::error::Error* pb) {
  pb->set_errcode(static_cast<pb::error::Errno>(status.Errno()));
  if (!status.ok()) {
    pb->set_errmsg(status.ToString());
  }
}

// Reconstruct a Status from the wire. The Status(pb::error::Errno, msg)
// constructor carries the exact errcode in errno_ (so the original error type
// round-trips losslessly) and derives the coarse Code for Is##Name() /
// ToSysErrNo(); see Status::PBErrnoToCode in common/status.cc.
Status PBToStatus(const pb::error::Error& pb) {
  if (pb.errcode() == pb::error::OK) {
    return Status::OK();
  }
  return Status(pb.errcode(), pb.errmsg());
}

}  // namespace cache
}  // namespace dingofs
