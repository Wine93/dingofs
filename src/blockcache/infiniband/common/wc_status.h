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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_WC_STATUS_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_WC_STATUS_H_

#include <infiniband/verbs.h>

#include <cstdint>

#include "common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

// The verbs status lands in Errno().
Status WcStatus(ibv_wc_status status, const char* what);

// Flush completions during an orderly close are the close, not failures.
inline bool IsUnexpectedWc(int32_t wc_status, bool closing) {
  return wc_status != IBV_WC_SUCCESS &&
         (wc_status != IBV_WC_WR_FLUSH_ERR || !closing);
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_COMMON_WC_STATUS_H_
