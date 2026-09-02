# Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(common)
add_subdirectory(iutil)
add_subdirectory(infiniband)
add_subdirectory(local)
add_subdirectory(node)
add_subdirectory(remote)
add_subdirectory(tier)
add_subdirectory(tools)

file(GLOB CACHE_LIB_SRCS
    "node/dingo_cache.cc"
)
add_library(cache_lib ${CACHE_LIB_SRCS})
target_link_libraries(cache_lib
    cache_common
    cache_iutil
    cache_infiniband
    cache_local
    cache_node
    cache_remote
    cache_tier
    dingofs_options
)

add_executable(dingo-cache node/main.cc)
target_link_libraries(dingo-cache
    ibverbs::ibverbs

    cache_lib
)
target_link_options(dingo-cache PRIVATE
    "-Wl,--allow-multiple-definition"
)
