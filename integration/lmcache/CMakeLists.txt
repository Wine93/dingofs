cmake_minimum_required(VERSION 3.14)
project(dingofs_connector CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# DingoFS source root (for includes and linking)
set(DINGOFS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH
    "DingoFS source root directory")
set(DINGOFS_BUILD_DIR "${DINGOFS_ROOT}/build" CACHE PATH
    "DingoFS build directory")

# LMCache source root (for ConnectorBase headers)
set(LMCACHE_ROOT "/home/wine93/workspace/LMCache" CACHE PATH
    "LMCache source root directory")
set(LMCACHE_CSRC_DIR "${LMCACHE_ROOT}/csrc/storage_backends")

# Third-party install paths (bRPC, protobuf, abseil, etc.)
set(THIRD_PARTY_INSTALL_PATH "$ENV{HOME}/.local/dingo-eureka" CACHE PATH
    "Third-party libraries install path")
set(DINGOSDK_INSTALL_PATH "$ENV{HOME}/.local/dingo-sdk" CACHE PATH
    "DingoSDK install path")

find_package(pybind11 REQUIRED)

# pybind11 module
pybind11_add_module(_native
    src/connector/dingofs_connector.cpp
    src/connector/pybind.cpp
)

target_include_directories(_native PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/connector
    ${DINGOFS_ROOT}/src
    ${DINGOFS_BUILD_DIR}/protos
    ${LMCACHE_CSRC_DIR}
    ${THIRD_PARTY_INSTALL_PATH}/include
    ${DINGOSDK_INSTALL_PATH}/include
)

target_compile_options(_native PRIVATE -O3 -Wall -Wno-sign-compare -Wno-deprecated-declarations)

# Library search paths
target_link_directories(_native PRIVATE
    ${DINGOFS_BUILD_DIR}/src/cache/tiercache
    ${DINGOFS_BUILD_DIR}/src/cache/blockcache
    ${DINGOFS_BUILD_DIR}/src/cache/remotecache
    ${DINGOFS_BUILD_DIR}/src/cache/common
    ${DINGOFS_BUILD_DIR}/src/cache/iutil
    ${DINGOFS_BUILD_DIR}/src/cache/cachegroup
    ${DINGOFS_BUILD_DIR}/src/common
    ${DINGOFS_BUILD_DIR}/src/utils/executor
    ${THIRD_PARTY_INSTALL_PATH}/lib
    ${DINGOSDK_INSTALL_PATH}/lib
)

# Collect all DingoFS static libraries
file(GLOB_RECURSE DINGOFS_LIBS "${DINGOFS_BUILD_DIR}/src/*.a")
file(GLOB ABSL_LIBS "${THIRD_PARTY_INSTALL_PATH}/lib/libabsl_*.a")

# Link everything with --start-group to resolve circular dependencies
target_link_libraries(_native PRIVATE
    -Wl,--start-group
    ${DINGOFS_LIBS}
    brpc
    protobuf
    gflags
    glog
    leveldb
    jsoncpp
    fmt
    uring
    utf8_range
    utf8_validity
    ${ABSL_LIBS}
    ssl
    crypto
    -Wl,--end-group
    pthread
    dl
    z
    uuid_static
    resolv
)

# Install
install(TARGETS _native DESTINATION dingofs_connector)

# Tests
option(BUILD_TESTS "Build C++ tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests/connector)
endif()
