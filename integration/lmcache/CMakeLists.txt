cmake_minimum_required(VERSION 3.14)
project(dingofs_connector C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# DingoFS source root (for includes and linking)
set(DINGOFS_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH
    "DingoFS source root directory")
set(DINGOFS_BUILD_DIR "${DINGOFS_ROOT}/build" CACHE PATH
    "DingoFS build directory")

# Main shared library
add_library(dingofs_connector SHARED
    src/cache_engine/cache_engine.cpp
    src/cache_engine/cache_engine_capi.cpp
)

target_include_directories(dingofs_connector PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/cache_engine>
    $<INSTALL_INTERFACE:include>
)

target_include_directories(dingofs_connector PRIVATE
    ${DINGOFS_ROOT}/src
    ${DINGOFS_BUILD_DIR}/protos
)

target_compile_options(dingofs_connector PRIVATE -O3 -Wall -Wextra)

# Link DingoFS cache libraries
target_link_libraries(dingofs_connector PRIVATE
    cache_tiercache
    cache_blockcache
    cache_remotecache
    cache_common
    cache_iutil
    common
    dingofs_executor
    pthread
)

set_target_properties(dingofs_connector PROPERTIES
    OUTPUT_NAME "dingofs_connector"
)

# Install
install(TARGETS dingofs_connector
    LIBRARY DESTINATION dingofs_connector
    ARCHIVE DESTINATION dingofs_connector
)
install(FILES src/cache_engine/cache_engine_capi.h DESTINATION include)

# Tests
option(BUILD_TESTS "Build C++ tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests/cache_engine)
endif()
