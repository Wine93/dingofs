add_executable(test_dingofs_connector
    test_dingofs_connector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../../src/connector/dingofs_connector.cpp
)

target_include_directories(test_dingofs_connector PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../src/connector
    ${DINGOFS_ROOT}/src
    ${DINGOFS_BUILD_DIR}/protos
    ${LMCACHE_CSRC_DIR}
)

target_link_libraries(test_dingofs_connector PRIVATE
    cache_tiercache
    cache_blockcache
    cache_remotecache
    cache_common
    cache_iutil
    common
    dingofs_executor
    pthread
    stdc++fs
)

add_test(NAME test_dingofs_connector COMMAND test_dingofs_connector)
