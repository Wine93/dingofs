add_executable(echo_server server.cc)
add_executable(echo_client client.cc)

target_link_libraries(echo_server cache_lib)
target_link_libraries(echo_client cache_lib)
