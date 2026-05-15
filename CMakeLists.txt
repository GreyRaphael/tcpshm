cmake_minimum_required(VERSION 3.20)
project(tcpshm VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")

add_executable(client client.cpp)
target_include_directories(client PRIVATE ${CMAKE_SOURCE_DIR}/third_party)

add_executable(server server.cpp)
target_include_directories(server PRIVATE ${CMAKE_SOURCE_DIR}/third_party)