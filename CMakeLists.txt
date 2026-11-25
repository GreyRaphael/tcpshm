cmake_minimum_required(VERSION 3.20)
project(tcpshm VERSION 0.1.0 LANGUAGES CXX)

if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 20)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")

find_package(Threads REQUIRED)

add_library(tcpshm INTERFACE)
add_library(tcpshm::tcpshm ALIAS tcpshm)
target_include_directories(tcpshm INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(tcpshm INTERFACE cxx_std_20)
target_link_libraries(tcpshm INTERFACE
    Threads::Threads
    $<$<PLATFORM_ID:Linux>:rt>
    $<$<PLATFORM_ID:Windows>:ws2_32>
)

add_executable(client client.cpp)
target_link_libraries(client PRIVATE tcpshm::tcpshm)
target_include_directories(client SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

add_executable(server server.cpp)
target_link_libraries(server PRIVATE tcpshm::tcpshm)
target_include_directories(server SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

enable_testing()
add_executable(tcpshm_test tests/tcpshm_test.cpp)
target_link_libraries(tcpshm_test PRIVATE tcpshm::tcpshm)
add_test(NAME tcpshm_test COMMAND tcpshm_test)
