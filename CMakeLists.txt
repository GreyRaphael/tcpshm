cmake_minimum_required(VERSION 3.16)
project(tcpshm VERSION 2.1.0 LANGUAGES CXX)

string(COMPARE EQUAL "${CMAKE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}" TCPSHM_IS_TOP_LEVEL)

option(TCPSHM_BUILD_TESTS      "Build unit and integration tests" ${TCPSHM_IS_TOP_LEVEL})
option(TCPSHM_BUILD_EXAMPLES   "Build usage examples"             ${TCPSHM_IS_TOP_LEVEL})
option(TCPSHM_BUILD_BENCHMARKS "Build performance benchmarks"     ${TCPSHM_IS_TOP_LEVEL})

find_package(Threads REQUIRED)

# Header-only C++20 library target
add_library(tcpshm INTERFACE)
add_library(tcpshm::tcpshm ALIAS tcpshm)
target_compile_features(tcpshm INTERFACE cxx_std_20)
target_include_directories(tcpshm INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include")
if(MSVC)
  target_compile_options(tcpshm INTERFACE /utf-8)
endif()
target_link_libraries(tcpshm INTERFACE
  Threads::Threads
  $<$<PLATFORM_ID:Linux>:rt>
  $<$<PLATFORM_ID:Windows>:ws2_32>
)

if(TCPSHM_BUILD_TESTS)
  include(CTest)
  enable_testing()
  add_subdirectory(tests)
endif()

if(TCPSHM_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(TCPSHM_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
