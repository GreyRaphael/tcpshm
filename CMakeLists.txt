cmake_minimum_required(VERSION 3.20)
project(tcpshm VERSION 2.0.0 LANGUAGES CXX)

if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 23)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TCPSHM_BUILD_EXAMPLES "Build tcpshm example binaries" ON)
option(TCPSHM_BUILD_TESTS "Build tcpshm unit and integration tests" ON)
option(TCPSHM_BUILD_BENCHMARKS "Build tcpshm benchmark binary" ON)
option(TCPSHM_INSTALL "Enable CMake install targets" ON)

add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")

find_package(Threads REQUIRED)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

add_library(tcpshm INTERFACE)
add_library(tcpshm::tcpshm ALIAS tcpshm)
target_include_directories(tcpshm INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_compile_features(tcpshm INTERFACE cxx_std_23)
target_link_libraries(tcpshm INTERFACE
    Threads::Threads
    $<$<PLATFORM_ID:Linux>:rt>
    $<$<PLATFORM_ID:Windows>:ws2_32>
)

if(TCPSHM_BUILD_EXAMPLES)
    add_executable(echo_client examples/echo_client.cpp)
    target_link_libraries(echo_client PRIVATE tcpshm::tcpshm)
    target_include_directories(echo_client SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

    add_executable(echo_server examples/echo_server.cpp)
    target_link_libraries(echo_server PRIVATE tcpshm::tcpshm)
    target_include_directories(echo_server SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

    add_executable(echo_server_async examples/echo_server_async.cpp)
    target_link_libraries(echo_server_async PRIVATE tcpshm::tcpshm)
    target_include_directories(echo_server_async SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
endif()

if(TCPSHM_BUILD_TESTS)
    enable_testing()

    add_executable(unit_spsc_test tests/unit_spsc_test.cpp)
    target_link_libraries(unit_spsc_test PRIVATE tcpshm::tcpshm)
    add_test(NAME unit_spsc_test COMMAND unit_spsc_test)

    add_executable(unit_ptcp_test tests/unit_ptcp_test.cpp)
    target_link_libraries(unit_ptcp_test PRIVATE tcpshm::tcpshm)
    add_test(NAME unit_ptcp_test COMMAND unit_ptcp_test)

    add_executable(integration_handshake_test tests/integration_handshake_test.cpp)
    target_link_libraries(integration_handshake_test PRIVATE tcpshm::tcpshm)
    add_test(NAME integration_handshake_test COMMAND integration_handshake_test)

    add_executable(integration_server_client_test tests/integration_server_client_test.cpp)
    target_link_libraries(integration_server_client_test PRIVATE tcpshm::tcpshm)
    add_test(NAME integration_server_client_test COMMAND integration_server_client_test)

    add_library(tcpshm_noexcept_check OBJECT tests/noexcept_compile_check.cpp)
    target_link_libraries(tcpshm_noexcept_check PRIVATE tcpshm::tcpshm)
    target_compile_options(tcpshm_noexcept_check PRIVATE
        $<$<OR:$<CXX_COMPILER_ID:GNU>,$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:-fno-exceptions -Wall -Wextra -Wconversion -Wshadow -Werror>
    )
endif()

if(TCPSHM_BUILD_BENCHMARKS)
    add_executable(tcpshm_bench benchmarks/tcpshm_bench.cpp)
    target_link_libraries(tcpshm_bench PRIVATE tcpshm::tcpshm)
    if(TCPSHM_BUILD_TESTS)
        add_test(NAME tcpshm_bench_smoke
                 COMMAND tcpshm_bench --mode=all --iters=200 --warmup=20 --sizes=16,64)
    endif()
endif()

if(TCPSHM_INSTALL)
    install(TARGETS tcpshm EXPORT tcpshmTargets)
    install(DIRECTORY include/tcpshm DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    install(EXPORT tcpshmTargets
            FILE tcpshmTargets.cmake
            NAMESPACE tcpshm::
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tcpshm)
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/tcpshmConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
        ARCH_INDEPENDENT
    )
    configure_file(cmake/tcpshmConfig.cmake.in
                   "${CMAKE_CURRENT_BINARY_DIR}/tcpshmConfig.cmake"
                   @ONLY)
    install(FILES
            "${CMAKE_CURRENT_BINARY_DIR}/tcpshmConfig.cmake"
            "${CMAKE_CURRENT_BINARY_DIR}/tcpshmConfigVersion.cmake"
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tcpshm)
endif()
