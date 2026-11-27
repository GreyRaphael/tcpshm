add_executable(tcpshm_bench tcpshm_bench.cpp)
target_link_libraries(tcpshm_bench PRIVATE tcpshm::tcpshm)
target_compile_features(tcpshm_bench PRIVATE cxx_std_20)
