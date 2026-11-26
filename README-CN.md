[English](README.md) | [中文](README-CN.md)

**TCPSHM v2 是一个基于现代 C++23、面向 Linux 与 Windows 的高性能共享内存 (SHM) IPC 与持久化 TCP 消息队列框架。**

在原生 TCP 传输中，进程崩溃或网络闪断极易导致两端消息状态不一致。**TCPSHM** 通过“序列号 + ACK + mmap 持久化发送队列 (`PtcpQueue` `.ptcp`)”提供全自动、零丢失的断线重连恢复能力。

当通信双方部署在同一物理主机时，切换到**共享内存模式 (`TransportKind::Shm`)** 可绕过内核网络栈，直接运行在无锁 `SpscQueue` 环形缓冲区上（Ping-Pong P50 RTT **`200 ns`**，相比本地环回 TCP 快 **`33x`**，流水线回显吞吐高达 **`90.7M msg/s`**）。框架在接入阶段自动执行**全非阻塞物理 SHM 探测握手**，防止跨容器或无权限环境下误用 SHM 引发崩溃。

业务消息采用 **8 字节极简 `FrameHeader`**（单包有效载荷上限 `65527` 字节），并通过零开销 RAII `WriteGuard` / `ReadGuard` / `MsgView`（`std::span<std::byte>`、`write_pod`、`emplace`、`read_as<T>`）安全访问。既可零拷贝收发 POD 结构体，也可搭配附带的 [`iguana`](https://github.com/qicosmos/iguana) + `std::variant` 进行 Protobuf 序列化。

## v2.0.0 核心架构演进

* **全面升级 C++23**：原生采用 `std::expected`（`Result<T, Error>`）、`std::byteswap`、`std::endian`、`std::span`、`std::atomic_ref`、`std::chrono::nanoseconds` 与 `ConfigConcept`/`HandlerConcept` 编译期约束；核心头文件严格零异常（通过 `-fno-exceptions -Werror` 验证）。
* **零开销 RAII 收发 API**：彻底告别裸 `MsgHeader*` 指针算术和忘调 `Pop()`/`Push()` 卡死隐患。`WriteGuard` 离开作用域未提交会自动回滚 `cancel()`；`ReadGuard` 析构自动 `pop()`，也支持显式 `defer()` 实现背压重试。
* **真·批量提交与消费**：`WriteGuard::stage()` + `flush()` 与 `drain(fn, max_batch)` 合并跨核原子发布与 TCP `send()` 系统调用，TCP 流水线吞吐最高提升 **20.5x**，SHM 吞吐最高提升 **2.8x**。
* **静态分离传输通道**：拆分为紧凑独立的 [`ShmChannel`](include/tcpshm/shm_channel.hpp) 与 [`TcpChannel`](include/tcpshm/tcp_channel.hpp)，消灭热路径 `if (use_shm)` 分支并缩减通道对象内存占用。
* **全非阻塞控制面状态机**：抛弃 v1 最长阻塞 300ms 的同步探测，`Server::poll_ctl()` 全程非阻塞推进 `ReadingLogin -> SendingProbeReq -> ReadingProbeRsp -> SendingLoginRsp`，任何卡死/慢连接 **0ms 阻塞** SHM 心跳与其他正常连接接入。
* **原生代际连接句柄 `ConnHandle`**：8 字节 `{group_id, slot_id, generation, transport}` 句柄配合 `server.with_channel(handle, fn)` 天然免疫重连槽位复用的 ABA 问题，大幅简化异步业务架构。
* **可插拔 TCP Poller 策略**：默认采用零系统调用延迟的 [`BusyPoller`](include/tcpshm/poller.hpp)，Linux 下支持编译期切换 [`EpollPoller`](include/tcpshm/poller.hpp) 以扩展多连接场景。
* **协议与配置指纹防呆**：握手强制校验 `kProtocolMagic`、`kProtocolVersion = 2` 及 `config_fingerprint<Config>()`，`.ptcp` 文件校验魔数、版本与容量配置一致性。

## 目录与模块说明 (`include/tcpshm/`)

* [`tcpshm.hpp`](include/tcpshm/tcpshm.hpp)：统一聚合头文件。
* [`config.hpp`](include/tcpshm/config.hpp)：`DefaultConfig`、`ConfigConcept`、`TimePoint`、`config_fingerprint<Config>()`。
* [`error.hpp`](include/tcpshm/error.hpp)：零堆分配 `ErrorCode`、`Error`、`Result<T>`、`VoidResult`。
* [`frame.hpp`](include/tcpshm/frame.hpp)：8 字节 `FrameHeader`、`MsgView`、`WriteGuard<Producer>`、`ReadGuard<Consumer>`。
* [`conn_handle.hpp`](include/tcpshm/conn_handle.hpp)：8 字节代际连接句柄 `ConnHandle` 与 `TransportKind`。
* [`spsc_queue.hpp`](include/tcpshm/spsc_queue.hpp)：隐式生命周期 `SpscRingLayout`、单缓存行隔离 `SpscProducer`/`SpscConsumer` 及进程内 `SpscQueue`。
* [`ptcp_queue.hpp`](include/tcpshm/ptcp_queue.hpp)：支持魔数/版本/容量校验的 mmap 持久化 `PtcpQueue`。
* [`shm_channel.hpp`](include/tcpshm/shm_channel.hpp) 与 [`tcp_channel.hpp`](include/tcpshm/tcp_channel.hpp)：静态分离的 SHM / PTCP 通道实现。
* [`poller.hpp`](include/tcpshm/poller.hpp)：`BusyPoller`（跨平台默认）与 Linux `EpollPoller`。
* [`server.hpp`](include/tcpshm/server.hpp) 与 [`client.hpp`](include/tcpshm/client.hpp)：组合式 `Server`、`ShmClient`、`TcpClient` 及运行时可切换 `Client`。
* `detail/`：底层跨平台抽象（[`os_socket.hpp`](include/tcpshm/detail/os_socket.hpp)、[`os_shm.hpp`](include/tcpshm/detail/os_shm.hpp)、[`endian.hpp`](include/tcpshm/detail/endian.hpp)、[`protocol.hpp`](include/tcpshm/detail/protocol.hpp)）。

## 构建与 CMake 集成

需要 C++23 编译器（已在 Linux GCC 14.2.0 全面验证，Windows WinSock2 支持保留在 `detail/` 中）。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

支持标准 `cmake --install` 安装与 `find_package(tcpshm CONFIG REQUIRED)` 导入，下游直接链接纯头文件目标：

```cmake
target_link_libraries(my_app PRIVATE tcpshm::tcpshm)
```

## 文档与示例

* 接口开发指南：[中文](doc/interface-CN.md) | [English](doc/interface.md)
* 分片同步服务端与客户端（基于 `iguana` Protobuf）：[`examples/common.hpp`](examples/common.hpp)、[`examples/echo_server.cpp`](examples/echo_server.cpp)、[`examples/echo_client.cpp`](examples/echo_client.cpp)
* 双线程零锁异步服务端（原生 `ConnHandle` + `SpscQueue`）：[`examples/echo_server_async.cpp`](examples/echo_server_async.cpp)

## 测试矩阵

CTest 自动运行 4 套单元测试/集成测试、`-fno-exceptions` 编译门禁及 Benchmark 冒烟：

* [`tests/unit_spsc_test.cpp`](tests/unit_spsc_test.cpp)：长度上限、RAII Guard 自动取消/消费、回绕安全、多 `BlkSz`（16/32/64）、批量 `stage()+flush()`、`drain()` 背压重试及多线程压测。
* [`tests/unit_ptcp_test.cpp`](tests/unit_ptcp_test.cpp)：持久化重启恢复、`uint32_t` 序列号回绕、损坏文件与容量不匹配识别。
* [`tests/integration_handshake_test.cpp`](tests/integration_handshake_test.cpp)：慢连接/卡死 SHM 探针 **0ms 非阻塞** 隔离、配置不匹配/非法名/重复登录/超限拒收、`ConnHandle` ABA 防护。
* [`tests/integration_server_client_test.cpp`](tests/integration_server_client_test.cpp)：SHM、`BusyPoller` TCP 与 Linux `EpollPoller` TCP 端到端全链路（含背压、重连续传与序列号错配）。
* [`tests/noexcept_compile_check.cpp`](tests/noexcept_compile_check.cpp)：持续确保全部核心头文件可在 `-fno-exceptions -Werror` 下无错编译。

## 性能基准对比 (`v1.0.0` vs `v2.0.0`)

同机环境实测（Linux x86_64，GCC 14.2.0 `-O3 -DNDEBUG`，未绑核，`--iters=50000 --warmup=2000 --sizes=16,64,256`，参见 [`benchmarks/tcpshm_bench.cpp`](benchmarks/tcpshm_bench.cpp)）：

```bash
./build/tcpshm_bench --mode=all --iters=50000 --warmup=2000 --sizes=16,64,256
# 可选绑核参数：--pin-server=2 --pin-client=3
```

| 基准测试项 | v1.0.0 (16B / 64B / 256B) | v2.0.0 (16B / 64B / 256B) | 相对提升 |
| --- | --- | --- | --- |
| **Raw SPSC 单条吞吐** | 68.1M / 68.7M / 62.8M msg/s | 67.4M / 54.4M / 62.4M msg/s | 持平 (~1.0x) |
| **Raw SPSC Batch-32** | — | **113.3M / 147.0M / 100.1M msg/s** | **1.59x – 2.14x** |
| **SHM Ping-Pong P50 RTT** | 200 ns（均值 240 / 256 / 233 ns） | **200 ns**（均值 228 / 260 / 234 ns） | 相比 TCP 快 **33.0x** |
| **TCP Ping-Pong P50 / P99 RTT** | 6.2 us / 16.3 us | Busy 6.6 / 16.3 us；Epoll 6.9 / **13.7–15.7 us** | Epoll P99 尾延迟更优 |
| **流水线回显吞吐 (SHM)** | 39.0M / 29.6M / 11.3M msg/s | **90.7M / 69.2M / 31.9M msg/s** (`SHM-Batch`) | **2.32x – 2.82x** |
| **流水线回显吞吐 (TCP)** | 273k / 270k / 268k msg/s | **5.59M / 4.85M / 4.41M msg/s** (`TCP-Busy`) | **16.5x – 20.5x** |
| **流水线回显吞吐 (TCP Epoll)** | — | **5.24M / 4.64M / 4.34M msg/s** (`TCP-Epoll`) | **16.2x – 19.2x** |
