[English](README.md) | [中文](README-CN.md)

**TCPSHM v2 是一个基于纯 C++20 标准库、面向 Linux 与 Windows 的高性能共享内存 (SHM) IPC 与持久化 TCP 消息队列框架。**

在原生 TCP 传输中，进程崩溃或网络闪断极易导致两端消息状态不一致。**TCPSHM** 通过“序列号 + ACK + mmap 持久化发送队列 (`PtcpQueue` `.ptcp`)”提供全自动、零丢失的断线重连恢复能力。

当通信双方部署在同一物理主机时，切换到**共享内存模式 (`TransportKind::Shm`)** 可绕过内核网络栈，直接运行在无锁 `SpscQueue` 环形缓冲区上（Ping-Pong P50 RTT **`200 ns`**，相比本地环回 TCP 快 **`31.5x–33.0x`**，流水线回显吞吐高达 **`81.1M msg/s`**）。框架在接入阶段自动执行**全非阻塞物理 SHM 探测握手**，防止跨容器或无权限环境下误用 SHM 引发崩溃。

业务消息采用 **16 字节 `alignas(16)` 扩展帧头 (`FrameHeader`)**，彻底突破 64 KiB 单包限制（上限 `kMaxPayloadSize = 64 MiB - 16` 并原生保证载荷 16B SIMD 对齐），并通过零开销 RAII `WriteGuard` / `ReadGuard` / `MsgView` 安全访问。既可零拷贝收发 POD 结构体，也可搭配附带的 [`iguana`](https://github.com/qicosmos/iguana) + `std::variant` 进行 Protobuf 序列化。

## v2 核心架构与演进亮点

* **纯 C++20 零依赖底座**：仅依赖标准 C++20（同时在 `-std=c++23` 下零警告通过），内置零堆分配 `Expected<T, Error>`（`Result<T>`）与纯 C++20 字节序翻转实现，配合 `std::span`、`std::atomic_ref`、`std::chrono::nanoseconds` 与 `ConfigConcept`/`HandlerConcept` 编译期约束；核心头文件严格零异常（`-fno-exceptions -Werror` 持续验证）。
* **16 字节 16B 对齐帧头 (`FrameHeader`)**：包含 `uint32_t size`、`uint16_t msg_type`、`uint16_t flags`、`uint32_t ack_seq`、`uint32_t reserved`，一举支持 >64 KiB 大包（如行情全量快照、批量风控列表）及 `alignas(16)` 载荷零拷贝映射，并预留 `flags`/`reserved` 扩展空间。
* **零开销 RAII 收发 API**：彻底告别裸指针算术和忘调 `Pop()`/`Push()` 卡死隐患。`WriteGuard` 离开作用域未提交会自动 `cancel()`；`ReadGuard` 析构自动 `pop()`，并支持 `defer()` 实现背压重试。
* **真·批量提交与消费 + 即时 ACK 捎带**：`WriteGuard::stage()` + `flush()` 与 `drain(fn, max_batch)` 合并跨核原子发布与 TCP `send()` 系统调用，并在回调前先递进 `my_ack()` 使同步应答立即捎带最新 ACK（背压返回 `false` 时回滚），TCP 流水线吞吐提升至 **20.4x**、SHM 提升至 **2.38x**。
* **静态分离传输通道**：拆分为紧凑独立的 [`ShmChannel`](include/tcpshm/shm_channel.hpp) 与 [`TcpChannel`](include/tcpshm/tcp_channel.hpp)，消灭热路径 `if (use_shm)` 分支并缩减通道对象体积。
* **全非阻塞控制面状态机**：抛弃 v1 最长阻塞 300ms 的同步探测，`Server::poll_ctl()` 全程非阻塞推进 `ReadingLogin -> SendingProbeReq -> ReadingProbeRsp -> SendingLoginRsp`，慢/卡死连接 **0ms 阻塞** SHM 心跳与其他连接接入。
* **原生代际连接句柄 `ConnHandle`**：8 字节 `{group_id, slot_id, generation, transport}` 句柄配合 `server.with_channel(handle, fn)` 天然免疫重连槽位复用的 ABA 问题。
* **可插拔 TCP Poller 策略**：默认采用零系统调用延迟的 [`BusyPoller`](include/tcpshm/poller.hpp)，Linux 下支持编译期切换 [`EpollPoller`](include/tcpshm/poller.hpp) 扩展多连接场景。
* **协议与配置指纹防呆**：握手强制校验 `kProtocolMagic`、`kProtocolVersion = 2` 及 `config_fingerprint<Config>()`，`.ptcp` 文件校验魔数、版本、容量与 `header_size_`（16）一致性。

## 目录与模块说明 (`include/tcpshm/`)

* [`tcpshm.hpp`](include/tcpshm/tcpshm.hpp)：统一聚合头文件。
* [`config.hpp`](include/tcpshm/config.hpp)：`DefaultConfig`、`ConfigConcept`、`TimePoint`、`config_fingerprint<Config>()`。
* [`error.hpp`](include/tcpshm/error.hpp)：零堆分配 `ErrorCode`、`Error`、`Unexpected<E>`、`Expected<T, E>`、`Result<T>`、`VoidResult`。
* [`frame.hpp`](include/tcpshm/frame.hpp)：16 字节 `alignas(16)` `FrameHeader`、`align_frame_size()`、`MsgView`、`WriteGuard<Producer>`、`ReadGuard<Consumer>`。
* [`conn_handle.hpp`](include/tcpshm/conn_handle.hpp)：8 字节代际连接句柄 `ConnHandle` 与 `TransportKind`。
* [`spsc_queue.hpp`](include/tcpshm/spsc_queue.hpp)：隐式生命周期 `SpscRingLayout`、单缓存行隔离 `SpscProducer`/`SpscConsumer` 及进程内 `SpscQueue`。
* [`ptcp_queue.hpp`](include/tcpshm/ptcp_queue.hpp)：支持魔数/版本/容量/帧头尺寸校验的 mmap 持久化 `PtcpQueue`。
* [`shm_channel.hpp`](include/tcpshm/shm_channel.hpp) 与 [`tcp_channel.hpp`](include/tcpshm/tcp_channel.hpp)：静态分离的 SHM / PTCP 通道实现。
* [`poller.hpp`](include/tcpshm/poller.hpp)：`BusyPoller`（跨平台默认）与 Linux `EpollPoller`。
* [`server.hpp`](include/tcpshm/server.hpp) 与 [`client.hpp`](include/tcpshm/client.hpp)：组合式 `Server`、`ShmClient`、`TcpClient` 及运行时可切换 `Client`。
* `detail/`：底层跨平台抽象（[`os_socket.hpp`](include/tcpshm/detail/os_socket.hpp)、[`os_shm.hpp`](include/tcpshm/detail/os_shm.hpp)、[`endian.hpp`](include/tcpshm/detail/endian.hpp)、[`protocol.hpp`](include/tcpshm/detail/protocol.hpp)）。

## 构建与 CMake 集成

仅要求 C++20 编译器（已在 Linux GCC 14.2.0 `-std=c++20` 与 `-std=c++23` 下全面验证，Windows WinSock2 支持保留在 `detail/` 中）。

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

* [`tests/unit_spsc_test.cpp`](tests/unit_spsc_test.cpp)：`kMaxPayloadSize` 边界、`alignas(16)` 零拷贝 `emplace`/`as_ptr`、>64 KiB（`128 KiB`）大包收发、RAII Guard 自动取消/消费、回绕安全、多 `BlkSz`（16/32/64）、批量 `stage()+flush()`、`drain()` 背压与多线程压测。
* [`tests/unit_ptcp_test.cpp`](tests/unit_ptcp_test.cpp)：持久化重启恢复（含 `128 KiB` 大包）、`uint32_t` 序列号回绕、损坏文件与容量/`header_size` 不匹配识别。
* [`tests/integration_handshake_test.cpp`](tests/integration_handshake_test.cpp)：慢连接/卡死 SHM 探针 **0ms 非阻塞** 隔离、配置不匹配/非法名/重复登录/超限拒收、`ConnHandle` ABA 防护。
* [`tests/integration_server_client_test.cpp`](tests/integration_server_client_test.cpp)：SHM、`BusyPoller` TCP 与 Linux `EpollPoller` TCP 端到端全链路，含 >64 KiB（`128 KiB + 13B`）连发 3 轮验证即时 ACK 捎带、`flags`/`reserved` 透传、背压、重连续传与序列号错配。
* [`tests/noexcept_compile_check.cpp`](tests/noexcept_compile_check.cpp)：持续确保全部核心头文件可在 `-fno-exceptions -Werror` 下无错编译。

## 三版本同机性能对比 (`v1.0.0` vs `v2-8B/C++23` vs `v2-16B/C++20`)

同机环境实测（Linux x86_64，GCC 14.2.0 `-O3 -DNDEBUG`，未绑核，`--iters=50000 --warmup=2000 --sizes=16,64,256`，参见 [`benchmarks/tcpshm_bench.cpp`](benchmarks/tcpshm_bench.cpp)）：

```bash
./build/tcpshm_bench --mode=all --iters=50000 --warmup=2000 --sizes=16,64,256
# >64 KiB 大包延迟测试示例：
./build/tcpshm_bench --mode=rtt --iters=2000 --warmup=200 --sizes=131072
```

| 基准测试项 (`16B / 64B / 256B`) | `v1.0.0` (C++20 / 8B) | `v2` 上一版 (C++23 / 8B) | `v2` 当前版 (C++20 / 16B) |
| --- | --- | --- | --- |
| **Raw SPSC 单条吞吐** | 77.8M / 58.3M / 58.4M msg/s | 79.3M / 72.8M / 64.3M msg/s | **81.7M / 58.5M / 66.9M msg/s** |
| **Raw SPSC Batch-32** | — | 158.8M / 158.9M / 102.6M msg/s | **101.3M / 127.7M / 104.2M msg/s** |
| **SHM Ping-Pong P50 RTT** | **200 ns**（均值 252 / 266 / 230 ns） | **200 ns**（均值 199 / 262 / 270 ns） | **200 ns**（均值 225 / 254 / 243 ns） |
| **TCP Ping-Pong P50 RTT** | 6.3 / 6.2 / 6.3 us | Busy 6.4 / Epoll 6.6–6.8 us | Busy **6.3–6.6 us** / Epoll **6.7–6.8 us** |
| **流水线回显 (SHM / SHM-Batch)** | 47.1M / 28.4M / 17.9M msg/s | 89.1M / 63.6M / 33.7M msg/s | **81.1M / 67.4M / 32.8M msg/s** |
| **流水线回显 (TCP-Busy)** | 278k / 281k / 266k msg/s | 5.69M / 5.13M / 4.32M msg/s | **5.67M / 5.29M / 4.59M msg/s** |
| **流水线回显 (TCP-Epoll)** | — | 5.42M / 5.02M / 4.09M msg/s | **5.37M / 5.13M / 4.36M msg/s** |
| **128 KiB (`131072 B`) P50 RTT** | *（不支持 >64 KiB）* | *（不支持 >64 KiB）* | **SHM 2.3 us** (`364k msg/s`) / **TCP 49.1–49.6 us** |
