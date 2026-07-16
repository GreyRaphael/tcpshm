[English](README.md) | [中文](README-CN.md)

**TCPSHM is a connection-oriented persistent message queue framework based on TCP or SHM IPC for Linux & Windows (C++20/23)**

When using TCP to transfer data, sent out messages are not guaranteed to be received or handled by the receiver, and even worse, we often get unexpected disconnections due to network issues or program crash, so efforts have been made on recovery procedure to ensure both sides are synced. TCPSHM provides a reliable and efficient solution based on a sequence number and acknowledge mechanism, that every sent out msg is persisted in a send queue until sender got ack that it's been consumed by the receiver, so that disconnects/crashes are tolerated and the recovery process is purely automatic.

And as the name implies, shared memory (`use_shm`) is also supported when communicating on the same host, and it provides the exact same API and behavior as TCP (so whether TCP or SHM underlies the connection is transparent to the user), but it's more than 20 times faster than TCP on localhost. The shared memory communication is based on a real-time single-producer single-consumer (`SPSCVarQueue`) lock-free ring buffer.

The user message format is just a general purpose binary string, it's user's responsibility to encode/decode it. E.g. user can simply use C/C++ struct for simplicity/efficiency, or modern serialization libraries like [iguana](https://github.com/qicosmos/iguana) / Google Protocol Buffers with `std::variant` for extensibility.

Additionally, both sides of a connection have a specified name, and a pair of such names uniquely identifies a persistent connection. If one side disconnects, changes its name and reconnects with the same remote side, the connection will be brand new and will not recover from the old one. This can sometimes be useful, e.g: A daily trading server starts before market open and stops after market close every trading day, and every day when it starts it expects the connection with its clients to be new and any unhandled msgs from yesterday are silently discarded (obsolete order requests don't make any sense in a new trading day!), so the server can set its name to be something like "Server20180714".

This is a header-only framework that provides server-side and client-side C++ template classes (`TcpShmServer` and `TcpShmClient`), which implement a typical TCP/SHM server and client and are highly configurable and customizable. For the server, the framework supports connection sharding: user predefines a set of connection groups, having one or more threads polling these groups, and once there's a new connection user can decide which group to assign to, giving the user full control over the mapping between serving threads and client connections.

## Technical Features
  * **Cross-Platform Support**: Seamlessly supports both **Linux and Windows** via a clean operating system abstraction layer (`os.h`), bridging `WinSock2` / `CreateFileMapping` / `MapViewOfFile` with POSIX sockets / `shm_open` / `mmap`.
  * **Modern C++20 / C++23**: Built with modern C++ memory models (`std::atomic` with `memory_order_acquire/release` lock-free ring buffers, `std::endian`, `std::span`, `std::has_single_bit`, `std::atomic::wait`).
  * Header-only (`#include "tcpshm/..."`), no static/dynamic library to build and link.
  * Zero external library dependencies.
  * Non-blocking data transmission (except client `Connect()` and initial SHM probing handshake).
  * No creating threads internally (the framework gives user full control over polling and threading).
  * No getting any kind of timestamp from system in critical paths (user passes timestamps into polling methods).
  * No C++ exceptions.
  * No writing to stdout/stderr or log files internally.
  * No use of heavy mutexes or atomic CAS locks in the fast data path (`Alloc()` / `Push()` / `Front()` / `Pop()`).
  * Lightweight, clean, and efficient.

## SHM Probe (物理共享内存握手探测机制)

当客户端尝试连接服务端并请求开启共享内存模式 (`use_shm = 1`) 时，为防止跨物理机部署、跨容器网络隔离或权限不足导致直接打开 SHM 崩溃或通信异常，`tcpshm` 在 TCP 登录阶段自动执行物理内存就绪探测握手协议：

1. **发起登录请求**：客户端通过 TCP 发送 `LoginMsg`，携带 `use_shm = 1` 及客户端名称。
2. **服务端创建探测内存**：服务端发现 `use_shm = 1` 后，生成唯一的防冲突随机 Token (`MakeShmProbeToken(client_name)`)，并在系统临时区创建临时探测共享内存（如 `/tcpshm_probe_server_client_token`），写入 `token` 并将 `ack` 置零。
3. **下发探测协议**：服务端通过 TCP 发出 `ShmProbeReqMsg (msg_type = 3)`，包含探测内存名称与 `token`。
4. **客户端验证与物理内存回写**：
   * 客户端收到 `ShmProbeReqMsg` 后，必须以**只读已存在模式** (`my_mmap_existing`，严禁 Create 权限) 打开该共享内存。
   * 客户端校验读出的 `probe->token == req->token` 无误后，计算 `ack = token ^ 0x5a5a5a5a5a5a5a5aULL`，并将该 `ack` 直接写入**物理共享内存 (`probe->ack`)**。
5. **TCP 回执确认**：客户端通过 TCP 回发 `ShmProbeRspMsg (msg_type = 4)` 报告 `ack` 和 `ok = 1`。
6. **双重校验允许升级**：服务端收到 TCP 回执后，除了验证 TCP 包中的 `rsp->ack` 与 `rsp->ok`，还会对**物理内存 `probe->ack`** 重新读取验证。只有当 TCP 协议层与物理内存映射层双重验证完毕，证明双方确实处在同一主机物理内存空间且拥有完备读写权限后，方才允许 `use_shm = 1` 登录并开启超高速 SHM 双向队列；否则拒绝登录。

## Limitations
  * It won't persist data on disk, so it can't recover from power down (unconsumed messages in `.ptcp` / `.shm` files survive process crash, but not system reboot or power loss).
  * As it's non-blocking and busy polling for the purpose of low latency, CPU usage would be high in busy-polling loops (`PollTcp` / `PollShm`). User can insert `std::this_thread::yield()` or `sleep_for` if absolute minimum latency is not required.
  * Currently user can only read/write to a connection (`Alloc/Push` and `Front/Pop`) in its designated polling thread. If needing to write msg from other threads, user has to push it to some lock-free queue which is then consumed by the polling thread.
  * Transactions across multiple messages are not supported. If you have multiple `Push()` or `Pop()` actions in a batch, be prepared that some succeed and some fail in case of a process crash during the batch.
  * Currently the message length must fit in a `uint16_t` (up to 65535 bytes including the 8-byte `MsgHeader`).

## Documentation
  [Interface Doc (English)](doc/interface.md) | [接口文档 (中文)](doc/interface-CN.md)

## Example
  Check [`server.cpp`](server.cpp) and [`client.cpp`](client.cpp) for a complete, modernized C++20/23 example demonstrating zero-copy allocation, Protobuf serialization (`iguana`), and `std::variant` dispatch.

## Performance
The echo client/server example (`test` / `server.cpp` & `client.cpp`) is used for performance testing, where the client sends a message to the server on the same host, waiting for the response before sending the next one. Average RTT (Round Trip Time = total time elapsed / number of messages processed):

* RTT in **TCP** mode: **~8.8 us**
* RTT in **SHM** mode: **~0.34 us**

*(Shared memory mode is over 20x faster than TCP loopback on localhost due to zero-copy kernel bypass (`SPSCVarQueue`).)*

## Guide to header files (`include/tcpshm`):

* **`tcpshm_client.h`**: The client-side template class (`TcpShmClient`). Handling connection handshake, SHM probing, and polling.
* **`tcpshm_server.h`**: The server-side template class (`TcpShmServer`). Supporting connection sharding, group polling, SHM probe verification, and heartbeat monitoring.
* **`tcpshm_conn.h`**: A general connection class (`TcpShmConnection`) that encapsulates TCP (`PTCPConnection`) or SHM (`SPSCVarQueue`). Use `Alloc()` / `Push()` and `Front()` / `Pop()` to send and receive messages.
* **`msg_header.h`**: Defines the 8-byte `MsgHeader` (`size`, `msg_type`, `ack_seq`) and automatic endianness conversion.
* **`os.h`**: Cross-platform OS abstraction layer (Windows `WinSock2` / `CreateFileMapping` vs Linux POSIX sockets / `shm_open` / `mmap`).
* **`spsc_varq.h`**: Modern C++20 `std::atomic` single-producer single-consumer lock-free ring buffer with variable-length message allocation.
* **`ptcp_conn.h` & `ptcp_queue.h`**: TCP connection management, protocol message templates (`LoginMsg`, `LoginRspMsg`, `ShmProbeReqMsg`, `ShmProbeRspMsg`), and persistent mmap-backed TCP send/ack queues (`PTCPQueue`).
* **`endian.h` & `mmap.h`**: Utility templates for compile-time endian conversions (`std::endian`) and memory mapping wrappers.
