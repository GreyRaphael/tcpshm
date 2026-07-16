[English](README.md) | [中文](README-CN.md)

**TCPSHM 是一个支持 Linux 和 Windows (C++20/23) 平台、基于 TCP 与共享内存 (SHM) IPC 的面向连接持久化消息队列框架**

当使用 TCP 进行网络数据传输时，发送出的消息不能保证必定被对端接收并处理；更糟糕的是，由于网络异常或程序崩溃，两端经常会遇到突发的连接断开。因此，传统高可靠系统往往需要复杂的对账和恢复机制来确保通信两端的状态同步。**TCPSHM** 依靠严格的“序列号 + 确认应答 (Sequence Number & ACK)”机制提供了可靠且高效的解决方案：每一条发出的消息都会持久化存储在发送队列 (`.ptcp` 或 `.shm`) 中，直到发送方收到对端已成功消费该消息的 ACK 为止。因此，程序对断链与宕机崩溃具有完全的容错能力，断线重连后的数据恢复过程**纯自动且零丢失**。

正如库名所示，当通信双端部署在同一台物理主机上时，TCPSHM 支持无缝切换为**共享内存 (`use_shm = true`)** 模式。该模式对底层提供的 API 和行为表现与 TCP 完全一致（使用 TCP 还是 SHM 对应用层完全透明），但其吞吐量和低延迟表现**比本地环回 TCP 比高出 20 倍以上**。共享内存通信底层核心基于一个实时单生产者单消费者无锁环形队列 (`SPSCVarQueue`)。

用户的业务消息内容在框架层面被视为通用二进制字节流，由业务调用方自行负责编解码。例如，用户为了极致性能可以使用 C/C++ POD 结构体，也可以使用现代 C++ 零拷贝序列化库（如 [iguana](https://github.com/qicosmos/iguana)）或 Google Protocol Buffers + `std::variant` 以获得强大的扩展性。

此外，连接的双方均持有指定唯一名称 (`client_name` 与 `server_name`)，由双方名称对唯一标识一条持久化连接。如果一端断开连接并更改了自身名称后以新名称连接对端，该连接将被系统视为全新的会话，不再从原有的会话队列中恢复未处理消息。这一点在特定业务场景下非常实用：例如，每日高频交易服务器在收盘后停止、开盘前启动，每天启动时将服务器命名改为当天的日期名称（如 `"Server20260716"`），此时它预期与所有客户端建立的是全新会话，昨天收盘遗留的过期报单消息会被自动舍弃（昨天的废单在新的交易日已无意义）。

TCPSHM 采用纯头文件 (Header-only) 设计，提供了服务端与客户端 C++ 模板类 (`TcpShmServer` 和 `TcpShmClient`)。对于服务端，框架原生的多分组轮询机制支持了**连接分片 (Connection Sharding)**：用户预定义一组或多组连接分组，并指派一个或多个线程轮询这些分组。当新连接接入时，用户回调可以自由决定将其指派到哪个分组中，从而拥有了对服务线程与客户端连接映射关系的完全掌控力。

## 技术特性 (Technical Features)
  * **多平台全面适配**：通过清晰干净的跨平台操作系统抽象层 (`os.h`)，打通并桥接了 **Windows** (`WinSock2` / `CreateFileMapping` / `MapViewOfFile`) 与 **Linux** (`POSIX sockets` / `shm_open` / `mmap`)。
  * **现代 C++20 / C++23 构建**：深度应用现代 C++ 内存模型规范（使用 `std::atomic` 配合 `memory_order_acquire/release` 构建无锁环形缓冲区、`std::endian` 编译期字节序转换、`std::span` 内存安全边界校验以及 `std::atomic::wait`）。
  * **纯头文件 (Header-only)**：只需 `#include "tcpshm/..."`，无需编译和链接静态库/动态库。
  * **零外部依赖**：不依赖任何第三方基础库。
  * **极低延迟非阻塞**：除客户端发起的 `Connect()` 和首次 SHM 物理内存探测握手外，数据传输全链路非阻塞。
  * **内部不主动创建任何线程**：轮询循环与线程调度完全交由应用层开发者自由设计。
  * **热路径零系统时钟调用**：数据发送和收取的极速轮询链路不调用任何内核获取时间戳系统函数（由用户轮询循环传入时钟戳）。
  * **无 C++ 异常 (No Exceptions)**：确保嵌入式或硬实时交易系统的稳定与可预测性。
  * **内部不进行任何 IO 打印或写日志**：无屏幕输出或文件写锁开销。
  * **热路径零互斥锁或 CAS 操作**：在核心的 `Alloc()` / `Push()` / `Front()` / `Pop()` 路径中绝不使用有锁 Mutex 或原子 CAS 开销操作。
  * **极其轻量、干净、高效**。

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

## 使用限制 (Limitations)
  * 不提供持久化到本地硬盘的机制，因此无法从掉电重启或系统崩溃中恢复（`.ptcp` 或 `.shm` 中的未消费消息在进程宕机崩溃重启后可完好恢复，但机器掉电或重启后不可恢复）。
  * 由于为追求极致低延迟采用非阻塞忙轮询 (`PollTcp` / `PollShm`)，CPU 占用率在忙轮询循环中较高；如果应用对延迟不追求极端微秒级，可在轮询循环内部插入 `std::this_thread::yield()` 或微秒级 `sleep_for`。
  * 当前仅允许在绑定的轮询接收线程内部对某条连接执行消息读写 (`Alloc/Push` 与 `Front/Pop`)；若需在业务多线程环境中向某连接写消息，必须先将其 push 到一个无锁队列中，再由轮询线程取出并发出。
  * 不支持跨多条消息的事务操作。如果在同一批次中调用多次 `Push()` 或 `Pop()`，若中途进程崩溃，要做好部分成功部分失败的容错准备。
  * 单条消息的最大长度须适合放入 `uint16_t`（包含 8 字节 `MsgHeader` 在内最大 65535 字节）。

## 接口文档与指南 (Documentation)
  [接口文档 (中文)](doc/interface-CN.md) | [Interface Doc (English)](doc/interface.md)

## 代码示例 (Example)
  请参考 [`server.cpp`](server.cpp) 与 [`client.cpp`](client.cpp)，这是一个完整的、基于 C++20/23 改造的现代工程化示例，演示了零拷贝消息申请、[iguana](https://github.com/qicosmos/iguana) 序列化以及 `std::variant` 消息匹配派发。

## 性能测试 (Performance)
示例 `server.cpp` 与 `client.cpp` 同时用作吞吐和延迟性能基准测试：客户端在同一台主机上向服务端持续发送不同大小（16/36/68/200 字节随机选取）的包，并在等待服务端应答回执后发送下一包。平均单趟往返耗时 RTT（Round Trip Time = 总耗时 / 消息处理总数）：

* **TCP** 环回模式 RTT：**~8.8 us**
* **SHM** 共享内存模式 RTT：**~0.34 us**

*(由于内核态网络协议栈绕过与零拷贝 `SPSCVarQueue`，共享内存模式在同一主机上的传输延迟比 TCP 环回快 20 倍以上。)*

## 头文件架构指南 (`include/tcpshm`):

* **`tcpshm_client.h`**：客户端模板类 (`TcpShmClient`)。负责发起连接握手、SHM 物理就绪探测处理及事件驱动轮询。
* **`tcpshm_server.h`**：服务端模板类 (`TcpShmServer`)。支持多线程分组分片、SHM 握手校验、心跳检测与新连接接入管理。
* **`tcpshm_conn.h`**：核心连接通用类 (`TcpShmConnection`)，透明封装了 TCP 队列 (`PTCPConnection`) 或 SHM 队列 (`SPSCVarQueue`)。提供 `Alloc()` / `Push()` 与 `Front()` / `Pop()` 收发接口。
* **`msg_header.h`**：定义 8 字节对齐的 `MsgHeader` (`size`, `msg_type`, `ack_seq`) 及基于 `std::endian` 的字节序自适应转换。
* **`os.h`**：跨平台操作系统底座抽象层（处理 Windows `WinSock2` / `CreateFileMapping` 与 Linux POSIX 套接字 / `shm_open` / `mmap` 的多路适配）。
* **`spsc_varq.h`**：现代 C++20 `std::atomic` 单生产者单消费者无锁可变长消息环形缓冲区队列。
* **`ptcp_conn.h` & `ptcp_queue.h`**：TCP 连接封装、内置通信协议模板 (`LoginMsg`, `LoginRspMsg`, `ShmProbeReqMsg`, `ShmProbeRspMsg`) 以及支持 mmap 内存持久化的 TCP 队列 (`PTCPQueue`)。
* **`endian.h` & `mmap.h`**：底层字节序静态推导转换模板与跨平台文件映射工具包装。
