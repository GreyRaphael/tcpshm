[English](interface.md) | [中文](interface-CN.md)

# tcpshm v2 接口与开发指南 (C++20)

`tcpshm` v2 是基于纯 **C++20** 标准库实现的头文件 IPC 与持久化 TCP 消息队列框架，支持 **Linux 与 Windows**。

---

## 1. 帧协议与零开销 RAII API (`frame.hpp`)

### 16 字节 `alignas(16)` 帧头 (`FrameHeader`)
每条消息均携带 16 字节 `alignas(16)` 帧头，支持突破 64 KiB 的大消息载荷（上限 `kMaxPayloadSize = kMaxFrameSize - 16`，默认 `64 MiB - 16`）、载荷天然 16B SIMD 对齐，以及 `flags`/`reserved` 扩展槽位：

```cpp
struct alignas(16) FrameHeader {
    uint32_t size;     // 消息总字节大小（含 16B 帧头；0 为 SPSC 回绕哨兵）
    uint16_t msg_type; // 应用消息类型 ID（0 为底层心跳保留）
    uint16_t flags;    // 可扩展协议/业务标志位（端到端透传）
    uint32_t ack_seq;  // PTCP ACK 序列号，或本地 SPSC 队列元数据
    uint32_t reserved; // 预留扩展槽位/自定义元数据（端到端透传）
};
```

> **`msg_type` 保留值说明**：
> * `0` (`kHeartbeatMsgType`)：TCP 底层心跳保活帧，应用层消息严禁使用 `msg_type == 0`。
> * `1..4` 在 TCP 登录握手期间内部使用；连接建立完成（触发 `on_login_success` / `on_client_logon`）后，业务层可自由复用 `msg_type >= 1`。

### 使用 `WriteGuard` 安全发送
通过 `ch.try_alloc(msg_type, payload_bytes)` 获取 `std::optional<WriteGuard<...>>`：

```cpp
// 单条立即发送：
if (auto tx = ch.try_alloc(tcpshm::kFirstAppMsgType, payload_len)) {
    tx->set_flags(my_flags);
    tx->set_reserved(my_tag);
    tx->write_str(data);       // 也支持 write_pod(pod)、emplace<T>(...)、write_bytes(span)
    tx->commit();              // 立即提交并刷出
}

// 批量流水线发送：
for (int i = 0; i < 16; ++i) {
    if (auto tx = ch.try_alloc(1, sizeof(Order))) {
        tx->emplace<Order>(/* ... */); // 支持高达 16B 对齐的结构体零拷贝构造
        tx->stage();                   // 仅暂存本地批，暂不触发原子 store 或 TCP send()
    }
}
ch.flush();                            // 批末一次性原子发布 (SHM) 或单次 syscall 合并发送 (TCP)
```

若 `WriteGuard` 未调用 `commit()` 或 `stage()` 即离开作用域，析构会自动调用 `cancel()`，杜绝半写帧污染。

### 使用 `MsgView` 安全接收
服务端回调 `on_message(ch, MsgView)` 与客户端回调 `on_server_msg(MsgView)` 直接获得不可变视图 `MsgView`：

* 常用方法：`msg.type()`、`msg.flags()`、`msg.reserved()`、`msg.payload_size()`、`msg.payload()` (`std::span<const std::byte>`)、`msg.payload_str()` (`std::string_view`)、`msg.read_as<T>()`、`msg.as_ptr<T>()`（支持最高 16 字节对齐零拷贝读取）。
* 返回 `void`（或返回 `true`）代表消费完成；在 TCP 通道中，回调前会先递进 `my_ack()`，确保在 `on_message` 内立即回复时可以直接捎带当前请求的 ACK。
* 若回调返回类型为 `bool` 且返回 `false`，代表**背压拒绝消费**：回滚未消费 ACK 并停止推进读取游标，留待下一次轮询重投递。

---

## 2. 编译期配置与 `ConfigConcept` (`config.hpp`)

用户配置只需继承 `tcpshm::DefaultConfig` 并覆盖所需静态常量，框架通过 `ConfigConcept` 在编译期拦截非法配置（如非 2 的幂或未按 16 字节对齐）：

```cpp
struct MyConfig : tcpshm::DefaultConfig {
    static constexpr uint32_t name_size = 16;
    static constexpr uint32_t shm_queue_bytes = 1u << 20;      // shm_queue_bytes / shm_block_bytes 须为 2 的幂
    static constexpr uint32_t shm_block_bytes = 64;            // 须为 16 的 2 次幂倍数
    static constexpr uint32_t tcp_queue_bytes = 1u << 20;      // 须为 16 的倍数
    static constexpr uint32_t tcp_recv_init_bytes = 4096;      // 须为 16 的倍数
    static constexpr uint32_t tcp_recv_max_bytes = 1u << 20;   // 须为 16 的倍数且 <= kMaxFrameSize
    static constexpr uint32_t max_shm_groups = 1;
    static constexpr uint32_t max_shm_conns_per_group = 4;
    static constexpr uint32_t max_tcp_groups = 1;
    static constexpr uint32_t max_tcp_conns_per_group = 16;
    static constexpr uint32_t max_pending_conns = 16;
    static constexpr uint32_t poll_batch_size = 64;
    static constexpr std::endian wire_endian = std::endian::little;
    static constexpr std::chrono::nanoseconds handshake_timeout = std::chrono::seconds(3);
    static constexpr std::chrono::nanoseconds connection_timeout = std::chrono::seconds(10);
    static constexpr std::chrono::nanoseconds heartbeat_interval = std::chrono::seconds(3);

    using LoginUserData = tcpshm::EmptyUserData;
    using LoginRspUserData = tcpshm::EmptyUserData;
    using ConnectionUserData = tcpshm::EmptyUserData;
};
```

握手阶段会自动校对 `config_fingerprint<Config>()`（涵盖字节序、16B 帧头尺寸、`name_size`、SHM 布局参数及登录附加数据结构大小），防止服务端与客户端配置错配导致静默数据损坏。

---

## 3. 错误处理 (`error.hpp`)

核心路径全程无 C++ 异常、零堆分配，全部结果通过 `tcpshm::Result<T>`（纯 C++20 `tcpshm::Expected<T, tcpshm::Error>`）与 `tcpshm::VoidResult` 返回，可通过 `err.code`、`err.sys_errno` 与 `err.message()` 精准诊断错误原因。

---

## 4. 独立通道与代际句柄 `ConnHandle`

* `tcpshm::ShmChannel<Config>` 与 `tcpshm::TcpChannel<Config>` 静态分离，内部不包含对方多余成员。
* 每条上线连接自动分配 8 字节代际句柄 `tcpshm::ConnHandle`（包含 `group_id`、`slot_id`、31 位非零 `generation` 及 `TransportKind`）。
* 在控制线程/单轮询架构中，调用 `server.with_channel(handle, fn)` 可完成 O(1) 槽位定位与代际比对，断线重连槽位复用时旧句柄自动失效（防止 ABA 误发）。

---

## 5. 服务端 (`server.hpp`) 与双 TCP Poller (`poller.hpp`)

`Server<Handler, Config, TcpPoller>` 采用组合模式构造：`Server server(handler, server_name, ptcp_dir)`。

### Handler 回调接口
仅 `on_message` 为必需，其余控制面与错误回调按需实现（框架通过 `if constexpr (requires ...)` 自动识别）：

```cpp
struct MyHandler {
    void on_message(auto& ch, tcpshm::MsgView msg); // 或返回 bool 支持背压

    // 以下回调按需实现（全部可选）：
    int on_new_connection(const struct sockaddr_in& addr,
                          const tcpshm::detail::LoginMsg<MyConfig>& login,
                          tcpshm::detail::LoginRspMsg<MyConfig>& rsp); // 返回目标 grpid 或 -1 拒绝
    void on_client_logon(const struct sockaddr_in& addr, auto& ch);
    void on_client_disconnected(auto& ch, tcpshm::Error reason);
    void on_client_file_error(auto& ch, tcpshm::Error err);
    void on_seq_mismatch(auto& ch, const tcpshm::SeqMismatchInfo& info);
    void on_system_error(tcpshm::Error err);
};
```

### 轮询策略与线程模型
* **TCP 轮询策略 (`TcpPoller`)**：
  * `tcpshm::BusyPoller`（默认）：跨平台零 syscall 空闲扫描，适合极少 TCP 连接追求极限低延迟的场景。
  * `tcpshm::EpollPoller`（Linux 可选）：基于 `epoll_wait(..., 0)` 仅轮询就绪 socket，自动刷新未读完的 `recvbuf_` 缓冲与定期空闲心跳，避免大量 TCP 空连接产生 `O(N)` `recv()` 系统调用。
* **控制面 `poll_ctl(steady_now())`**：**0ms 全非阻塞**状态机驱动 `accept`、登录、`ShmProbe` 探测、登录回包、SHM 心跳与退槽清理。
* **数据面**：`poll_shm(grpid)` 与 `poll_tcp(steady_now(), grpid)` 可同线程依次轮询（参考 [`examples/echo_server_async.cpp`](../examples/echo_server_async.cpp)），也可拆分到各组独立绑核线程并行轮询（参考 [`examples/echo_server.cpp`](../examples/echo_server.cpp)）。

---

## 6. 客户端 (`client.hpp`)

* `tcpshm::ShmClient<Handler, Config>`：纯 SHM 客户端（`connect(ip, port)` + `poll_shm()` + `poll_keepalive()`），零 PTCP 冗余开销。
* `tcpshm::TcpClient<Handler, Config>`：纯持久化 TCP 客户端（`connect(ip, port)` + `poll_tcp()`），零 SHM 冗余开销。
* `tcpshm::Client<Handler, Config>`：运行时可选择模式（`connect(TransportKind::Shm | Tcp, ip, port)`），提供 `with_channel(fn)` 静态分发发送（示例参见 [`examples/echo_client.cpp`](../examples/echo_client.cpp)）。

客户端 `Handler` 仅需实现 `on_server_msg(tcpshm::MsgView)`，其余登录/断线/序列号错配回调均为可选。
