[English](interface.md) | [中文](interface-CN.md)

# tcpshm v2 Interface Documentation (C++23)

`tcpshm` v2 is a header-only C++23 IPC & persistent TCP messaging library for Linux and Windows.

---

## 1. Frame Protocol & Zero-Overhead RAII API (`frame.hpp`)

### 8-Byte `FrameHeader`
Every message uses a compact 8-byte `alignas(8)` header, capping total frame length at `65535` bytes (maximum payload `kMaxPayloadSize = 65527` bytes):

```cpp
struct alignas(8) FrameHeader {
    uint16_t size;     // Total frame size in bytes including 8B header
    uint16_t msg_type; // Application message ID (0 is reserved for internal heartbeat)
    uint32_t ack_seq;  // PTCP peer ACK sequence or local SPSC metadata
};
```

> **Reserved `msg_type` values**:
> * `0` (`kHeartbeatMsgType`): Internal TCP heartbeat frame. Application frames must use `msg_type != 0`.
> * `1..4` during TCP handshake: Used internally for `LoginMsg` / `LoginRspMsg` / `ShmProbeReqMsg` / `ShmProbeRspMsg`. Once connected, applications may freely use `msg_type >= 1`.

### Sending with `WriteGuard`
Instead of raw pointer arithmetic, `try_alloc()` returns `std::optional<WriteGuard<...>>`:

```cpp
// Immediate commit + send/publish:
if (auto tx = ch.try_alloc(tcpshm::kFirstAppMsgType, payload_len)) {
    tx->write_str(data);       // or write_pod(pod), emplace<T>(...), write_bytes(span)
    tx->commit();
}

// Pipelined batch send:
for (int i = 0; i < 16; ++i) {
    if (auto tx = ch.try_alloc(1, sizeof(Order))) {
        tx->emplace<Order>(/* ... */);
        tx->stage();           // Stays local until flush()
    }
}
ch.flush();                    // Single atomic release store (SHM) or batched tcp_send (TCP)
```

If a `WriteGuard` leaves scope without `.commit()` or `.stage()`, its destructor automatically calls `.cancel()`, preventing partial-write stalls.

### Receiving with `MsgView`
Server (`on_message(ch, MsgView)`) and client (`on_server_msg(MsgView)`) callbacks receive an immutable `MsgView`:

* `msg.type()`, `msg.payload_size()`, `msg.payload()` (`std::span<const std::byte>`), `msg.payload_str()` (`std::string_view`), `msg.read_as<T>()` (`std::optional<T>`).
* Returning `void` (or `true`) automatically consumes the frame (`drain` commits the entire batch's `read_idx` with a single `release` store at batch end).
* Returning `bool` (`false`) triggers **backpressure**: consumption stops without advancing past that frame, so the next poll re-delivers it.

---

## 2. Configuration (`config.hpp`)

Custom configs can inherit from `tcpshm::DefaultConfig` and are statically checked by `tcpshm::ConfigConcept`:

```cpp
struct MyConfig : tcpshm::DefaultConfig {
    static constexpr uint32_t name_size = 16;
    static constexpr uint32_t shm_queue_bytes = 1u << 20;      // blk_cnt = shm_queue_bytes / shm_block_bytes must be power of 2
    static constexpr uint32_t shm_block_bytes = 64;            // power of 2 multiple of 8
    static constexpr uint32_t tcp_queue_bytes = 256u << 10;    // multiple of 8
    static constexpr uint32_t tcp_recv_init_bytes = 4096;
    static constexpr uint32_t tcp_recv_max_bytes = 64u << 10;
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

Wait-free handshakes exchange `config_fingerprint<Config>()` (hashing endianness, header size, name size, SHM layout geometry, and login user-data sizes) to reject incompatible clients before opening shared memory.

---

## 3. Error Handling (`error.hpp`)

All setup, connect, and I/O errors use zero-allocation `tcpshm::Result<T>` (`std::expected<T, tcpshm::Error>`) and `tcpshm::VoidResult`. Inspect `err.code` (`ErrorCode`), `err.sys_errno`, and `err.message()`. No exceptions are thrown.

---

## 4. Channels & Generational `ConnHandle`

* `tcpshm::ShmChannel<Config>` and `tcpshm::TcpChannel<Config>` are statically separate classes (`user_data`, `try_alloc`, `flush`, `close`, `is_closed`, `close_error`, `remote_name`, `handle`).
* Every accepted connection receives a 64-bit-packable `tcpshm::ConnHandle` (`group_id`, `slot_id`, 31-bit non-zero `generation`, `TransportKind`).
* On the control-plane or worker thread, `server.with_channel(handle, fn)` resolves `handle` in O(1) while verifying slot liveliness and matching generation — safely ignoring stale handles after disconnects or reconnects.

---

## 5. Server (`server.hpp`) & Poller Policies (`poller.hpp`)

`Server<Handler, Config, TcpPoller>` uses composition (`Server server(handler, name, ptcp_dir)`):

### Handler Callbacks
Only `on_message` is required; all lifecycle/error callbacks are optional (detected via `if constexpr (requires ...)`):

```cpp
struct MyHandler {
    void on_message(auto& ch, tcpshm::MsgView msg); // or bool on_message(...) for backpressure

    // Optional callbacks:
    int on_new_connection(const struct sockaddr_in& addr,
                          const tcpshm::detail::LoginMsg<MyConfig>& login,
                          tcpshm::detail::LoginRspMsg<MyConfig>& rsp); // return group_id or -1 to reject
    void on_client_logon(const struct sockaddr_in& addr, auto& ch);
    void on_client_disconnected(auto& ch, tcpshm::Error reason);
    void on_client_file_error(auto& ch, tcpshm::Error err);
    void on_seq_mismatch(auto& ch, const tcpshm::SeqMismatchInfo& info);
    void on_system_error(tcpshm::Error err);
};
```

### Polling Model & `TcpPoller`
* **TCP Poller Policy**:
  * `tcpshm::BusyPoller` (default): Zero syscalls on idle checks; minimum 1-in-flight RTT tail latency.
  * `tcpshm::EpollPoller` (Linux): Event-driven `epoll_wait(..., 0)` avoiding `O(N)` idle `recv()` syscalls when serving many TCP sockets, while automatically draining buffered frames and running periodic idle heartbeats.
* **Control Plane (`poll_ctl(steady_now())`)**: Drives non-blocking `accept`, `LoginMsg`, asynchronous `ShmProbe` verification, `LoginRsp`, SHM keepalive sockets, and disconnect cleanup with **0ms blocking**.
* **Data Plane**: Call `poll_shm(grpid)` and `poll_tcp(steady_now(), grpid)` from the same thread (simple loop) or dedicated sharded worker threads (low-latency isolation).

---

## 6. Clients (`client.hpp`)

Choose the client specialization that fits your deployment:

* `tcpshm::ShmClient<Handler, Config>`: Pure SHM (`connect(ip, port)` -> `poll_shm()` + `poll_keepalive()` + `channel()`). Zero PTCP buffers allocated.
* `tcpshm::TcpClient<Handler, Config>`: Pure Persistent TCP (`connect(ip, port)` -> `poll_tcp()` + `channel()`). Zero SHM state allocated.
* `tcpshm::Client<Handler, Config>`: Runtime-selectable (`connect(TransportKind::Shm | Tcp, ip, port)` -> `poll_shm()`, `poll_tcp()`, `with_channel(fn)`).

Client handlers only need `void on_server_msg(tcpshm::MsgView)` (or `bool` for backpressure); `on_login_success`, `on_login_reject`, `on_seq_mismatch`, `on_disconnected`, and `on_system_error` are optional.
