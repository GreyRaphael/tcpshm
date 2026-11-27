[English](README.md) | [中文](README-CN.md)

**TCPSHM v2 is a modern C++20 connection-oriented persistent message queue framework over SHM IPC and Persistent TCP for Linux & Windows.**

When transmitting messages over raw TCP, disconnects or process crashes can leave application state desynchronized. **TCPSHM** solves this transparently with a sequence-number & ACK persistence mechanism: every outbound TCP message is stored in an mmap-backed `PtcpQueue` (`.ptcp`) until acknowledged by the peer, making crash/reconnect recovery automatic and lossless.

On the same physical host, **shared memory (`TransportKind::Shm`)** switches the data plane onto lock-free `SpscQueue` ring buffers (`200 ns` P50 RTT, **`31.5x–33.0x`** lower median RTT than TCP loopback, up to **`81.1M msg/s`** pipelined echo), while preserving a unified handler model. Before enabling SHM, the server executes a **non-blocking physical SHM probe handshake** to guarantee both endpoints share physical host memory and permissions without risking crashes or stalls.

User payloads are framed by a **16-byte `alignas(16)` `FrameHeader`** (supporting payloads beyond 64 KiB up to `kMaxPayloadSize = 64 MiB - 16` and 16-byte SIMD alignment) and exposed via zero-overhead RAII `WriteGuard` / `ReadGuard` / `MsgView` APIs (`std::span<std::byte>`, `write_pod`, `emplace`, `read_as<T>`). Callers can transfer raw POD structs for zero-copy efficiency or use serialization libraries such as bundled [`iguana`](https://github.com/qicosmos/iguana) with `std::variant`.

## Key Features in v2

* **Pure C++20 Standard Library Core**: Built strictly on C++20 (`cxx_std_20`, also tested cleanly under `-std=c++23`) with zero-allocation, exception-free `Expected<T, Error>` (`Result<T>`), pure C++20 bit-swap endian helpers, `std::span`, `std::atomic_ref`, `std::chrono::nanoseconds`, and compile-time `ConfigConcept` / `HandlerConcept` checks (`-fno-exceptions -Werror` verified).
* **16-Byte Aligned Extensible FrameHeader**: Replaces the old 8-byte 64 KiB cap with `uint32_t size`, `uint16_t msg_type`, `uint16_t flags`, `uint32_t ack_seq`, and `uint32_t reserved` — unlocking >64 KiB frames and native 16B payload alignment.
* **Safe Zero-Overhead RAII API**: Replaces raw pointer arithmetic and manual `Pop()`/`Push()` with `MsgView`, `WriteGuard` (auto-cancels if dropped uncommitted), and `ReadGuard` (auto-pops or `defer()`).
* **Batch Staging & Drain + Instant ACK Piggyback**: `WriteGuard::stage()` + `flush()`, `drain(fn, max_batch)`, and pre-callback `my_ack()` increment coalesce cross-core `atomic_ref` stores and TCP `send()` syscalls while piggybacking fresh ACKs in immediate request-response replies — boosting TCP pipelined throughput up to **20.4x** and SHM echo up to **2.38x**.
* **Statically Separated Channels**: Dedicated [`ShmChannel`](include/tcpshm/shm_channel.hpp) and [`TcpChannel`](include/tcpshm/tcp_channel.hpp) eliminate hot-path `if (use_shm)` branches and remove unused buffers from each transport pool.
* **100% Non-Blocking Control Plane FSM**: Replaces v1's blocking 300ms `VerifyShmProbe` with a fully non-blocking `PendingConn` state machine (`ReadingLogin -> SendingProbeReq -> ReadingProbeRsp -> SendingLoginRsp`), preventing slow or stalled clients from blocking SHM heartbeats or other logins.
* **Generational `ConnHandle`**: 8-byte `{group_id, slot_id, generation, transport}` handle resolved via `server.with_channel(handle, fn)` with built-in ABA protection across disconnect/reconnect.
* **Pluggable TCP Poller Policies**: Defaults to zero-syscall [`BusyPoller`](include/tcpshm/poller.hpp) for HFT tail latency, with compile-time [`EpollPoller`](include/tcpshm/poller.hpp) on Linux for scalable multi-connection TCP workloads.
* **Protocol & Layout Fingerprinting**: Handshake validates `kProtocolMagic`, `kProtocolVersion = 2`, and `config_fingerprint<Config>()` (wire endianness, 16B header size, name size, SHM geometry, user-data sizes), while `PtcpQueue` verifies magic/version/capacity/header_size on reopen.

## Architecture Overview (`include/tcpshm/`)

* [`tcpshm.hpp`](include/tcpshm/tcpshm.hpp): Convenience umbrella header.
* [`config.hpp`](include/tcpshm/config.hpp): `DefaultConfig`, `ConfigConcept`, `TimePoint`, `config_fingerprint<Config>()`.
* [`error.hpp`](include/tcpshm/error.hpp): Zero-allocation `ErrorCode`, `Error`, `Unexpected<E>`, `Expected<T, E>`, `Result<T>`, `VoidResult`.
* [`frame.hpp`](include/tcpshm/frame.hpp): 16-byte `alignas(16)` `FrameHeader`, `align_frame_size()`, `MsgView`, `WriteGuard<Producer>`, `ReadGuard<Consumer>`.
* [`conn_handle.hpp`](include/tcpshm/conn_handle.hpp): 8-byte generational `ConnHandle` and `TransportKind`.
* [`spsc_queue.hpp`](include/tcpshm/spsc_queue.hpp): Implicit-lifetime `SpscRingLayout`, single-cache-line `SpscProducer`/`SpscConsumer`, and in-process `SpscQueue`.
* [`ptcp_queue.hpp`](include/tcpshm/ptcp_queue.hpp): Persistent mmap-backed `PtcpQueue` with magic/version/capacity/header_size validation.
* [`shm_channel.hpp`](include/tcpshm/shm_channel.hpp) & [`tcp_channel.hpp`](include/tcpshm/tcp_channel.hpp): Statically separated transport channels.
* [`poller.hpp`](include/tcpshm/poller.hpp): `BusyPoller` (cross-platform default) and `EpollPoller` (Linux).
* [`server.hpp`](include/tcpshm/server.hpp) & [`client.hpp`](include/tcpshm/client.hpp): Composition-based `Server`, `ShmClient`, `TcpClient`, and runtime-selectable `Client`.
* `detail/`: Platform abstractions ([`os_socket.hpp`](include/tcpshm/detail/os_socket.hpp), [`os_shm.hpp`](include/tcpshm/detail/os_shm.hpp), [`endian.hpp`](include/tcpshm/detail/endian.hpp), [`protocol.hpp`](include/tcpshm/detail/protocol.hpp)).

## Build & CMake Integration

Requires a C++20 compiler (tested with GCC 14.2 under both `-std=c++20` and `-std=c++23` on Linux; Windows WinSock2 support is preserved in `detail/`).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Downstream CMake projects can consume `tcpshm::tcpshm` directly (`find_package(tcpshm CONFIG REQUIRED)` after `cmake --install`, or `add_subdirectory`):

```cmake
target_link_libraries(my_app PRIVATE tcpshm::tcpshm)
```

## Documentation & Examples

* Interface Guide: [English](doc/interface.md) | [中文](doc/interface-CN.md)
* Synchronous Sharded Server & Client: [`examples/common.hpp`](examples/common.hpp), [`examples/echo_server.cpp`](examples/echo_server.cpp), [`examples/echo_client.cpp`](examples/echo_client.cpp)
* Lock-Free 2-Thread Async Server (native `ConnHandle` + `SpscQueue`): [`examples/echo_server_async.cpp`](examples/echo_server_async.cpp)

## Testing & Verification

CTest runs 4 unit/integration suites plus `-fno-exceptions` compile verification and a benchmark smoke check:

* [`tests/unit_spsc_test.cpp`](tests/unit_spsc_test.cpp): Payload bounds (`kMaxPayloadSize`), `alignas(16)` zero-copy `emplace`/`as_ptr`, >64 KiB (`128 KiB`) transfers, wrap-around cancel safety, `BlkSz` 16/32/64, batch `stage()+flush()`, `drain()` backpressure, multi-thread SPSC stress.
* [`tests/unit_ptcp_test.cpp`](tests/unit_ptcp_test.cpp): Persist/reopen recovery (including `128 KiB` large frames), `uint32_t` sequence wraparound, corrupt magic & capacity/`header_size` mismatch detection.
* [`tests/integration_handshake_test.cpp`](tests/integration_handshake_test.cpp): Non-blocking `poll_ctl` under stalled TCP & stalled SHM-probe clients, config mismatch/invalid name/duplicate login/capacity rejection, generational `ConnHandle` ABA protection.
* [`tests/integration_server_client_test.cpp`](tests/integration_server_client_test.cpp): End-to-end SHM, `BusyPoller` TCP, and Linux `EpollPoller` TCP suites including backpressure, >64 KiB (`128 KiB + 13B`) round-trips with immediate ACK piggyback, `flags`/`reserved` metadata preservation, reconnect persistence, and sequence mismatch handling.
* [`tests/noexcept_compile_check.cpp`](tests/noexcept_compile_check.cpp): Enforces `-fno-exceptions -Werror` compilation of all core headers.

## Benchmark Comparison (`v1.0.0` vs `v2-8B/C++23` vs `v2-16B/C++20`)

Measured on the same Linux host (GCC 14.2.0 `-O3 -DNDEBUG`, unpinned `--iters=50000 --warmup=2000 --sizes=16,64,256`, [`benchmarks/tcpshm_bench.cpp`](benchmarks/tcpshm_bench.cpp)):

```bash
./build/tcpshm_bench --mode=all --iters=50000 --warmup=2000 --sizes=16,64,256
# >64 KiB large-frame example:
./build/tcpshm_bench --mode=rtt --iters=2000 --warmup=200 --sizes=131072
```

| Benchmark Case (16B / 64B / 256B) | `v1.0.0` (C++20 / 8B) | `v2` Prev (C++23 / 8B) | `v2` Current (C++20 / 16B) |
| --- | --- | --- | --- |
| **Raw SPSC 1-by-1** | 77.8M / 58.3M / 58.4M msg/s | 79.3M / 72.8M / 64.3M msg/s | **81.7M / 58.5M / 66.9M msg/s** |
| **Raw SPSC Batch-32** | — | 158.8M / 158.9M / 102.6M msg/s | **101.3M / 127.7M / 104.2M msg/s** |
| **SHM Ping-Pong P50 RTT** | **200 ns** (Avg 252 / 266 / 230 ns) | **200 ns** (Avg 199 / 262 / 270 ns) | **200 ns** (Avg 225 / 254 / 243 ns) |
| **TCP Ping-Pong P50 RTT** | 6.3 / 6.2 / 6.3 us | Busy 6.4 / Epoll 6.6–6.8 us | Busy **6.3–6.6 us** / Epoll **6.7–6.8 us** |
| **Pipelined Echo (SHM / SHM-Batch)** | 47.1M / 28.4M / 17.9M msg/s | 89.1M / 63.6M / 33.7M msg/s | **81.1M / 67.4M / 32.8M msg/s** |
| **Pipelined Echo (TCP Busy)** | 278k / 281k / 266k msg/s | 5.69M / 5.13M / 4.32M msg/s | **5.67M / 5.29M / 4.59M msg/s** |
| **Pipelined Echo (TCP Epoll)** | — | 5.42M / 5.02M / 4.09M msg/s | **5.37M / 5.13M / 4.36M msg/s** |
| **128 KiB (`131072 B`) P50 RTT** | *(unsupported >64 KiB)* | *(unsupported >64 KiB)* | **SHM 2.3 us** (`364k msg/s`) / **TCP 49.1–49.6 us** |
