[English](README.md) | [中文](README-CN.md)

**TCPSHM v2 is a modern C++23 connection-oriented persistent message queue framework over SHM IPC and Persistent TCP for Linux & Windows.**

When transmitting messages over raw TCP, disconnects or process crashes can leave application state desynchronized. **TCPSHM** solves this transparently with a sequence-number & ACK persistence mechanism: every outbound TCP message is stored in an mmap-backed `PtcpQueue` (`.ptcp`) until acknowledged by the peer, making crash/reconnect recovery automatic and lossless.

On the same physical host, **shared memory (`TransportKind::Shm`)** switches the data plane onto lock-free `SpscQueue` ring buffers (`200 ns` P50 RTT, **`33x`** lower median RTT than TCP loopback, up to **`90.7M msg/s`** pipelined echo), while preserving a unified handler model. Before enabling SHM, the server executes a **non-blocking physical SHM probe handshake** to guarantee both endpoints share physical host memory and permissions without risking crashes or stalls.

User payloads are framed by an **8-byte `FrameHeader`** (max payload `65527` bytes) and exposed via zero-overhead RAII `WriteGuard` / `ReadGuard` / `MsgView` APIs (`std::span<std::byte>`, `write_pod`, `emplace`, `read_as<T>`). Callers can transfer raw POD structs for zero-copy efficiency or use serialization libraries such as bundled [`iguana`](https://github.com/qicosmos/iguana) with `std::variant`.

## What's New in v2.0.0

* **Modern C++23 Core**: Uses `std::expected` (`Result<T, Error>`), `std::byteswap`, `std::endian`, `std::span`, `std::atomic_ref`, `std::chrono::nanoseconds`, and compile-time `ConfigConcept` / `HandlerConcept` checks — zero C++ exceptions in core headers (`-fno-exceptions` verified).
* **Safe Zero-Overhead RAII API**: Replaces raw `MsgHeader*` pointer arithmetic and manual `Pop()`/`Push()` with `MsgView`, `WriteGuard` (auto-cancels if dropped uncommitted), and `ReadGuard` (auto-pops or `defer()`).
* **Batch Staging & Drain**: `WriteGuard::stage()` + `flush()` and `drain(fn, max_batch)` coalesce cross-core `atomic_ref` index stores and TCP `send()` syscalls — boosting TCP pipelined throughput up to **20.5x** and SHM throughput up to **2.8x**.
* **Statically Separated Channels**: Dedicated [`ShmChannel`](include/tcpshm/shm_channel.hpp) and [`TcpChannel`](include/tcpshm/tcp_channel.hpp) eliminate hot-path `if (use_shm)` branches and remove unused buffers from each transport pool.
* **100% Non-Blocking Control Plane FSM**: Replaces v1's blocking 300ms `VerifyShmProbe` with a fully non-blocking `PendingConn` state machine (`ReadingLogin -> SendingProbeReq -> ReadingProbeRsp -> SendingLoginRsp`), preventing slow or stalled clients from blocking SHM heartbeats or other logins.
* **Generational `ConnHandle`**: 8-byte `{group_id, slot_id, generation, transport}` handle resolved via `server.with_channel(handle, fn)` with built-in ABA protection across disconnect/reconnect.
* **Pluggable TCP Poller Policies**: Defaults to zero-syscall [`BusyPoller`](include/tcpshm/poller.hpp) for HFT tail latency, with compile-time [`EpollPoller`](include/tcpshm/poller.hpp) on Linux for scalable multi-connection TCP workloads.
* **Protocol & Layout Fingerprinting**: Handshake validates `kProtocolMagic`, `kProtocolVersion = 2`, and `config_fingerprint<Config>()` (wire endianness, name size, SHM geometry, user-data sizes), while `PtcpQueue` verifies magic/version/capacity on reopen.

## Architecture Overview (`include/tcpshm/`)

* [`tcpshm.hpp`](include/tcpshm/tcpshm.hpp): Convenience umbrella header.
* [`config.hpp`](include/tcpshm/config.hpp): `DefaultConfig`, `ConfigConcept`, `TimePoint`, `config_fingerprint<Config>()`.
* [`error.hpp`](include/tcpshm/error.hpp): Zero-allocation `ErrorCode`, `Error`, `Result<T>`, `VoidResult`.
* [`frame.hpp`](include/tcpshm/frame.hpp): 8-byte `FrameHeader`, `MsgView`, `WriteGuard<Producer>`, `ReadGuard<Consumer>`.
* [`conn_handle.hpp`](include/tcpshm/conn_handle.hpp): 8-byte generational `ConnHandle` and `TransportKind`.
* [`spsc_queue.hpp`](include/tcpshm/spsc_queue.hpp): Implicit-lifetime `SpscRingLayout`, single-cache-line `SpscProducer`/`SpscConsumer`, and in-process `SpscQueue`.
* [`ptcp_queue.hpp`](include/tcpshm/ptcp_queue.hpp): Persistent mmap-backed `PtcpQueue` with magic/version/capacity validation.
* [`shm_channel.hpp`](include/tcpshm/shm_channel.hpp) & [`tcp_channel.hpp`](include/tcpshm/tcp_channel.hpp): Statically separated transport channels.
* [`poller.hpp`](include/tcpshm/poller.hpp): `BusyPoller` (cross-platform default) and `EpollPoller` (Linux).
* [`server.hpp`](include/tcpshm/server.hpp) & [`client.hpp`](include/tcpshm/client.hpp): Composition-based `Server`, `ShmClient`, `TcpClient`, and runtime-selectable `Client`.
* `detail/`: Platform abstractions ([`os_socket.hpp`](include/tcpshm/detail/os_socket.hpp), [`os_shm.hpp`](include/tcpshm/detail/os_shm.hpp), [`endian.hpp`](include/tcpshm/detail/endian.hpp), [`protocol.hpp`](include/tcpshm/detail/protocol.hpp)).

## Build & CMake Integration

Requires a C++23 compiler (tested with GCC 14.2 on Linux; cross-platform Windows WinSock2 support is preserved in `detail/`).

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

* [`tests/unit_spsc_test.cpp`](tests/unit_spsc_test.cpp): Payload bounds, RAII `WriteGuard`/`ReadGuard`, wrap-around cancel safety, `BlkSz` 16/32/64, batch `stage()+flush()`, `drain()` backpressure, multi-thread SPSC stress.
* [`tests/unit_ptcp_test.cpp`](tests/unit_ptcp_test.cpp): Persist/reopen recovery, `uint32_t` sequence wraparound, corrupt magic & capacity mismatch detection.
* [`tests/integration_handshake_test.cpp`](tests/integration_handshake_test.cpp): Non-blocking `poll_ctl` under stalled TCP & stalled SHM-probe clients, config mismatch/invalid name/duplicate login/capacity rejection, generational `ConnHandle` ABA protection.
* [`tests/integration_server_client_test.cpp`](tests/integration_server_client_test.cpp): End-to-end SHM, `BusyPoller` TCP, and Linux `EpollPoller` TCP suites including backpressure, reconnect persistence, and sequence mismatch handling.
* [`tests/noexcept_compile_check.cpp`](tests/noexcept_compile_check.cpp): Enforces `-fno-exceptions -Werror` compilation of all core headers.

## Benchmark (`v1.0.0` vs `v2.0.0`)

Measured on the same Linux host (GCC 14.2.0 `-O3 -DNDEBUG`, unpinned `--iters=50000 --warmup=2000 --sizes=16,64,256`, [`benchmarks/tcpshm_bench.cpp`](benchmarks/tcpshm_bench.cpp)):

```bash
./build/tcpshm_bench --mode=all --iters=50000 --warmup=2000 --sizes=16,64,256
# Optional core pinning: --pin-server=2 --pin-client=3
```

| Benchmark Case | v1.0.0 (16B / 64B / 256B) | v2.0.0 (16B / 64B / 256B) | Speedup |
| --- | --- | --- | --- |
| **Raw SPSC 1-by-1** | 68.1M / 68.7M / 62.8M msg/s | 67.4M / 54.4M / 62.4M msg/s | ~1.0x |
| **Raw SPSC Batch-32** | — | **113.3M / 147.0M / 100.1M msg/s** | **1.59x – 2.14x** |
| **SHM Ping-Pong P50 RTT** | 200 ns (Avg 240 / 256 / 233 ns) | **200 ns** (Avg 228 / 260 / 234 ns) | **33.0x** vs TCP P50 |
| **TCP Ping-Pong P50 / P99 RTT** | 6.2 us / 16.3 us | Busy 6.6 / 16.3 us, Epoll 6.9 / **13.7–15.7 us** | Epoll P99 improved |
| **Pipelined Echo (SHM)** | 39.0M / 29.6M / 11.3M msg/s | **90.7M / 69.2M / 31.9M msg/s** (`SHM-Batch`) | **2.32x – 2.82x** |
| **Pipelined Echo (TCP)** | 273k / 270k / 268k msg/s | **5.59M / 4.85M / 4.41M msg/s** (`TCP-Busy`) | **16.5x – 20.5x** |
| **Pipelined Echo (TCP Epoll)** | — | **5.24M / 4.64M / 4.34M msg/s** (`TCP-Epoll`) | **16.2x – 19.2x** |
