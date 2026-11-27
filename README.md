[English](README.md) | [中文](README-CN.md)

**TCPSHM v2 is a modern C++20 connection-oriented persistent message queue framework over SHM IPC and Persistent TCP for Linux & Windows.**

When transmitting messages over raw TCP, disconnects or process crashes can leave application state desynchronized. **TCPSHM** solves this transparently with a sequence-number & ACK persistence mechanism: every outbound TCP message is stored in an mmap-backed `PtcpQueue` (`.ptcp`) until acknowledged by the peer, making crash/reconnect recovery automatic and lossless.

On the same physical host, **shared memory (`TransportKind::Shm`)** switches the data plane onto lock-free `SpscQueue` ring buffers (`200 ns` P50 RTT / `194–226 ns` Avg, **`31.5x–32.5x`** lower median RTT than TCP loopback, up to **`92.6M msg/s`** pipelined echo), while preserving a unified handler model. Before enabling SHM, the server executes a **non-blocking physical SHM probe handshake** to guarantee both endpoints share physical host memory and permissions without risking crashes or stalls.

User payloads are framed by a **16-byte `alignas(16)` `FrameHeader`** (supporting payloads beyond 64 KiB up to `kMaxPayloadSize = 64 MiB - 16` and 16-byte SIMD alignment) and exposed via zero-overhead RAII `WriteGuard` / `ReadGuard` / `MsgView` APIs (`std::span<std::byte>`, `write_pod`, `emplace`, `read_as<T>`). Callers can transfer raw POD structs for zero-copy efficiency or use serialization libraries such as bundled [`iguana`](https://github.com/qicosmos/iguana) with `std::variant`.

## Key Features in v2

* **Pure C++20 Standard Library Core**: Built strictly on C++20 (`cxx_std_20`, also tested cleanly under `-std=c++23`) with zero-allocation, exception-free `Expected<T, Error>` (`Result<T>`), pure C++20 bit-swap endian helpers, `std::span`, `std::atomic_ref`, `std::chrono::nanoseconds`, and compile-time `ConfigConcept` / `HandlerConcept` checks (`-fno-exceptions -Werror` verified).
* **16-Byte Aligned Extensible FrameHeader**: Replaces the old 8-byte 64 KiB cap with `uint32_t size`, `uint16_t msg_type`, `uint16_t flags`, `uint32_t ack_seq`, and `uint32_t reserved` — unlocking >64 KiB frames and native 16B payload alignment.
* **Safe Zero-Overhead RAII API**: Replaces raw pointer arithmetic and manual `Pop()`/`Push()` with `MsgView`, `WriteGuard` (auto-cancels if dropped uncommitted), and `ReadGuard` (auto-pops or `defer()`).
* **Batch Staging & Drain + Instant ACK Piggyback**: `WriteGuard::stage()` + `flush()`, `drain(fn, max_batch)`, and pre-callback `my_ack()` increment coalesce cross-core `atomic_ref` stores and TCP `send()` syscalls while piggybacking fresh ACKs in immediate request-response replies — reaching **`5.74M msg/s`** on TCP pipelined echo (up ~20x over v1) and **`92.6M msg/s`** on SHM batch echo.
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

## Build & Integration

### Requirements

* **C++20 compiler**: GCC >= 11 / Clang >= 14 / MSVC 2022+ (Linux & Windows; note `tcpshm_bench` uses C++20 `<format>`, requiring GCC >= 13 / Clang >= 17 / MSVC 2022+ when `TCPSHM_BUILD_BENCHMARKS=ON`)
* **CMake >= 3.16**

### Local Build & Test

```bash
git clone https://github.com/GreyRaphael/tcpshm.git
cd tcpshm
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Common CMake options:

| Option | Default | Description |
| :--- | :---: | :--- |
| `TCPSHM_BUILD_TESTS` | `ON` (top-level) | Build the 4 CTest unit and integration suites in `tests/` |
| `TCPSHM_BUILD_EXAMPLES` | `ON` (top-level) | Build the 3 usage examples in `examples/` |
| `TCPSHM_BUILD_BENCHMARKS` | `ON` (top-level) | Build `tcpshm_bench` in `benchmarks/` |

### CMake `add_subdirectory` / `FetchContent` Integration

Option 1 — `add_subdirectory`:

```cmake
add_subdirectory(tcpshm)
target_link_libraries(your_app PRIVATE tcpshm::tcpshm)
```

Option 2 — `FetchContent`:

```cmake
include(FetchContent)
FetchContent_Declare(
  tcpshm
  GIT_REPOSITORY https://github.com/GreyRaphael/tcpshm.git
  GIT_TAG v2.1.0
)
FetchContent_MakeAvailable(tcpshm)
target_link_libraries(your_app PRIVATE tcpshm::tcpshm)
```

When included as a subproject, `TCPSHM_BUILD_TESTS`, `TCPSHM_BUILD_EXAMPLES`, and `TCPSHM_BUILD_BENCHMARKS` automatically default to `OFF`. Linking `tcpshm::tcpshm` automatically configures `cxx_std_20`, include directories, `Threads::Threads`, `rt` (Linux), and `ws2_32` / `/utf-8` (Windows/MSVC).

### Direct Compilation without CMake

No CMake setup is required — add `-Iinclude` and enable C++20:

```bash
# Linux
g++ -std=c++20 -O3 -Iinclude your_app.cpp -pthread -lrt -o your_app
```

```powershell
# Windows (MSVC Developer Command Prompt)
cl /std:c++20 /O2 /utf-8 /Iinclude your_app.cpp ws2_32.lib
```

### Copying Headers into a Third-Party Project (`third_party/`)

To vendor `tcpshm` directly into your project's `third_party/` directory without git submodules or `FetchContent` (you can also extract `tcpshm-vX.Y.Z-headers.tar.gz` / `.zip` from [GitHub Releases](https://github.com/GreyRaphael/tcpshm/releases)), use the following layout:

```text
your_project/
├── include/
├── src/
│   └── main.cpp
└── third_party/
    └── tcpshm/             # copied from include/tcpshm/ (or Release bundle)
        ├── detail/
        ├── client.hpp
        ├── server.hpp
        └── tcpshm.hpp ...
```

**3 important notes**:
1. **The include search path must include `third_party`** (because internal headers use `#include "tcpshm/..."`):
   * **CMake**:
     ```cmake
     find_package(Threads REQUIRED)
     target_include_directories(your_app PRIVATE ${CMAKE_SOURCE_DIR}/third_party)
     target_compile_features(your_app PRIVATE cxx_std_20)
     target_link_libraries(your_app PRIVATE
       Threads::Threads
       $<$<PLATFORM_ID:Linux>:rt>
       $<$<PLATFORM_ID:Windows>:ws2_32>
     )
     ```
   * **Direct CLI compilation**:
     ```bash
     g++ -std=c++20 -O3 -Iinclude -Ithird_party src/main.cpp -pthread -lrt -o your_app
     ```
2. **Include `#include "tcpshm/tcpshm.hpp"` directly** in your application code: do NOT write `#include "third_party/tcpshm/tcpshm.hpp"` without `-Ithird_party`, or internal header includes will fail to resolve.
3. **Zero third-party dependencies for the core library**: `include/tcpshm/` relies solely on the C++20 standard library and OS runtime libraries (`-pthread -lrt` on Linux; `ws2_32` on Windows, auto-linked via `#pragma comment(lib, "ws2_32.lib")` on MSVC). Note that `third_party/iguana` and `third_party/frozen` in this repository are only used by `examples/` and do not need to be copied.

## Documentation & Examples

* Interface Guide: [English](doc/interface.md) | [中文](doc/interface-CN.md)
* Synchronous Sharded Server & Client: [`examples/common.hpp`](examples/common.hpp), [`examples/echo_server.cpp`](examples/echo_server.cpp), [`examples/echo_client.cpp`](examples/echo_client.cpp)
* Lock-Free 2-Thread Async Server (native `ConnHandle` + `SpscQueue`): [`examples/echo_server_async.cpp`](examples/echo_server_async.cpp)

## Testing & Verification

CTest runs 4 unit and integration suites (`-fno-exceptions -Werror` strict compilation and benchmark smoke runs are enforced in GitHub Actions CI):

* [`tests/unit_spsc_test.cpp`](tests/unit_spsc_test.cpp): Payload bounds (`kMaxPayloadSize`), `alignas(16)` zero-copy `emplace`/`as_ptr`, >64 KiB (`128 KiB`) transfers, wrap-around cancel safety, `BlkSz` 16/32/64, batch `stage()+flush()`, `drain()` backpressure, multi-thread SPSC stress.
* [`tests/unit_ptcp_test.cpp`](tests/unit_ptcp_test.cpp): Persist/reopen recovery (including `128 KiB` large frames), `uint32_t` sequence wraparound, corrupt magic & capacity/`header_size` mismatch detection.
* [`tests/integration_handshake_test.cpp`](tests/integration_handshake_test.cpp): Non-blocking `poll_ctl` under stalled TCP & stalled SHM-probe clients, config mismatch/invalid name/duplicate login/capacity rejection, generational `ConnHandle` ABA protection.
* [`tests/integration_server_client_test.cpp`](tests/integration_server_client_test.cpp): End-to-end SHM, `BusyPoller` TCP, and Linux `EpollPoller` TCP suites including backpressure, >64 KiB (`128 KiB + 13B`) round-trips with immediate ACK piggyback, `flags`/`reserved` metadata preservation, reconnect persistence, and sequence mismatch handling.

## Benchmark Comparison (`v1.0.0` vs `v2-8B/C++23` vs `v2-16B/C++20`)

Measured on the same Linux host (AMD Ryzen 7 255, GCC 14.2.0 `-O3 -DNDEBUG`) with threads pinned to distinct physical cores (`--pin-server=0`, `--pin-client=2`, `--pin-recv=4`), [`benchmarks/tcpshm_bench.cpp`](benchmarks/tcpshm_bench.cpp):

* `--pin-server=<core>`: pins the server worker thread (`worker_thr`).
* `--pin-client=<core>`: pins the client sender / RTT thread / TCP poller (`client_thr`, `send_thr`, `tcp_cli_thr`).
* `--pin-recv=<core>`: pins the dedicated SHM pipelined-throughput receiver thread (`recv_thr`).

```bash
./build/benchmarks/tcpshm_bench --mode=spsc       --iters=3000000 --warmup=100000 --sizes=16,48,56,64,256 --pin-server=0 --pin-client=2
./build/benchmarks/tcpshm_bench --mode=rtt        --iters=50000   --warmup=5000   --sizes=16,64,256       --pin-server=0 --pin-client=2
./build/benchmarks/tcpshm_bench --mode=throughput --iters=400000  --warmup=30000  --sizes=16,48,56,64,256 --pin-server=0 --pin-client=2 --pin-recv=4
# >64 KiB large-frame RTT example (unpinned):
./build/benchmarks/tcpshm_bench --mode=rtt        --iters=2000    --warmup=200    --sizes=131072
```

### Pinned Cross-Version Comparison (`16B / 64B / 256B`)

| Benchmark Case | `v1.0.0` (C++20 / 8B) | `v2` Prev (C++23 / 8B) | `v2` Current (C++20 / 16B) |
| --- | --- | --- | --- |
| **Raw SPSC 1-by-1** (`iters=3M`) | 63.6M / 62.7M / 61.9M msg/s | 64.5M / 63.0M / 62.8M msg/s | **65.9M / 64.5M / 65.0M msg/s** |
| **Raw SPSC Batch-32** (`iters=3M`) | — | 123.2M / 158.1M / 102.0M msg/s | **121.9M / 164.5M / 104.4M msg/s** (`25.49 GB/s`) |
| **SHM Ping-Pong RTT** (`iters=50k`) | P50 **200 ns** (Avg 226 / 249 / 232, P99 500–600) | P50 **200 ns** (Avg 218 / 280 / 246, P99 600) | P50 **200 ns** (Avg **194 / 226 / 221**, P99 **400–500**, up to **5.14M msg/s**) |
| **TCP Ping-Pong P50 RTT** (`iters=50k`) | 6.2 / 6.4 / 6.3 us | Busy 6.5 / 6.4 / 6.5 us \| Epoll 6.7 / 6.9 / 7.0 us | Busy **6.5 / 6.4 / 6.3 us** \| Epoll **6.7 / 6.9 / 7.0 us** |
| **Pipelined SHM Echo 1-by-1** *(3-core driver, `iters=400k`)* | 27.4M / 27.2M / 19.2M msg/s | 33.8M / 22.9M / 22.5M msg/s | **43.7M / 31.2M / 18.6M msg/s** |
| **Pipelined SHM Echo Batch-32** *(3-core driver, `iters=400k`)* | — | 79.4M / 68.7M / 31.9M msg/s | **78.5M / 55.8M / 32.9M msg/s** |
| **128 KiB (`131072 B`) P50 RTT** *(unpinned, `iters=2k`)* | *(unsupported >64 KiB)* | *(unsupported >64 KiB)* | **SHM 2.3 us** (`364k msg/s`) / **TCP 49.1–49.6 us** |

### Current `v2` Full `tcpshm_bench --mode=throughput` (`pin=0,2,4`, `iters=400000`)

| Payload Size | `TCP-Busy` *(v1 TCP: ~266k–287k)* | `TCP-Epoll` | `SHM-1by1` | `SHM-Batch` |
| --- | --- | --- | --- | --- |
| **16 B** | 5.74M msg/s (`174.2 ns`) | 4.71M msg/s (`212.5 ns`) | **35.48M msg/s** (`28.2 ns`) | **92.59M msg/s** (`10.8 ns`) |
| **48 B** | 5.02M msg/s (`199.1 ns`) | 5.12M msg/s (`195.3 ns`) | **27.20M msg/s** (`36.8 ns`) | **78.04M msg/s** (`12.8 ns`) |
| **56 B** *(crosses 64B SHM block under 16B hdr)* | 5.43M msg/s (`184.3 ns`) | 4.85M msg/s (`206.0 ns`) | **24.61M msg/s** (`40.6 ns`) | **82.88M msg/s** (`12.1 ns`) |
| **64 B** | 5.37M msg/s (`186.3 ns`) | 4.74M msg/s (`211.2 ns`) | **20.94M msg/s** (`47.8 ns`) | **66.37M msg/s** (`15.1 ns`) |
| **256 B** | 4.56M msg/s (`219.1 ns`) | 4.15M msg/s (`240.8 ns`) | **16.77M msg/s** (`59.6 ns`) | **31.08M msg/s** (`32.2 ns`) |
