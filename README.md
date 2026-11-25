[English](README.md) | [中文](README-CN.md)

**TCPSHM is a connection-oriented persistent message queue framework based on TCP or SHM IPC for Linux & Windows (C++20)**

When using TCP to transfer data, sent out messages are not guaranteed to be received or handled by the receiver, and even worse, we often get unexpected disconnections due to network issues or program crash, so efforts have been made on recovery procedure to ensure both sides are synced. TCPSHM provides a reliable and efficient solution based on a sequence number and acknowledge mechanism, that every sent out msg is persisted in a send queue until sender got ack that it's been consumed by the receiver, so that disconnects/crashes are tolerated and the recovery process is purely automatic.

And as the name implies, shared memory (`use_shm`) is also supported when communicating on the same host, and it provides the exact same API and behavior as TCP (so whether TCP or SHM underlies the connection is transparent to the user), but it's more than 20 times faster than TCP on localhost. The shared memory communication is based on a real-time single-producer single-consumer (`SPSCVarQueue`) lock-free ring buffer.

The user message format is just a general purpose binary string, it's user's responsibility to encode/decode it. E.g. user can simply use C/C++ struct for simplicity/efficiency, or modern serialization libraries like [iguana](https://github.com/qicosmos/iguana) / Google Protocol Buffers with `std::variant` for extensibility.

Additionally, both sides of a connection have a specified name, and a pair of such names uniquely identifies a persistent connection. If one side disconnects, changes its name and reconnects with the same remote side, the connection will be brand new and will not recover from the old one. This can sometimes be useful, e.g: A daily trading server starts before market open and stops after market close every trading day, and every day when it starts it expects the connection with its clients to be new and any unhandled msgs from yesterday are silently discarded (obsolete order requests don't make any sense in a new trading day!), so the server can set its name to be something like `"Server20260716"`.

This is a header-only framework that provides server-side and client-side C++ template classes (`TcpShmServer` and `TcpShmClient`), which implement a typical TCP/SHM server and client and are highly configurable and customizable. For the server, the framework supports connection sharding: user predefines a set of connection groups, having one or more threads polling these groups, and once there's a new connection user can decide which group to assign to, giving the user full control over the mapping between serving threads and client connections.

## Technical Features
  * **Cross-Platform Support**: Seamlessly supports both **Linux and Windows** via a clean operating system abstraction layer (`os.h`), bridging `WinSock2` / `CreateFileMappingA` / `MapViewOfFile` with POSIX sockets / `shm_open` / `mmap`.
  * **Modern C++20**: Built with modern C++20 memory models and utilities (`std::atomic` / `std::atomic_ref` with `memory_order_acquire`/`release` lock-free ring buffers, `std::endian`, `std::bit_cast`, `std::span`, `std::has_single_bit`). Compatible with C++23.
  * Header-only (`#include "tcpshm/..."`), no static/dynamic library to build and link.
  * Zero external library dependencies in the core `tcpshm/` headers (the example programs optionally use bundled `third_party/iguana` for Protobuf serialization).
  * Non-blocking data transmission (except client `Connect()` and initial SHM probing handshake).
  * No creating threads internally (the framework gives user full control over polling and threading).
  * No getting any kind of timestamp from system in critical paths (user passes timestamps into polling methods).
  * No C++ exceptions in core library headers.
  * No writing to stdout/stderr or log files internally.
  * No use of heavy mutexes or atomic CAS locks in the fast data path (`Alloc()` / `Push()` / `Front()` / `Pop()`).
  * Lightweight, clean, and efficient.

## SHM Probe (Physical Shared Memory Handshake)

When a client connects and requests shared memory mode (`use_shm = 1`), `tcpshm` automatically runs a physical shared memory readiness probe during the TCP login phase (after `OnNewConnection` authorization) to prevent crashes or misbehavior across separate hosts, isolated containers, or insufficient permissions:

1. **Login Request**: The client sends `LoginMsg` (`msg_type = 1`) over TCP with `use_shm = 1` and its `client_name`.
2. **Server Probe Creation**: After `OnNewConnection` authorizes the connection, the server generates a collision-resistant random token (`MakeShmProbeToken(client_name)`) and exclusively creates a temporary probe shared memory object (e.g., `/tcpshm_probe_<token>_<seq>`), writing `token` and initializing `ack = 0`.
3. **Probe Request**: The server sends `ShmProbeReqMsg` (`msg_type = 3`) over TCP containing the probe shared memory name and `token`.
4. **Client Verification & Physical Memory Write**:
   * Upon receiving `ShmProbeReqMsg` (accepted only when `use_shm = true` and the probe name passes `/tcpshm_probe_` validation), the client opens the shared memory object in **open-existing read-write mode** (`my_mmap_existing`, strictly without create permission).
   * After verifying `probe->token == req->token`, the client computes `ack = token ^ 0x5a5a5a5a5a5a5a5aULL` and writes `ack` directly into physical shared memory (`probe->ack`).
5. **TCP Confirmation**: The client sends `ShmProbeRspMsg` (`msg_type = 4`) over TCP carrying `ack` and `ok = 1`.
6. **Dual Verification & Upgrade**: The server validates both the TCP response (`rsp->ok` and `rsp->ack`) and the physical shared memory value (`probe->ack`). Only when both TCP and shared memory checks succeed is `use_shm = 1` granted to initialize and open the bidirectional `SPSCVarQueue` ring buffers; otherwise login is rejected.

## Limitations
  * It won't persist data on disk, so it can't recover from power down (unconsumed messages in `.ptcp` / `.shm` files survive process crash, but not system reboot or power loss).
  * As it's non-blocking and busy polling for the purpose of low latency, CPU usage would be high in busy-polling loops (`PollTcp` / `PollShm`). User can insert `std::this_thread::yield()` or `sleep_for` if absolute minimum latency is not required.
  * Currently user can only read/write to a connection (`Alloc/Push` and `Front/Pop`) in its designated polling thread. If needing to write msg from other threads, user has to push it to a synchronized/lock-free queue which is then consumed by the polling thread.
  * Transactions across multiple messages are not supported. If you have multiple `Push()` or `Pop()` actions in a batch, be prepared that some succeed and some fail in case of a process crash during the batch.
  * Currently the message length must fit in a `uint16_t` (up to 65535 bytes total including the 8-byte `MsgHeader`, i.e., up to 65527 bytes of payload).

## Documentation
  [Interface Doc (English)](doc/interface.md) | [接口文档 (中文)](doc/interface-CN.md)

## Build & CMake Integration
  CMake defaults to **C++20** (respecting caller overrides like `-DCMAKE_CXX_STANDARD=23`) and provides the header-only `INTERFACE` target `tcpshm::tcpshm`:

  ```bash
  cmake -S . -B build
  cmake --build build
  ```

  Downstream projects can link against `tcpshm::tcpshm` (which configures include paths, `cxx_std_20`, and platform libraries `Threads::Threads`, `rt` on Linux, and `ws2_32` on Windows) and include headers with `#include "tcpshm/..."`.

## Example
  Check [`common.h`](common.h), [`server.cpp`](server.cpp), and [`client.cpp`](client.cpp) for a C++20 example demonstrating interactive client-server login/response communication over TCP or SHM, Protobuf serialization (`iguana`), `std::variant` dispatch, and single-polling-thread message submission.

## Testing
  The repository includes a zero-external-dependency CTest regression suite in [`tests/tcpshm_test.cpp`](tests/tcpshm_test.cpp) covering `Alloc()` overflow bounds, `SPSCVarQueue` wrap-around and concurrent SPSC stress, `PTCPQueue::Ack()`/`SanityCheckAndGetSeq()` bounds, invalid framing handling, non-8-byte-aligned partial `EAGAIN` recovery, fragmented `LoginMsg` reception, TCP/SHM round-trip and reconnect recovery, and SHM probe/name validation:

  ```bash
  ctest --test-dir build --output-on-failure
  ```

## Performance
  Historical same-host ping-pong RTT measurements (round-trip time = total time elapsed / number of messages processed):

  * RTT in **TCP** mode: **~8.8 us**
  * RTT in **SHM** mode: **~0.34 us**

  *(Shared memory mode is over 20x faster than TCP loopback on localhost due to zero-copy kernel bypass (`SPSCVarQueue`).)*

## Guide to header files (`tcpshm/`):

* **`tcpshm_client.h`**: The client-side template class (`TcpShmClient`). Handles connection handshake, SHM probing, and polling.
* **`tcpshm_server.h`**: The server-side template class (`TcpShmServer`). Supports connection sharding, group polling, SHM probe verification, and heartbeat monitoring.
* **`tcpshm_conn.h`**: A general connection class (`TcpShmConnection`) that encapsulates TCP (`PTCPConnection`) or SHM (`SPSCVarQueue`). Use `Alloc()` / `Push()` and `Front()` / `Pop()` to send and receive messages.
* **`msg_header.h`**: Defines the 8-byte `alignas(8)` `MsgHeader` (`size`, `msg_type`, `ack_seq`) and automatic endianness conversion.
* **`os.h`**: Cross-platform OS abstraction layer (Windows `WinSock2` / `CreateFileMappingA` vs Linux POSIX sockets / `shm_open` / `mmap`) including `my_mmap`, `my_mmap_existing`, `my_munmap`, and `my_shm_unlink`.
* **`spsc_varq.h`**: Modern C++20 `std::atomic` single-producer single-consumer lock-free ring buffer with variable-length message allocation.
* **`ptcp_conn.h` & `ptcp_queue.h`**: TCP connection management, protocol message templates (`LoginMsgTpl`, `LoginRspMsgTpl`, `ShmProbeReqMsg`, `ShmProbeRspMsg`), and persistent mmap-backed TCP send/ack queues (`PTCPQueue`).
* **`endian.h` & `mmap.h`**: Compile-time endian conversion templates (`std::endian` + `std::bit_cast`) and memory mapping header wrapper (`os.h`).
