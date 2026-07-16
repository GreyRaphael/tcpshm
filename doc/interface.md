[English](interface.md) | [中文](interface-CN.md)

tcpshm Interface Documentation
==============================

`tcpshm` is a high-performance, connection-oriented persistent message queue framework written in modern C++ (C++20/C++23) supporting both **Linux and Windows**.

## MsgHeader and Reserved Message Types

Every message has an 8-byte `MsgHeader` automatically prepended, regardless of control message or application message, in host endianness:

```c++
struct MsgHeader
{
    // size of this msg, including header itself (max 65535)
    // auto set by lib, can be read by user
    uint16_t size;
    // msg type of app msg is set by user
    uint16_t msg_type;
    // internally used for ptcp sequence and ack, must not be modified by user
    uint32_t ack_seq;

    template<bool ToLittle>
    void ConvertByteOrder();
};
```

The framework automatically applies `std::endian`-based byte-order conversion on `MsgHeader` if sending over the TCP channel (`ToLittleEndian` configuration).

### Reserved `msg_type` IDs
When defining application messages, note the following internal reserved or special message types:
* `0`: `HeartbeatMsg` (Internal heartbeat ping/pong sent during connection idle intervals).
* `3`: `ShmProbeReqMsg` (Internal SHM probe request sent by server to verify physical shared memory accessibility).
* `4`: `ShmProbeRspMsg` (Internal SHM probe response returned by client).
* `1` & `2`: `LoginMsg` (`msg_type=1`) and `LoginRspMsg` (`msg_type=2`). These are used during the initial connection login phase. **Once the login handshake completes (`OnLoginSuccess` / `OnClientLogon`), application messages are free to reuse `msg_type = 1` and `msg_type = 2` safely.**

---

## TcpShmConnection Class

`TcpShmConnection` is the core connection abstraction encapsulating either TCP (`PTCPConnection`) or Shared Memory (`SPSCVarQueue`).

**Note: Reading/writing messages on a single connection must happen in the same thread: its designated polling thread.**

### Sending Messages
To send a message, allocate space in the send queue using `Alloc()`:

```c++
// allocate a msg of specified size in send queue
// the returned address is guaranteed to be 8-byte aligned
// return nullptr if buffer space is full
MsgHeader* Alloc(uint16_t size);
```

In the returned `MsgHeader` pointer, set the `msg_type` field and copy your message payload (e.g., C struct or serialized Protobuf bytes) immediately after the header (`header + 1`). Then call `Push()` or `PushMore()` to submit:

```c++
// submit the last msg from Alloc() and send out immediately
void Push();

// for shm, same as Push()
// for tcp, buffers and does not flush out immediately (useful when sending multiple msgs in a batch)
void PushMore();
```

### Receiving Messages
Polling functions (`PollTcp` / `PollShm`) check for incoming messages and call your `OnServerMsg` or `OnClientMsg` callback when data arrives. Inside your callback, `Front()` yields the current message and `Pop()` consumes it:

```c++
// get the next msg from recv queue, return nullptr if queue is empty
// the returned address is guaranteed to be 8-byte aligned
MsgHeader* Front();

// consume the msg we got from Front() or polling callback
void Pop();
```

> **Best Practice for Request-Response Handling:**  
> When handling a message from `Front()` and immediately sending back a response, call `Pop()` first and then `Alloc()/Push()`:
> 1. For TCP, `Push()` sends to the network socket immediately. Calling `Pop()` before `Push()` piggybacks the updated `ack_seq` on the response message, confirming consumption faster to the remote peer.
> 2. If a crash occurs between `Pop()` and `Push()`, the received message is marked as consumed, preventing duplicate message processing on recovery.

### Connection Metadata & Control
You can query and manage the connection state anytime using the following methods:

```c++
// check if underlying socket/queue is closed
bool IsClosed();

// request closing the connection
void Close();

// get close reason string and system errno
const char* GetCloseReason(int* sys_errno);

// get remote and local connection names
char* GetRemoteName();
const char* GetLocalName();

// get the directory of ptcp persistence files
const char* GetPtcpDir();
std::string GetPtcpFile();
```

In your application, you do not instantiate `TcpShmConnection` directly; instead, you obtain a reference from `GetConnection()` on the client side or via `OnClientLogon / OnClientMsg` on the server side.

---

## Configuration (`Conf`)

Both client and server rely on a `Conf` struct (or `CommonConf` base) to define sizes, timeouts, and custom data types:

```c++
struct Conf
{
    // the size of client/server name in chars, including ending null
    static constexpr uint32_t NameSize = 16;
    
    // shm ring-buffer queue size in bytes, must be a power of 2
    static constexpr uint32_t ShmQueueSize = 1024 * 1024;

    // set to the endian of majority of the hosts (e.g. true for x86/x64)
    static constexpr bool ToLittleEndian = true; 

    // tcp send queue size in bytes, must be a multiple of 8
    static constexpr uint32_t TcpQueueSize = 3000; 

    // tcp recv buffer init and max sizes, must be multiples of 8
    static constexpr uint32_t TcpRecvBufInitSize = 1000;
    static constexpr uint32_t TcpRecvBufMaxSize = 2000;

    // enable TCP_NODELAY option
    static constexpr bool TcpNoDelay = true;

    // connection timeout and heartbeat interval, measured in user-provided timestamps (e.g. nanoseconds)
    static constexpr int64_t ConnectionTimeout = 10LL * 1000000000LL;
    static constexpr int64_t HeartBeatInverval = 3LL * 1000000000LL;

    // user defined data structures attached to LoginMsg / LoginRspMsg / Connection
    using LoginUserData = char;
    using LoginRspUserData = char;
    using ConnectionUserData = char;
};
```

---

## Client Side (`TcpShmClient`)

To implement a client, derive from `TcpShmClient<MyClient, Conf>` and define **public** callback methods:

```c++
#include "tcpshm/tcpshm_client.h"

class MyClient : public tcpshm::TcpShmClient<MyClient, Conf>
{
public:
    MyClient(const std::string& ptcp_dir, const std::string& name)
        : TcpShmClient(ptcp_dir, name), conn_(GetConnection()) {}

    // --- Public Callback Functions ---

    void OnSystemError(const char* error_msg, int sys_errno) {
        std::cout << "System Error: " << error_msg << " errno: " << sys_errno << std::endl;
    }

    void OnLoginReject(const LoginRspMsg* login_rsp) {
        std::cout << "Login Rejected: " << login_rsp->error_msg << std::endl;
    }

    int64_t OnLoginSuccess(const LoginRspMsg* login_rsp) {
        std::cout << "Login Success with server: " << login_rsp->server_name << std::endl;
        return get_timestamp(); // return current timestamp in nanoseconds
    }

    void OnServerMsg(MsgHeader* header) {
        // handle message data at header + 1
        conn_.Pop();
    }

    void OnDisconnected(const char* reason, int sys_errno) {
        std::cout << "Disconnected: " << reason << std::endl;
    }

    void OnSeqNumberMismatch(uint32_t local_ack, uint32_t local_start, uint32_t local_end,
                             uint32_t remote_ack, uint32_t remote_start, uint32_t remote_end) {
        std::cout << "Sequence mismatch detected." << std::endl;
    }

    // --- Connection Lifecycle & Polling ---

    void Run(const char* server_ip, uint16_t port, bool use_shm) {
        if (!Connect(use_shm, server_ip, port, 0)) return;

        // poll in loop until connection closes
        while (!conn_.IsClosed()) {
            if (use_shm) PollShm();
            PollTcp(get_timestamp());
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        Stop();
    }

private:
    Connection& conn_;
};
```

### Client Methods Summary
* `bool Connect(bool use_shm, const char* server_ipv4, uint16_t server_port, const LoginUserData& login_user_data)`: Connects, executes SHM probe handshake if `use_shm=true`, and logs in.
* `Connection& GetConnection()`: Returns the persistent connection reference.
* `void PollTcp(int64_t now)`: Serves TCP data, heartbeats, and handles disconnections. Must be called even in SHM mode.
* `void PollShm()`: Polls the lock-free shared memory queue for messages.
* `void Stop()`: Safely closes files and stops the client.

---

## Server Side (`TcpShmServer`)

To implement a server, derive from `TcpShmServer<MyServer, Conf>` and define **public** callback methods:

```c++
#include "tcpshm/tcpshm_server.h"

class MyServer : public tcpshm::TcpShmServer<MyServer, Conf>
{
public:
    MyServer(const std::string& ptcp_dir, const std::string& name)
        : TcpShmServer(ptcp_dir, name) {}

    // --- Public Callback Functions ---

    void OnSystemError(const char* errno_msg, int sys_errno) {
        std::cout << "System Error: " << errno_msg << std::endl;
    }

    int OnNewConnection(const struct sockaddr_in& addr, const LoginMsg* login, LoginRspMsg* login_rsp) {
        std::cout << "New Connection from: " << login->client_name << " use_shm=" << (bool)login->use_shm << std::endl;
        // return assigned group ID (0 to MaxTcpGrps-1 or MaxShmGrps-1), or -1 to reject
        return 0;
    }

    void OnClientLogon(const struct sockaddr_in& addr, Connection& conn) {
        std::cout << "Client Logged on: " << conn.GetRemoteName() << std::endl;
    }

    void OnClientDisconnected(Connection& conn, const char* reason, int sys_errno) {
        std::cout << "Client Disconnected: " << conn.GetRemoteName() << " reason: " << reason << std::endl;
    }

    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        // handle client message payload at recv_header + 1
        conn.Pop();
    }

    void OnClientFileError(Connection& conn, const char* reason, int sys_errno) {
        std::cout << "File error on " << conn.GetRemoteName() << ": " << reason << std::endl;
    }

    void OnSeqNumberMismatch(Connection& conn, uint32_t local_ack, uint32_t local_start, uint32_t local_end,
                             uint32_t remote_ack, uint32_t remote_start, uint32_t remote_end) {}
};
```

### Threading and Polling Model
The server gives you complete control over threading and connection sharding. You can run one thread per group or poll everything from a single loop:

```c++
// Start listening on ip and port
bool Start(const char* listen_ipv4, uint16_t listen_port);

// Poll control: accepts new connections, performs SHM probe checks, and cleans up dead connections
void PollCtl(int64_t now);

// Poll specific TCP connection group
void PollTcp(int64_t now, int grpid);

// Poll specific SHM connection group
void PollShm(int grpid);

// Stop listening and release all connections
void Stop();
```

---

## SHM Handshake Probing Protocol (`use-shm` Probe)

When `login->use_shm == 1`, `tcpshm` ensures that both processes reside on the same physical host and have valid read/write access to shared memory before opening the ring-buffer queues:

1. **`LoginMsg` (`msg_type=1`)**: Client connects over TCP and requests `use_shm = 1`.
2. **Token Generation & Probe Creation**: Server calls `VerifyShmProbe()`, generates `token = MakeShmProbeToken(client_name)`, and creates `/tcpshm_probe_<server>_<client>_<token>` (`sizeof(ShmProbeData)` bytes containing `token` and `ack=0`).
3. **`ShmProbeReqMsg` (`msg_type=3`)**: Server sends probe request containing `token` and `/tcpshm_probe_...` filename via TCP.
4. **Client Physical Memory Write**: Client receives `ShmProbeReqMsg`, opens the mmap file using `my_mmap_existing` (open-only, no create permission), verifies `probe->token == req->token`, calculates `ack = token ^ 0x5a5a5a5a5a5a5a5aULL`, and writes `ack` directly into `probe->ack` in shared memory.
5. **`ShmProbeRspMsg` (`msg_type=4`)**: Client sends TCP response confirming `ack` and `ok=1`.
6. **Server Final Verification**: Server receives `ShmProbeRspMsg`, checks `rsp->ok == 1` and `rsp->ack == token ^ 0x5a5a5a5a5a5a5a5aULL`, and reads physical shared memory `probe->ack` to ensure it matches. If verified, Server unlinks the probe file (`my_shm_unlink`) and grants login `LoginRspMsg (`status=0`)` with `use_shm=true`.

---

## Cross-Platform Layer & Serialization Integration

### OS Abstraction (`os.h`)
`tcpshm` includes a complete cross-platform OS layer `os.h` that masks platform differences:
* On Windows, it handles `WSAStartup/WSACleanup` via `WSAInitializer`, manages file mappings (`CreateFileMappingA` / `OpenFileMappingA` / `MapViewOfFile`) using `MMapManager`, and simulates vector reads (`tcp_readv`) using `WSARecv`.
* On Linux, it uses `shm_open / mmap / munmap`, `fcntl` non-blocking flags, and native `readv`.

### Zero-Copy Protobuf / Struct Serialization
Because `Alloc(size)` gives direct, 8-byte aligned access to the underlying memory queue (`header + 1`), you can serialize directly into the communication buffer without intermediate copies. See `server.cpp` / `client.cpp` for how modern C++ serialization (such as `iguana::to_pb` and `std::variant`) works with `tcpshm`.
