[English](interface.md) | [中文](interface-CN.md)

tcpshm 接口与开发指南 (C++20/23)
==============================

`tcpshm` 是一个采用现代 C++（C++20/C++23）编写的高性能、面向连接的持久化消息队列通信框架，全面支持 **Linux 与 Windows** 跨平台部署。

## MsgHeader 消息头部与保留消息类型

无论控制消息还是业务应用消息，每一条在 `tcpshm` 中传输的消息包都会自动由框架在最前方附加一个 8 字节对齐的 `MsgHeader`（以当前主机的本地字节序存储）：

```c++
struct MsgHeader
{
    // 当前消息包的总字节大小（包含 MsgHeader 自身 8 字节在内，最大 65535）
    // 由底层 Alloc() 自动设置，应用端可直接读取
    uint16_t size;
    // 业务层自定义的应用消息类型 ID（必须由用户在 Alloc() 后自行设置）
    uint16_t msg_type;
    // 内部用于 PTCP 序列号与 ACK 对齐应答的字段，严禁业务层篡改
    uint32_t ack_seq;

    template<bool ToLittle>
    void ConvertByteOrder();
};
```

当通过 TCP 网络通道发送数据时，框架会在底层按照 `ToLittleEndian` 配置利用 `std::endian` 自动对 `MsgHeader` 进行字节序转换。

### 内部保留的 `msg_type` ID 说明
在业务侧定义自己的协议消息 `msg_type` 时，请务必注意以下由框架内部保留或特定阶段占用的 ID：
* `0`: `HeartbeatMsg`（底层心跳包，在连接空闲时用于双向探测与保活）。
* `3`: `ShmProbeReqMsg`（SHM 握手探测请求包，由服务端发出以验证对共享内存的物理访问权限）。
* `4`: `ShmProbeRspMsg`（SHM 握手探测响应包，由客户端验证成功后回发）。
* `1` & `2`: `LoginMsg` (`msg_type=1`) 和 `LoginRspMsg` (`msg_type=2`)。仅在初次建立连接登录协商阶段被占用。**一旦登录握手成功完成（触发了 `OnLoginSuccess` / `OnClientLogon` 回调后），业务应用即可安全、自由地复用 `msg_type = 1` 与 `msg_type = 2` 作为普通业务消息 ID。**

---

## TcpShmConnection 核心连接类

`TcpShmConnection` 是对底层的 TCP 传输通道 (`PTCPConnection`) 或共享内存通道 (`SPSCVarQueue`) 的统一抽象连接类。

**核心注意事项：对同一条连接上所有的读写调用（如 `Alloc/Push` 与 `Front/Pop`），必须严格限制在同一个绑定它的轮询线程内部发生。**

### 发送消息 (Sending Messages)
要发送一条消息，首先通过调用 `Alloc()` 在发件队列中申请一段指定字节大小的内存空间：

```c++
// 在发送缓冲区分配指定大小的消息空间 (传参 payload 大小即可，不必包含 MsgHeader 大小)
// 返回的地址保证保证严格 8 字节对齐
// 如果当前缓冲队列已满或剩余空间不足，将返回 nullptr
MsgHeader* Alloc(uint16_t size);
```

在获取到返回的 `MsgHeader* header` 成功指针后，你需要在其上显式赋予 `header->msg_type` 业务类型，并将实际的业务负载（例如 C/C++ POD 结构体或序列化后的 Protobuf/JSON 字节流）精准拷贝进紧随头部之后的地址空间 (`header + 1`)。完成填充后，调用 `Push()` 或 `PushMore()` 提交并发往对端：

```c++
// 提交刚才 Alloc() 的最后一条消息并立刻触发发送处理
void Push();

// 针对 SHM 模式：该调用与 Push() 完全等价
// 针对 TCP 模式：将数据推入缓冲区但在本次调用中不立即冲刷发往网络端（常用于多条消息连续批量发送的场景）
void PushMore();
```

### 接收消息 (Receiving Messages)
当应用层的轮询函数 (`PollTcp` / `PollShm`) 探测到有新包到达时，会自动调用你所提供的 `OnServerMsg` 或 `OnClientMsg` 回调方法。在你的业务回调方法内部，通过 `Front()` 取得当前首条消息，并在处理结束后调用 `Pop()` 予以消费出队：

```c++
// 获取接收缓冲队列中的当前首条待处理业务消息；若队列为空则返回 nullptr
// 返回的消息指针保证 8 字节对齐
MsgHeader* Front();

// 标记消费出队 Front() 所指向的当前首条业务消息
void Pop();
```

> **🔥 Request-Response 模式下的核心最佳实践：**  
> 当业务对 `Front()` 返回的消息处理完毕、并想要马上向对端回覆一条应答响应包时，**强烈建议先调用 `Pop()`，然后再调用 `Alloc()/Push()`**：
> 1. 在 TCP 传输模式下，`Push()` 内部会立刻尝试将数据发送至系统套接字。先执行 `Pop()` 可以将最新的对端确认序列号 (`ack_seq`) 直接捎带（Piggyback）在即将发出的应答包中，极大提升网络确认与吞吐效率。
> 2. 如果程序在 `Pop()` 和 `Push()` 之间发生突发宕机崩溃，当前收到的消息已明确被标记为确认消费。当进程重启进行自动从底层 `.ptcp` / `.shm` 恢复时，避免了对同一条消息再次去重发应答，保障了业务幂等性。

### 连接元数据与管理 (Connection Metadata & Control)
你可以在业务生命周期的任意时刻通过 `TcpShmConnection` 查询当前连接状态或执行断连：

```c++
// 判断底层 TCP 套接字或共享内存连接是否已经断开
bool IsClosed();

// 向底层的控制逻辑发出主动断开本连接的请求
void Close();

// 获取当前连接断开的原因描述字符串及底层系统 errno 错误码
const char* GetCloseReason(int* sys_errno);

// 获取对端与本地的会话名称
char* GetRemoteName();
const char* GetLocalName();

// 获取持久化文件 (.ptcp / .shm) 存放的根目录路径及本连接的 ptcp 文件全名
const char* GetPtcpDir();
std::string GetPtcpFile();
```

在实际编写业务逻辑时，开发者不允许手动直接实例化 `TcpShmConnection` 对象；你应该从客户端的 `GetConnection()` 接口、或者从服务端的 `OnClientLogon / OnClientMsg` 回调传参中提取并持有该连接的引用。

---

## 全局配置模板类 (`Conf`)

客户端与服务端均依赖一个静态配置类 (`Conf` 或继承自 `CommonConf`) 来定义会话命名限制、内存大小、延迟心跳及业务自定义关联字段：

```c++
struct Conf
{
    // 会话名称最大字符数（包含结尾的 '\0'，默认为 16）
    static constexpr uint32_t NameSize = 16;
    
    // 共享内存环形队列缓冲区大小（单位字节，必须是 2 的幂次方，如 1MB）
    static constexpr uint32_t ShmQueueSize = 1024 * 1024;

    // 当前架构主导端大小端配置（对于 x86/x64 主机通常设为 true 即小端序）
    static constexpr bool ToLittleEndian = true; 

    // TCP 发送队列缓冲空间大小（必须是 8 的倍数）
    static constexpr uint32_t TcpQueueSize = 3000; 

    // TCP 接收缓冲池的初始分配大小与最大动态扩容大小（必须是 8 的倍数）
    static constexpr uint32_t TcpRecvBufInitSize = 1000;
    static constexpr uint32_t TcpRecvBufMaxSize = 2000;

    // 是否开启 TCP_NODELAY 选项（禁用 Nagle 算法降低延迟）
    static constexpr bool TcpNoDelay = true;

    // 连接超时判断与心跳发包的时间间隔阈值（基于用户传入的时间戳单位，如纳秒）
    static constexpr int64_t ConnectionTimeout = 10LL * 1000000000LL;
    static constexpr int64_t HeartBeatInverval = 3LL * 1000000000LL;

    // 附着在 LoginMsg / LoginRspMsg / TcpShmConnection 上的业务拓展数据类型
    using LoginUserData = char;
    using LoginRspUserData = char;
    using ConnectionUserData = char;
};
```

---

## 客户端开发指南 (`TcpShmClient`)

要构建自定义客户端，继承自 `TcpShmClient<MyClient, Conf>` 并在你的类中直接定义**公开 (`public`)** 的回调处理函数：

```c++
#include "tcpshm/tcpshm_client.h"

class MyClient : public tcpshm::TcpShmClient<MyClient, Conf>
{
public:
    MyClient(const std::string& ptcp_dir, const std::string& name)
        : TcpShmClient(ptcp_dir, name), conn_(GetConnection()) {}

    // --- Public 业务与事件回调函数 ---

    void OnSystemError(const char* error_msg, int sys_errno) {
        std::cout << "系统底层错误: " << error_msg << " 错误码: " << sys_errno << std::endl;
    }

    void OnLoginReject(const LoginRspMsg* login_rsp) {
        std::cout << "登录被服务端拒绝: " << login_rsp->error_msg << std::endl;
    }

    int64_t OnLoginSuccess(const LoginRspMsg* login_rsp) {
        std::cout << "登录成功！当前对接服务端: " << login_rsp->server_name << std::endl;
        return get_timestamp(); // 返回当前系统的真实纳秒时间戳
    }

    void OnServerMsg(MsgHeader* header) {
        // 解析处理位于 header + 1 处的业务消息体
        conn_.Pop();
    }

    void OnDisconnected(const char* reason, int sys_errno) {
        std::cout << "连接已断开: " << reason << std::endl;
    }

    void OnSeqNumberMismatch(uint32_t local_ack, uint32_t local_start, uint32_t local_end,
                             uint32_t remote_ack, uint32_t remote_start, uint32_t remote_end) {
        std::cout << "检测到通信序列号与对端不匹配，需干预修复。" << std::endl;
    }

    // --- 客户端控制循环方法示例 ---

    void Run(const char* server_ip, uint16_t port, bool use_shm) {
        if (!Connect(use_shm, server_ip, port, 0)) return;

        // 在连接未断开的生命周期中持续轮询收包
        while (!conn_.IsClosed()) {
            if (use_shm) PollShm();
            PollTcp(get_timestamp());
            std::this_thread::sleep_for(std::chrono::microseconds(100)); // 避免纯忙轮询消耗 100% CPU
        }
        Stop();
    }

private:
    Connection& conn_;
};
```

### 客户端核心方法清单
* `bool Connect(bool use_shm, const char* server_ipv4, uint16_t server_port, const LoginUserData& login_user_data)`：向服务端发起 TCP 连接。如传参 `use_shm=true`，会在登录过程中自动完成物理内存探测握手。
* `Connection& GetConnection()`：获取当前客户端所包装的持久化通信连接引用。
* `void PollTcp(int64_t now)`：轮询并处理 TCP 通道数据、底层心跳 ping/pong 及异常断线检测。**即便你工作在 SHM 模式下，也一定要在循环中定期调用 `PollTcp()` 以维持 TCP 保活与控制。**
* `void PollShm()`：轮询并收取无锁物理共享内存环形队列中的最新业务数据。
* `void Stop()`：安全退出并关闭所有底层句柄和映射文件。

---

## 服务端开发指南 (`TcpShmServer`)

要构建自定义服务端，继承自 `TcpShmServer<MyServer, Conf>` 并在派生类中定义**公开 (`public`)** 的回调处理函数：

```c++
#include "tcpshm/tcpshm_server.h"

class MyServer : public tcpshm::TcpShmServer<MyServer, Conf>
{
public:
    MyServer(const std::string& ptcp_dir, const std::string& name)
        : TcpShmServer(ptcp_dir, name) {}

    // --- Public 业务与事件回调函数 ---

    void OnSystemError(const char* errno_msg, int sys_errno) {
        std::cout << "服务端系统底层错误: " << errno_msg << std::endl;
    }

    int OnNewConnection(const struct sockaddr_in& addr, const LoginMsg* login, LoginRspMsg* login_rsp) {
        std::cout << "新连接请求: " << login->client_name << " 期望开启 SHM: " << (bool)login->use_shm << std::endl;
        // 返回分配该客户端的目标线程分组 ID（范围为 0 到 MaxTcpGrps-1 或 MaxShmGrps-1）
        // 如果想要拒绝该次登录请求，请返回 -1，并可在 login_rsp->error_msg 中设置拒绝说明
        return 0;
    }

    void OnClientLogon(const struct sockaddr_in& addr, Connection& conn) {
        std::cout << "客户端登录成功: " << conn.GetRemoteName() << std::endl;
    }

    void OnClientDisconnected(Connection& conn, const char* reason, int sys_errno) {
        std::cout << "客户端断开: " << conn.GetRemoteName() << " 原因: " << reason << std::endl;
    }

    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        // 读取并处理位于 recv_header + 1 的业务消息体内容
        conn.Pop();
    }

    void OnClientFileError(Connection& conn, const char* reason, int sys_errno) {
        std::cout << "映射或操作队列文件错误 [" << conn.GetRemoteName() << "]: " << reason << std::endl;
    }

    void OnSeqNumberMismatch(Connection& conn, uint32_t local_ack, uint32_t local_start, uint32_t local_end,
                             uint32_t remote_ack, uint32_t remote_start, uint32_t remote_end) {}
};
```

### 服务端线程模型与分片轮询 (`Polling Model`)
服务端提供极佳的连接分片设计（Connection Sharding）。你可以为每个连接组指派专门的独占线程进行处理，也可以在一个单线程主循环中轮询处理全部组：

```c++
// 绑定 IP 和端口并启动监听
bool Start(const char* listen_ipv4, uint16_t listen_port);

// 主控制线程轮询：处理 accept 新连接、执行 SHM 探测握手验证并清理已死亡的残留连接
void PollCtl(int64_t now);

// 业务线程轮询指定的 TCP 连接组 ID (0 <= grpid < MaxTcpGrps)
void PollTcp(int64_t now, int grpid);

// 业务线程轮询指定的 SHM 共享内存连接组 ID (0 <= grpid < MaxShmGrps)
void PollShm(int grpid);

// 停止监听并安全释放所有在线连接
void Stop();
```

---

## SHM 握手探测底层通信规范 (`use-shm` Probe)

当客户端 `login->use_shm == 1` 时，框架严密防范未授权或跨虚拟化的直接打开内存请求，其通信状态机规范如下：

1. **`LoginMsg` 发送 (`msg_type=1`)**：客户端连接 TCP 并在登录包中带上 `use_shm = 1`。
2. **随机 Token 生成与内存文件创建**：服务端执行 `VerifyShmProbe()`，生成一个包含自身物理指针与时间戳混淆的 64 位防冲突 Token `MakeShmProbeToken(client_name)`，接着在临时路径创建大小为 `sizeof(ShmProbeData)` 的探测映射文件 `/tcpshm_probe_<server>_<client>_<token>`，置入 `token` 及 `ack = 0`。
3. **`ShmProbeReqMsg` 发出 (`msg_type=3`)**：服务端将带有探测内存名和 `token` 的请求经由 TCP 下发给客户端。
4. **客户端物理内存映射回写**：客户端收到 `ShmProbeReqMsg` 后，必须严格使用**只读已存在模式 (`my_mmap_existing`)** 映射该共享内存。当核对物理内存里的 `probe->token == req->token` 成立时，客户端计算应答值 `ack = token ^ 0x5a5a5a5a5a5a5a5aULL`，并**直接将其写回物理内存地址处的 `probe->ack` 中**。
5. **`ShmProbeRspMsg` 应答 (`msg_type=4`)**：客户端通过 TCP 回复应答包确认已经完成计算和回写 (`ok = 1`)。
6. **服务端双重校验与放行**：服务端确认 TCP 包内 `rsp->ok == 1` 与 `rsp->ack` 匹配后，还要在**本地读取物理内存中的 `probe->ack`**。确认物理内存的的确确被对方改写成功后，服务端立即销毁并解绑探测文件 (`my_shm_unlink`)，随后发放 `LoginRspMsg (status=0)` 并为你正式分配对齐的超高速双向共享内存消息缓冲环。

---

## 跨平台操作底座与序列化对接

### OS 抽象层设计 (`os.h`)
框架在 `os.h` 中针对跨操作系统网络编程和内存映射提供了零感知适配：
* 在 Windows 上，封装 `WSAInitializer` 自理 `WSAStartup / WSACleanup`；利用 `MMapManager` 单例自动管理 `CreateFileMappingA` 与 `MapViewOfFile` 生成的 Mapping Handle 关闭引用链；将 Linux `readv` 优雅适配为了原生高效的 `WSARecv` 聚集读。
* 在 Linux 上，原生采用 `shm_open / mmap / munmap` 内存映射，以及 `fcntl` 非阻塞设置和 `readv` 分散聚集 IO。

### 零拷贝现代序列化实战 (`Protobuf` & `std::variant`)
由于 `conn.Alloc(size)` 直接给你分配了通信对齐缓冲环中 `header + 1` 位置的直写内存指针，开发者可跳过任何局部中间变量转储。例如借助现代 C++ 的 `iguana::to_pb` 直接把 Protobuf 格式序列化结果写进底层环形内存；在收包时通过 `std::string_view` 构建指向该共享缓冲区的零拷贝视图，并利用 `std::visit(overload{...})` 实现类型优雅派发。详细工程写法请参考项目的 `server.cpp` 与 `client.cpp`。
