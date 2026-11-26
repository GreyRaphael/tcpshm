#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <exception>
#include <iguana/iguana.hpp>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

#include "common.h"
#include "tcpshm/msg_header.h"
#include "tcpshm/tcpshm_server.h"

using namespace std;
using namespace tcpshm;

struct ServerConf : public CommonConf {
    static constexpr int64_t NanoInSecond = 1000000000LL;

    static constexpr uint32_t MaxNewConnections = 5;
    static constexpr uint32_t MaxShmConnsPerGrp = 4;
    static constexpr uint32_t MaxShmGrps = 1;
    static constexpr uint32_t MaxTcpConnsPerGrp = 4;
    static constexpr uint32_t MaxTcpGrps = 1;

    // echo server's TcpQueueSize should be larger than that of client if client is in fast mode
    // otherwise server's send queue could be blocked and ack_seq can only be sent through HB which is slow
    static constexpr uint32_t TcpQueueSize = 3000;        // must be a multiple of 8
    static constexpr uint32_t TcpRecvBufInitSize = 1000;  // must be a multiple of 8
    static constexpr uint32_t TcpRecvBufMaxSize = 2000;   // must be a multiple of 8
    static constexpr bool TcpNoDelay = true;

    static constexpr int64_t NewConnectionTimeout = 3 * NanoInSecond;
    static constexpr int64_t ConnectionTimeout = 10 * NanoInSecond;
    static constexpr int64_t HeartBeatInverval = 3 * NanoInSecond;

    using ConnectionUserData = char;
};

class StringServer;
using TSServer = TcpShmServer<StringServer, ServerConf>;
static std::atomic<bool> stopped{false};

class StringServer : public TSServer {
   public:
    StringServer(const string& name, const string& ptcp_dir) : TSServer(name, ptcp_dir) {
        signal(SIGTERM, StringServer::SignalHandler);
        signal(SIGINT, StringServer::SignalHandler);  // 补充支持 Ctrl+C
    }

    static void SignalHandler(int) noexcept { stopped.store(true, std::memory_order_relaxed); }

    // --- Public 回调函数，移除 friend 依赖 ---

    void OnSystemError(const char* errno_msg, int /*sys_errno*/) {
        cout << "System Error: " << errno_msg << endl;
    }

    int OnNewConnection(const struct sockaddr_in& addr, const LoginMsg* login, LoginRspMsg* /*login_rsp*/) {
        char ipbuf[INET_ADDRSTRLEN]{};
        inet_ntop(AF_INET, &addr.sin_addr, ipbuf, sizeof(ipbuf));
        cout << "New Connection: " << login->client_name << " from " << ipbuf << ", use_shm=" << static_cast<bool>(login->use_shm) << endl;
        // 简单映射到 group 0
        return 0;
    }

    void OnClientLogon(const struct sockaddr_in& /*addr*/, Connection& conn) {
        cout << "Client Logged on: " << conn.GetRemoteName() << endl;
    }

    void OnClientDisconnected(Connection& conn, const char* reason, int /*sys_errno*/) {
        cout << "Client Disconnected: " << conn.GetRemoteName() << " Reason: " << reason << endl;
    }

    // 核心逻辑：处理客户端发来的字符串并 Echo
    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        if (recv_header->size < sizeof(MsgHeader)) {
            conn.Pop();
            return;
        }
        auto body_size = recv_header->size - sizeof(MsgHeader);
        std::string_view view{reinterpret_cast<char const*>(recv_header + 1), body_size};
        Request req{};
        try {
            iguana::from_pb(req, view);
        } catch (const std::exception& e) {
            cout << "Parse Request failed: " << e.what() << endl;
            conn.Pop();
            return;
        }
        conn.Pop();

        std::visit(overload{
                       [&](Login& msg) {
                           cout << "name=" << msg.name << ",pwd=" << msg.password << endl;
                           if (msg.name == "gewei") {
                               auto login_rsp = Response{LoginResponse{0, "success"}};
                               writeResponse(conn, login_rsp);
                           } else {
                               auto login_rsp = Response{LoginResponse{-1, "failure"}};
                               writeResponse(conn, login_rsp);
                           }
                       },
                       [&](Order&) {},
                       [&](auto const&) { /* 忽略 Login 或其他类型 */ }},
                   req.message_type);
    }

    // 必要的空白回调
    void OnClientFileError(Connection& /*conn*/, const char* r, int e) {
        std::cout << "OnClientFileError" << e << "msg:" << r << std::endl;
    }
    void OnSeqNumberMismatch(Connection& /*c*/, uint32_t /*a*/, uint32_t /*b*/, uint32_t /*d*/, uint32_t /*e*/, uint32_t /*f*/, uint32_t /*g*/) {}

    void writeResponse(Connection& conn, Response const& rsp) {
        std::string rsp_body{};
        iguana::to_pb(rsp, rsp_body);
        if (rsp_body.size() > 65535 - sizeof(MsgHeader)) return;
        auto rsp_header = conn.Alloc(static_cast<uint16_t>(rsp_body.size()));
        if (!rsp_header) return;
        rsp_header->msg_type = 1;
        memcpy(rsp_header + 1, rsp_body.data(), rsp_body.size());
        conn.Push();
    }

    // 路由与线程分工说明：
    // 1. PollCtl() 在握手阶段依据客户端 login->use_shm 与 OnNewConnection() 返回的 grpid 分配归属分组：
    //    - 客户端为 TCP 模式（use_shm=0）：进入 tcp_grps_[grpid]，其消息收发、心跳与断线检测均由对应的 PollTcp(now, grpid) 线程独占服务（不会进入 PollShm）。
    //    - 客户端为 SHM 模式（use_shm=1）：进入 shm_grps_[grpid]，其业务消息（OnClientMsg）由对应的 PollShm(grpid) 线程独占服务（不会进入 PollTcp），
    //      而 SHM 连接保活所需的 TCP 心跳由 PollCtl() 顺带轮询。
    // 2. 无论 TCP 还是 SHM，接入握手（OnNewConnection / OnClientLogon）与断线清理（OnClientDisconnected）均统一在 PollCtl() 线程执行。
    // 3. 拆分多线程的原因：将 PollCtl() 中的 tcp_accept/mmap/VerifyShmProbe（阻塞探测最高 300ms）与 PollTcp() 系统调用同亚微秒级 PollShm() 热路径隔离，
    //    并支持多分组并行扩展；若场景简单且不介意新连接握手抖动，也可在主线程单循环中依次调用 PollTcp/PollShm/PollCtl（详见 doc/interface-CN.md）。
    void Run(const char* ip, uint16_t port) {
        if (!Start(ip, port)) return;

        vector<thread> threads;
        threads.reserve(ServerConf::MaxTcpGrps + ServerConf::MaxShmGrps);
        // TCP 轮询线程：仅服务 use_shm=0 且被路由到对应 grpid 的 TCP 客户端（含消息、心跳与断线检测）
        for (uint32_t i = 0; i < ServerConf::MaxTcpGrps; ++i) {
            threads.emplace_back([this, grpid = static_cast<int>(i)]() {
                while (!stopped.load(std::memory_order_relaxed)) {
                    PollTcp(get_timestamp(), grpid); // TCP模式下: PollTcp() -> OnServerMsg()
                    std::this_thread::yield();  // 比固定 sleep 更适合低延迟轮询
                }
            });
        }

        // SHM 轮询线程：仅服务 use_shm=1 且被路由到对应 grpid 的 SHM 客户端业务消息（不受 TCP/PollCtl 阻塞影响）
        for (uint32_t i = 0; i < ServerConf::MaxShmGrps; ++i) {
            threads.emplace_back([this, grpid = static_cast<int>(i)]() {
                while (!stopped.load(std::memory_order_relaxed)) {
                    PollShm(grpid); // SHM模式下: PollShm() -> OnServerMsg()
                    std::this_thread::yield();
                }
            });
        }

        // 主控制线程：负责新连接接入/登录/VerifyShmProbe 探测、SHM 连接的 TCP 心跳保活，以及全部连接的断线清理
        cout << "Server is running on " << ip << ":" << port << "..." << endl;
        while (!stopped.load(std::memory_order_relaxed)) {
            PollCtl(get_timestamp()); // 控制线程
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
        Stop();
        cout << "Server stopped gracefully." << endl;
    }
};

int main() {
    // 确保 server 目录存在，用于存放 ptcp 文件
    StringServer server("server01", "server_ptcp");
    server.Run("127.0.0.1", 12345);
}