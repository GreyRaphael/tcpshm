#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <iguana/iguana.hpp>
#include <iostream>
#include <print>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

#include "common.h"
#include "msg_header.h"
#include "tcpshm_server.h"

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
    StringServer(const string& ptcp_dir, const string& name) : TSServer(ptcp_dir, name) {
        signal(SIGTERM, StringServer::SignalHandler);
        signal(SIGINT, StringServer::SignalHandler);  // 补充支持 Ctrl+C
    }

    static void SignalHandler(int) noexcept { stopped.store(true, std::memory_order_relaxed); }

    // --- Public 回调函数，移除 friend 依赖 ---

    void OnSystemError(const char* errno_msg, int sys_errno) {
        cout << "System Error: " << errno_msg << endl;
    }

    int OnNewConnection(const struct sockaddr_in& addr, const LoginMsg* login, LoginRspMsg* login_rsp) {
        cout << "New Connection: " << login->client_name << " from " << inet_ntoa(addr.sin_addr) << ", use_shm=" << (bool)login->use_shm << endl;
        // 简单映射到 group 0
        return 0;
    }

    void OnClientLogon(const struct sockaddr_in& addr, Connection& conn) {
        cout << "Client Logged on: " << conn.GetRemoteName() << endl;
    }

    void OnClientDisconnected(Connection& conn, const char* reason, int sys_errno) {
        cout << "Client Disconnected: " << conn.GetRemoteName() << " Reason: " << reason << endl;
    }

    // 核心逻辑：处理客户端发来的字符串并 Echo
    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        auto body_size = recv_header->size - sizeof(MsgHeader);
        std::string_view view{reinterpret_cast<char const*>(recv_header + 1), body_size};
        Request req{};
        iguana::from_pb(req, view);
        conn.Pop();

        std::visit(overload{
                       [&](Login& msg) {
                           std::println("name={},pwd={}", msg.name, msg.password);
                           if (msg.name == "gewei") {
                               auto login_rsp = Response{LoginResponse{0, "success"}};
                               writeResponse(conn, login_rsp);
                           } else {
                               auto login_rsp = Response{LoginResponse{-1, "failure"}};
                               writeResponse(conn, login_rsp);
                           }
                       },
                       [&](Order& msg) {},
                       [&](auto const&) { /* 忽略 Login 或其他类型 */ }},
                   req.message_type);
    }

    // 必要的空白回调
    void OnClientFileError(Connection& conn, const char* r, int e) {
        std::cout << "OnClientFileError" << e << "msg:" << r;
    }
    void OnSeqNumberMismatch(Connection& c, uint32_t a, uint32_t b, uint32_t d, uint32_t e, uint32_t f, uint32_t g) {}

    void writeResponse(Connection& conn, Response const& rsp) {
        std::string rsp_body{};
        iguana::to_pb(rsp, rsp_body);
        auto rsp_header = conn.Alloc(rsp_body.size());
        if (!rsp_header) return;
        rsp_header->msg_type = 1;
        memcpy(rsp_header + 1, rsp_body.data(), rsp_body.size());
        conn.Push();
    }

    void Run(const char* ip, uint16_t port) {
        if (!Start(ip, port)) return;

        vector<thread> threads;
        threads.reserve(ServerConf::MaxTcpGrps + ServerConf::MaxShmGrps);
        // TCP 轮询线程
        for (int i = 0; i < ServerConf::MaxTcpGrps; ++i) {
            threads.emplace_back([this, i]() {
                while (!stopped.load(std::memory_order_relaxed)) {
                    PollTcp(GetTimestamp(), i);
                    std::this_thread::yield();  // 比固定 sleep 更适合低延迟轮询
                }
            });
        }

        // SHM 轮询线程
        for (int i = 0; i < ServerConf::MaxShmGrps; ++i) {
            threads.emplace_back([this, i]() {
                while (!stopped.load(std::memory_order_relaxed)) {
                    PollShm(i);
                    std::this_thread::yield();
                }
            });
        }

        // 主控制循环
        cout << "Server is running on " << ip << ":" << port << "..." << endl;
        while (!stopped.load(std::memory_order_relaxed)) {
            PollCtl(GetTimestamp());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
        cout << "Server stopped gracefully." << endl;
    }

   private:
    int64_t GetTimestamp() {
        return std::chrono::steady_clock::now().time_since_epoch().count();
    }
};

int main() {
    // 确保 server 目录存在，用于存放 ptcp 文件
    StringServer server("server_ptcp", "server01");
    server.Run("127.0.0.1", 12345);
}