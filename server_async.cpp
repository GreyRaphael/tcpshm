#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iguana/iguana.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <variant>

#include "common.h"
#include "tcpshm/msg_header.h"
#include "tcpshm/spsc_varq.h"
#include "tcpshm/tcpshm_server.h"

using namespace std;
using namespace tcpshm;

struct AsyncServerConf : public CommonConf {
    static constexpr int64_t NanoInSecond = 1000000000LL;

    static constexpr uint32_t MaxNewConnections = 5;
    static constexpr uint32_t MaxShmConnsPerGrp = 4;
    static constexpr uint32_t MaxShmGrps = 1;
    static constexpr uint32_t MaxTcpConnsPerGrp = 4;
    static constexpr uint32_t MaxTcpGrps = 1;

    static constexpr uint32_t TcpQueueSize = 3000;        // must be a multiple of 8
    static constexpr uint32_t TcpRecvBufInitSize = 1000;  // must be a multiple of 8
    static constexpr uint32_t TcpRecvBufMaxSize = 2000;   // must be a multiple of 8
    static constexpr bool TcpNoDelay = true;

    static constexpr int64_t NewConnectionTimeout = 3 * NanoInSecond;
    static constexpr int64_t ConnectionTimeout = 10 * NanoInSecond;
    static constexpr int64_t HeartBeatInverval = 3 * NanoInSecond;

    static constexpr uint32_t QueueBytes = 64 * 1024;  // 内部线程间无锁队列大小（64 的倍数且块数为 2 的幂）

    using ConnectionUserData = uint32_t;  // 直接存放单调递增的 conn_id（0 表示空闲）
};

class AsyncServer;
using TSAsyncServer = TcpShmServer<AsyncServer, AsyncServerConf>;
static std::atomic<bool> stopped{false};

// 极简无锁版方案 A（2 个线程 + 2 条无锁 SPSCVarQueue，全程零 mutex / 零动态容器）：
// 1. 主线程（I/O 线程）：统一运行 PollTcp / PollShm / PollCtl / DrainTx 并独占管理 conns_，天然无连接锁与断线 Release() UAF
// 2. 子线程（biz_thread）：只读 rx_q_、反序列化并执行业务、序列化写回 tx_q_，绝不直接触碰 Connection*
// 3. 路由技巧：SPSCVarQueue 本身只关心 MsgHeader::size，因此内部 rx_q_/tx_q_ 可直接复用 MsgHeader::ack_seq 携带 conn_id
//    （与 TcpShmConnection 底层对端网络传输用的 ack_seq 相互独立，互不干扰）
class AsyncServer : public TSAsyncServer {
   public:
    AsyncServer(const string& name, const string& ptcp_dir) : TSAsyncServer(name, ptcp_dir) {
        signal(SIGTERM, AsyncServer::SignalHandler);
        signal(SIGINT, AsyncServer::SignalHandler);
    }

    static void SignalHandler(int) noexcept { stopped.store(true, std::memory_order_release); }

    void OnSystemError(const char* errno_msg, int sys_errno) {
        std::fprintf(stderr, "System Error: %s (errno=%d)\n", errno_msg, sys_errno);
    }

    int OnNewConnection(const struct sockaddr_in& addr, const LoginMsg* login, LoginRspMsg* /*login_rsp*/) {
        char ipbuf[INET_ADDRSTRLEN]{};
        inet_ntop(AF_INET, &addr.sin_addr, ipbuf, sizeof(ipbuf));
        std::fprintf(stdout, "New Connection: %s from %s, use_shm=%d\n",
                     login->client_name, ipbuf, login->use_shm ? 1 : 0);
        return 0;  // 全部客户端默认路由到 group 0
    }

    void OnClientLogon(const struct sockaddr_in& /*addr*/, Connection& conn) {
        for (uint32_t i = 0; i < MaxTotalConns; ++i) {
            if (!conns_[i]) {
                uint32_t id = next_conn_id_++;
                if (next_conn_id_ == 0) next_conn_id_ = 1;  // 跳过保留值 0
                conn.user_data = id;
                conns_[i] = &conn;
                std::fprintf(stdout, "Client Logged on: %s (conn_id=%u)\n", conn.GetRemoteName(), id);
                return;
            }
        }
    }

    // 与 OnClientLogon / DrainTx 同在主线程，摘除指针后框架再执行 conn.Release()，绝无悬垂指针
    void OnClientDisconnected(Connection& conn, const char* reason, int /*sys_errno*/) {
        const uint32_t id = conn.user_data;
        for (uint32_t i = 0; i < MaxTotalConns; ++i) {
            if (conns_[i] == &conn) {
                conns_[i] = nullptr;
                break;
            }
        }
        conn.user_data = 0;
        std::fprintf(stdout, "Client Disconnected: %s (conn_id=%u) Reason: %s\n", conn.GetRemoteName(), id, reason);
    }

    // I/O 主线程收包：零解析、零锁，原封不动写入 rx_q_ 后立即 Pop() 释放接收环空间
    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        if (recv_header->size >= sizeof(MsgHeader)) {
            const size_t body_len = static_cast<size_t>(recv_header->size) - sizeof(MsgHeader);
            PushQueue(rx_q_, conn.user_data, recv_header->msg_type, recv_header + 1, body_len);
        }
        conn.Pop();
    }

    void OnClientFileError(Connection& /*conn*/, const char* r, int e) {
        std::fprintf(stderr, "OnClientFileError %d msg: %s\n", e, r);
    }

    void OnSeqNumberMismatch(Connection& /*c*/, uint32_t /*a*/, uint32_t /*b*/, uint32_t /*d*/,
                             uint32_t /*e*/, uint32_t /*f*/, uint32_t /*g*/) {}

    void Run(const char* ip, uint16_t port) {
        if (!Start(ip, port)) return;

        std::thread biz_thread(&AsyncServer::BusinessLoop, this);
        std::fprintf(stdout, "AsyncServer (2-Thread Lock-Free Plan A) is running on %s:%u...\n", ip, port);
        std::fflush(stdout);

        while (!stopped.load(std::memory_order_acquire)) {
            const int64_t ts = get_timestamp();
            for (int i = 0; i < static_cast<int>(AsyncServerConf::MaxTcpGrps); ++i) PollTcp(ts, i);
            for (int i = 0; i < static_cast<int>(AsyncServerConf::MaxShmGrps); ++i) PollShm(i);
            PollCtl(ts);
            DrainTx();  // 消费业务线程产出的响应，在主线程统一调用 Alloc/Push 发出
            std::this_thread::yield();
        }

        if (biz_thread.joinable()) biz_thread.join();
        Stop();
        std::fprintf(stdout, "AsyncServer stopped gracefully.\n");
        std::fflush(stdout);
    }

   private:
    static constexpr uint32_t MaxTotalConns =
        AsyncServerConf::MaxShmConnsPerGrp * AsyncServerConf::MaxShmGrps +
        AsyncServerConf::MaxTcpConnsPerGrp * AsyncServerConf::MaxTcpGrps;

    // 复用 SPSCVarQueue 头部：size 由 Alloc 设置，ack_seq 携带 conn_id，msg_type 携带业务消息类型
    static void PushQueue(SPSCVarQueue<AsyncServerConf::QueueBytes>& q,
                          uint32_t conn_id,
                          uint16_t msg_type,
                          const void* data,
                          size_t len) {
        if (len > 65535u - sizeof(MsgHeader)) return;
        const auto sz = static_cast<uint16_t>(len);
        MsgHeader* hdr = q.Alloc(sz);
        for (int spin = 0; !hdr && spin < 1000 && !stopped.load(std::memory_order_acquire); ++spin) {
            std::this_thread::yield();
            hdr = q.Alloc(sz);
        }
        if (!hdr) return;
        hdr->msg_type = msg_type;
        hdr->ack_seq = conn_id;
        if (len > 0) std::memcpy(hdr + 1, data, len);
        q.Push();
    }

    // 业务子线程：从 rx_q_ 取请求 -> 反序列化与处理 -> 序列化后写入 tx_q_
    void BusinessLoop() {
        std::string out_buf;
        out_buf.reserve(256);

        while (!stopped.load(std::memory_order_acquire)) {
            MsgHeader* hdr = rx_q_.Front();
            if (!hdr) {
                std::this_thread::yield();
                continue;
            }
            if (hdr->size < sizeof(MsgHeader)) {
                rx_q_.Pop();
                continue;
            }
            const uint32_t conn_id = hdr->ack_seq;
            const size_t body_len = static_cast<size_t>(hdr->size) - sizeof(MsgHeader);
            std::string_view view{reinterpret_cast<const char*>(hdr + 1), body_len};
            Request req{};
            bool ok = true;
            try {
                iguana::from_pb(req, view);
            } catch (const std::exception&) {
                ok = false;
            }
            rx_q_.Pop();
            if (!ok) continue;

            std::visit(overload{
                           [&](const Login& login) {
                               std::fprintf(stdout, "Business handle conn_id=%u name=%s,pwd=%s\n",
                                            conn_id, login.name.c_str(), login.password.c_str());
                               Response rsp = (login.name == "gewei")
                                                  ? Response{LoginResponse{0, "success"}}
                                                  : Response{LoginResponse{-1, "failure"}};
                               out_buf.clear();
                               iguana::to_pb(rsp, out_buf);
                               PushQueue(tx_q_, conn_id, 1, out_buf.data(), out_buf.size());
                           },
                           [&](const Order&) {},
                           [&](const auto&) {}},
                       req.message_type);
        }
    }

    // I/O 主线程发包：按 conn_id 找到仍在线的连接，将 tx_q_ 响应拷贝至发送队列并发出
    void DrainTx() {
        while (MsgHeader* hdr = tx_q_.Front()) {
            if (hdr->size >= sizeof(MsgHeader)) {
                const uint32_t conn_id = hdr->ack_seq;
                const size_t len = static_cast<size_t>(hdr->size) - sizeof(MsgHeader);
                for (Connection* conn : conns_) {
                    if (conn && conn->user_data == conn_id && !conn->IsClosed()) {
                        if (auto* out_hdr = conn->Alloc(static_cast<uint16_t>(len))) {
                            out_hdr->msg_type = hdr->msg_type;
                            if (len > 0) std::memcpy(out_hdr + 1, hdr + 1, len);
                            conn->Push();
                        }
                        break;
                    }
                }
            }
            tx_q_.Pop();
        }
    }

    uint32_t next_conn_id_ = 1;
    Connection* conns_[MaxTotalConns]{};

    SPSCVarQueue<AsyncServerConf::QueueBytes> rx_q_;
    SPSCVarQueue<AsyncServerConf::QueueBytes> tx_q_;
};

int main() {
    auto server = std::make_unique<AsyncServer>("server01", "server_ptcp");
    server->Run("127.0.0.1", 12345);
}
