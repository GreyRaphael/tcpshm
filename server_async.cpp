#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <deque>
#include <exception>
#include <iguana/iguana.hpp>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "common.h"
#include "tcpshm/msg_header.h"
#include "tcpshm/tcpshm_server.h"

using namespace std;
using namespace tcpshm;

struct ConnMeta {
    uint64_t conn_id = 0;
    uint32_t grpid = 0;
    bool is_shm = false;
};

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

    using ConnectionUserData = ConnMeta;
};

class AsyncServer;
using TSAsyncServer = TcpShmServer<AsyncServer, AsyncServerConf>;
static std::atomic<bool> stopped{false};

// 方案 A：I/O 轮询线程与业务发送线程完全解耦
// - I/O 线程（PollTcp / PollShm）：只负责收包投递 rx_queue_ 并从各自 GroupChannel::tx_queue 取出调用 Alloc/Push
// - 业务线程（biz_thread）：只负责消费 rx_queue_、序列化响应并投递到目标 GroupChannel::tx_queue，绝不直接触碰 Connection*
// - 因此 TCP 与 SHM 均无需对连接或 PollTcp 加 tcp_mtx，且避免了断线 Release() UAF
class AsyncServer : public TSAsyncServer {
   public:
    AsyncServer(const string& name, const string& ptcp_dir) : TSAsyncServer(name, ptcp_dir) {
        signal(SIGTERM, AsyncServer::SignalHandler);
        signal(SIGINT, AsyncServer::SignalHandler);
    }

    static void SignalHandler(int) noexcept { stopped.store(true, std::memory_order_release); }

    void OnSystemError(const char* errno_msg, int /*sys_errno*/) {
        PrintLine("System Error: ", errno_msg);
    }

    int OnNewConnection(const struct sockaddr_in& addr, const LoginMsg* login, LoginRspMsg* /*login_rsp*/) {
        char ipbuf[INET_ADDRSTRLEN]{};
        inet_ntop(AF_INET, &addr.sin_addr, ipbuf, sizeof(ipbuf));
        bool use_shm = (login->use_shm != 0);
        PrintLine("New Connection: ", login->client_name, " from ", ipbuf, ", use_shm=", use_shm);
        // OnNewConnection 与紧随其后的 OnClientLogon 均在同一条 PollCtl 控制线程内串行执行
        pending_is_shm_ = use_shm;
        pending_grpid_ = 0;
        return 0;
    }

    void OnClientLogon(const struct sockaddr_in& /*addr*/, Connection& conn) {
        uint64_t id = next_conn_id_++;
        ConnMeta meta{id, pending_grpid_, pending_is_shm_};
        conn.user_data = meta;

        GroupChannel& ch = GetChannel(meta.is_shm, meta.grpid);
        {
            std::lock_guard<std::mutex> lock(ch.mtx);
            ch.conns[id] = &conn;
        }
        PrintLine("Client Logged on: ", conn.GetRemoteName(), " (conn_id=", id,
                  ", mode=", (meta.is_shm ? "SHM" : "TCP"), ")");
    }

    void OnClientDisconnected(Connection& conn, const char* reason, int /*sys_errno*/) {
        const ConnMeta meta = conn.user_data;
        GroupChannel& ch = GetChannel(meta.is_shm, meta.grpid);
        {
            std::lock_guard<std::mutex> lock(ch.mtx);
            ch.conns.erase(meta.conn_id);
        }
        PrintLine("Client Disconnected: ", conn.GetRemoteName(), " (conn_id=", meta.conn_id, ") Reason: ", reason);
    }

    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        if (recv_header->size < sizeof(MsgHeader)) {
            conn.Pop();
            return;
        }
        auto body_size = recv_header->size - sizeof(MsgHeader);
        std::string_view view{reinterpret_cast<const char*>(recv_header + 1), body_size};
        Request req{};
        try {
            iguana::from_pb(req, view);
        } catch (const std::exception& e) {
            PrintLine("Parse Request failed: ", e.what());
            conn.Pop();
            return;
        }
        conn.Pop();

        const ConnMeta meta = conn.user_data;
        {
            std::lock_guard<std::mutex> lock(rx_mtx_);
            rx_queue_.push_back(InboundMsg{meta.conn_id, meta.grpid, meta.is_shm, std::move(req)});
        }
        rx_cv_.notify_one();
    }

    void OnClientFileError(Connection& /*conn*/, const char* r, int e) {
        PrintLine("OnClientFileError ", e, " msg: ", r);
    }

    void OnSeqNumberMismatch(Connection& /*c*/, uint32_t /*a*/, uint32_t /*b*/, uint32_t /*d*/,
                             uint32_t /*e*/, uint32_t /*f*/, uint32_t /*g*/) {}

    void Run(const char* ip, uint16_t port) {
        if (!Start(ip, port)) return;

        std::thread biz_thread(&AsyncServer::BusinessLoop, this);

        vector<thread> io_threads;
        io_threads.reserve(AsyncServerConf::MaxTcpGrps + AsyncServerConf::MaxShmGrps);

        for (uint32_t i = 0; i < AsyncServerConf::MaxTcpGrps; ++i) {
            io_threads.emplace_back([this, grpid = static_cast<int>(i)]() {
                GroupChannel& ch = tcp_channels_[static_cast<uint32_t>(grpid)];
                while (!stopped.load(std::memory_order_acquire)) {
                    PollTcp(get_timestamp(), grpid);
                    DrainTx(ch);
                    std::this_thread::yield();
                }
            });
        }

        for (uint32_t i = 0; i < AsyncServerConf::MaxShmGrps; ++i) {
            io_threads.emplace_back([this, grpid = static_cast<int>(i)]() {
                GroupChannel& ch = shm_channels_[static_cast<uint32_t>(grpid)];
                while (!stopped.load(std::memory_order_acquire)) {
                    PollShm(grpid);
                    DrainTx(ch);
                    std::this_thread::yield();
                }
            });
        }

        PrintLine("AsyncServer (Plan A) is running on ", ip, ":", port, "...");
        while (!stopped.load(std::memory_order_acquire)) {
            PollCtl(get_timestamp());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        {
            std::lock_guard<std::mutex> lock(rx_mtx_);
        }
        rx_cv_.notify_all();

        if (biz_thread.joinable()) biz_thread.join();
        for (auto& t : io_threads) {
            if (t.joinable()) t.join();
        }
        Stop();
        PrintLine("AsyncServer stopped gracefully.");
    }

   private:
    struct InboundMsg {
        uint64_t conn_id;
        uint32_t grpid;
        bool is_shm;
        Request req;
    };

    struct OutboundMsg {
        uint64_t conn_id;
        uint16_t msg_type;
        std::string payload;
    };

    struct GroupChannel {
        std::mutex mtx;
        std::atomic<bool> has_tx{false};
        std::unordered_map<uint64_t, Connection*> conns;
        std::deque<OutboundMsg> tx_queue;
    };

    GroupChannel& GetChannel(bool is_shm, uint32_t grpid) {
        return is_shm ? shm_channels_[grpid] : tcp_channels_[grpid];
    }

    void BusinessLoop() {
        while (true) {
            std::deque<InboundMsg> batch;
            {
                std::unique_lock<std::mutex> lock(rx_mtx_);
                rx_cv_.wait(lock, [this]() {
                    return stopped.load(std::memory_order_acquire) || !rx_queue_.empty();
                });
                if (rx_queue_.empty() && stopped.load(std::memory_order_acquire)) {
                    break;
                }
                batch.swap(rx_queue_);
            }

            for (auto& msg : batch) {
                std::visit(overload{
                               [&](const Login& login) {
                                   PrintLine("Business handle conn_id=", msg.conn_id,
                                             " name=", login.name, ",pwd=", login.password);
                                   Response rsp = (login.name == "gewei")
                                                      ? Response{LoginResponse{0, "success"}}
                                                      : Response{LoginResponse{-1, "failure"}};
                                   EnqueueResponse(msg.is_shm, msg.grpid, msg.conn_id, rsp);
                               },
                               [&](const Order&) {},
                               [&](const auto&) {}},
                           msg.req.message_type);
            }
        }
    }

    void EnqueueResponse(bool is_shm, uint32_t grpid, uint64_t conn_id, const Response& rsp) {
        std::string body;
        iguana::to_pb(rsp, body);
        if (body.size() > 65535 - sizeof(MsgHeader)) {
            PrintLine("Response too large for conn_id=", conn_id);
            return;
        }
        GroupChannel& ch = GetChannel(is_shm, grpid);
        {
            std::lock_guard<std::mutex> lock(ch.mtx);
            ch.tx_queue.push_back(OutboundMsg{conn_id, 1, std::move(body)});
            ch.has_tx.store(true, std::memory_order_release);
        }
    }

    // 仅由绑定该 Group 的唯一 I/O 线程调用，独占执行 Alloc/Push，不阻塞 PollCtl
    void DrainTx(GroupChannel& ch) {
        if (!ch.has_tx.load(std::memory_order_acquire)) return;

        std::lock_guard<std::mutex> lock(ch.mtx);
        ch.has_tx.store(false, std::memory_order_relaxed);
        while (!ch.tx_queue.empty()) {
            OutboundMsg& out = ch.tx_queue.front();
            auto it = ch.conns.find(out.conn_id);
            if (it != ch.conns.end() && !it->second->IsClosed()) {
                SendRaw(*it->second, out.msg_type, out.payload);
            }
            ch.tx_queue.pop_front();
        }
    }

    void SendRaw(Connection& conn, uint16_t msg_type, const std::string& payload) {
        auto* hdr = conn.Alloc(static_cast<uint16_t>(payload.size()));
        if (!hdr) {
            PrintLine("Send buffer full for ", conn.GetRemoteName());
            return;
        }
        hdr->msg_type = msg_type;
        if (!payload.empty()) {
            std::memcpy(hdr + 1, payload.data(), payload.size());
        }
        conn.Push();
    }

    template <typename... Args>
    void PrintLine(Args&&... args) {
        std::lock_guard<std::mutex> lock(io_mtx_);
        (cout << ... << std::forward<Args>(args)) << endl;
    }

    uint64_t next_conn_id_ = 1;
    uint32_t pending_grpid_ = 0;
    bool pending_is_shm_ = false;

    std::mutex rx_mtx_;
    std::condition_variable rx_cv_;
    std::deque<InboundMsg> rx_queue_;

    GroupChannel shm_channels_[AsyncServerConf::MaxShmGrps]{};
    GroupChannel tcp_channels_[AsyncServerConf::MaxTcpGrps]{};
    std::mutex io_mtx_;
};

int main() {
    AsyncServer server("server01", "server_ptcp");
    server.Run("127.0.0.1", 12345);
}
