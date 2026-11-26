#include <chrono>
#include <cstring>
#include <exception>
#include <functional>
#include <iguana/iguana.hpp>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>

#include "common.h"
#include "tcpshm/msg_header.h"
#include "tcpshm/tcpshm_client.h"

using namespace std;
using namespace tcpshm;

struct ClientConf : public CommonConf {
    static inline const int64_t NanoInSecond = 1000000000LL;

    static inline const uint32_t TcpQueueSize = 2000;        // must be a multiple of 8
    static inline const uint32_t TcpRecvBufInitSize = 1000;  // must be a multiple of 8
    static inline const uint32_t TcpRecvBufMaxSize = 2000;   // must be a multiple of 8
    static inline const bool TcpNoDelay = true;

    static inline const int64_t ConnectionTimeout = 10 * NanoInSecond;
    static inline const int64_t HeartBeatInverval = 3 * NanoInSecond;

    using ConnectionUserData = char;
};

class StringClient;
using TSClient = TcpShmClient<StringClient, ClientConf>;

class StringClient : public TSClient {
   public:
    StringClient(const string& name, const string& ptcp_dir)
        : TSClient(name, ptcp_dir), conn(GetConnection()) {}

    // --- 所有的回调函数现在都是 public，不再需要 friend 声明 ---

    void OnSystemError(const char* error_msg, int /*sys_errno*/) {
        PrintLine("System Error: ", error_msg);
    }

    void OnLoginReject(const LoginRspMsg* login_rsp) {
        PrintLine("Login Rejected: ", login_rsp->error_msg);
    }

    int64_t OnLoginSuccess(const LoginRspMsg* /*login_rsp*/) {
        PrintLine("Login Success");
        return get_timestamp();  // 返回当前纳秒
    }

    void OnServerMsg(MsgHeader* header) {
        if (header->size < sizeof(MsgHeader)) {
            conn.Pop();
            return;
        }
        auto body_size = header->size - sizeof(MsgHeader);
        if (header->msg_type == 1) {
            auto view = std::string_view{reinterpret_cast<char const*>(header + 1), body_size};
            Response rsp{};
            try {
                iguana::from_pb(rsp, view);
                std::visit(overload{
                               [&](LoginResponse& msg) {
                                   PrintLine("code=", msg.error_code, ",msg=", msg.error_message);
                               },
                               [&](auto&) {}},
                           rsp.message_type);
            } catch (const std::exception& e) {
                PrintLine("Parse Response failed: ", e.what());
            }
        }
        conn.Pop();
    }

    void OnDisconnected(const char* reason, int /*sys_errno*/) {
        PrintLine("Disconnected: ", reason);
    }

    void OnSeqNumberMismatch(uint32_t /*local_ack*/, uint32_t /*local_start*/, uint32_t /*local_end*/,
                             uint32_t /*remote_ack*/, uint32_t /*remote_start*/, uint32_t /*remote_end*/) {
        PrintLine("Sequence mismatch detected.");
    }

    // --- 业务逻辑：主线程 Poll 收包 + 单个子线程 Send 发包，并将 SHM（无锁）与 TCP（加锁）彻底分离 ---

    void Run(const char* server_ip, uint16_t port, bool use_shm) {
        if (!Connect(use_shm, server_ip, port, 0)) return;

        if (use_shm) {
            RunShm();
        } else {
            RunTcp();
        }
        Stop();
    }

   private:
    // 1) SHM 链路：主线程执行 PollShm/PollTcp，子线程无锁写入独立的 shm_sendq_（无需 tcp_mtx）
    void RunShm() {
        std::thread shm_send_thread(&StringClient::ShmSendLoop, this);
        ShmRecvLoop();  // 主线程：轮询收包（同步回调 OnServerMsg / OnDisconnected）
        shm_send_thread.join();
    }

    void ShmRecvLoop() {
        while (!conn.IsClosed()) {
            PollShm();
            PollTcp(get_timestamp());  // SHM 模式下仅维持 TCP 心跳与断线检测，不碰 shm_sendq_
            this_thread::sleep_for(chrono::microseconds(100));
        }
        // 连接关闭后补一次 PollTcp()，确保由主线程关闭 fd 并触发 OnDisconnected
        PollTcp(get_timestamp());
    }

    void ShmSendLoop() {
        RunInputLoop([this](const string& input) {
            Send(input);  // 无锁并发：直接写入独立的 shm_sendq_
        });
    }

    // 2) TCP 链路：主线程 PollTcp 与子线程 Send 共用 PTCPConnection，需用 tcp_mtx 串行保护
    void RunTcp() {
        std::mutex tcp_mtx;
        std::thread tcp_send_thread(&StringClient::TcpSendLoop, this, std::ref(tcp_mtx));
        TcpRecvLoop(tcp_mtx);  // 主线程：持锁轮询 PollTcp（同步回调 OnServerMsg / OnDisconnected）
        tcp_send_thread.join();
    }

    void TcpRecvLoop(std::mutex& tcp_mtx) {
        while (!conn.IsClosed()) {
            {
                std::lock_guard<std::mutex> lock(tcp_mtx);
                PollTcp(get_timestamp());
            }
            this_thread::sleep_for(chrono::microseconds(100));
        }
        std::lock_guard<std::mutex> lock(tcp_mtx);
        PollTcp(get_timestamp());
    }

    void TcpSendLoop(std::mutex& tcp_mtx) {
        RunInputLoop([this, &tcp_mtx](const string& input) {
            std::lock_guard<std::mutex> lock(tcp_mtx);
            if (!conn.IsClosed()) Send(input);
        });
    }

    template <typename SendFn>
    void RunInputLoop(SendFn&& send_fn) {
        PrintLine("Enter text to send (type 'exit' to stop):");
        string input;
        while (!conn.IsClosed()) {
            PrintPrompt();
            if (!getline(cin, input) || input == "exit") break;
            if (!conn.IsClosed()) send_fn(input);
        }
        conn.Close();  // RequestClose() 本身原子线程安全，无需加锁
    }

    void Send(const string& s) {
        auto login_req = Request{Login{s, "123456"}};
        std::string out;
        iguana::to_pb(login_req, out);

        if (out.size() > 65535 - sizeof(MsgHeader)) {
            PrintLine("Message too large!");
            return;
        }
        auto header = conn.Alloc(static_cast<uint16_t>(out.size()));
        if (!header) {
            PrintLine("Send buffer full!");
            return;
        }
        header->msg_type = 1;
        std::memcpy(header + 1, out.data(), out.size());

        conn.Push();
    }

    template <typename... Args>
    void PrintLine(Args&&... args) {
        std::lock_guard<std::mutex> lock(io_mtx_);
        (cout << ... << std::forward<Args>(args)) << endl;
    }

    void PrintPrompt() {
        std::lock_guard<std::mutex> lock(io_mtx_);
        cout << "> " << flush;
    }

    std::mutex io_mtx_;
    Connection& conn;
};

int main(int argc, const char** argv) {
    if (argc < 3) {
        std::cout << "usage: ./client name use_shm[0|1]\n";
        return -1;
    }
    StringClient client(argv[1], "client_ptcp");
    bool use_shm = argv[2][0] != '0';
    client.Run("127.0.0.1", 12345, use_shm);
}