#include <atomic>
#include <chrono>
#include <deque>
#include <exception>
#include <iguana/iguana.hpp>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
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
        cout << "System Error: " << error_msg << endl;
    }

    void OnLoginReject(const LoginRspMsg* login_rsp) {
        cout << "Login Rejected: " << login_rsp->error_msg << endl;
    }

    int64_t OnLoginSuccess(const LoginRspMsg* /*login_rsp*/) {
        cout << "Login Success" << endl;
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
                                   cout << "code=" << msg.error_code << ",msg=" << msg.error_message << endl;
                               },
                               [&](auto&) {}},
                           rsp.message_type);
            } catch (const std::exception& e) {
                cout << "Parse Response failed: " << e.what() << endl;
            }
        }
        conn.Pop();
    }

    void OnDisconnected(const char* reason, int /*sys_errno*/) {
        cout << "Disconnected: " << reason << endl;
    }

    void OnSeqNumberMismatch(uint32_t /*local_ack*/, uint32_t /*local_start*/, uint32_t /*local_end*/,
                             uint32_t /*remote_ack*/, uint32_t /*remote_start*/, uint32_t /*remote_end*/) {
        cout << "Sequence mismatch detected." << endl;
    }

    // --- 业务逻辑 ---

    void Run(const char* server_ip, uint16_t port, bool use_shm) {
        if (!Connect(use_shm, server_ip, port, 0)) return;

        std::mutex tx_mtx;
        std::deque<std::string> tx_queue;
        std::atomic<bool> quit{false};

        // 轮询线程：独占调用 Alloc/Push/Front/Pop/Close
        thread poll_thr([this, use_shm, &tx_mtx, &tx_queue, &quit]() {
            while (!conn.IsClosed()) {
                if (use_shm) PollShm();
                PollTcp(get_timestamp());
                if (conn.IsClosed()) break;

                std::deque<std::string> batch;
                {
                    std::lock_guard<std::mutex> lock(tx_mtx);
                    batch.swap(tx_queue);
                }
                for (const auto& msg : batch) {
                    Send(msg);
                }
                if (quit.load(std::memory_order_acquire)) {
                    conn.Close();
                    PollTcp(get_timestamp());
                    break;
                }
                this_thread::sleep_for(chrono::microseconds(100));
            }
        });

        cout << "Enter text to send (type 'exit' to stop):" << endl;
        string input;
        while (!conn.IsClosed()) {
            cout << "> ";
            if (!getline(cin, input) || input == "exit") break;
            {
                std::lock_guard<std::mutex> lock(tx_mtx);
                tx_queue.push_back(std::move(input));
            }
        }

        quit.store(true, std::memory_order_release);
        if (poll_thr.joinable()) poll_thr.join();
        Stop();
    }

   private:
    void Send(const string& s) {
        auto login_req = Request{Login{s, "123456"}};
        std::string out;
        iguana::to_pb(login_req, out);

        if (out.size() > 65535 - sizeof(MsgHeader)) {
            cout << "Message too large!" << endl;
            return;
        }
        auto header = conn.Alloc(static_cast<uint16_t>(out.size()));
        if (!header) {
            cout << "Send buffer full!" << endl;
            return;
        }
        header->msg_type = 1;
        memcpy(header + 1, out.data(), out.size());

        conn.Push();
    }

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