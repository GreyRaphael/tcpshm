#include <chrono>
#include <iguana/iguana.hpp>
#include <iostream>
#include <print>
#include <string>
#include <string_view>
#include <thread>
#include <variant>

#include "common.h"
#include "msg_header.h"
#include "tcpshm_client.h"

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
    StringClient(const string& ptcp_dir, const string& name)
        : TSClient(ptcp_dir, name), conn(GetConnection()) {}

    // --- 所有的回调函数现在都是 public，不再需要 friend 声明 ---

    void OnSystemError(const char* error_msg, int sys_errno) {
        cout << "System Error: " << error_msg << endl;
    }

    void OnLoginReject(const LoginRspMsg* login_rsp) {
        cout << "Login Rejected: " << login_rsp->error_msg << endl;
    }

    int64_t OnLoginSuccess(const LoginRspMsg* login_rsp) {
        cout << "Login Success" << endl;
        return GetTimestamp();  // 返回当前纳秒
    }

    void OnServerMsg(MsgHeader* header) {
        auto body_size = header->size - sizeof(MsgHeader);
        if (header->msg_type == 1) {
            auto view = std::string_view{reinterpret_cast<char const*>(header + 1), body_size};
            Response rsp{};
            iguana::from_pb(rsp, view);
            std::visit(overload{
                           [&](LoginResponse& msg) {
                               std::println("code={},msg={}", msg.error_code, msg.error_message);
                           },
                           [&](auto& msg) {}},
                       rsp.message_type);
        }
        conn.Pop();
    }

    void OnDisconnected(const char* reason, int sys_errno) {
        cout << "Disconnected: " << reason << endl;
    }

    void OnSeqNumberMismatch(uint32_t local_ack, uint32_t local_start, uint32_t local_end,
                             uint32_t remote_ack, uint32_t remote_start, uint32_t remote_end) {
        cout << "Sequence mismatch detected." << endl;
    }

    // --- 业务逻辑 ---

    void Run(const char* server_ip, uint16_t port, bool use_shm) {
        if (!Connect(use_shm, server_ip, port, 0)) return;

        // 轮询线程：不再使用 cpupin，降低对系统的独占
        thread poll_thr([this, use_shm]() {
            while (!conn.IsClosed()) {
                if (use_shm) PollShm();
                PollTcp(GetTimestamp());
                // 适当休眠防止 100% CPU 占用（如果不追求极致延迟）
                this_thread::sleep_for(chrono::microseconds(100));
            }
        });

        cout << "Enter text to send (type 'exit' to stop):" << endl;
        string input;
        while (true) {
            cout << "> ";
            if (!getline(cin, input) || input == "exit") break;
            Send(input);
        }

        conn.Close();
        if (poll_thr.joinable()) poll_thr.join();
        Stop();
    }

   private:
    void Send(const string& s) {
        auto login_req = Request{Login{s, "123456"}};
        std::string out;
        iguana::to_pb(login_req, out);

        auto header = conn.Alloc(out.size());
        if (!header) {
            cout << "Send buffer full!" << endl;
            return;
        }
        header->msg_type = 1;
        memcpy(header + 1, out.data(), out.size());

        conn.Push();
    }

    // 使用 std::chrono 获取当前纳秒数
    int64_t GetTimestamp() {
        return std::chrono::system_clock::now().time_since_epoch().count();
    }

    Connection& conn;
};

int main(int argc, const char** argv) {
    if (argc < 3) {
        std::cout << "usage: ./client name use_shm[0|1]\n";
        return -1;
    }
    StringClient client(argv[1], argv[1]);
    bool use_shm = argv[2][0] != '0';
    client.Run("127.0.0.1", 12345, use_shm);
}