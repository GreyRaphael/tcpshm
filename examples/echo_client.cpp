#include "common.hpp"
#include "tcpshm/tcpshm.hpp"

#include <chrono>
#include <exception>
#include <iguana/iguana.hpp>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <variant>

class EchoClientApp
{
public:
    EchoClientApp(std::string_view name, std::string_view ptcp_dir)
        : client_(*this, name, ptcp_dir) {}

    void on_system_error(tcpshm::Error err) {
        print_line("System Error: ", err.message(), " (errno=", err.sys_errno, ")");
    }

    void on_login_reject(const tcpshm::detail::LoginRspMsg<ExampleConfig>& rsp) {
        print_line("Login Rejected: ", rsp.error_msg);
    }

    tcpshm::TimePoint on_login_success(const tcpshm::detail::LoginRspMsg<ExampleConfig>& rsp) {
        print_line("Login Success with server: ", rsp.server_name);
        return tcpshm::steady_now();
    }

    void on_server_msg(tcpshm::MsgView msg) {
        if (msg.type() == tcpshm::kFirstAppMsgType) {
            Response rsp{};
            try {
                iguana::from_pb(rsp, msg.payload_str());
                std::visit(
                    overload{
                        [&](const LoginResponse& lr) {
                            print_line("code=", lr.error_code, ",msg=", lr.error_message);
                        },
                        [&](const auto&) {},
                    },
                    rsp.message_type);
            } catch (const std::exception& e) {
                print_line("Parse Response failed: ", e.what());
            }
        }
    }

    void on_disconnected(tcpshm::Error reason) {
        print_line("Disconnected: ", reason.message());
    }

    void on_seq_mismatch(const tcpshm::SeqMismatchInfo&) {
        print_line("Sequence mismatch detected.");
    }

    int run(std::string_view server_ip, uint16_t port, bool use_shm) {
        const auto kind = use_shm ? tcpshm::TransportKind::Shm : tcpshm::TransportKind::Tcp;
        if (auto res = client_.connect(kind, server_ip, port); !res) {
            return 1;
        }
        if (use_shm) run_shm();
        else run_tcp();
        client_.stop();
        return 0;
    }

private:
    // SHM: main thread polls recv & keepalive; child send thread locks nothing (dedicated SPSC producer).
    void run_shm() {
        std::thread send_thr([this]() {
            run_input_loop([this](const std::string& input) { send_login(client_.shm_channel(), input); });
        });
        while (!client_.is_closed()) {
            client_.poll_shm();
            client_.poll_tcp(tcpshm::steady_now());
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        client_.poll_tcp(tcpshm::steady_now());
        send_thr.join();
    }

    // TCP: main thread & child thread share TcpChannel state, synchronized by tcp_mtx.
    void run_tcp() {
        std::mutex tcp_mtx;
        std::thread send_thr([this, &tcp_mtx]() {
            run_input_loop([this, &tcp_mtx](const std::string& input) {
                std::lock_guard<std::mutex> lock(tcp_mtx);
                if (!client_.is_closed()) {
                    send_login(client_.tcp_channel(), input);
                }
            });
        });
        while (!client_.is_closed()) {
            {
                std::lock_guard<std::mutex> lock(tcp_mtx);
                client_.poll_tcp(tcpshm::steady_now());
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        std::lock_guard<std::mutex> lock(tcp_mtx);
        client_.poll_tcp(tcpshm::steady_now());
        send_thr.join();
    }

    template<typename SendFn>
    void run_input_loop(SendFn&& send_fn) {
        print_line("Enter text to send (type 'exit' to stop):");
        std::string input;
        while (!client_.is_closed()) {
            print_prompt();
            if (!std::getline(std::cin, input) || input == "exit") break;
            if (!client_.is_closed()) send_fn(input);
        }
        client_.close();
    }

    void send_login(auto& ch, const std::string& name) {
        Request req{Login{name, "123456"}};
        std::string out;
        iguana::to_pb(req, out);
        if (out.size() > tcpshm::kMaxPayloadSize) {
            print_line("Message too large!");
            return;
        }
        auto tx = ch.try_alloc(tcpshm::kFirstAppMsgType, static_cast<uint16_t>(out.size()));
        if (!tx) {
            print_line("Send buffer full!");
            return;
        }
        tx->write_str(out);
        tx->commit();
    }

    template<typename... Args>
    void print_line(Args&&... args) {
        std::lock_guard<std::mutex> lock(io_mtx_);
        (std::cout << ... << std::forward<Args>(args)) << '\n';
    }

    void print_prompt() {
        std::lock_guard<std::mutex> lock(io_mtx_);
        std::cout << "> " << std::flush;
    }

    std::mutex io_mtx_;
    tcpshm::Client<EchoClientApp, ExampleConfig> client_;
};

int main(int argc, const char** argv) {
    if (argc < 3) {
        std::cout << "usage: ./echo_client name use_shm[0|1]\n";
        return 1;
    }
    EchoClientApp app(argv[1], "client_ptcp");
    const bool use_shm = argv[2][0] != '0';
    return app.run("127.0.0.1", 12345, use_shm);
}
