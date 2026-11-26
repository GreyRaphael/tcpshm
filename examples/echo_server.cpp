#include "common.hpp"
#include "tcpshm/tcpshm.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <iguana/iguana.hpp>
#include <iostream>
#include <string>
#include <thread>
#include <variant>
#include <vector>

static std::atomic<bool> stopped{false};

void signal_handler(int) noexcept {
    stopped.store(true, std::memory_order_release);
}

class EchoServerHandler
{
public:
    void on_system_error(tcpshm::Error err) {
        std::cout << "System Error: " << err.message() << " (errno=" << err.sys_errno << ")\n";
    }

    int on_new_connection(const struct sockaddr_in& addr,
                          const tcpshm::detail::LoginMsg<ExampleConfig>& login,
                          tcpshm::detail::LoginRspMsg<ExampleConfig>& /*rsp*/) {
        char ipbuf[INET_ADDRSTRLEN]{};
        ::inet_ntop(AF_INET, &addr.sin_addr, ipbuf, sizeof(ipbuf));
        std::cout << "New Connection: " << login.client_name
                  << " from " << ipbuf
                  << ", use_shm=" << static_cast<bool>(login.use_shm) << '\n';
        return 0;
    }

    void on_client_logon(const struct sockaddr_in& /*addr*/, auto& ch) {
        std::cout << "Client Logged on: " << ch.remote_name()
                  << " handle=" << ch.handle().to_u64() << '\n';
    }

    void on_client_disconnected(auto& ch, tcpshm::Error reason) {
        std::cout << "Client Disconnected: " << ch.remote_name()
                  << " Reason: " << reason.message() << '\n';
    }

    void on_client_file_error(auto& ch, tcpshm::Error err) {
        std::cout << "Client File Error [" << ch.remote_name() << "]: "
                  << err.message() << " (errno=" << err.sys_errno << ")\n";
    }

    // Receive path receives MsgView directly; frame is auto-consumed on callback return.
    void on_message(auto& ch, tcpshm::MsgView msg) {
        Request req{};
        try {
            iguana::from_pb(req, msg.payload_str());
        } catch (const std::exception& e) {
            std::cout << "Parse Request failed: " << e.what() << '\n';
            return;
        }

        std::visit(
            overload{
                [&](const Login& login_msg) {
                    std::cout << "name=" << login_msg.name << ",pwd=" << login_msg.password << '\n';
                    Response rsp = (login_msg.name == "gewei")
                                       ? Response{LoginResponse{0, "success"}}
                                       : Response{LoginResponse{-1, "failure"}};
                    write_response(ch, rsp);
                },
                [&](const Order&) {},
                [&](const auto&) {},
            },
            req.message_type);
    }

private:
    static void write_response(auto& ch, const Response& rsp) {
        std::string rsp_body;
        iguana::to_pb(rsp, rsp_body);
        if (rsp_body.size() > tcpshm::kMaxPayloadSize) return;
        if (auto tx = ch.try_alloc(tcpshm::kFirstAppMsgType, static_cast<uint16_t>(rsp_body.size()))) {
            tx->write_str(rsp_body);
            tx->commit();
        }
    }
};

int main() {
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGINT, signal_handler);

    EchoServerHandler handler;
#ifdef __linux__
    tcpshm::Server<EchoServerHandler, ExampleConfig, tcpshm::EpollPoller> server(
        handler, "server01", "server_ptcp");
#else
    tcpshm::Server<EchoServerHandler, ExampleConfig, tcpshm::BusyPoller> server(
        handler, "server01", "server_ptcp");
#endif
    if (auto res = server.start("127.0.0.1", 12345); !res) {
        std::cerr << "Start failed: " << res.error().message() << '\n';
        return 1;
    }

    std::vector<std::thread> threads;
    threads.reserve(ExampleConfig::max_tcp_groups + ExampleConfig::max_shm_groups);
    for (uint32_t g = 0; g < ExampleConfig::max_tcp_groups; ++g) {
        threads.emplace_back([&server, g]() {
            while (!stopped.load(std::memory_order_acquire)) {
                server.poll_tcp(tcpshm::steady_now(), g);
                std::this_thread::yield();
            }
        });
    }
    for (uint32_t g = 0; g < ExampleConfig::max_shm_groups; ++g) {
        threads.emplace_back([&server, g]() {
            while (!stopped.load(std::memory_order_acquire)) {
                server.poll_shm(g);
                std::this_thread::yield();
            }
        });
    }

    std::cout << "EchoServer (v2) is running on 127.0.0.1:12345...\n";
    while (!stopped.load(std::memory_order_acquire)) {
        server.poll_ctl(tcpshm::steady_now());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
    server.stop();
    std::cout << "EchoServer stopped gracefully.\n";
    return 0;
}
