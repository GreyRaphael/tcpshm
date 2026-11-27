#include "common.hpp"
#include "tcpshm/tcpshm.hpp"

#include <arpa/inet.h>
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

static std::atomic<bool> stopped{false};

void signal_handler(int) noexcept {
    stopped.store(true, std::memory_order_release);
}

// Lock-free 2-thread async architecture powered by tcpshm v2 native generational ConnHandle + SpscQueue:
// 1. Main I/O thread: polls TCP/SHM/Ctl, pushes [ConnHandle + payload] into rx_q_, drains tx_q_ via server.with_channel().
// 2. Child biz thread: batch-drains rx_q_, runs protobuf deserialization/logic, stages & flushes [ConnHandle + reply] into tx_q_.
// Zero mutexes, zero dynamic containers, zero manual connection table, ABA-safe across disconnect/reconnect.
class AsyncEchoApp
{
public:
    static constexpr uint32_t kInternalQueueBytes = 64u * 1024u;

    AsyncEchoApp(std::string_view server_name, std::string_view ptcp_dir)
        : server_(*this, server_name, ptcp_dir) {}

    void on_system_error(tcpshm::Error err) {
        std::fprintf(stderr, "System Error: %s (errno=%d)\n", err.message().data(), err.sys_errno);
    }

    int on_new_connection(const struct sockaddr_in& addr,
                          const tcpshm::detail::LoginMsg<ExampleConfig>& login,
                          tcpshm::detail::LoginRspMsg<ExampleConfig>& /*rsp*/) {
        char ipbuf[INET_ADDRSTRLEN]{};
        ::inet_ntop(AF_INET, &addr.sin_addr, ipbuf, sizeof(ipbuf));
        std::fprintf(stdout, "New Connection: %s from %s, use_shm=%d\n",
                     login.client_name, ipbuf, login.use_shm ? 1 : 0);
        return 0;
    }

    void on_client_logon(const struct sockaddr_in& /*addr*/, auto& ch) {
        std::fprintf(stdout, "Client Logged on: %s (handle=%llu)\n",
                     ch.remote_name(), static_cast<unsigned long long>(ch.handle().to_u64()));
    }

    void on_client_disconnected(auto& ch, tcpshm::Error reason) {
        std::fprintf(stdout, "Client Disconnected: %s (handle=%llu) Reason: %s\n",
                     ch.remote_name(),
                     static_cast<unsigned long long>(ch.handle().to_u64()),
                     reason.message().data());
    }

    void on_message(auto& ch, tcpshm::MsgView msg) {
        enqueue_frame(rx_q_, ch.handle(), msg.type(), msg.payload());
    }

    int run(std::string_view ip, uint16_t port) {
        if (auto res = server_.start(ip, port); !res) {
            on_system_error(res.error());
            return 1;
        }

        std::thread biz_thread(&AsyncEchoApp::business_loop, this);
        std::fprintf(stdout, "AsyncEchoServer (v2, native ConnHandle + SpscQueue) listening on %.*s:%u...\n",
                     static_cast<int>(ip.size()), ip.data(), port);
        std::fflush(stdout);

        while (!stopped.load(std::memory_order_acquire)) {
            const auto now = tcpshm::steady_now();
            for (uint32_t g = 0; g < ExampleConfig::max_tcp_groups; ++g) server_.poll_tcp(now, g);
            for (uint32_t g = 0; g < ExampleConfig::max_shm_groups; ++g) server_.poll_shm(g);
            server_.poll_ctl(now);
            drain_tx();
            std::this_thread::yield();
        }

        if (biz_thread.joinable()) biz_thread.join();
        server_.stop();
        std::fprintf(stdout, "AsyncEchoServer stopped gracefully.\n");
        return 0;
    }

private:
    static bool enqueue_frame(tcpshm::SpscQueue<kInternalQueueBytes>& q,
                              tcpshm::ConnHandle handle,
                              uint16_t msg_type,
                              std::span<const std::byte> body,
                              bool batch_stage = false) {
        const size_t total = sizeof(tcpshm::ConnHandle) + body.size();
        if (total > tcpshm::kMaxPayloadSize) return false;
        const auto sz = static_cast<uint32_t>(total);

        auto tx = q.try_alloc(msg_type, sz);
        for (int spin = 0; !tx && spin < 1000 && !stopped.load(std::memory_order_acquire); ++spin) {
            q.flush();
            std::this_thread::yield();
            tx = q.try_alloc(msg_type, sz);
        }
        if (!tx) return false;
        tx->write_pod(handle);
        if (!body.empty()) {
            std::memcpy(tx->data() + sizeof(tcpshm::ConnHandle), body.data(), body.size());
        }
        if (batch_stage) tx->stage();
        else tx->commit();
        return true;
    }

    void business_loop() {
        std::string out_buf;
        out_buf.reserve(256);

        while (!stopped.load(std::memory_order_acquire)) {
            const uint32_t n = rx_q_.drain(
                [&](tcpshm::MsgView msg) {
                    if (msg.payload_size() < sizeof(tcpshm::ConnHandle)) return;
                    tcpshm::ConnHandle h{};
                    std::memcpy(&h, msg.data(), sizeof(h));
                    const std::string_view pb_view{
                        reinterpret_cast<const char*>(msg.data() + sizeof(tcpshm::ConnHandle)),
                        msg.payload_size() - sizeof(tcpshm::ConnHandle)};

                    Request req{};
                    try {
                        iguana::from_pb(req, pb_view);
                    } catch (const std::exception&) {
                        return;
                    }

                    std::visit(
                        overload{
                            [&](const Login& login) {
                                std::fprintf(stdout, "Business handle=%llu name=%s,pwd=%s\n",
                                             static_cast<unsigned long long>(h.to_u64()),
                                             login.name.c_str(),
                                             login.password.c_str());
                                Response rsp = (login.name == "gewei")
                                                   ? Response{LoginResponse{0, "success"}}
                                                   : Response{LoginResponse{-1, "failure"}};
                                out_buf.clear();
                                iguana::to_pb(rsp, out_buf);
                                const auto bytes = std::span<const std::byte>{
                                    reinterpret_cast<const std::byte*>(out_buf.data()), out_buf.size()};
                                enqueue_frame(tx_q_, h, tcpshm::kFirstAppMsgType, bytes, true);
                            },
                            [&](const Order&) {},
                            [&](const auto&) {},
                        },
                        req.message_type);
                },
                32);

            tx_q_.flush();
            if (n == 0) std::this_thread::yield();
        }
    }

    void drain_tx() {
        tx_q_.drain(
            [this](tcpshm::MsgView item) {
                if (item.payload_size() < sizeof(tcpshm::ConnHandle)) return;
                tcpshm::ConnHandle h{};
                std::memcpy(&h, item.data(), sizeof(h));
                const std::span<const std::byte> body{
                    item.data() + sizeof(tcpshm::ConnHandle),
                    item.payload_size() - sizeof(tcpshm::ConnHandle)};

                server_.with_channel(h, [&](auto& ch) {
                    if (body.size() > tcpshm::kMaxPayloadSize) return;
                    if (auto tx = ch.try_alloc(item.type(), static_cast<uint32_t>(body.size()))) {
                        tx->write_bytes(body);
                        tx->commit();
                    }
                });
            },
            32);
    }

    tcpshm::Server<AsyncEchoApp, ExampleConfig> server_;
    tcpshm::SpscQueue<kInternalQueueBytes> rx_q_;
    tcpshm::SpscQueue<kInternalQueueBytes> tx_q_;
};

int main() {
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGINT, signal_handler);

    auto app = std::make_unique<AsyncEchoApp>("server01", "server_ptcp");
    return app->run("127.0.0.1", 12345);
}
