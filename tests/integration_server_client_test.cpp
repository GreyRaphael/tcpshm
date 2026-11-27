#include "tcpshm/tcpshm.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

using namespace tcpshm;

#define CHECK(cond)                                                                      \
    do {                                                                                 \
        if (!(cond)) {                                                                   \
            std::cerr << "CHECK failed at line " << __LINE__ << ": " #cond << std::endl; \
            std::exit(1);                                                                \
        }                                                                                \
    } while (0)

struct IntTestConfig : DefaultConfig
{
    static constexpr uint32_t name_size = 16;
    static constexpr uint32_t shm_queue_bytes = 4096;
    static constexpr uint32_t shm_block_bytes = 64;
    static constexpr uint32_t tcp_queue_bytes = 65536;
    static constexpr uint32_t tcp_recv_init_bytes = 1024;
    static constexpr uint32_t tcp_recv_max_bytes = 4096;
    static constexpr uint32_t max_shm_groups = 1;
    static constexpr uint32_t max_shm_conns_per_group = 4;
    static constexpr uint32_t max_tcp_groups = 1;
    static constexpr uint32_t max_tcp_conns_per_group = 4;
    static constexpr uint32_t max_pending_conns = 8;
    static constexpr uint32_t poll_batch_size = 16;
    static constexpr std::chrono::nanoseconds handshake_timeout = std::chrono::seconds(2);
    static constexpr std::chrono::nanoseconds connection_timeout = std::chrono::seconds(3);
    static constexpr std::chrono::nanoseconds heartbeat_interval = std::chrono::milliseconds(500);
};

struct LargeTestConfig : DefaultConfig
{
    static constexpr uint32_t name_size = 16;
    static constexpr uint32_t shm_queue_bytes = 512u * 1024u;
    static constexpr uint32_t shm_block_bytes = 64;
    static constexpr uint32_t tcp_queue_bytes = 256u * 1024u;
    static constexpr uint32_t tcp_recv_init_bytes = 4096;
    static constexpr uint32_t tcp_recv_max_bytes = 256u * 1024u;
    static constexpr uint32_t max_shm_groups = 1;
    static constexpr uint32_t max_shm_conns_per_group = 2;
    static constexpr uint32_t max_tcp_groups = 1;
    static constexpr uint32_t max_tcp_conns_per_group = 2;
    static constexpr uint32_t max_pending_conns = 4;
    static constexpr uint32_t poll_batch_size = 16;
    static constexpr std::chrono::nanoseconds handshake_timeout = std::chrono::seconds(2);
    static constexpr std::chrono::nanoseconds connection_timeout = std::chrono::seconds(3);
    static constexpr std::chrono::nanoseconds heartbeat_interval = std::chrono::milliseconds(500);
};

struct CleanupGuard
{
    std::string dir;
    std::vector<std::string> shm_names;

    ~CleanupGuard() {
        for (const auto& s : shm_names) {
            detail::shm_unlink_name(s.c_str());
        }
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void track_pair(const std::string& a, const std::string& b) {
        shm_names.push_back("/" + a + "_" + b + ".shm");
        shm_names.push_back("/" + b + "_" + a + ".shm");
    }
};

uint16_t pick_unused_port() {
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(s >= 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    addr.sin_port = 0;
    CHECK(::bind(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    socklen_t len = sizeof(addr);
    CHECK(::getsockname(s, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0);
    uint16_t port = ntohs(addr.sin_port);
    ::close(s);
    return port;
}

struct EchoSrvHandler
{
    std::atomic<bool> force_backpressure{false};
    std::atomic<uint32_t> backpressure_hits{0};
    std::atomic<uint32_t> rx_count{0};
    std::atomic<uint32_t> logon_count{0};
    std::atomic<uint32_t> disc_count{0};
    std::atomic<uint32_t> seq_mismatch_count{0};

    bool on_message(auto& ch, MsgView msg) {
        if (force_backpressure.load(std::memory_order_acquire)) {
            backpressure_hits.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        rx_count.fetch_add(1, std::memory_order_relaxed);
        auto tx = ch.try_alloc(msg.type(), msg.payload_size());
        if (!tx) return false;
        tx->set_flags(msg.flags());
        tx->set_reserved(msg.reserved());
        CHECK(tx->write_bytes(msg.payload()));
        tx->commit();
        return true;
    }

    void on_client_logon(const struct sockaddr_in&, auto&) {
        logon_count.fetch_add(1, std::memory_order_relaxed);
    }

    void on_client_disconnected(auto&, Error) {
        disc_count.fetch_add(1, std::memory_order_relaxed);
    }

    void on_seq_mismatch(auto&, const SeqMismatchInfo&) {
        seq_mismatch_count.fetch_add(1, std::memory_order_relaxed);
    }
};

struct RecordingClientHandler
{
    std::vector<uint64_t> replies;
    bool backpressure = false;
    bool disconnected = false;
    bool seq_mismatch = false;

    bool on_server_msg(MsgView msg) {
        if (backpressure) return false;
        auto v = msg.read_as<uint64_t>();
        CHECK(v.has_value());
        replies.push_back(*v);
        return true;
    }

    void on_disconnected(Error) { disconnected = true; }
    void on_seq_mismatch(const SeqMismatchInfo&) { seq_mismatch = true; }
};

struct LargeClientHandler
{
    struct Packet
    {
        uint16_t flags = 0;
        uint32_t reserved = 0;
        std::vector<std::byte> bytes;
    };
    std::vector<Packet> packets;

    void on_server_msg(MsgView msg) {
        Packet pkt;
        pkt.flags = msg.flags();
        pkt.reserved = msg.reserved();
        auto span = msg.payload();
        pkt.bytes.assign(span.begin(), span.end());
        packets.push_back(std::move(pkt));
    }
};

template<template<uint32_t> class TcpPoller>
void run_end_to_end_suite(std::string_view tag) {
    CleanupGuard cg;
    cg.dir = (std::filesystem::temp_directory_path() /
              ("tcpshm_v2_e2e_" + std::string(tag) + "_" + std::to_string(::getpid())))
                 .string();
    std::filesystem::remove_all(cg.dir);
    std::filesystem::create_directories(cg.dir);
    cg.track_pair("srv_e2e", "shm_u1");

    const uint16_t port = pick_unused_port();
    EchoSrvHandler srv_h;
    auto srv = std::make_unique<Server<EchoSrvHandler, IntTestConfig, TcpPoller>>(srv_h, "srv_e2e", cg.dir + "/srv");
    CHECK(srv->start("127.0.0.1", port).has_value());

    std::atomic<bool> stop_flag{false};
    std::thread io_thread([&]() {
        while (!stop_flag.load(std::memory_order_acquire)) {
            const TimePoint now = steady_now();
            srv->poll_ctl(now);
            srv->poll_shm(0);
            srv->poll_tcp(now, 0);
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    // 1. Unified Client in SHM mode with batched send/drain + server-side backpressure
    {
        RecordingClientHandler ch;
        Client<RecordingClientHandler, IntTestConfig> cli(ch, "shm_u1", cg.dir + "/c_shm");
        CHECK(cli.connect(TransportKind::Shm, "127.0.0.1", port).has_value());

        srv_h.force_backpressure.store(true, std::memory_order_release);
        for (uint64_t i = 1; i <= 10; ++i) {
            auto tx = cli.shm_channel().try_alloc(5, sizeof(uint64_t));
            CHECK(tx.has_value());
            CHECK(tx->write_pod(i));
            tx->stage();
        }
        cli.shm_channel().flush();

        const TimePoint bp_wait = steady_now() + std::chrono::milliseconds(100);
        while (srv_h.backpressure_hits.load() == 0 && steady_now() < bp_wait) {
            cli.poll_shm();
            cli.poll_tcp(steady_now());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        CHECK(srv_h.backpressure_hits.load() > 0 && ch.replies.empty());

        srv_h.force_backpressure.store(false, std::memory_order_release);
        const TimePoint deadline = steady_now() + std::chrono::seconds(2);
        while (ch.replies.size() < 10 && steady_now() < deadline) {
            cli.poll_shm();
            cli.poll_tcp(steady_now());
            std::this_thread::yield();
        }
        CHECK(ch.replies.size() == 10);
        for (uint64_t i = 0; i < 10; ++i) CHECK(ch.replies[i] == i + 1);
        cli.stop();
    }

    // 2. Two concurrent TcpClients + PTCP reconnect & SeqMismatch
    {
        RecordingClientHandler ch1;
        RecordingClientHandler ch2;
        auto cli1 = std::make_unique<TcpClient<RecordingClientHandler, IntTestConfig>>(ch1, "tcp_c1", cg.dir + "/c1");
        auto cli2 = std::make_unique<TcpClient<RecordingClientHandler, IntTestConfig>>(ch2, "tcp_c2", cg.dir + "/c2");
        CHECK(cli1->connect("127.0.0.1", port).has_value());
        CHECK(cli2->connect("127.0.0.1", port).has_value());

        constexpr uint64_t kCount = 40;
        for (uint64_t i = 0; i < kCount; ++i) {
            auto tx1 = cli1->channel().try_alloc(2, sizeof(uint64_t));
            auto tx2 = cli2->channel().try_alloc(3, sizeof(uint64_t));
            CHECK(tx1.has_value() && tx2.has_value());
            CHECK(tx1->write_pod(i + 100));
            CHECK(tx2->write_pod(i + 2000));
            tx1->stage();
            tx2->stage();
        }
        CHECK(cli1->channel().flush());
        CHECK(cli2->channel().flush());

        ch1.backpressure = true;
        for (int k = 0; k < 5; ++k) {
            cli1->poll_tcp(steady_now());
            cli2->poll_tcp(steady_now());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        CHECK(ch1.replies.empty());
        ch1.backpressure = false;

        const TimePoint deadline = steady_now() + std::chrono::seconds(3);
        while ((ch1.replies.size() < kCount || ch2.replies.size() < kCount) && steady_now() < deadline) {
            cli1->poll_tcp(steady_now());
            cli2->poll_tcp(steady_now());
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        CHECK(ch1.replies.size() == kCount);
        CHECK(ch2.replies.size() == kCount);
        for (uint64_t i = 0; i < kCount; ++i) {
            CHECK(ch1.replies[i] == i + 100);
            CHECK(ch2.replies[i] == i + 2000);
        }

        cli1->stop();
        const TimePoint d_wait = steady_now() + std::chrono::seconds(2);
        while (srv_h.disc_count.load() < 2 && steady_now() < d_wait) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        CHECK(srv_h.disc_count.load() >= 2);
        ch1.replies.clear();
        CHECK(cli1->connect("127.0.0.1", port).has_value());
        {
            auto tx = cli1->channel().try_alloc(2, sizeof(uint64_t));
            CHECK(tx.has_value());
            CHECK(tx->write_pod(uint64_t{99999}));
            tx->commit();
        }
        const TimePoint r_wait = steady_now() + std::chrono::seconds(2);
        while (ch1.replies.empty() && steady_now() < r_wait) {
            cli1->poll_tcp(steady_now());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        CHECK(ch1.replies.size() == 1 && ch1.replies[0] == 99999);

        cli1->stop();
        const TimePoint d_wait2 = steady_now() + std::chrono::seconds(2);
        while (srv_h.disc_count.load() < 3 && steady_now() < d_wait2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        CHECK(srv_h.disc_count.load() >= 3);
        std::filesystem::remove_all(cg.dir + "/c1");
        std::filesystem::create_directories(cg.dir + "/c1");
        {
            std::array<char, IntTestConfig::name_size>* ls = nullptr;
            std::string ls_path = cg.dir + "/c1/tcp_c1.lastserver";
            auto ls_res = detail::mmap_create_or_open<std::array<char, IntTestConfig::name_size>>(ls_path.c_str(), false);
            CHECK(ls_res.has_value());
            ls = *ls_res;
            detail::copy_cstr(ls->data(), ls->size(), "srv_e2e");
            detail::munmap_region(ls);

            auto res = cli1->connect("127.0.0.1", port);
            CHECK(!res.has_value() && res.error().code == ErrorCode::SeqMismatch);
            CHECK(ch1.seq_mismatch);
            CHECK(srv_h.seq_mismatch_count.load() >= 1);
        }

        cli2->stop();
    }

    stop_flag.store(true, std::memory_order_release);
    io_thread.join();
    srv->stop();
}

template<template<uint32_t> class TcpPoller>
void run_large_over_64k_suite(std::string_view tag) {
    CleanupGuard cg;
    cg.dir = (std::filesystem::temp_directory_path() /
              ("tcpshm_v2_big_" + std::string(tag) + "_" + std::to_string(::getpid())))
                 .string();
    std::filesystem::remove_all(cg.dir);
    std::filesystem::create_directories(cg.dir);
    cg.track_pair("srv_big", "shm_big");

    const uint16_t port = pick_unused_port();
    EchoSrvHandler srv_h;
    auto srv = std::make_unique<Server<EchoSrvHandler, LargeTestConfig, TcpPoller>>(srv_h, "srv_big", cg.dir + "/srv");
    CHECK(srv->start("127.0.0.1", port).has_value());

    std::atomic<bool> stop_flag{false};
    std::thread io_thread([&]() {
        while (!stop_flag.load(std::memory_order_acquire)) {
            const TimePoint now = steady_now();
            srv->poll_ctl(now);
            srv->poll_shm(0);
            srv->poll_tcp(now, 0);
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    constexpr uint32_t kLargePayload = 128u * 1024u + 13u; // > 64 KiB & non-multiple-of-16
    std::vector<std::byte> pattern(kLargePayload);
    for (uint32_t i = 0; i < kLargePayload; ++i) {
        pattern[i] = static_cast<std::byte>((i * 31u + 7u) & 0xFFu);
    }

    // SHM >64 KiB round-trip with flags & reserved preservation
    {
        LargeClientHandler ch;
        ShmClient<LargeClientHandler, LargeTestConfig> cli(ch, "shm_big", cg.dir + "/c_shm");
        CHECK(cli.connect("127.0.0.1", port).has_value());
        {
            auto tx = cli.channel().try_alloc(9, kLargePayload);
            CHECK(tx.has_value());
            tx->set_flags(0xA55Au);
            tx->set_reserved(0x13579BDFu);
            CHECK(tx->write_bytes(pattern));
            tx->commit();
        }
        const TimePoint deadline = steady_now() + std::chrono::seconds(2);
        while (ch.packets.empty() && steady_now() < deadline) {
            cli.poll_shm();
            cli.poll_keepalive(steady_now());
            std::this_thread::yield();
        }
        CHECK(ch.packets.size() == 1);
        CHECK(ch.packets[0].flags == 0xA55Au);
        CHECK(ch.packets[0].reserved == 0x13579BDFu);
        CHECK(ch.packets[0].bytes == pattern);
        cli.stop();
    }

    // TCP >64 KiB round-trip x3: verifies immediate ACK piggyback reclaims 256 KiB tcp_queue without waiting for 500ms HB
    {
        LargeClientHandler ch;
        TcpClient<LargeClientHandler, LargeTestConfig> cli(ch, "tcp_big", cg.dir + "/c_tcp");
        CHECK(cli.connect("127.0.0.1", port).has_value());
        for (uint32_t round = 0; round < 3; ++round) {
            const TimePoint round_deadline = steady_now() + std::chrono::milliseconds(300);
            while (true) {
                if (auto tx = cli.channel().try_alloc(11, kLargePayload)) {
                    tx->set_flags(static_cast<uint16_t>(0x5AA5u + round));
                    tx->set_reserved(0x2468ACE0u + round);
                    CHECK(tx->write_bytes(pattern));
                    tx->commit();
                    break;
                }
                CHECK(steady_now() < round_deadline);
                cli.poll_tcp(steady_now());
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            while (ch.packets.size() <= round && steady_now() < round_deadline) {
                cli.poll_tcp(steady_now());
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            CHECK(ch.packets.size() == round + 1u);
            CHECK(ch.packets[round].flags == static_cast<uint16_t>(0x5AA5u + round));
            CHECK(ch.packets[round].reserved == 0x2468ACE0u + round);
            CHECK(ch.packets[round].bytes == pattern);
        }
        cli.stop();
    }

    stop_flag.store(true, std::memory_order_release);
    io_thread.join();
    srv->stop();
}

} // namespace

int main() {
    run_end_to_end_suite<BusyPoller>("busy");
    run_large_over_64k_suite<BusyPoller>("busy");
#ifdef __linux__
    run_end_to_end_suite<EpollPoller>("epoll");
    run_large_over_64k_suite<EpollPoller>("epoll");
#endif
    std::cout << "integration_server_client_test passed\n";
    return 0;
}
