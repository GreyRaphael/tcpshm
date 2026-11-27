#include "tcpshm/tcpshm.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using namespace tcpshm;

#define CHECK(cond)                                                                      \
    do {                                                                                 \
        if (!(cond)) {                                                                   \
            std::cerr << "CHECK failed at line " << __LINE__ << ": " #cond << std::endl; \
            std::exit(1);                                                                \
        }                                                                                \
    } while (0)

struct HandshakeTestConfig : DefaultConfig
{
    static constexpr uint32_t name_size = 16;
    static constexpr uint32_t shm_queue_bytes = 4096;
    static constexpr uint32_t shm_block_bytes = 64;
    static constexpr uint32_t tcp_queue_bytes = 8192;
    static constexpr uint32_t tcp_recv_init_bytes = 1024;
    static constexpr uint32_t tcp_recv_max_bytes = 4096;
    static constexpr uint32_t max_shm_groups = 1;
    static constexpr uint32_t max_shm_conns_per_group = 2;
    static constexpr uint32_t max_tcp_groups = 1;
    static constexpr uint32_t max_tcp_conns_per_group = 2;
    static constexpr uint32_t max_pending_conns = 8;
    static constexpr std::chrono::nanoseconds handshake_timeout = std::chrono::milliseconds(150);
    static constexpr std::chrono::nanoseconds connection_timeout = std::chrono::seconds(2);
    static constexpr std::chrono::nanoseconds heartbeat_interval = std::chrono::milliseconds(500);
};

struct MismatchShmConfig : HandshakeTestConfig
{
    static constexpr uint32_t shm_queue_bytes = 8192;
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

int connect_raw_tcp(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    addr.sin_port = htons(port);
    CHECK(::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    return fd;
}

struct TestServerHandler
{
    std::atomic<int> logon_count{0};
    std::atomic<int> disconnect_count{0};
    std::atomic<uint64_t> last_handle_raw{0};

    void on_message(auto& ch, MsgView msg) {
        if (auto tx = ch.try_alloc(msg.type(), msg.payload_size())) {
            tx->write_bytes(msg.payload());
            tx->commit();
        }
    }

    void on_client_logon(const struct sockaddr_in&, auto& ch) {
        last_handle_raw.store(ch.handle().to_u64(), std::memory_order_release);
        logon_count.fetch_add(1, std::memory_order_release);
    }

    void on_client_disconnected(auto&, Error) {
        disconnect_count.fetch_add(1, std::memory_order_release);
    }
};

struct TestClientHandler
{
    std::string last_reply;
    std::string last_reject;
    int sys_errors = 0;
    int disconnects = 0;

    void on_server_msg(MsgView msg) {
        last_reply = std::string(msg.payload_str());
    }

    template<class Rsp>
    void on_login_reject(const Rsp& rsp) {
        last_reject = rsp.error_msg;
    }

    void on_system_error(Error) { ++sys_errors; }
    void on_disconnected(Error) { ++disconnects; }
};

void test_slow_client_zero_blocking_and_timeout_cleanup() {
    CleanupGuard cg;
    cg.dir = (std::filesystem::temp_directory_path() / ("tcpshm_v2_hs1_" + std::to_string(::getpid()))).string();
    std::filesystem::remove_all(cg.dir);
    std::filesystem::create_directories(cg.dir);
    cg.track_pair("srv_hs1", "fast_cli");

    const uint16_t port = pick_unused_port();
    TestServerHandler srv_h;
    auto srv = std::make_unique<Server<TestServerHandler, HandshakeTestConfig>>(srv_h, "srv_hs1", cg.dir + "/srv");
    CHECK(srv->start("127.0.0.1", port).has_value());

    std::atomic<bool> stop_flag{false};
    std::atomic<int64_t> max_poll_ctl_ns{0};

    std::thread io_thread([&]() {
        while (!stop_flag.load(std::memory_order_acquire)) {
            const TimePoint t0 = steady_now();
            srv->poll_ctl(t0);
            const TimePoint t1 = steady_now();
            const int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
            int64_t prev = max_poll_ctl_ns.load(std::memory_order_relaxed);
            while (ns > prev && !max_poll_ctl_ns.compare_exchange_weak(prev, ns)) {}
            srv->poll_shm(0);
            srv->poll_tcp(t1, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    // 1. Slow stalled TCP client (connects and sends nothing)
    int stalled_fd = connect_raw_tcp(port);

    // 2. Stalled SHM probe client (sends valid LoginMsg with use_shm=1, receives ShmProbeReq, never replies!)
    int probe_stall_fd = connect_raw_tcp(port);
    {
        std::byte buf[sizeof(FrameHeader) + sizeof(detail::LoginMsg<HandshakeTestConfig>)]{};
        FrameHeader hdr{static_cast<uint32_t>(sizeof(buf)), detail::kLoginMsgType, 0u, 0u, 0u};
        hdr.convert_byte_order<HandshakeTestConfig::wire_endian>();
        detail::LoginMsg<HandshakeTestConfig> login{};
        login.magic = detail::kProtocolMagic;
        login.version = detail::kProtocolVersion;
        login.cfg_fingerprint = config_fingerprint<HandshakeTestConfig>();
        login.use_shm = 1;
        detail::copy_cstr(login.client_name, sizeof(login.client_name), "stall_shm");
        login.convert_byte_order();
        std::memcpy(buf, &hdr, sizeof(FrameHeader));
        std::memcpy(buf + sizeof(FrameHeader), &login, sizeof(login));
        CHECK(detail::send_all(probe_stall_fd, buf, static_cast<int>(sizeof(buf)), 0));
    }

    // 3. Normal SHM client connects and exchanges messages immediately while slow clients sit stalled!
    {
        TestClientHandler ch;
        ShmClient<TestClientHandler, HandshakeTestConfig> cli(ch, "fast_cli", cg.dir + "/cli");
        CHECK(cli.connect("127.0.0.1", port).has_value());
        {
            auto tx = cli.channel().try_alloc(10, 5);
            CHECK(tx.has_value());
            CHECK(tx->write_str("ping!"));
            tx->commit();
        }
        const TimePoint deadline = steady_now() + std::chrono::seconds(2);
        while (ch.last_reply.empty() && steady_now() < deadline) {
            cli.poll_shm();
            cli.poll_keepalive(steady_now());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        CHECK(ch.last_reply == "ping!");
        cli.stop();
    }

    // Wait beyond handshake_timeout (150ms) so slow clients are evicted and probe SHM unlinked
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    // Polling stalled sockets should hit EOF because server closed timed-out handshakes
    char junk = 0;
    CHECK(::recv(stalled_fd, &junk, 1, 0) == 0);
    // probe_stall_fd got ShmProbeReq first, drain it then verify EOF
    std::byte drain_buf[256]{};
    ssize_t r1 = ::recv(probe_stall_fd, drain_buf, sizeof(drain_buf), 0);
    CHECK(r1 > 0);
    CHECK(::recv(probe_stall_fd, drain_buf, sizeof(drain_buf), 0) == 0);

    ::close(stalled_fd);
    ::close(probe_stall_fd);

    // Ensure poll_ctl never blocked anywhere near v1's 300ms (check < 30ms even under sanitizers/CI jitter)
    CHECK(max_poll_ctl_ns.load() < 30'000'000LL);

    stop_flag.store(true, std::memory_order_release);
    io_thread.join();
    srv->stop();
}

void test_rejections_and_generational_handle_aba() {
    CleanupGuard cg;
    cg.dir = (std::filesystem::temp_directory_path() / ("tcpshm_v2_hs2_" + std::to_string(::getpid()))).string();
    std::filesystem::remove_all(cg.dir);
    std::filesystem::create_directories(cg.dir);
    cg.track_pair("srv_hs2", "cli_a");
    cg.track_pair("srv_hs2", "cli_b");

    const uint16_t port = pick_unused_port();
    TestServerHandler srv_h;
    auto srv = std::make_unique<Server<TestServerHandler, HandshakeTestConfig>>(srv_h, "srv_hs2", cg.dir + "/srv");
    CHECK(srv->start("127.0.0.1", port).has_value());

    std::atomic<bool> stop_flag{false};
    std::thread io_thread([&]() {
        while (!stop_flag.load(std::memory_order_acquire)) {
            const TimePoint now = steady_now();
            srv->poll_ctl(now);
            srv->poll_shm(0);
            srv->poll_tcp(now, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    // 1. Config mismatch rejection
    {
        TestClientHandler ch;
        ShmClient<TestClientHandler, MismatchShmConfig> bad_cli(ch, "mismatch", cg.dir + "/c_bad");
        auto res = bad_cli.connect("127.0.0.1", port);
        CHECK(!res.has_value() && res.error().code == ErrorCode::LoginRejected);
        CHECK(ch.last_reject == "Config layout mismatch");
    }

    // 2. Invalid client_name rejection
    {
        TestClientHandler ch;
        TcpClient<TestClientHandler, HandshakeTestConfig> bad_cli(ch, "../evil", cg.dir + "/c_evil");
        auto res = bad_cli.connect("127.0.0.1", port);
        CHECK(!res.has_value() && res.error().code == ErrorCode::LoginRejected);
        CHECK(ch.last_reject == "Invalid client name");
    }

    // 3. Generational ConnHandle ABA protection + duplicate login rejection + max clients limit
    ConnHandle h1{};
    {
        TestClientHandler ch1;
        TcpClient<TestClientHandler, HandshakeTestConfig> cli1(ch1, "cli_tcp1", cg.dir + "/c1");
        CHECK(cli1.connect("127.0.0.1", port).has_value());
        while (srv_h.logon_count.load(std::memory_order_acquire) < 1) {
            std::this_thread::yield();
        }
        h1 = ConnHandle::from_u64(srv_h.last_handle_raw.load(std::memory_order_acquire));
        CHECK(h1.valid() && h1.transport() == TransportKind::Tcp);

        // Duplicate login with same name must be rejected
        {
            TestClientHandler ch_dup;
            TcpClient<TestClientHandler, HandshakeTestConfig> cli_dup(ch_dup, "cli_tcp1", cg.dir + "/c1_dup");
            auto dup_res = cli_dup.connect("127.0.0.1", port);
            CHECK(!dup_res.has_value() && dup_res.error().code == ErrorCode::LoginRejected);
            CHECK(ch_dup.last_reject == "Already logged in");
        }

        // Second TCP client fills max_tcp_conns_per_group = 2
        TestClientHandler ch2;
        TcpClient<TestClientHandler, HandshakeTestConfig> cli2(ch2, "cli_tcp2", cg.dir + "/c2");
        CHECK(cli2.connect("127.0.0.1", port).has_value());

        // Third TCP client exceeds capacity
        TestClientHandler ch3;
        TcpClient<TestClientHandler, HandshakeTestConfig> cli3(ch3, "cli_tcp3", cg.dir + "/c3");
        auto max_res = cli3.connect("127.0.0.1", port);
        CHECK(!max_res.has_value() && max_res.error().code == ErrorCode::LoginRejected);
        CHECK(ch3.last_reject == "Max client count exceeded");

        cli1.stop();
        cli2.stop();
    }

    // Wait for disconnect cleanup on server
    const TimePoint wait_deadline = steady_now() + std::chrono::seconds(2);
    while (srv_h.disconnect_count.load(std::memory_order_acquire) < 2 && steady_now() < wait_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    CHECK(srv_h.disconnect_count.load(std::memory_order_acquire) >= 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    // Reconnect on same slot 0 -> gets new generation h2, and h1 must NOT resolve!
    stop_flag.store(true, std::memory_order_release);
    io_thread.join();

    // After io_thread stops, drive server single-threaded to verify try_get_tcp deterministically
    TestClientHandler ch_new;
    TcpClient<TestClientHandler, HandshakeTestConfig> cli_new(ch_new, "cli_tcp_re", cg.dir + "/cre");
    std::atomic<bool> stop2{false};
    std::thread srv_pump([&]() {
        while (!stop2.load(std::memory_order_acquire)) {
            srv->poll_ctl(steady_now());
            srv->poll_tcp(steady_now(), 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    CHECK(cli_new.connect("127.0.0.1", port).has_value());
    stop2.store(true, std::memory_order_release);
    srv_pump.join();

    const ConnHandle h2 = ConnHandle::from_u64(srv_h.last_handle_raw.load(std::memory_order_acquire));
    CHECK(h2.valid() && h2.slot_id == h1.slot_id && h2.generation() != h1.generation());
    CHECK(srv->try_get_tcp(h1) == nullptr); // Stale handle rejected!
    CHECK(srv->try_get_tcp(h2) != nullptr); // Current handle resolves!

    cli_new.stop();
    srv->stop();
}

} // namespace

int main() {
    test_slow_client_zero_blocking_and_timeout_cleanup();
    test_rejections_and_generational_handle_aba();
    std::cout << "integration_handshake_test passed\n";
    return 0;
}
