#include "tcpshm/tcpshm.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <format>
#include <iostream>
#include <latch>
#include <memory>
#include <ranges>
#include <source_location>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#ifdef _MSC_VER
#include <intrin.h>
#else
#include <immintrin.h>
#endif
#endif

namespace {

using namespace tcpshm;

consteval uint32_t KiB(uint32_t n) { return n * 1024u; }
consteval uint32_t MiB(uint32_t n) { return n * 1024u * 1024u; }

struct BenchConfig : DefaultConfig
{
    static constexpr uint32_t name_size = 16;
    static constexpr uint32_t shm_queue_bytes = MiB(1);
    static constexpr uint32_t shm_block_bytes = 64;
    static constexpr uint32_t tcp_queue_bytes = KiB(256);
    static constexpr uint32_t tcp_recv_init_bytes = KiB(64);
    static constexpr uint32_t tcp_recv_max_bytes = KiB(256);
    static constexpr uint32_t max_pending_conns = 4;
    static constexpr uint32_t max_shm_groups = 1;
    static constexpr uint32_t max_shm_conns_per_group = 4;
    static constexpr uint32_t max_tcp_groups = 1;
    static constexpr uint32_t max_tcp_conns_per_group = 4;
    static constexpr uint32_t poll_batch_size = 64;
    static constexpr std::chrono::nanoseconds handshake_timeout = std::chrono::seconds(5);
    static constexpr std::chrono::nanoseconds connection_timeout = std::chrono::seconds(10);
    static constexpr std::chrono::nanoseconds heartbeat_interval = std::chrono::seconds(3);
};

static_assert(ConfigConcept<BenchConfig>);

inline void bench_check(bool cond,
                        std::string_view expr,
                        std::source_location loc = std::source_location::current()) {
    if (!cond) [[unlikely]] {
        std::cerr << std::format("{}:{}: benchmark check failed: {}\n",
                                 loc.file_name(),
                                 loc.line(),
                                 expr);
        std::exit(1);
    }
}

inline uint64_t now_ns_u64() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            steady_now().time_since_epoch())
            .count());
}

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
    asm volatile("yield" ::: "memory");
#else
    std::this_thread::yield();
#endif
}

inline void pin_current_thread(int core) noexcept {
    if (core < 0) return;
#ifdef _WIN32
    if (core < 64) {
        SetThreadAffinityMask(GetCurrentThread(), DWORD_PTR{1} << core);
    }
#elif defined(__linux__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(static_cast<size_t>(core), &cpuset);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#else
    (void)core;
#endif
}

inline int current_pid() noexcept {
#ifdef _WIN32
    return _getpid();
#else
    return static_cast<int>(::getpid());
#endif
}

inline void write_seq_payload(std::byte* dst, uint64_t seq) noexcept {
    std::memcpy(dst, &seq, sizeof(seq));
}

inline uint64_t read_seq_payload(MsgView msg) noexcept {
    uint64_t seq = 0;
    std::memcpy(&seq, msg.data(), sizeof(seq));
    return seq;
}

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
    detail::socket_t s = detail::create_socket(AF_INET, SOCK_STREAM, 0);
    bench_check(s != detail::kInvalidSocket, "create_socket");
    struct sockaddr_in addr{};
    bench_check(detail::parse_ipv4_endpoint("127.0.0.1", 0, addr), "parse_ipv4_endpoint");
    bench_check(detail::bind_socket(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0, "bind_socket");
    socklen_t len = sizeof(addr);
    bench_check(::getsockname(s, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0, "getsockname");
    uint16_t port = ntohs(addr.sin_port);
    detail::close_socket(s);
    return port;
}

struct EchoServerHandler
{
    bool batch_stage = false;

    bool on_message(ShmChannel<BenchConfig>& ch, MsgView msg) {
        auto tx = ch.try_alloc(msg.type(), msg.payload_size());
        if (!tx) [[unlikely]] {
            if (batch_stage) ch.flush();
            return false;
        }
        if (msg.payload_size() > 0) [[likely]] {
            std::memcpy(tx->data(), msg.data(), msg.payload_size());
        }
        if (batch_stage) tx->stage();
        else tx->commit();
        return true;
    }

    bool on_message(TcpChannel<BenchConfig>& ch, MsgView msg) {
        auto tx = ch.try_alloc(msg.type(), msg.payload_size());
        if (!tx) [[unlikely]] {
            ch.flush();
            return false;
        }
        if (msg.payload_size() > 0) [[likely]] {
            std::memcpy(tx->data(), msg.data(), msg.payload_size());
        }
        if (batch_stage) tx->stage();
        else tx->commit();
        return true;
    }
};

struct BenchClientHandler
{
    void on_system_error(Error err) {
        std::cerr << std::format("[BenchClient] System error: {} (errno={})\n", err.message(), err.sys_errno);
    }

    void on_login_reject(const detail::LoginRspMsg<BenchConfig>& rsp) {
        std::cerr << std::format("[BenchClient] Login rejected: {}\n", rsp.error_msg);
    }

    void on_seq_mismatch(const SeqMismatchInfo&) {
        std::cerr << "[BenchClient] Sequence mismatch\n";
    }

    void on_server_msg(MsgView msg) {
        bench_check(msg.payload_size() >= sizeof(uint64_t), "reply payload size");
        const uint64_t seq = read_seq_payload(msg);
        bench_check(seq == expected_seq_, "monotonic echo sequence");
        ++expected_seq_;
        recv_count_.fetch_add(1, std::memory_order_release);
    }

    void reset_counter() noexcept {
        recv_count_.store(0, std::memory_order_relaxed);
        expected_seq_ = 0;
    }

    [[nodiscard]] uint64_t recv_count() const noexcept {
        return recv_count_.load(std::memory_order_acquire);
    }

private:
    uint64_t expected_seq_ = 0;
    std::atomic<uint64_t> recv_count_{0};
};

enum class BenchMode
{
    All,
    Rtt,
    Throughput,
    Spsc
};

struct BenchOptions
{
    BenchMode mode = BenchMode::All;
    uint32_t iters = 50'000;
    uint32_t warmup = 2'000;
    std::vector<uint32_t> sizes = {16u, 64u, 256u, 1024u};
    int pin_server = -1;
    int pin_client = -1;
};

struct LatencyStats
{
    uint64_t avg_ns = 0;
    uint64_t min_ns = 0;
    uint64_t p50_ns = 0;
    uint64_t p90_ns = 0;
    uint64_t p99_ns = 0;
    uint64_t p999_ns = 0;
    uint64_t max_ns = 0;
    double msg_per_sec = 0.0;
};

struct ThroughputStats
{
    double msg_per_sec = 0.0;
    double mb_per_sec = 0.0;
    double ns_per_msg = 0.0;
    uint64_t elapsed_ns = 0;
};

LatencyStats compute_latency_stats(std::span<uint64_t> samples_ns, uint64_t total_ns) {
    bench_check(!samples_ns.empty(), "samples_ns must not be empty");
    std::ranges::sort(samples_ns);
    const size_t n = samples_ns.size();
    auto pct = [&](double q) -> uint64_t {
        size_t idx = static_cast<size_t>(q * static_cast<double>(n - 1));
        return samples_ns[std::min(idx, n - 1)];
    };
    uint64_t sum = 0;
    for (uint64_t v : samples_ns) sum += v;
    const double total_sec = static_cast<double>(std::max<uint64_t>(total_ns, 1)) / 1e9;
    return LatencyStats{
        .avg_ns = sum / static_cast<uint64_t>(n),
        .min_ns = samples_ns.front(),
        .p50_ns = pct(0.50),
        .p90_ns = pct(0.90),
        .p99_ns = pct(0.99),
        .p999_ns = pct(0.999),
        .max_ns = samples_ns.back(),
        .msg_per_sec = static_cast<double>(n) / total_sec,
    };
}

std::string make_name(std::string_view prefix, int pid, uint32_t seq) {
    std::string full = std::format("{}{}_{}", prefix, pid % 10000, seq);
    bench_check(full.size() < BenchConfig::name_size, "name fits within name_size");
    return full;
}

ThroughputStats run_spsc_case(bool batch_mode,
                              uint32_t payload_size,
                              uint32_t warmup,
                              uint32_t iters,
                              int pin_prod,
                              int pin_cons) {
    auto q = std::make_unique<SpscQueue<BenchConfig::shm_queue_bytes, BenchConfig::shm_block_bytes>>();
    const uint64_t total = static_cast<uint64_t>(warmup) + static_cast<uint64_t>(iters);
    const uint32_t sz = payload_size;
    std::latch ready{2};
    std::atomic<uint64_t> warmup_end_ns{0};

    std::jthread consumer([&](std::stop_token) {
        pin_current_thread(pin_cons);
        ready.arrive_and_wait();
        uint64_t expected = 0;
        if (batch_mode) {
            while (expected < total) {
                uint32_t n = q->drain(
                    [&](MsgView msg) {
                        bench_check(read_seq_payload(msg) == expected, "spsc batch seq");
                        ++expected;
                        if (expected == warmup) {
                            warmup_end_ns.store(now_ns_u64(), std::memory_order_release);
                        }
                    },
                    32);
                if (n == 0) cpu_relax();
            }
        } else {
            while (expected < total) {
                auto rx = q->try_front();
                if (!rx) {
                    cpu_relax();
                    continue;
                }
                bench_check(read_seq_payload(rx->view()) == expected, "spsc monotonic seq");
                ++expected;
                if (expected == warmup) {
                    warmup_end_ns.store(now_ns_u64(), std::memory_order_release);
                }
            }
        }
    });

    std::jthread producer([&](std::stop_token) {
        pin_current_thread(pin_prod);
        ready.arrive_and_wait();
        if (batch_mode) {
            constexpr uint32_t kBatch = 32;
            uint64_t seq = 0;
            while (seq < total) {
                uint32_t b = 0;
                while (seq < total && b < kBatch) {
                    auto tx = q->try_alloc(1, sz);
                    if (!tx) {
                        q->flush();
                        cpu_relax();
                        continue;
                    }
                    write_seq_payload(tx->data(), seq);
                    tx->stage();
                    ++seq;
                    ++b;
                }
                q->flush();
            }
        } else {
            for (uint64_t seq = 0; seq < total; ++seq) {
                while (true) {
                    auto tx = q->try_alloc(1, sz);
                    if (tx) {
                        write_seq_payload(tx->data(), seq);
                        tx->commit();
                        break;
                    }
                    cpu_relax();
                }
            }
        }
    });

    producer.join();
    consumer.join();

    const uint64_t t1 = now_ns_u64();
    const uint64_t t0 = warmup_end_ns.load(std::memory_order_acquire);
    const uint64_t elapsed = (t1 > t0) ? (t1 - t0) : 1u;
    const double sec = static_cast<double>(elapsed) / 1e9;
    const double msg_s = static_cast<double>(iters) / sec;
    return ThroughputStats{
        .msg_per_sec = msg_s,
        .mb_per_sec = (msg_s * static_cast<double>(payload_size)) / (1024.0 * 1024.0),
        .ns_per_msg = static_cast<double>(elapsed) / static_cast<double>(iters),
        .elapsed_ns = elapsed,
    };
}

template<template<uint32_t> class TcpPoller>
LatencyStats run_rtt_case(const std::string& dir,
                          CleanupGuard& guard,
                          uint32_t run_id,
                          bool use_shm,
                          uint32_t payload_size,
                          uint32_t warmup,
                          uint32_t iters,
                          int pin_server,
                          int pin_client) {
    const int pid = current_pid();
    const std::string srv_name = make_name("bs", pid, run_id);
    const std::string cli_name = make_name("bc", pid, run_id);
    if (use_shm) guard.track_pair(srv_name, cli_name);

    const uint16_t port = pick_unused_port();
    EchoServerHandler srv_h;
    Server<EchoServerHandler, BenchConfig, TcpPoller> srv(srv_h, srv_name, dir);
    bench_check(srv.start("127.0.0.1", port).has_value(), "server start");

    std::latch srv_ready{1};
    std::jthread ctl_thr([&](std::stop_token st) {
        srv_ready.count_down();
        while (!st.stop_requested()) {
            srv.poll_ctl(steady_now());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::jthread worker_thr([&](std::stop_token st) {
        pin_current_thread(pin_server);
        while (!st.stop_requested()) {
            if (use_shm) srv.poll_shm(0, 1);
            else srv.poll_tcp(steady_now(), 0, 1);
            cpu_relax();
        }
    });
    srv_ready.wait();

    BenchClientHandler cli_h;
    Client<BenchClientHandler, BenchConfig> cli(cli_h, cli_name, dir);
    bench_check(cli.connect(use_shm ? TransportKind::Shm : TransportKind::Tcp, "127.0.0.1", port).has_value(),
                "client connect");

    std::jthread cli_hb_thr;
    if (use_shm) {
        cli_hb_thr = std::jthread([&](std::stop_token st) {
            while (!st.stop_requested() && !cli.is_closed()) {
                cli.poll_tcp(steady_now());
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    pin_current_thread(pin_client);
    cli_h.reset_counter();

    const uint32_t sz = payload_size;
    const uint64_t total = static_cast<uint64_t>(warmup) + static_cast<uint64_t>(iters);
    std::vector<uint64_t> samples_ns(iters);
    uint64_t measured_ns = 0;

    for (uint64_t seq = 0; seq < total; ++seq) {
        const auto t_start = steady_now();
        cli.with_channel([&](auto& ch) {
            while (true) {
                auto tx = ch.try_alloc(1, sz);
                if (tx) {
                    write_seq_payload(tx->data(), seq);
                    tx->commit();
                    break;
                }
                bench_check(!cli.is_closed(), "client closed during try_alloc");
                if (use_shm) cli.poll_shm(1);
                else cli.poll_tcp(steady_now(), 1);
                cpu_relax();
            }
        });

        const uint64_t target = seq + 1;
        while (cli_h.recv_count() < target) {
            bench_check(!cli.is_closed(), "client closed waiting for echo");
            if (use_shm) cli.poll_shm(1);
            else cli.poll_tcp(steady_now(), 1);
            cpu_relax();
        }
        const auto t_end = steady_now();
        if (seq >= warmup) {
            const uint64_t rtt = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count());
            samples_ns[seq - warmup] = rtt;
            measured_ns += rtt;
        }
    }

    if (cli_hb_thr.joinable()) {
        cli_hb_thr.request_stop();
        cli_hb_thr.join();
    }
    cli.stop();
    worker_thr.request_stop();
    ctl_thr.request_stop();
    worker_thr.join();
    ctl_thr.join();
    srv.stop();

    return compute_latency_stats(samples_ns, measured_ns);
}

template<template<uint32_t> class TcpPoller>
ThroughputStats run_throughput_case(const std::string& dir,
                                    CleanupGuard& guard,
                                    uint32_t run_id,
                                    bool use_shm,
                                    bool batch_mode,
                                    uint32_t payload_size,
                                    uint32_t warmup,
                                    uint32_t iters,
                                    int pin_server,
                                    int pin_client) {
    const int pid = current_pid();
    const std::string srv_name = make_name("ts", pid, run_id);
    const std::string cli_name = make_name("tc", pid, run_id);
    if (use_shm) guard.track_pair(srv_name, cli_name);

    const uint16_t port = pick_unused_port();
    EchoServerHandler srv_h;
    srv_h.batch_stage = batch_mode;
    Server<EchoServerHandler, BenchConfig, TcpPoller> srv(srv_h, srv_name, dir);
    bench_check(srv.start("127.0.0.1", port).has_value(), "server start");

    std::latch srv_ready{1};
    std::jthread ctl_thr([&](std::stop_token st) {
        srv_ready.count_down();
        while (!st.stop_requested()) {
            srv.poll_ctl(steady_now());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::jthread worker_thr([&](std::stop_token st) {
        pin_current_thread(pin_server);
        const uint32_t batch_lim = batch_mode ? 32u : 1u;
        while (!st.stop_requested()) {
            if (use_shm) {
                srv.poll_shm(0, batch_lim);
            } else {
                srv.poll_tcp(steady_now(), 0, batch_lim);
            }
            cpu_relax();
        }
    });
    srv_ready.wait();

    BenchClientHandler cli_h;
    Client<BenchClientHandler, BenchConfig> cli(cli_h, cli_name, dir);
    bench_check(cli.connect(use_shm ? TransportKind::Shm : TransportKind::Tcp, "127.0.0.1", port).has_value(),
                "client connect");
    cli_h.reset_counter();

    const uint32_t sz = payload_size;
    const uint64_t total = static_cast<uint64_t>(warmup) + static_cast<uint64_t>(iters);
    std::atomic<uint64_t> measure_start_ns{0};
    uint64_t measure_end_ns = 0;

    if (use_shm) {
        std::latch start_gate{2};
        std::jthread hb_thr([&](std::stop_token st) {
            while (!st.stop_requested() && !cli.is_closed()) {
                cli.poll_tcp(steady_now());
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
        std::jthread recv_thr([&](std::stop_token) {
            start_gate.arrive_and_wait();
            const uint32_t rx_batch = batch_mode ? 32u : 1u;
            while (cli_h.recv_count() < total) {
                bench_check(!cli.is_closed(), "SHM client closed in throughput recv");
                cli.poll_shm(rx_batch);
                if (measure_start_ns.load(std::memory_order_acquire) == 0 && cli_h.recv_count() >= warmup) {
                    measure_start_ns.store(now_ns_u64(), std::memory_order_release);
                }
                cpu_relax();
            }
            measure_end_ns = now_ns_u64();
        });
        std::jthread send_thr([&](std::stop_token) {
            pin_current_thread(pin_client);
            start_gate.arrive_and_wait();
            auto& ch = cli.shm_channel();
            if (batch_mode) {
                uint64_t seq = 0;
                while (seq < total) {
                    uint32_t b = 0;
                    while (seq < total && b < 32u) {
                        auto tx = ch.try_alloc(1, sz);
                        if (!tx) {
                            bench_check(!cli.is_closed(), "SHM client closed in throughput try_alloc");
                            ch.flush();
                            cpu_relax();
                            continue;
                        }
                        write_seq_payload(tx->data(), seq);
                        tx->stage();
                        ++seq;
                        ++b;
                    }
                    ch.flush();
                }
            } else {
                for (uint64_t seq = 0; seq < total; ++seq) {
                    while (true) {
                        auto tx = ch.try_alloc(1, sz);
                        if (tx) {
                            write_seq_payload(tx->data(), seq);
                            tx->commit();
                            break;
                        }
                        bench_check(!cli.is_closed(), "SHM client closed in throughput try_alloc");
                        cpu_relax();
                    }
                }
            }
        });
        send_thr.join();
        recv_thr.join();
        hb_thr.request_stop();
        hb_thr.join();
    } else {
        pin_current_thread(pin_client);
        auto& ch = cli.tcp_channel();
        uint64_t sent = 0;
        while (cli_h.recv_count() < total) {
            bench_check(!cli.is_closed(), "TCP client closed in throughput");
            uint32_t batch = 0;
            while (sent < total && batch < 32u) {
                auto tx = ch.try_alloc(1, sz);
                if (!tx) break;
                write_seq_payload(tx->data(), sent);
                ++sent;
                ++batch;
                if (sent == total || batch == 32u) tx->commit();
                else tx->stage();
            }
            uint64_t prev_recv;
            do {
                prev_recv = cli_h.recv_count();
                cli.poll_tcp(steady_now(), 32u);
            } while (cli_h.recv_count() > prev_recv && cli_h.recv_count() < total);
            if (measure_start_ns.load(std::memory_order_relaxed) == 0 && cli_h.recv_count() >= warmup) {
                measure_start_ns.store(now_ns_u64(), std::memory_order_relaxed);
            }
        }
        measure_end_ns = now_ns_u64();
    }

    cli.stop();
    worker_thr.request_stop();
    ctl_thr.request_stop();
    worker_thr.join();
    ctl_thr.join();
    srv.stop();

    const uint64_t t0 = measure_start_ns.load(std::memory_order_acquire);
    const uint64_t elapsed = (measure_end_ns > t0) ? (measure_end_ns - t0) : 1u;
    const double sec = static_cast<double>(elapsed) / 1e9;
    const double msg_s = static_cast<double>(iters) / sec;
    return ThroughputStats{
        .msg_per_sec = msg_s,
        .mb_per_sec = (msg_s * static_cast<double>(payload_size)) / (1024.0 * 1024.0),
        .ns_per_msg = static_cast<double>(elapsed) / static_cast<double>(iters),
        .elapsed_ns = elapsed,
    };
}

bool parse_u32(std::string_view sv, uint32_t& out) noexcept {
    if (sv.empty()) return false;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    return ec == std::errc{} && ptr == sv.data() + sv.size();
}

bool parse_i32(std::string_view sv, int& out) noexcept {
    if (sv.empty()) return false;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    return ec == std::errc{} && ptr == sv.data() + sv.size();
}

void print_usage(std::string_view prog) {
    std::cout << std::format(
        "Usage: {} [options]\n"
        "  --mode=all|rtt|throughput|spsc   Benchmark suite to run (default: all)\n"
        "  --iters=<N>                      Measured messages per case (default: 50000)\n"
        "  --warmup=<N>                     Warmup messages per case (default: 2000)\n"
        "  --sizes=16,64,256,1024           Comma-separated payload sizes in bytes (8..{})\n"
        "  --pin-server=<core>              Pin server worker thread to CPU core (-1 = unpinned)\n"
        "  --pin-client=<core>              Pin client worker thread to CPU core (-1 = unpinned)\n"
        "  --help, -h                       Show this message\n",
        prog,
        BenchConfig::tcp_recv_max_bytes - sizeof(FrameHeader));
}

bool parse_args(int argc, char** argv, BenchOptions& opts) {
    for (int i = 1; i < argc; ++i) {
        std::string_view arg{argv[i]};
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            std::exit(0);
        }
        if (arg.starts_with("--mode=")) {
            auto val = arg.substr(7);
            if (val == "all") opts.mode = BenchMode::All;
            else if (val == "rtt") opts.mode = BenchMode::Rtt;
            else if (val == "throughput") opts.mode = BenchMode::Throughput;
            else if (val == "spsc") opts.mode = BenchMode::Spsc;
            else return false;
        } else if (arg.starts_with("--iters=")) {
            if (!parse_u32(arg.substr(8), opts.iters) || opts.iters == 0u) return false;
        } else if (arg.starts_with("--warmup=")) {
            if (!parse_u32(arg.substr(9), opts.warmup)) return false;
        } else if (arg.starts_with("--sizes=")) {
            opts.sizes.clear();
            auto list_sv = arg.substr(8);
            for (auto part : list_sv | std::views::split(',')) {
                std::string_view token{std::to_address(part.begin()),
                                       static_cast<size_t>(std::ranges::distance(part))};
                uint32_t sz = 0;
                if (!parse_u32(token, sz) || sz < sizeof(uint64_t) ||
                    sz > kMaxPayloadSize ||
                    sz + sizeof(FrameHeader) > BenchConfig::tcp_recv_max_bytes) {
                    return false;
                }
                opts.sizes.push_back(sz);
            }
            if (opts.sizes.empty()) return false;
        } else if (arg.starts_with("--pin-server=")) {
            if (!parse_i32(arg.substr(13), opts.pin_server)) return false;
        } else if (arg.starts_with("--pin-client=")) {
            if (!parse_i32(arg.substr(13), opts.pin_client)) return false;
        } else {
            return false;
        }
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    BenchOptions opts{};
    if (!parse_args(argc, argv, opts)) {
        print_usage(argv[0]);
        return 1;
    }

    const std::string dir = (std::filesystem::temp_directory_path() /
                             std::format("tcpshm_v2_bench_{}", current_pid()))
                                .string();
    CleanupGuard guard{dir, {}};
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    std::cout << std::format(
        "tcpshm v2.0 C++20 (16B Header) Benchmark | iters={} | warmup={} | ShmQueue={} KiB | TcpQueue={} KiB | pin(srv={}, cli={})\n",
        opts.iters,
        opts.warmup,
        BenchConfig::shm_queue_bytes / 1024u,
        BenchConfig::tcp_queue_bytes / 1024u,
        opts.pin_server,
        opts.pin_client);

    uint32_t run_id = 1;

    if (opts.mode == BenchMode::All || opts.mode == BenchMode::Spsc) {
        std::cout << "\n[1] SpscQueue Raw SPSC Throughput (2 threads, 1-way)\n";
        std::cout << std::format("{:<12} {:<8} {:>14} {:>12} {:>12}\n",
                                 "Variant", "Bytes", "Msg/sec", "MB/sec", "ns/msg");
        std::cout << std::string(62, '-') << '\n';
        for (uint32_t sz : opts.sizes) {
            auto st1 = run_spsc_case(false, sz, opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<12} {:<8} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "1-by-1", sz, st1.msg_per_sec, st1.mb_per_sec, st1.ns_per_msg);

            auto st_b = run_spsc_case(true, sz, opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<12} {:<8} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "Batch-32", sz, st_b.msg_per_sec, st_b.mb_per_sec, st_b.ns_per_msg);
        }
    }

    if (opts.mode == BenchMode::All || opts.mode == BenchMode::Rtt) {
        std::cout << "\n[2] Ping-Pong Round-Trip Latency (RTT, 1-in-flight)\n";
        std::cout << std::format("{:<10} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12}\n",
                                 "Mode", "Bytes", "Avg(ns)", "Min(ns)", "P50(ns)", "P99(ns)", "P99.9", "Max(ns)", "Msg/sec");
        std::cout << std::string(92, '-') << '\n';
        for (uint32_t sz : opts.sizes) {
            auto tcp_busy = run_rtt_case<BusyPoller>(dir, guard, run_id++, false, sz,
                                                     opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<10} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12.0f}\n",
                                     "TCP-Busy", sz, tcp_busy.avg_ns, tcp_busy.min_ns, tcp_busy.p50_ns,
                                     tcp_busy.p99_ns, tcp_busy.p999_ns, tcp_busy.max_ns, tcp_busy.msg_per_sec);

#ifdef __linux__
            auto tcp_ep = run_rtt_case<EpollPoller>(dir, guard, run_id++, false, sz,
                                                    opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<10} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12.0f}\n",
                                     "TCP-Epoll", sz, tcp_ep.avg_ns, tcp_ep.min_ns, tcp_ep.p50_ns,
                                     tcp_ep.p99_ns, tcp_ep.p999_ns, tcp_ep.max_ns, tcp_ep.msg_per_sec);
#endif

            auto shm = run_rtt_case<BusyPoller>(dir, guard, run_id++, true, sz,
                                                opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<10} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12.0f}\n",
                                     "SHM", sz, shm.avg_ns, shm.min_ns, shm.p50_ns, shm.p99_ns,
                                     shm.p999_ns, shm.max_ns, shm.msg_per_sec);

            const double speedup = static_cast<double>(tcp_busy.p50_ns) /
                                   static_cast<double>(std::max<uint64_t>(shm.p50_ns, 1u));
            std::cout << std::format("  -> {}B P50 RTT speedup (TCP-Busy/SHM): {:.2f}x\n", sz, speedup);
        }
    }

    if (opts.mode == BenchMode::All || opts.mode == BenchMode::Throughput) {
        std::cout << "\n[3] Pipelined Echo Throughput\n";
        std::cout << std::format("{:<11} {:>7} {:>14} {:>12} {:>12}\n",
                                 "Mode", "Bytes", "Msg/sec", "MB/sec", "ns/msg");
        std::cout << std::string(60, '-') << '\n';
        for (uint32_t sz : opts.sizes) {
            auto tcp_busy = run_throughput_case<BusyPoller>(dir, guard, run_id++, false, true, sz,
                                                            opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<11} {:>7} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "TCP-Busy", sz, tcp_busy.msg_per_sec, tcp_busy.mb_per_sec, tcp_busy.ns_per_msg);

#ifdef __linux__
            auto tcp_ep = run_throughput_case<EpollPoller>(dir, guard, run_id++, false, true, sz,
                                                           opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<11} {:>7} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "TCP-Epoll", sz, tcp_ep.msg_per_sec, tcp_ep.mb_per_sec, tcp_ep.ns_per_msg);
#endif

            auto shm_1 = run_throughput_case<BusyPoller>(dir, guard, run_id++, true, false, sz,
                                                         opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<11} {:>7} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "SHM-1by1", sz, shm_1.msg_per_sec, shm_1.mb_per_sec, shm_1.ns_per_msg);

            auto shm_b = run_throughput_case<BusyPoller>(dir, guard, run_id++, true, true, sz,
                                                         opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<11} {:>7} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "SHM-Batch", sz, shm_b.msg_per_sec, shm_b.mb_per_sec, shm_b.ns_per_msg);
        }
    }

    return 0;
}
