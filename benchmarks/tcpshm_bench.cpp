#include "tcpshm/tcpshm_client.h"
#include "tcpshm/tcpshm_server.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <concepts>
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
#include <pthread.h>
#include <sched.h>
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

template <class Conf>
concept ValidBenchConf = requires {
    { Conf::NameSize } -> std::convertible_to<uint32_t>;
    { Conf::ShmQueueSize } -> std::convertible_to<uint32_t>;
    { Conf::TcpQueueSize } -> std::convertible_to<uint32_t>;
    { Conf::TcpRecvBufInitSize } -> std::convertible_to<uint32_t>;
    { Conf::TcpRecvBufMaxSize } -> std::convertible_to<uint32_t>;
    { Conf::ToLittleEndian } -> std::convertible_to<bool>;
    { Conf::TcpNoDelay } -> std::convertible_to<bool>;
    { Conf::MaxNewConnections } -> std::convertible_to<uint32_t>;
    { Conf::MaxShmGrps } -> std::convertible_to<uint32_t>;
    { Conf::MaxShmConnsPerGrp } -> std::convertible_to<uint32_t>;
    { Conf::MaxTcpGrps } -> std::convertible_to<uint32_t>;
    { Conf::MaxTcpConnsPerGrp } -> std::convertible_to<uint32_t>;
    typename Conf::LoginUserData;
    typename Conf::LoginRspUserData;
    typename Conf::ConnectionUserData;
} && (Conf::NameSize >= 8u)
  && (Conf::ShmQueueSize >= 64u)
  && (Conf::ShmQueueSize % 64u == 0u)
  && std::has_single_bit(Conf::ShmQueueSize / 64u)
  && (Conf::TcpQueueSize >= 8u && Conf::TcpQueueSize % 8u == 0u)
  && (Conf::TcpRecvBufInitSize >= 8u && Conf::TcpRecvBufInitSize % 8u == 0u)
  && (Conf::TcpRecvBufMaxSize >= Conf::TcpRecvBufInitSize && Conf::TcpRecvBufMaxSize % 8u == 0u);

template <class Q>
concept SpscQueueLike = requires(Q& q, uint16_t sz) {
    { q.Alloc(sz) } -> std::same_as<MsgHeader*>;
    { q.Push() } -> std::same_as<void>;
    { q.Front() } -> std::same_as<MsgHeader*>;
    { q.Pop() } -> std::same_as<void>;
};

struct BenchConf {
    static constexpr uint32_t NameSize = 16;
    static constexpr uint32_t ShmQueueSize = MiB(1);
    static constexpr bool ToLittleEndian = true;

    static constexpr uint32_t MaxNewConnections = 4;
    static constexpr uint32_t MaxShmGrps = 1;
    static constexpr uint32_t MaxShmConnsPerGrp = 4;
    static constexpr uint32_t MaxTcpGrps = 1;
    static constexpr uint32_t MaxTcpConnsPerGrp = 4;

    static constexpr uint32_t TcpQueueSize = KiB(256);
    static constexpr uint32_t TcpRecvBufInitSize = KiB(64);
    static constexpr uint32_t TcpRecvBufMaxSize = KiB(256);
    static constexpr bool TcpNoDelay = true;

    static constexpr int64_t NewConnectionTimeout = 5'000'000'000LL;
    static constexpr int64_t ConnectionTimeout = 10'000'000'000LL;
    static constexpr int64_t HeartBeatInverval = 3'000'000'000LL;

    using LoginUserData = char;
    using LoginRspUserData = char;
    using ConnectionUserData = uint64_t;
};

static_assert(ValidBenchConf<BenchConf>, "BenchConf does not satisfy ValidBenchConf");
static_assert(SpscQueueLike<SPSCVarQueue<BenchConf::ShmQueueSize>>);

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

inline int64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
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

inline void write_seq_payload(MsgHeader* hdr, uint16_t payload_size, uint64_t seq) noexcept {
    auto bytes = std::span<std::byte>{reinterpret_cast<std::byte*>(hdr + 1), payload_size};
    auto raw = std::bit_cast<std::array<std::byte, sizeof(uint64_t)>>(seq);
    std::ranges::copy(raw, bytes.begin());
}

inline uint64_t read_seq_payload(const MsgHeader* hdr) noexcept {
    auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte*>(hdr + 1), sizeof(uint64_t)};
    std::array<std::byte, sizeof(uint64_t)> raw{};
    std::ranges::copy(bytes, raw.begin());
    return std::bit_cast<uint64_t>(raw);
}

struct CleanupGuard {
    std::string dir;
    std::vector<std::string> shm_names;

    ~CleanupGuard() {
        for (const auto& s : shm_names) {
            my_shm_unlink(s.c_str());
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
    tcp_socket_t s = tcp_socket(AF_INET, SOCK_STREAM, 0);
    bench_check(s != INVALID_TCP_SOCKET, "tcp_socket");
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    addr.sin_port = 0;
    bench_check(tcp_bind(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0, "tcp_bind");
    socklen_t len = sizeof(addr);
    bench_check(::getsockname(s, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0, "getsockname");
    uint16_t port = ntohs(addr.sin_port);
    tcp_close(s);
    return port;
}

class BenchServer : public TcpShmServer<BenchServer, BenchConf> {
public:
    BenchServer(const std::string& name, const std::string& dir)
        : TcpShmServer(name, dir) {}

    bool StartServer(const char* ip, uint16_t port) { return Start(ip, port); }
    void StopServer() { Stop(); }
    void PollCtlOnce(int64_t now) { PollCtl(now); }
    void PollTcpOnce(int64_t now, int grpid) { PollTcp(now, grpid); }
    void PollShmOnce(int grpid) { PollShm(grpid); }

    void OnSystemError(const char* msg, int sys_errno) {
        std::cerr << std::format("[BenchServer] System error: {} (errno={})\n", msg, sys_errno);
    }

    int OnNewConnection(const struct sockaddr_in& /*addr*/, const LoginMsg* /*login*/, LoginRspMsg* /*rsp*/) {
        return 0;
    }

    void OnClientLogon(const struct sockaddr_in& /*addr*/, Connection& /*conn*/) {}
    void OnClientDisconnected(Connection& /*conn*/, const char* /*reason*/, int /*sys_errno*/) {}

    void OnClientFileError(Connection& /*conn*/, const char* msg, int sys_errno) {
        std::cerr << std::format("[BenchServer] File error: {} (errno={})\n", msg, sys_errno);
    }

    void OnSeqNumberMismatch(Connection& /*conn*/,
                             uint32_t /*la*/, uint32_t /*ls*/, uint32_t /*le*/,
                             uint32_t /*ra*/, uint32_t /*rs*/, uint32_t /*re*/) {
        std::cerr << "[BenchServer] Sequence mismatch\n";
    }

    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        if (recv_header->size < sizeof(MsgHeader)) [[unlikely]] {
            conn.Pop();
            return;
        }
        uint16_t payload_sz = static_cast<uint16_t>(recv_header->size - sizeof(MsgHeader));
        MsgHeader* send_header = conn.Alloc(payload_sz);
        if (!send_header) [[unlikely]] {
            // Backpressure: defer Pop() so next Poll re-delivers after client drains replies
            return;
        }
        send_header->msg_type = recv_header->msg_type;
        if (payload_sz > 0) [[likely]] {
            std::memcpy(send_header + 1, recv_header + 1, payload_sz);
        }
        conn.Pop();
        conn.Push();
    }
};

class BenchClient : public TcpShmClient<BenchClient, BenchConf> {
public:
    BenchClient(const std::string& name, const std::string& dir)
        : TcpShmClient(name, dir), conn_(GetConnection()) {}

    bool ConnectTo(bool use_shm, const char* ip, uint16_t port) {
        return Connect(use_shm, ip, port, 0);
    }
    void StopClient() { Stop(); }
    void PollTcpOnce(int64_t now) { PollTcp(now); }
    void PollShmOnce() { PollShm(); }
    Connection& Conn() noexcept { return conn_; }

    void ResetCounter() noexcept {
        recv_count_.store(0, std::memory_order_relaxed);
        expected_seq_ = 0;
    }

    uint64_t RecvCount() const noexcept {
        return recv_count_.load(std::memory_order_acquire);
    }

    void OnSystemError(const char* msg, int sys_errno) {
        std::cerr << std::format("[BenchClient] System error: {} (errno={})\n", msg, sys_errno);
    }

    void OnLoginReject(const LoginRspMsg* rsp) {
        std::cerr << std::format("[BenchClient] Login rejected: {}\n", rsp->error_msg);
    }

    int64_t OnLoginSuccess(const LoginRspMsg* /*rsp*/) { return now_ns(); }

    void OnDisconnected(const char* /*reason*/, int /*sys_errno*/) {}

    void OnSeqNumberMismatch(uint32_t /*la*/, uint32_t /*ls*/, uint32_t /*le*/,
                             uint32_t /*ra*/, uint32_t /*rs*/, uint32_t /*re*/) {
        std::cerr << "[BenchClient] Sequence mismatch\n";
    }

    void OnServerMsg(MsgHeader* header) {
        bench_check(header->size >= sizeof(MsgHeader) + sizeof(uint64_t), "reply payload size");
        uint64_t seq = read_seq_payload(header);
        bench_check(seq == expected_seq_, "monotonic echo sequence");
        ++expected_seq_;
        conn_.Pop();
        recv_count_.fetch_add(1, std::memory_order_release);
    }

private:
    Connection& conn_;
    uint64_t expected_seq_ = 0;
    std::atomic<uint64_t> recv_count_{0};
};

enum class BenchMode { All, Rtt, Throughput, Spsc };

struct BenchOptions {
    BenchMode mode = BenchMode::All;
    uint32_t iters = 50'000;
    uint32_t warmup = 2'000;
    std::vector<uint32_t> sizes = {16u, 64u, 256u, 1024u};
    int pin_server = -1;
    int pin_client = -1;
};

struct LatencyStats {
    uint64_t avg_ns = 0;
    uint64_t min_ns = 0;
    uint64_t p50_ns = 0;
    uint64_t p90_ns = 0;
    uint64_t p99_ns = 0;
    uint64_t p999_ns = 0;
    uint64_t max_ns = 0;
    double msg_per_sec = 0.0;
};

struct ThroughputStats {
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
    bench_check(full.size() < BenchConf::NameSize, "name fits within NameSize");
    return full;
}

template <SpscQueueLike Q>
ThroughputStats run_spsc_case(uint32_t payload_size, uint32_t warmup, uint32_t iters, int pin_prod, int pin_cons) {
    auto q = std::make_unique<Q>();
    const uint64_t total = static_cast<uint64_t>(warmup) + static_cast<uint64_t>(iters);
    const uint16_t sz = static_cast<uint16_t>(payload_size);
    std::latch ready{2};
    std::atomic<uint64_t> warmup_end_ns{0};

    std::jthread consumer([&](std::stop_token) {
        pin_current_thread(pin_cons);
        ready.arrive_and_wait();
        uint64_t expected = 0;
        while (expected < total) {
            MsgHeader* hdr = q->Front();
            if (!hdr) {
                cpu_relax();
                continue;
            }
            bench_check(read_seq_payload(hdr) == expected, "spsc monotonic seq");
            q->Pop();
            ++expected;
            if (expected == warmup) {
                warmup_end_ns.store(static_cast<uint64_t>(now_ns()), std::memory_order_release);
            }
        }
    });

    std::jthread producer([&](std::stop_token) {
        pin_current_thread(pin_prod);
        ready.arrive_and_wait();
        for (uint64_t seq = 0; seq < total; ++seq) {
            MsgHeader* hdr = nullptr;
            while ((hdr = q->Alloc(sz)) == nullptr) {
                cpu_relax();
            }
            hdr->msg_type = 1;
            write_seq_payload(hdr, sz, seq);
            q->Push();
        }
    });

    producer.join();
    consumer.join();

    uint64_t t1 = static_cast<uint64_t>(now_ns());
    uint64_t t0 = warmup_end_ns.load(std::memory_order_acquire);
    uint64_t elapsed = (t1 > t0) ? (t1 - t0) : 1u;
    double sec = static_cast<double>(elapsed) / 1e9;
    double msg_s = static_cast<double>(iters) / sec;
    double mb_s = (msg_s * static_cast<double>(payload_size)) / (1024.0 * 1024.0);
    return ThroughputStats{
        .msg_per_sec = msg_s,
        .mb_per_sec = mb_s,
        .ns_per_msg = static_cast<double>(elapsed) / static_cast<double>(iters),
        .elapsed_ns = elapsed,
    };
}

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
    BenchServer srv(srv_name, dir);
    bench_check(srv.StartServer("127.0.0.1", port), "StartServer");

    std::latch srv_ready{1};
    std::jthread ctl_thr([&](std::stop_token st) {
        srv_ready.count_down();
        while (!st.stop_requested()) {
            srv.PollCtlOnce(now_ns());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::jthread worker_thr([&](std::stop_token st) {
        pin_current_thread(pin_server);
        while (!st.stop_requested()) {
            if (use_shm) {
                srv.PollShmOnce(0);
            } else {
                srv.PollTcpOnce(now_ns(), 0);
            }
            cpu_relax();
        }
    });
    srv_ready.wait();

    BenchClient cli(cli_name, dir);
    bench_check(cli.ConnectTo(use_shm, "127.0.0.1", port), "ConnectTo");

    // In SHM mode, PollTcp runs on a separate thread solely for heartbeats / disconnect checks.
    std::jthread cli_hb_thr;
    if (use_shm) {
        cli_hb_thr = std::jthread([&](std::stop_token st) {
            while (!st.stop_requested() && !cli.Conn().IsClosed()) {
                cli.PollTcpOnce(now_ns());
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    pin_current_thread(pin_client);
    cli.ResetCounter();

    const uint16_t sz = static_cast<uint16_t>(payload_size);
    const uint64_t total = static_cast<uint64_t>(warmup) + static_cast<uint64_t>(iters);
    std::vector<uint64_t> samples_ns(iters);
    uint64_t measured_ns = 0;

    for (uint64_t seq = 0; seq < total; ++seq) {
        const auto t_start = std::chrono::steady_clock::now();
        MsgHeader* hdr = nullptr;
        while ((hdr = cli.Conn().Alloc(sz)) == nullptr) {
            bench_check(!cli.Conn().IsClosed(), "client closed during Alloc");
            if (use_shm) cli.PollShmOnce();
            else cli.PollTcpOnce(now_ns());
            cpu_relax();
        }
        hdr->msg_type = 1;
        write_seq_payload(hdr, sz, seq);
        cli.Conn().Push();

        const uint64_t target = seq + 1;
        while (cli.RecvCount() < target) {
            bench_check(!cli.Conn().IsClosed(), "client closed waiting for echo");
            if (use_shm) cli.PollShmOnce();
            else cli.PollTcpOnce(now_ns());
            cpu_relax();
        }
        const auto t_end = std::chrono::steady_clock::now();
        if (seq >= warmup) {
            uint64_t rtt = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count());
            samples_ns[seq - warmup] = rtt;
            measured_ns += rtt;
        }
    }

    if (cli_hb_thr.joinable()) {
        cli_hb_thr.request_stop();
        cli_hb_thr.join();
    }
    cli.StopClient();
    worker_thr.request_stop();
    ctl_thr.request_stop();
    worker_thr.join();
    ctl_thr.join();
    srv.StopServer();

    return compute_latency_stats(samples_ns, measured_ns);
}

ThroughputStats run_throughput_case(const std::string& dir,
                                    CleanupGuard& guard,
                                    uint32_t run_id,
                                    bool use_shm,
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
    BenchServer srv(srv_name, dir);
    bench_check(srv.StartServer("127.0.0.1", port), "StartServer");

    std::latch srv_ready{1};
    std::jthread ctl_thr([&](std::stop_token st) {
        srv_ready.count_down();
        while (!st.stop_requested()) {
            srv.PollCtlOnce(now_ns());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::jthread worker_thr([&](std::stop_token st) {
        pin_current_thread(pin_server);
        while (!st.stop_requested()) {
            if (use_shm) srv.PollShmOnce(0);
            else srv.PollTcpOnce(now_ns(), 0);
            cpu_relax();
        }
    });
    srv_ready.wait();

    BenchClient cli(cli_name, dir);
    bench_check(cli.ConnectTo(use_shm, "127.0.0.1", port), "ConnectTo");
    cli.ResetCounter();

    const uint16_t sz = static_cast<uint16_t>(payload_size);
    const uint64_t total = static_cast<uint64_t>(warmup) + static_cast<uint64_t>(iters);
    std::atomic<int64_t> measure_start_ns{0};
    int64_t measure_end_ns = 0;

    if (use_shm) {
        std::latch start_gate{2};
        std::jthread hb_thr([&](std::stop_token st) {
            while (!st.stop_requested() && !cli.Conn().IsClosed()) {
                cli.PollTcpOnce(now_ns());
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
        std::jthread recv_thr([&](std::stop_token) {
            start_gate.arrive_and_wait();
            while (cli.RecvCount() < total) {
                bench_check(!cli.Conn().IsClosed(), "SHM client closed in throughput recv");
                cli.PollShmOnce();
                if (measure_start_ns.load(std::memory_order_acquire) == 0 && cli.RecvCount() >= warmup) {
                    measure_start_ns.store(now_ns(), std::memory_order_release);
                }
                cpu_relax();
            }
            measure_end_ns = now_ns();
        });
        std::jthread send_thr([&](std::stop_token) {
            pin_current_thread(pin_client);
            start_gate.arrive_and_wait();
            for (uint64_t seq = 0; seq < total; ++seq) {
                MsgHeader* hdr = nullptr;
                while ((hdr = cli.Conn().Alloc(sz)) == nullptr) {
                    bench_check(!cli.Conn().IsClosed(), "SHM client closed in throughput Alloc");
                    cpu_relax();
                }
                hdr->msg_type = 1;
                write_seq_payload(hdr, sz, seq);
                cli.Conn().Push();
            }
        });
        send_thr.join();
        recv_thr.join();
        hb_thr.request_stop();
        hb_thr.join();
    } else {
        pin_current_thread(pin_client);
        uint64_t sent = 0;
        while (cli.RecvCount() < total) {
            bench_check(!cli.Conn().IsClosed(), "TCP client closed in throughput");
            uint32_t batch = 0;
            while (sent < total && batch < 32u) {
                MsgHeader* hdr = cli.Conn().Alloc(sz);
                if (!hdr) break;
                hdr->msg_type = 1;
                write_seq_payload(hdr, sz, sent);
                ++sent;
                ++batch;
                if (sent == total || batch == 32u) {
                    cli.Conn().Push();
                } else {
                    cli.Conn().PushMore();
                }
            }
            // Drain all currently buffered/available replies so recvbuf_ does not thrash with memmove
            uint64_t prev_recv;
            do {
                prev_recv = cli.RecvCount();
                cli.PollTcpOnce(now_ns());
            } while (cli.RecvCount() > prev_recv && cli.RecvCount() < total);
            if (measure_start_ns.load(std::memory_order_relaxed) == 0 && cli.RecvCount() >= warmup) {
                measure_start_ns.store(now_ns(), std::memory_order_relaxed);
            }
        }
        measure_end_ns = now_ns();
    }

    cli.StopClient();
    worker_thr.request_stop();
    ctl_thr.request_stop();
    worker_thr.join();
    ctl_thr.join();
    srv.StopServer();

    int64_t t0 = measure_start_ns.load(std::memory_order_acquire);
    uint64_t elapsed = (measure_end_ns > t0) ? static_cast<uint64_t>(measure_end_ns - t0) : 1u;
    double sec = static_cast<double>(elapsed) / 1e9;
    double msg_s = static_cast<double>(iters) / sec;
    double mb_s = (msg_s * static_cast<double>(payload_size)) / (1024.0 * 1024.0);
    return ThroughputStats{
        .msg_per_sec = msg_s,
        .mb_per_sec = mb_s,
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
        BenchConf::TcpRecvBufMaxSize - sizeof(MsgHeader));
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
                    sz > 65535u - sizeof(MsgHeader) ||
                    sz + sizeof(MsgHeader) > BenchConf::TcpRecvBufMaxSize) {
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
                             std::format("tcpshm_bench_{}", current_pid()))
                                .string();
    CleanupGuard guard{dir, {}};
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    std::cout << std::format(
        "tcpshm C++20 Benchmark | iters={} | warmup={} | ShmQueue={} KiB | TcpQueue={} KiB | pin(srv={}, cli={})\n",
        opts.iters,
        opts.warmup,
        BenchConf::ShmQueueSize / 1024u,
        BenchConf::TcpQueueSize / 1024u,
        opts.pin_server,
        opts.pin_client);

    uint32_t run_id = 1;

    if (opts.mode == BenchMode::All || opts.mode == BenchMode::Spsc) {
        std::cout << "\n[1] SPSCVarQueue Raw SPSC Throughput (2 threads, 1-way)\n";
        std::cout << std::format("{:<8} {:>14} {:>12} {:>12}\n", "Bytes", "Msg/sec", "MB/sec", "ns/msg");
        std::cout << std::string(50, '-') << '\n';
        for (uint32_t sz : opts.sizes) {
            auto st = run_spsc_case<SPSCVarQueue<BenchConf::ShmQueueSize>>(
                sz, opts.warmup, opts.iters, opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<8} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     sz,
                                     st.msg_per_sec,
                                     st.mb_per_sec,
                                     st.ns_per_msg);
        }
    }

    if (opts.mode == BenchMode::All || opts.mode == BenchMode::Rtt) {
        std::cout << "\n[2] Ping-Pong Round-Trip Latency (RTT, 1-in-flight)\n";
        std::cout << std::format("{:<6} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12}\n",
                                 "Mode", "Bytes", "Avg(ns)", "Min(ns)", "P50(ns)", "P99(ns)", "P99.9", "Max(ns)", "Msg/sec");
        std::cout << std::string(88, '-') << '\n';
        for (uint32_t sz : opts.sizes) {
            auto tcp = run_rtt_case(dir, guard, run_id++, false, sz, opts.warmup, opts.iters,
                                    opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<6} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12.0f}\n",
                                     "TCP", sz, tcp.avg_ns, tcp.min_ns, tcp.p50_ns, tcp.p99_ns,
                                     tcp.p999_ns, tcp.max_ns, tcp.msg_per_sec);

            auto shm = run_rtt_case(dir, guard, run_id++, true, sz, opts.warmup, opts.iters,
                                    opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<6} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>12.0f}\n",
                                     "SHM", sz, shm.avg_ns, shm.min_ns, shm.p50_ns, shm.p99_ns,
                                     shm.p999_ns, shm.max_ns, shm.msg_per_sec);

            double speedup = static_cast<double>(tcp.p50_ns) /
                             static_cast<double>(std::max<uint64_t>(shm.p50_ns, 1u));
            std::cout << std::format("  -> {}B P50 RTT speedup (TCP/SHM): {:.2f}x\n", sz, speedup);
        }
    }

    if (opts.mode == BenchMode::All || opts.mode == BenchMode::Throughput) {
        std::cout << "\n[3] Pipelined Echo Throughput (unidirectional echo rate)\n";
        std::cout << std::format("{:<6} {:>7} {:>14} {:>12} {:>12}\n",
                                 "Mode", "Bytes", "Msg/sec", "MB/sec", "ns/msg");
        std::cout << std::string(55, '-') << '\n';
        for (uint32_t sz : opts.sizes) {
            auto tcp = run_throughput_case(dir, guard, run_id++, false, sz, opts.warmup, opts.iters,
                                           opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<6} {:>7} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "TCP", sz, tcp.msg_per_sec, tcp.mb_per_sec, tcp.ns_per_msg);

            auto shm = run_throughput_case(dir, guard, run_id++, true, sz, opts.warmup, opts.iters,
                                           opts.pin_server, opts.pin_client);
            std::cout << std::format("{:<6} {:>7} {:>14.0f} {:>12.2f} {:>12.1f}\n",
                                     "SHM", sz, shm.msg_per_sec, shm.mb_per_sec, shm.ns_per_msg);
        }
    }

    return 0;
}
