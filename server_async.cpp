#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iguana/iguana.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>

#include "common.h"
#include "tcpshm/msg_header.h"
#include "tcpshm/spsc_varq.h"
#include "tcpshm/tcpshm_server.h"

using namespace std;
using namespace tcpshm;

struct ConnMeta {
    uint64_t conn_id = 0;
    uint32_t ch_idx = 0;
    uint32_t slot_idx = 0;
};

struct alignas(8) RouteEnvelope {
    uint64_t conn_id;
    uint32_t slot_idx;
    uint16_t msg_type;
    uint16_t reserved;
};
static_assert(sizeof(RouteEnvelope) == 16 && alignof(RouteEnvelope) == 8 &&
                  std::is_trivially_copyable_v<RouteEnvelope>,
              "RouteEnvelope layout must remain 8-byte aligned and trivially copyable");

struct AsyncServerConf : public CommonConf {
    static constexpr int64_t NanoInSecond = 1000000000LL;

    static constexpr uint32_t MaxNewConnections = 5;
    static constexpr uint32_t MaxShmConnsPerGrp = 4;
    static constexpr uint32_t MaxShmGrps = 1;
    static constexpr uint32_t MaxTcpConnsPerGrp = 4;
    static constexpr uint32_t MaxTcpGrps = 1;

    static constexpr uint32_t TcpQueueSize = 3000;        // must be a multiple of 8
    static constexpr uint32_t TcpRecvBufInitSize = 1000;  // must be a multiple of 8
    static constexpr uint32_t TcpRecvBufMaxSize = 2000;   // must be a multiple of 8
    static constexpr bool TcpNoDelay = true;

    static constexpr int64_t NewConnectionTimeout = 3 * NanoInSecond;
    static constexpr int64_t ConnectionTimeout = 10 * NanoInSecond;
    static constexpr int64_t HeartBeatInverval = 3 * NanoInSecond;

    static constexpr uint32_t InterThreadQueueBytes = 64 * 1024;  // 64B * 1024, power-of-2 blocks

    using ConnectionUserData = ConnMeta;
};

class AsyncServer;
using TSAsyncServer = TcpShmServer<AsyncServer, AsyncServerConf>;
static std::atomic<bool> stopped{false};

// 无锁生产版方案 A：零 mutex / 零 condition_variable / 零热路径堆分配与 I/O 打印
// - I/O 线程（每组 PollTcp / PollShm）：只负责搬运包到各自 rx_q、消费 tx_q 调 Alloc/Push，并确认 Retiring 槽位退场
// - 业务线程（biz_thread）：扫描全部通道 rx_q 完成反序列化与业务处理，序列化后投递回对应通道 tx_q，绝不碰 Connection*
// - 控制线程（PollCtl）：通过 ConnSlot::state (Active -> Retiring -> Empty) 无锁同步连接退场后再返回给框架 Release()
class AsyncServer : public TSAsyncServer {
   public:
    AsyncServer(const string& name, const string& ptcp_dir) : TSAsyncServer(name, ptcp_dir) {
        for (uint32_t i = 0; i < AsyncServerConf::MaxTcpGrps; ++i) {
            channels_[i].slot_cap = AsyncServerConf::MaxTcpConnsPerGrp;
        }
        for (uint32_t i = 0; i < AsyncServerConf::MaxShmGrps; ++i) {
            channels_[AsyncServerConf::MaxTcpGrps + i].slot_cap = AsyncServerConf::MaxShmConnsPerGrp;
        }
        signal(SIGTERM, AsyncServer::SignalHandler);
        signal(SIGINT, AsyncServer::SignalHandler);
    }

    static void SignalHandler(int) noexcept { stopped.store(true, std::memory_order_release); }

    void OnSystemError(const char* errno_msg, int sys_errno) {
        std::fprintf(stderr, "System Error: %s (errno=%d)\n", errno_msg, sys_errno);
    }

    int OnNewConnection(const struct sockaddr_in& addr, const LoginMsg* login, LoginRspMsg* /*login_rsp*/) {
        char ipbuf[INET_ADDRSTRLEN]{};
        inet_ntop(AF_INET, &addr.sin_addr, ipbuf, sizeof(ipbuf));
        bool use_shm = (login->use_shm != 0);
        std::fprintf(stdout, "New Connection: %s from %s, use_shm=%d\n",
                     login->client_name, ipbuf, use_shm ? 1 : 0);
        pending_ch_idx_ = ChannelIndex(use_shm, 0);
        return 0;
    }

    void OnClientLogon(const struct sockaddr_in& /*addr*/, Connection& conn) {
        GroupChannel& ch = channels_[pending_ch_idx_];
        uint32_t slot_idx = ch.slot_cap;
        for (uint32_t i = 0; i < ch.slot_cap; ++i) {
            if (ch.slots[i].state.load(std::memory_order_acquire) ==
                static_cast<uint8_t>(ConnSlotState::Empty)) {
                slot_idx = i;
                break;
            }
        }
        if (slot_idx == ch.slot_cap) return;

        uint64_t id = next_conn_id_++;
        conn.user_data = ConnMeta{id, pending_ch_idx_, slot_idx};
        ConnSlot& slot = ch.slots[slot_idx];
        slot.conn = &conn;
        slot.conn_id = id;
        slot.state.store(static_cast<uint8_t>(ConnSlotState::Active), std::memory_order_release);

        std::fprintf(stdout, "Client Logged on: %s (conn_id=%llu, mode=%s, slot=%u)\n",
                     conn.GetRemoteName(),
                     static_cast<unsigned long long>(id),
                     pending_ch_idx_ >= AsyncServerConf::MaxTcpGrps ? "SHM" : "TCP",
                     slot_idx);
    }

    void OnClientDisconnected(Connection& conn, const char* reason, int /*sys_errno*/) {
        const ConnMeta meta = conn.user_data;
        if (meta.ch_idx < TotalGroups) {
            GroupChannel& ch = channels_[meta.ch_idx];
            if (meta.slot_idx < ch.slot_cap && ch.slots[meta.slot_idx].conn_id == meta.conn_id) {
                ConnSlot& slot = ch.slots[meta.slot_idx];
                slot.state.store(static_cast<uint8_t>(ConnSlotState::Retiring), std::memory_order_release);
                ch.retire_pending.fetch_add(1, std::memory_order_release);
                while (slot.state.load(std::memory_order_acquire) !=
                           static_cast<uint8_t>(ConnSlotState::Empty) &&
                       !ch.io_exited.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
            }
        }
        std::fprintf(stdout, "Client Disconnected: %s (conn_id=%llu) Reason: %s\n",
                     conn.GetRemoteName(),
                     static_cast<unsigned long long>(meta.conn_id),
                     reason);
    }

    // 热路径：零解析、零堆分配、零锁、零 I/O 打印
    void OnClientMsg(Connection& conn, MsgHeader* recv_header) {
        if (recv_header->size < sizeof(MsgHeader)) {
            conn.Pop();
            return;
        }
        const size_t body_size = static_cast<size_t>(recv_header->size) - sizeof(MsgHeader);
        if (body_size > 65535u - sizeof(MsgHeader) - sizeof(RouteEnvelope)) {
            rx_drop_cnt_.fetch_add(1, std::memory_order_relaxed);
            conn.Pop();
            return;
        }
        const ConnMeta meta = conn.user_data;
        GroupChannel& ch = channels_[meta.ch_idx];
        const RouteEnvelope env{meta.conn_id, meta.slot_idx, recv_header->msg_type, 0};
        if (!TryEnqueueEnvelope(ch.rx_q, env, recv_header + 1, body_size)) {
            rx_drop_cnt_.fetch_add(1, std::memory_order_relaxed);
        }
        conn.Pop();
    }

    void OnClientFileError(Connection& /*conn*/, const char* r, int e) {
        std::fprintf(stderr, "OnClientFileError %d msg: %s\n", e, r);
    }

    void OnSeqNumberMismatch(Connection& /*c*/, uint32_t /*a*/, uint32_t /*b*/, uint32_t /*d*/,
                             uint32_t /*e*/, uint32_t /*f*/, uint32_t /*g*/) {}

    void Run(const char* ip, uint16_t port) {
        if (!Start(ip, port)) return;

        for (auto& ch : channels_) {
            ch.io_exited.store(false, std::memory_order_relaxed);
        }

        std::thread biz_thread(&AsyncServer::BusinessLoop, this);

        vector<thread> io_threads;
        io_threads.reserve(TotalGroups);

        for (uint32_t i = 0; i < AsyncServerConf::MaxTcpGrps; ++i) {
            io_threads.emplace_back([this, grpid = static_cast<int>(i), ch_idx = i]() {
                GroupChannel& ch = channels_[ch_idx];
                while (!stopped.load(std::memory_order_acquire)) {
                    PollTcp(get_timestamp(), grpid);
                    DrainTx(ch);
                    std::this_thread::yield();
                }
                AckRetiring(ch);
                ch.io_exited.store(true, std::memory_order_release);
            });
        }

        for (uint32_t i = 0; i < AsyncServerConf::MaxShmGrps; ++i) {
            const uint32_t ch_idx = AsyncServerConf::MaxTcpGrps + i;
            io_threads.emplace_back([this, grpid = static_cast<int>(i), ch_idx]() {
                GroupChannel& ch = channels_[ch_idx];
                while (!stopped.load(std::memory_order_acquire)) {
                    PollShm(grpid);
                    DrainTx(ch);
                    std::this_thread::yield();
                }
                AckRetiring(ch);
                ch.io_exited.store(true, std::memory_order_release);
            });
        }

        std::fprintf(stdout, "AsyncServer (Lock-Free Plan A) is running on %s:%u...\n", ip, port);
        std::fflush(stdout);
        while (!stopped.load(std::memory_order_acquire)) {
            PollCtl(get_timestamp());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        if (biz_thread.joinable()) biz_thread.join();
        for (auto& t : io_threads) {
            if (t.joinable()) t.join();
        }
        Stop();
        std::fprintf(stdout,
                     "AsyncServer stopped gracefully. stats: rx_drop=%llu tx_drop=%llu parse_err=%llu send_full=%llu\n",
                     static_cast<unsigned long long>(rx_drop_cnt_.load(std::memory_order_relaxed)),
                     static_cast<unsigned long long>(tx_drop_cnt_.load(std::memory_order_relaxed)),
                     static_cast<unsigned long long>(parse_err_cnt_.load(std::memory_order_relaxed)),
                     static_cast<unsigned long long>(send_full_cnt_.load(std::memory_order_relaxed)));
        std::fflush(stdout);
    }

   private:
    static constexpr uint32_t TotalGroups = AsyncServerConf::MaxTcpGrps + AsyncServerConf::MaxShmGrps;
    static constexpr uint32_t MaxSlotsPerGrp =
        AsyncServerConf::MaxTcpConnsPerGrp > AsyncServerConf::MaxShmConnsPerGrp
            ? AsyncServerConf::MaxTcpConnsPerGrp
            : AsyncServerConf::MaxShmConnsPerGrp;

    enum class ConnSlotState : uint8_t {
        Empty = 0,
        Active = 1,
        Retiring = 2,
    };

    struct alignas(64) ConnSlot {
        Connection* conn = nullptr;
        uint64_t conn_id = 0;
        std::atomic<uint8_t> state{static_cast<uint8_t>(ConnSlotState::Empty)};
    };

    struct alignas(64) GroupChannel {
        uint32_t slot_cap = 0;
        alignas(64) std::atomic<uint32_t> retire_pending{0};
        alignas(64) std::atomic<bool> io_exited{false};
        ConnSlot slots[MaxSlotsPerGrp]{};
        SPSCVarQueue<AsyncServerConf::InterThreadQueueBytes> rx_q;
        SPSCVarQueue<AsyncServerConf::InterThreadQueueBytes> tx_q;
    };

    static uint32_t ChannelIndex(bool is_shm, uint32_t grpid) {
        return (is_shm ? AsyncServerConf::MaxTcpGrps : 0u) + grpid;
    }

    static bool TryEnqueueEnvelope(SPSCVarQueue<AsyncServerConf::InterThreadQueueBytes>& q,
                                   const RouteEnvelope& env,
                                   const void* payload,
                                   size_t payload_len) {
        if (payload_len > 65535u - sizeof(MsgHeader) - sizeof(RouteEnvelope)) {
            return false;
        }
        const auto total_len = static_cast<uint16_t>(sizeof(RouteEnvelope) + payload_len);
        MsgHeader* hdr = q.Alloc(total_len);
        for (int spin = 0; !hdr && spin < 1000 && !stopped.load(std::memory_order_acquire); ++spin) {
            std::this_thread::yield();
            hdr = q.Alloc(total_len);
        }
        if (!hdr) return false;
        hdr->msg_type = 1;
        auto* dst = reinterpret_cast<std::byte*>(hdr + 1);
        std::memcpy(dst, &env, sizeof(RouteEnvelope));
        if (payload_len > 0) {
            std::memcpy(dst + sizeof(RouteEnvelope), payload, payload_len);
        }
        q.Push();
        return true;
    }

    void AckRetiring(GroupChannel& ch) {
        if (ch.retire_pending.load(std::memory_order_acquire) == 0) return;
        for (uint32_t i = 0; i < ch.slot_cap; ++i) {
            ConnSlot& slot = ch.slots[i];
            if (slot.state.load(std::memory_order_acquire) ==
                static_cast<uint8_t>(ConnSlotState::Retiring)) {
                slot.state.store(static_cast<uint8_t>(ConnSlotState::Empty), std::memory_order_release);
                ch.retire_pending.fetch_sub(1, std::memory_order_acq_rel);
            }
        }
    }

    void BusinessLoop() {
        std::string out_buf;
        out_buf.reserve(256);

        while (!stopped.load(std::memory_order_acquire)) {
            bool did_work = false;
            for (uint32_t ch_idx = 0; ch_idx < TotalGroups; ++ch_idx) {
                GroupChannel& ch = channels_[ch_idx];
                while (MsgHeader* hdr = ch.rx_q.Front()) {
                    did_work = true;
                    if (hdr->size < sizeof(MsgHeader) + sizeof(RouteEnvelope)) {
                        ch.rx_q.Pop();
                        parse_err_cnt_.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                    RouteEnvelope env{};
                    const auto* src = reinterpret_cast<const std::byte*>(hdr + 1);
                    std::memcpy(&env, src, sizeof(RouteEnvelope));
                    const size_t body_len =
                        static_cast<size_t>(hdr->size) - sizeof(MsgHeader) - sizeof(RouteEnvelope);
                    std::string_view view{reinterpret_cast<const char*>(src + sizeof(RouteEnvelope)),
                                          body_len};
                    Request req{};
                    bool ok = true;
                    try {
                        iguana::from_pb(req, view);
                    } catch (const std::exception&) {
                        ok = false;
                    }
                    ch.rx_q.Pop();
                    if (!ok) {
                        parse_err_cnt_.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }

                    std::visit(overload{
                                   [&](const Login& login) {
                                       std::fprintf(stdout, "Business handle conn_id=%llu name=%s,pwd=%s\n",
                                                    static_cast<unsigned long long>(env.conn_id),
                                                    login.name.c_str(),
                                                    login.password.c_str());
                                       Response rsp = (login.name == "gewei")
                                                          ? Response{LoginResponse{0, "success"}}
                                                          : Response{LoginResponse{-1, "failure"}};
                                       out_buf.clear();
                                       iguana::to_pb(rsp, out_buf);
                                       const RouteEnvelope rsp_env{env.conn_id, env.slot_idx, 1, 0};
                                       if (!TryEnqueueEnvelope(ch.tx_q, rsp_env, out_buf.data(), out_buf.size())) {
                                           tx_drop_cnt_.fetch_add(1, std::memory_order_relaxed);
                                       }
                                   },
                                   [&](const Order&) {},
                                   [&](const auto&) {}},
                               req.message_type);
                }
            }
            if (!did_work) {
                std::this_thread::yield();
            }
        }
    }

    // 仅由绑定该 GroupChannel 的唯一 I/O 线程调用，独占执行 Alloc/Push，热路径零锁
    void DrainTx(GroupChannel& ch) {
        AckRetiring(ch);
        while (MsgHeader* hdr = ch.tx_q.Front()) {
            if (hdr->size >= sizeof(MsgHeader) + sizeof(RouteEnvelope)) {
                RouteEnvelope env{};
                const auto* src = reinterpret_cast<const std::byte*>(hdr + 1);
                std::memcpy(&env, src, sizeof(RouteEnvelope));
                const size_t payload_len =
                    static_cast<size_t>(hdr->size) - sizeof(MsgHeader) - sizeof(RouteEnvelope);
                if (env.slot_idx < ch.slot_cap) {
                    ConnSlot& slot = ch.slots[env.slot_idx];
                    if (slot.state.load(std::memory_order_acquire) ==
                            static_cast<uint8_t>(ConnSlotState::Active) &&
                        slot.conn_id == env.conn_id &&
                        !slot.conn->IsClosed()) {
                        SendRaw(*slot.conn, env.msg_type, src + sizeof(RouteEnvelope), payload_len);
                    }
                }
            }
            ch.tx_q.Pop();
        }
        AckRetiring(ch);
    }

    void SendRaw(Connection& conn, uint16_t msg_type, const void* payload, size_t payload_len) {
        if (payload_len > 65535u - sizeof(MsgHeader)) {
            send_full_cnt_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        auto* hdr = conn.Alloc(static_cast<uint16_t>(payload_len));
        if (!hdr) {
            send_full_cnt_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        hdr->msg_type = msg_type;
        if (payload_len > 0) {
            std::memcpy(hdr + 1, payload, payload_len);
        }
        conn.Push();
    }

    uint64_t next_conn_id_ = 1;
    uint32_t pending_ch_idx_ = 0;

    std::atomic<uint64_t> rx_drop_cnt_{0};
    std::atomic<uint64_t> tx_drop_cnt_{0};
    std::atomic<uint64_t> parse_err_cnt_{0};
    std::atomic<uint64_t> send_full_cnt_{0};

    GroupChannel channels_[TotalGroups]{};
};

int main() {
    auto server = std::make_unique<AsyncServer>("server01", "server_ptcp");
    server->Run("127.0.0.1", 12345);
}
