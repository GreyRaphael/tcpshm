/*
MIT License

Copyright (c) 2018 Meng Rao <raomeng1@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#pragma once

#include "tcpshm/config.hpp"
#include "tcpshm/conn_handle.hpp"
#include "tcpshm/detail/os_shm.hpp"
#include "tcpshm/detail/os_socket.hpp"
#include "tcpshm/detail/protocol.hpp"
#include "tcpshm/error.hpp"
#include "tcpshm/frame.hpp"
#include "tcpshm/poller.hpp"
#include "tcpshm/shm_channel.hpp"
#include "tcpshm/tcp_channel.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>

namespace tcpshm {

template<typename H, typename Config>
concept ServerHandlerConcept =
    requires(H& h, ShmChannel<Config>& shm_ch, TcpChannel<Config>& tcp_ch, MsgView msg) {
        h.on_message(shm_ch, msg);
        h.on_message(tcp_ch, msg);
    };

// Composition-based high-frequency IPC & persistent-TCP server.
// Features a 100% non-blocking control-plane FSM, generational ConnHandles, and static Busy/Epoll TCP poller policy.
template<class Handler,
         class Config = DefaultConfig,
         template<uint32_t> class TcpPoller = BusyPoller>
class Server
{
    static_assert(ConfigConcept<Config>, "Config must satisfy ConfigConcept");
public:
    using ShmChannelType = ShmChannel<Config>;
    using TcpChannelType = TcpChannel<Config>;
    using LoginMsg = detail::LoginMsg<Config>;
    using LoginRspMsg = detail::LoginRspMsg<Config>;

    Server(Handler& handler, std::string_view server_name, std::string_view ptcp_dir)
        : handler_(handler), ptcp_dir_(ptcp_dir) {
        detail::copy_cstr(server_name_, sizeof(server_name_), server_name);
        detail::make_dir(ptcp_dir_.c_str());
        for (auto& ch : shm_pool_) {
            ch.init(server_name_);
        }
        for (auto& ch : tcp_pool_) {
            ch.init(ptcp_dir_.c_str(), server_name_);
        }
        size_t shm_idx = 0;
        for (auto& grp : shm_grps_) {
            for (uint32_t i = 0; i < Config::max_shm_conns_per_group; ++i) {
                grp.conns[i] = &shm_pool_[shm_idx++];
            }
        }
        size_t tcp_idx = 0;
        for (auto& grp : tcp_grps_) {
            for (uint32_t i = 0; i < Config::max_tcp_conns_per_group; ++i) {
                grp.conns[i] = &tcp_pool_[tcp_idx++];
            }
        }
    }

    ~Server() noexcept { stop(); }

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    [[nodiscard]] VoidResult start(std::string_view listen_ipv4, uint16_t listen_port) noexcept {
        static_assert(ServerHandlerConcept<Handler, Config>,
                      "Handler must implement on_message(ShmChannel&, MsgView) and on_message(TcpChannel&, MsgView)");
        if (listenfd_ != detail::kInvalidSocket) {
            Error err{ErrorCode::AlreadyStarted, 0};
            notify_system_error(err);
            return std::unexpected(err);
        }

        for (uint32_t g = 0; g < Config::max_tcp_groups; ++g) {
            if (auto res = tcp_grps_[g].poller.init(); !res) {
                for (uint32_t prev = 0; prev < g; ++prev) {
                    tcp_grps_[prev].poller.reset();
                }
                notify_system_error(res.error());
                return res;
            }
        }

        auto fail_cleanup = [&](Error err) -> VoidResult {
            if (listenfd_ != detail::kInvalidSocket) {
                detail::close_socket(listenfd_);
                listenfd_ = detail::kInvalidSocket;
            }
            for (auto& grp : tcp_grps_) {
                grp.poller.reset();
            }
            notify_system_error(err);
            return std::unexpected(err);
        };

        listenfd_ = detail::create_socket(AF_INET, SOCK_STREAM, 0);
        if (listenfd_ == detail::kInvalidSocket) {
            return fail_cleanup(Error{ErrorCode::SocketCreateFailed, detail::last_socket_error()});
        }
        if (detail::set_nonblocking(listenfd_) < 0) {
            return fail_cleanup(Error{ErrorCode::SocketOptionFailed, detail::last_socket_error()});
        }
        int yes = 1;
        if (detail::set_sockopt(listenfd_, SOL_SOCKET, SO_REUSEADDR,
                                reinterpret_cast<const char*>(&yes), sizeof(yes)) < 0) {
            return fail_cleanup(Error{ErrorCode::SocketOptionFailed, detail::last_socket_error()});
        }
        if constexpr (Config::tcp_no_delay) {
            if (detail::set_sockopt(listenfd_, IPPROTO_TCP, TCP_NODELAY,
                                    reinterpret_cast<const char*>(&yes), sizeof(yes)) < 0) {
                return fail_cleanup(Error{ErrorCode::SocketOptionFailed, detail::last_socket_error()});
            }
        }

        struct sockaddr_in local_addr{};
        if (!detail::parse_ipv4_endpoint(listen_ipv4, listen_port, local_addr)) {
            return fail_cleanup(Error{ErrorCode::InvalidAddress, detail::last_socket_error()});
        }
        if (detail::bind_socket(listenfd_, reinterpret_cast<struct sockaddr*>(&local_addr), sizeof(local_addr)) < 0) {
            return fail_cleanup(Error{ErrorCode::BindFailed, detail::last_socket_error()});
        }
        if (detail::listen_socket(listenfd_, 64) < 0) {
            return fail_cleanup(Error{ErrorCode::ListenFailed, detail::last_socket_error()});
        }

        const TimePoint init_now = steady_now();
        for (auto& grp : shm_grps_) {
            grp.ctl_dirty.store(false, std::memory_order_relaxed);
            grp.active_cnt = 0;
        }
        for (auto& grp : tcp_grps_) {
            grp.ctl_dirty.store(false, std::memory_order_relaxed);
            grp.active_cnt = 0;
            grp.last_hb_scan = init_now;
            grp.cur_stamp = 0;
            grp.pending_work.fill(0);
            grp.pending_work_cnt = 0;
            grp.is_active.fill(0);
            grp.slot_stamp.fill(0);
        }
        stopping_.store(false, std::memory_order_release);
        return {};
    }

    // Non-blocking control-plane poll: drives accept, non-blocking login/probe FSM, SHM keepalive, and disconnect cleanup.
    void poll_ctl(TimePoint now = steady_now()) {
        if (listenfd_ == detail::kInvalidSocket || stopping_.load(std::memory_order_acquire)) return;

        // Accept available non-blocking client connections into idle PendingConn slots.
        for (uint32_t i = 0; i < Config::max_pending_conns; ++i) {
            PendingConn& p = pending_conns_[i];
            if (p.state != PendingState::Empty) continue;
            struct sockaddr_in addr{};
            socklen_t addr_len = sizeof(addr);
            detail::socket_t fd = detail::accept_socket(listenfd_, reinterpret_cast<struct sockaddr*>(&addr), &addr_len);
            if (fd == detail::kInvalidSocket) break;
            if (detail::set_nonblocking(fd) < 0) {
                detail::close_socket(fd);
                continue;
            }
            if constexpr (Config::tcp_no_delay) {
                int yes = 1;
                (void)detail::set_sockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                                          reinterpret_cast<const char*>(&yes), sizeof(yes));
            }
            p.state = PendingState::ReadingLogin;
            p.fd = fd;
            p.addr = addr;
            p.deadline = now + Config::handshake_timeout;
            p.io_done = 0;
            p.io_total = kLoginWireSize;
            p.reserved_group = 0;
            p.reserved_slot = -1;
            p.reserved_is_shm = false;
        }

        // Advance all active handshake FSMs with zero blocking.
        for (uint32_t i = 0; i < Config::max_pending_conns; ++i) {
            PendingConn& p = pending_conns_[i];
            if (p.state == PendingState::Empty) continue;
            advance_pending(now, p);
            if (stopping_.load(std::memory_order_acquire)) return;
        }

        // Keep SHM sockets alive and handle retired SHM connections.
        for (uint32_t g = 0; g < Config::max_shm_groups; ++g) {
            auto& grp = shm_grps_[g];
            for (uint32_t i = 0; i < Config::max_shm_conns_per_group; ++i) {
                uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
                if (st == SlotFree || st == SlotHandshaking) continue;
                ShmChannelType& ch = *grp.conns[i];
                if (st == SlotPendingAdd || st == SlotLive) {
                    ch.poll_keepalive(now);
                    if (ch.try_close_fd()) {
                        uint8_t cur = st;
                        while (cur == SlotPendingAdd || cur == SlotLive) {
                            if (grp.slot_state[i].compare_exchange_weak(
                                    cur, SlotRetireReq, std::memory_order_acq_rel, std::memory_order_acquire)) {
                                grp.ctl_dirty.store(true, std::memory_order_release);
                                break;
                            }
                        }
                    }
                } else if (st == SlotRetired) {
                    Error err = ch.close_error();
                    notify_client_disconnected(ch, err);
                    if (stopping_.load(std::memory_order_acquire)) return;
                    ch.release();
                    grp.slot_state[i].store(SlotFree, std::memory_order_release);
                }
            }
        }

        // Clean up retired TCP connections.
        for (uint32_t g = 0; g < Config::max_tcp_groups; ++g) {
            auto& grp = tcp_grps_[g];
            for (uint32_t i = 0; i < Config::max_tcp_conns_per_group; ++i) {
                if (grp.slot_state[i].load(std::memory_order_acquire) == SlotRetired) {
                    TcpChannelType& ch = *grp.conns[i];
                    Error err = ch.close_error();
                    notify_client_disconnected(ch, err);
                    if (stopping_.load(std::memory_order_acquire)) return;
                    ch.release();
                    grp.slot_state[i].store(SlotFree, std::memory_order_release);
                }
            }
        }
    }

    uint32_t poll_shm(uint32_t grpid = 0, uint32_t max_batch = Config::poll_batch_size) {
        if (grpid >= Config::max_shm_groups) [[unlikely]] return 0;
        auto& grp = shm_grps_[grpid];
        if (stopping_.load(std::memory_order_relaxed)) [[unlikely]] {
            if (stopping_.load(std::memory_order_acquire)) {
                retire_active_shm(grp);
                return 0;
            }
        }
        if (grp.ctl_dirty.load(std::memory_order_acquire)) [[unlikely]] {
            sync_shm_group(grp);
        }
        uint32_t total = 0;
        for (uint32_t j = 0; j < grp.active_cnt; ++j) {
            ShmChannelType& ch = *grp.conns[grp.active_slots[j]];
            total += ch.drain(
                [this, &ch](MsgView view) -> bool {
                    return dispatch_message(ch, view);
                },
                max_batch);
            if (ch.has_staged()) [[unlikely]] {
                ch.flush();
            }
            if (stopping_.load(std::memory_order_acquire)) [[unlikely]] {
                retire_active_shm(grp);
                return total;
            }
        }
        return total;
    }

    uint32_t poll_tcp(TimePoint now = steady_now(),
                      uint32_t grpid = 0,
                      uint32_t max_batch = Config::poll_batch_size) {
        if (grpid >= Config::max_tcp_groups) [[unlikely]] return 0;
        auto& grp = tcp_grps_[grpid];
        if (stopping_.load(std::memory_order_relaxed)) [[unlikely]] {
            if (stopping_.load(std::memory_order_acquire)) {
                retire_active_tcp(grp);
                return 0;
            }
        }
        if (grp.ctl_dirty.load(std::memory_order_acquire)) [[unlikely]] {
            sync_tcp_group(grp);
        }

        uint32_t total = 0;
        if constexpr (TcpPoller<Config::max_tcp_conns_per_group>::is_event_driven) {
            if (++grp.cur_stamp == 0) {
                grp.slot_stamp.fill(0);
                grp.cur_stamp = 1;
            }
            bool any_closed = false;
            const int ready = grp.poller.poll_ready(
                [&](uint32_t slot) {
                    if (slot >= Config::max_tcp_conns_per_group || !grp.is_active[slot]) return;
                    if (stopping_.load(std::memory_order_acquire)) [[unlikely]] return;
                    grp.slot_stamp[slot] = grp.cur_stamp;
                    TcpChannelType& ch = *grp.conns[slot];
                    total += ch.poll_and_drain(
                        now,
                        [this, &ch](MsgView view) -> bool {
                            return dispatch_message(ch, view);
                        },
                        max_batch);
                    if (ch.is_closed()) [[unlikely]] {
                        any_closed = true;
                    } else {
                        update_pending_work_flag(grp, slot, ch);
                    }
                },
                0);
            if (ready < 0) [[unlikely]] {
                notify_system_error(Error{ErrorCode::EpollWaitFailed, detail::last_socket_error()});
            }

            if (stopping_.load(std::memory_order_acquire)) [[unlikely]] {
                retire_active_tcp(grp);
                return total;
            }

            constexpr Duration kMaxHbScanInterval = std::chrono::milliseconds(100);
            const Duration hb_interval = Config::heartbeat_interval / 4;
            const Duration scan_step = hb_interval < kMaxHbScanInterval ? hb_interval : kMaxHbScanInterval;
            const bool hb_due = (now - grp.last_hb_scan) >= scan_step;
            if (hb_due) {
                grp.last_hb_scan = now;
            }

            if (any_closed || grp.pending_work_cnt > 0 || hb_due) {
                for (uint32_t j = 0; j < grp.active_cnt;) {
                    const uint32_t slot = grp.active_slots[j];
                    TcpChannelType& ch = *grp.conns[slot];
                    if (grp.slot_stamp[slot] != grp.cur_stamp && !ch.is_closed()) {
                        if (grp.pending_work[slot]) {
                            total += ch.drain_buffered(
                                now,
                                [this, &ch](MsgView view) -> bool {
                                    return dispatch_message(ch, view);
                                },
                                max_batch);
                            if (stopping_.load(std::memory_order_acquire)) [[unlikely]] {
                                retire_active_tcp(grp);
                                return total;
                            }
                            if (!ch.is_closed()) {
                                update_pending_work_flag(grp, slot, ch);
                            }
                        } else if (hb_due) {
                            ch.maintain_idle(now);
                        }
                    }
                    if (ch.pending_close_fd() != detail::kInvalidSocket) [[unlikely]] {
                        retire_active_tcp_pos(grp, j);
                    } else {
                        ++j;
                    }
                }
            }
        } else {
            for (uint32_t j = 0; j < grp.active_cnt;) {
                TcpChannelType& ch = *grp.conns[grp.active_slots[j]];
                total += ch.poll_and_drain(
                    now,
                    [this, &ch](MsgView view) -> bool {
                        return dispatch_message(ch, view);
                    },
                    max_batch);
                if (stopping_.load(std::memory_order_acquire)) [[unlikely]] {
                    retire_active_tcp(grp);
                    return total;
                }
                if (ch.pending_close_fd() != detail::kInvalidSocket) [[unlikely]] {
                    retire_active_tcp_pos(grp, j);
                } else {
                    ++j;
                }
            }
        }
        return total;
    }

    // Resolves a generational ConnHandle to its live ShmChannel (nullptr if closed, wrong kind, or stale generation).
    [[nodiscard]] ShmChannelType* try_get_shm(ConnHandle h) noexcept {
        if (!h.valid() || h.transport() != TransportKind::Shm) return nullptr;
        if (h.group_id >= Config::max_shm_groups || h.slot_id >= Config::max_shm_conns_per_group) return nullptr;
        auto& grp = shm_grps_[h.group_id];
        const uint8_t st = grp.slot_state[h.slot_id].load(std::memory_order_acquire);
        if (st != SlotHandshaking && st != SlotPendingAdd && st != SlotLive) return nullptr;
        if (grp.slot_gen[h.slot_id].load(std::memory_order_acquire) != h.generation()) return nullptr;
        ShmChannelType* ch = grp.conns[h.slot_id];
        return (ch && !ch->is_closed()) ? ch : nullptr;
    }

    // Resolves a generational ConnHandle to its live TcpChannel (nullptr if closed, wrong kind, or stale generation).
    [[nodiscard]] TcpChannelType* try_get_tcp(ConnHandle h) noexcept {
        if (!h.valid() || h.transport() != TransportKind::Tcp) return nullptr;
        if (h.group_id >= Config::max_tcp_groups || h.slot_id >= Config::max_tcp_conns_per_group) return nullptr;
        auto& grp = tcp_grps_[h.group_id];
        const uint8_t st = grp.slot_state[h.slot_id].load(std::memory_order_acquire);
        if (st != SlotHandshaking && st != SlotPendingAdd && st != SlotLive) return nullptr;
        if (grp.slot_gen[h.slot_id].load(std::memory_order_acquire) != h.generation()) return nullptr;
        TcpChannelType* ch = grp.conns[h.slot_id];
        return (ch && !ch->is_closed()) ? ch : nullptr;
    }

    // Branch-minimal static dispatch helper for handlers holding a ConnHandle.
    template<typename Fn>
    bool with_channel(ConnHandle h, Fn&& fn) {
        if (!h.valid()) return false;
        if (h.transport() == TransportKind::Shm) {
            if (auto* ch = try_get_shm(h)) {
                fn(*ch);
                return true;
            }
        } else {
            if (auto* ch = try_get_tcp(h)) {
                fn(*ch);
                return true;
            }
        }
        return false;
    }

    void stop() noexcept {
        if (in_stop_.exchange(true, std::memory_order_acq_rel)) return;
        stopping_.store(true, std::memory_order_release);
        if (listenfd_ == detail::kInvalidSocket) {
            in_stop_.store(false, std::memory_order_release);
            return;
        }
        detail::close_socket(listenfd_);
        listenfd_ = detail::kInvalidSocket;

        for (auto& p : pending_conns_) {
            reset_pending_conn(p, true);
        }

        auto request_retire_all = [](auto& grps, uint32_t slots_per_grp) {
            bool any_pending = false;
            for (auto& grp : grps) {
                bool dirty = false;
                for (uint32_t i = 0; i < slots_per_grp; ++i) {
                    uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
                    while (st == SlotPendingAdd || st == SlotLive) {
                        if (grp.slot_state[i].compare_exchange_weak(
                                st, SlotRetireReq, std::memory_order_acq_rel, std::memory_order_acquire)) {
                            dirty = true;
                            any_pending = true;
                            break;
                        }
                    }
                    if (st == SlotRetireReq) {
                        dirty = true;
                        any_pending = true;
                    }
                }
                if (dirty) grp.ctl_dirty.store(true, std::memory_order_release);
            }
            return any_pending;
        };

        const bool pending_shm = request_retire_all(shm_grps_, Config::max_shm_conns_per_group);
        const bool pending_tcp = request_retire_all(tcp_grps_, Config::max_tcp_conns_per_group);

        if (pending_shm || pending_tcp) {
            const TimePoint deadline = steady_now() + std::chrono::milliseconds(20);
            while (steady_now() < deadline) {
                auto has_unretired = [](const auto& grps, uint32_t slots_per_grp) {
                    for (const auto& grp : grps) {
                        for (uint32_t i = 0; i < slots_per_grp; ++i) {
                            const uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
                            if (st != SlotFree && st != SlotRetired) return true;
                        }
                    }
                    return false;
                };
                if (!has_unretired(shm_grps_, Config::max_shm_conns_per_group) &&
                    !has_unretired(tcp_grps_, Config::max_tcp_conns_per_group)) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }

        for (auto& grp : shm_grps_) {
            for (uint32_t i = 0; i < Config::max_shm_conns_per_group; ++i) {
                if (grp.slot_state[i].load(std::memory_order_acquire) != SlotFree) {
                    grp.conns[i]->release();
                    grp.slot_state[i].store(SlotFree, std::memory_order_release);
                }
            }
            grp.ctl_dirty.store(false, std::memory_order_release);
        }
        for (auto& grp : tcp_grps_) {
            for (uint32_t i = 0; i < Config::max_tcp_conns_per_group; ++i) {
                if (grp.slot_state[i].load(std::memory_order_acquire) != SlotFree) {
                    if (grp.conns[i]->pending_close_fd() != detail::kInvalidSocket) {
                        grp.poller.on_remove(grp.conns[i]->pending_close_fd());
                    } else if (grp.conns[i]->socket_fd() != detail::kInvalidSocket) {
                        grp.poller.on_remove(grp.conns[i]->socket_fd());
                    }
                    grp.conns[i]->release();
                    grp.slot_state[i].store(SlotFree, std::memory_order_release);
                }
            }
            grp.poller.reset();
            grp.ctl_dirty.store(false, std::memory_order_release);
        }
        in_stop_.store(false, std::memory_order_release);
    }

private:
    enum SlotState : uint8_t
    {
        SlotFree = 0,
        SlotHandshaking = 1,
        SlotPendingAdd = 2,
        SlotLive = 3,
        SlotRetireReq = 4,
        SlotRetired = 5,
    };

    enum class PendingState : uint8_t
    {
        Empty = 0,
        ReadingLogin,
        SendingProbeReq,
        ReadingProbeRsp,
        SendingLoginRsp,
    };

    static constexpr size_t kLoginWireSize = sizeof(FrameHeader) + sizeof(LoginMsg);
    static constexpr size_t kLoginRspWireSize = sizeof(FrameHeader) + sizeof(LoginRspMsg);
    static constexpr size_t kProbeReqWireSize = sizeof(FrameHeader) + sizeof(detail::ShmProbeReqMsg);
    static constexpr size_t kProbeRspWireSize = sizeof(FrameHeader) + sizeof(detail::ShmProbeRspMsg);
    static constexpr size_t kMaxHandshakeRxSize =
        kLoginWireSize > kProbeRspWireSize ? kLoginWireSize : kProbeRspWireSize;
    static constexpr size_t kMaxHandshakeTxSize =
        kLoginRspWireSize > kProbeReqWireSize ? kLoginRspWireSize : kProbeReqWireSize;

    struct PendingConn
    {
        PendingState state = PendingState::Empty;
        detail::socket_t fd = detail::kInvalidSocket;
        TimePoint deadline{};
        struct sockaddr_in addr{};
        size_t io_done = 0;
        size_t io_total = 0;
        alignas(8) std::byte rx_buf[kMaxHandshakeRxSize]{};
        alignas(8) std::byte tx_buf[kMaxHandshakeTxSize]{};

        // Cached parsed LoginMsg across asynchronous SHM probe steps
        LoginMsg parsed_login{};
        uint32_t remote_ack_seq = 0;
        uint32_t target_grpid = 0;
        typename Config::LoginRspUserData callback_rsp_user_data{};

        // Active SHM probe state
        detail::ShmProbeData* probe_ptr = nullptr;
        uint64_t probe_token = 0;
        char probe_name[detail::kShmProbeNameSize]{};

        // Reserved slot state during SendingLoginRsp(status == 0)
        uint32_t reserved_group = 0;
        int32_t reserved_slot = -1;
        bool reserved_is_shm = false;
    };

    template<class ChannelType, uint32_t N>
    struct alignas(64) ShmGroup
    {
        std::array<ChannelType*, N> conns{};
        alignas(64) std::atomic<bool> ctl_dirty{false};
        std::array<std::atomic<uint8_t>, N> slot_state{};
        std::array<std::atomic<uint32_t>, N> slot_gen{};
        alignas(64) uint32_t active_cnt = 0;
        std::array<uint32_t, N> active_slots{};
    };

    template<class ChannelType, uint32_t N, template<uint32_t> class PollerTpl>
    struct alignas(64) TcpGroup
    {
        std::array<ChannelType*, N> conns{};
        alignas(64) std::atomic<bool> ctl_dirty{false};
        std::array<std::atomic<uint8_t>, N> slot_state{};
        std::array<std::atomic<uint32_t>, N> slot_gen{};
        alignas(64) uint32_t active_cnt = 0;
        std::array<uint32_t, N> active_slots{};
        std::array<uint8_t, N> is_active{};
        std::array<uint8_t, N> pending_work{};
        uint32_t pending_work_cnt = 0;
        std::array<uint32_t, N> slot_stamp{};
        uint32_t cur_stamp = 0;
        TimePoint last_hb_scan{};
        PollerTpl<N> poller{};
    };

    // Optional Handler notification helpers
    void notify_system_error(Error err) {
        if constexpr (requires { handler_.on_system_error(err); }) {
            handler_.on_system_error(err);
        }
    }

    template<class ChannelType>
    void notify_client_file_error(ChannelType& ch, Error err) {
        if constexpr (requires { handler_.on_client_file_error(ch, err); }) {
            handler_.on_client_file_error(ch, err);
        }
    }

    void notify_seq_mismatch(TcpChannelType& ch, const SeqMismatchInfo& info) {
        if constexpr (requires { handler_.on_seq_mismatch(ch, info); }) {
            handler_.on_seq_mismatch(ch, info);
        }
    }

    int notify_new_connection(const struct sockaddr_in& addr, const LoginMsg& login, LoginRspMsg& rsp) {
        if constexpr (requires { { handler_.on_new_connection(addr, login, rsp) } -> std::convertible_to<int>; }) {
            return handler_.on_new_connection(addr, login, rsp);
        } else {
            (void)addr;
            (void)login;
            (void)rsp;
            return 0;
        }
    }

    template<class ChannelType>
    void notify_client_logon(const struct sockaddr_in& addr, ChannelType& ch) {
        if constexpr (requires { handler_.on_client_logon(addr, ch); }) {
            handler_.on_client_logon(addr, ch);
        }
    }

    template<class ChannelType>
    void notify_client_disconnected(ChannelType& ch, Error err) {
        if constexpr (requires { handler_.on_client_disconnected(ch, err); }) {
            handler_.on_client_disconnected(ch, err);
        }
    }

    template<class ChannelType>
    bool dispatch_message(ChannelType& ch, MsgView view) {
        using Ret = decltype(handler_.on_message(ch, view));
        if constexpr (std::same_as<Ret, bool>) {
            return handler_.on_message(ch, view);
        } else {
            handler_.on_message(ch, view);
            return true;
        }
    }

    static uint32_t bump_generation(std::atomic<uint32_t>& gen_atom) noexcept {
        uint32_t g = (gen_atom.load(std::memory_order_relaxed) + 1u) & ConnHandle::kGenMask;
        if (g == 0u) g = 1u;
        gen_atom.store(g, std::memory_order_release);
        return g;
    }

    static void update_pending_work_flag(auto& grp, uint32_t slot, const TcpChannelType& ch) noexcept {
        const uint8_t want = (ch.has_buffered_rx() || ch.has_pending_send()) ? 1u : 0u;
        if (grp.pending_work[slot] != want) {
            grp.pending_work[slot] = want;
            grp.pending_work_cnt = want ? (grp.pending_work_cnt + 1u) : (grp.pending_work_cnt - 1u);
        }
    }

    template<uint32_t N>
    void sync_shm_group(ShmGroup<ShmChannelType, N>& grp) noexcept {
        grp.ctl_dirty.exchange(false, std::memory_order_acq_rel);
        for (uint32_t i = 0; i < N; ++i) {
            const uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
            if (st == SlotPendingAdd) {
                uint8_t expected = SlotPendingAdd;
                if (grp.slot_state[i].compare_exchange_strong(
                        expected, SlotLive, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    grp.active_slots[grp.active_cnt++] = i;
                } else if (expected == SlotRetireReq) {
                    grp.slot_state[i].store(SlotRetired, std::memory_order_release);
                }
            } else if (st == SlotRetireReq) {
                for (uint32_t j = 0; j < grp.active_cnt; ++j) {
                    if (grp.active_slots[j] == i) {
                        grp.active_slots[j] = grp.active_slots[--grp.active_cnt];
                        break;
                    }
                }
                grp.slot_state[i].store(SlotRetired, std::memory_order_release);
            }
        }
    }

    template<uint32_t N, template<uint32_t> class PollerTpl>
    void retire_active_tcp_pos(TcpGroup<TcpChannelType, N, PollerTpl>& grp, uint32_t pos) noexcept {
        const uint32_t slot = grp.active_slots[pos];
        TcpChannelType& ch = *grp.conns[slot];
        ch.close();
        const detail::socket_t fd = ch.pending_close_fd();
        if (fd != detail::kInvalidSocket) {
            grp.poller.on_remove(fd);
            ch.try_close_fd();
        }
        grp.is_active[slot] = 0;
        if (grp.pending_work[slot]) {
            grp.pending_work[slot] = 0;
            --grp.pending_work_cnt;
        }
        grp.active_slots[pos] = grp.active_slots[--grp.active_cnt];
        grp.slot_state[slot].store(SlotRetired, std::memory_order_release);
    }

    template<uint32_t N, template<uint32_t> class PollerTpl>
    void sync_tcp_group(TcpGroup<TcpChannelType, N, PollerTpl>& grp) {
        grp.ctl_dirty.exchange(false, std::memory_order_acq_rel);
        for (uint32_t i = 0; i < N; ++i) {
            const uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
            if (st == SlotPendingAdd) {
                uint8_t expected = SlotPendingAdd;
                if (grp.slot_state[i].compare_exchange_strong(
                        expected, SlotLive, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    TcpChannelType& ch = *grp.conns[i];
                    if (auto res = grp.poller.on_add(ch.socket_fd(), i); !res) {
                        ch.close_with(res.error());
                        ch.try_close_fd();
                        grp.slot_state[i].store(SlotRetired, std::memory_order_release);
                        notify_system_error(res.error());
                        continue;
                    }
                    grp.is_active[i] = 1;
                    grp.pending_work[i] = 0;
                    grp.slot_stamp[i] = grp.cur_stamp;
                    grp.active_slots[grp.active_cnt++] = i;
                    update_pending_work_flag(grp, i, ch);
                } else if (expected == SlotRetireReq) {
                    grp.conns[i]->close();
                    grp.conns[i]->try_close_fd();
                    grp.slot_state[i].store(SlotRetired, std::memory_order_release);
                }
            } else if (st == SlotRetireReq) {
                bool found = false;
                for (uint32_t j = 0; j < grp.active_cnt; ++j) {
                    if (grp.active_slots[j] == i) {
                        retire_active_tcp_pos(grp, j);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    grp.conns[i]->close();
                    grp.conns[i]->try_close_fd();
                    grp.slot_state[i].store(SlotRetired, std::memory_order_release);
                }
            }
        }
    }

    template<uint32_t N>
    static void retire_active_shm(ShmGroup<ShmChannelType, N>& grp) noexcept {
        for (uint32_t j = 0; j < grp.active_cnt; ++j) {
            const uint32_t s = grp.active_slots[j];
            if (grp.slot_state[s].load(std::memory_order_acquire) == SlotRetireReq) {
                grp.slot_state[s].store(SlotRetired, std::memory_order_release);
            }
        }
        grp.active_cnt = 0;
    }

    template<uint32_t N, template<uint32_t> class PollerTpl>
    void retire_active_tcp(TcpGroup<TcpChannelType, N, PollerTpl>& grp) noexcept {
        while (grp.active_cnt > 0) {
            const uint32_t pos = grp.active_cnt - 1u;
            const uint32_t s = grp.active_slots[pos];
            if (grp.slot_state[s].load(std::memory_order_acquire) == SlotRetireReq) {
                retire_active_tcp_pos(grp, pos);
            } else {
                const detail::socket_t fd = grp.conns[s]->socket_fd();
                if (fd != detail::kInvalidSocket) grp.poller.on_remove(fd);
                grp.is_active[s] = 0;
                --grp.active_cnt;
            }
        }
        grp.pending_work.fill(0);
        grp.pending_work_cnt = 0;
    }

    [[nodiscard]] bool is_client_name_live(std::string_view name) const noexcept {
        for (const auto& grp : shm_grps_) {
            for (uint32_t i = 0; i < Config::max_shm_conns_per_group; ++i) {
                if (grp.slot_state[i].load(std::memory_order_acquire) != SlotFree &&
                    std::string_view(grp.conns[i]->remote_name()) == name) {
                    return true;
                }
            }
        }
        for (const auto& grp : tcp_grps_) {
            for (uint32_t i = 0; i < Config::max_tcp_conns_per_group; ++i) {
                if (grp.slot_state[i].load(std::memory_order_acquire) != SlotFree &&
                    std::string_view(grp.conns[i]->remote_name()) == name) {
                    return true;
                }
            }
        }
        return false;
    }

    void cleanup_probe(PendingConn& p) noexcept {
        if (p.probe_ptr) {
            detail::munmap_region(p.probe_ptr);
            detail::shm_unlink_name(p.probe_name);
            p.probe_ptr = nullptr;
            p.probe_name[0] = '\0';
        }
    }

    void rollback_reserved_slot(PendingConn& p) noexcept {
        if (p.reserved_slot < 0) return;
        const uint32_t g = p.reserved_group;
        const uint32_t s = static_cast<uint32_t>(p.reserved_slot);
        if (p.reserved_is_shm) {
            shm_grps_[g].conns[s]->release();
            shm_grps_[g].slot_state[s].store(SlotFree, std::memory_order_release);
        } else {
            tcp_grps_[g].conns[s]->release();
            tcp_grps_[g].slot_state[s].store(SlotFree, std::memory_order_release);
        }
        p.reserved_slot = -1;
    }

    void reset_pending_conn(PendingConn& p, bool rollback_slot) noexcept {
        cleanup_probe(p);
        if (rollback_slot) {
            rollback_reserved_slot(p);
        } else {
            p.reserved_slot = -1;
        }
        if (p.fd != detail::kInvalidSocket) {
            detail::close_socket(p.fd);
            p.fd = detail::kInvalidSocket;
        }
        p.io_done = 0;
        p.io_total = 0;
        p.state = PendingState::Empty;
    }

    void prepare_login_rsp(PendingConn& p,
                           uint32_t ack_seq,
                           uint8_t status,
                           uint32_t seq_start,
                           uint32_t seq_end,
                           std::string_view err_msg,
                           const typename Config::LoginRspUserData& rsp_user_data = {}) noexcept {
        FrameHeader hdr{static_cast<uint16_t>(kLoginRspWireSize), detail::kLoginRspMsgType, ack_seq};
        hdr.template convert_byte_order<Config::wire_endian>();

        LoginRspMsg rsp{};
        rsp.magic = detail::kProtocolMagic;
        rsp.version = detail::kProtocolVersion;
        rsp.server_seq_start = seq_start;
        rsp.server_seq_end = seq_end;
        rsp.user_data = rsp_user_data;
        rsp.status = status;
        detail::copy_cstr(rsp.server_name, sizeof(rsp.server_name), server_name_);
        detail::copy_cstr(rsp.error_msg, sizeof(rsp.error_msg), err_msg);
        rsp.convert_byte_order();

        std::memcpy(p.tx_buf, &hdr, sizeof(FrameHeader));
        std::memcpy(p.tx_buf + sizeof(FrameHeader), &rsp, sizeof(LoginRspMsg));
        p.io_done = 0;
        p.io_total = kLoginRspWireSize;
        p.state = PendingState::SendingLoginRsp;
    }

    uint64_t make_probe_token(const char* client_name) const noexcept {
        const uint64_t now_ticks = static_cast<uint64_t>(steady_now().time_since_epoch().count());
        uint64_t salt = static_cast<uint64_t>(reinterpret_cast<std::uintptr_t>(this));
        for (const char* p = client_name; *p; ++p) {
            salt = (salt * 131u) + static_cast<unsigned char>(*p);
        }
        return now_ticks ^ (salt << 17) ^ (salt >> 7);
    }

    enum class IoStep : uint8_t
    {
        Again = 0,
        Complete = 1,
        Failed = 2,
    };

    static IoStep drive_nonblocking_recv(PendingConn& p, TimePoint now) noexcept {
        while (p.io_done < p.io_total) {
            const int remain = static_cast<int>(p.io_total - p.io_done);
            int ret = detail::recv_bytes(p.fd, reinterpret_cast<char*>(p.rx_buf) + p.io_done, remain, 0);
            if (ret > 0) {
                p.io_done += static_cast<size_t>(ret);
                continue;
            }
            if (ret == 0) return IoStep::Failed;
            int err = detail::last_socket_error();
            if (detail::is_retryable_err(err)) {
                return (now <= p.deadline) ? IoStep::Again : IoStep::Failed;
            }
            return IoStep::Failed;
        }
        return IoStep::Complete;
    }

    static IoStep drive_nonblocking_send(PendingConn& p, TimePoint now) noexcept {
        while (p.io_done < p.io_total) {
            const int remain = static_cast<int>(p.io_total - p.io_done);
            int ret = detail::send_bytes(p.fd, reinterpret_cast<const char*>(p.tx_buf) + p.io_done,
                                         remain, detail::kMsgNoSignal);
            if (ret > 0) {
                p.io_done += static_cast<size_t>(ret);
                continue;
            }
            if (ret == 0) return IoStep::Failed;
            int err = detail::last_socket_error();
            if (detail::is_retryable_err(err)) {
                return (now <= p.deadline) ? IoStep::Again : IoStep::Failed;
            }
            return IoStep::Failed;
        }
        return IoStep::Complete;
    }

    void on_login_received(PendingConn& p) {
        FrameHeader hdr{};
        std::memcpy(&hdr, p.rx_buf, sizeof(FrameHeader));
        hdr.template convert_byte_order<Config::wire_endian>();
        if (hdr.size != kLoginWireSize || hdr.msg_type != detail::kLoginMsgType) {
            reset_pending_conn(p, false);
            return;
        }

        LoginMsg login{};
        std::memcpy(&login, p.rx_buf + sizeof(FrameHeader), sizeof(LoginMsg));
        login.convert_byte_order();
        login.client_name[Config::name_size - 1] = '\0';
        login.last_server_name[Config::name_size - 1] = '\0';

        if (login.magic != detail::kProtocolMagic || login.version != detail::kProtocolVersion) {
            prepare_login_rsp(p, 0, 2, 0, 0, "Protocol version mismatch");
            return;
        }
        if (login.cfg_fingerprint != config_fingerprint<Config>()) {
            prepare_login_rsp(p, 0, 2, 0, 0, "Config layout mismatch");
            return;
        }
        if (!detail::is_valid_peer_name(login.client_name) ||
            std::string_view(login.client_name) == std::string_view(server_name_)) {
            prepare_login_rsp(p, 0, 2, 0, 0, "Invalid client name");
            return;
        }

        LoginRspMsg user_rsp{};
        const int grpid = notify_new_connection(p.addr, login, user_rsp);
        if (stopping_.load(std::memory_order_acquire)) {
            reset_pending_conn(p, false);
            return;
        }
        const uint32_t max_grps = login.use_shm ? Config::max_shm_groups : Config::max_tcp_groups;
        if (grpid < 0 || static_cast<uint32_t>(grpid) >= max_grps) {
            const std::string_view msg = (user_rsp.error_msg[0] != '\0')
                                             ? std::string_view(user_rsp.error_msg, strnlen(user_rsp.error_msg, sizeof(user_rsp.error_msg)))
                                             : (grpid < 0 ? std::string_view("Login rejected") : std::string_view("Invalid group id"));
            prepare_login_rsp(p, 0, 2, 0, 0, msg, user_rsp.user_data);
            return;
        }
        if (is_client_name_live(login.client_name)) {
            prepare_login_rsp(p, 0, 2, 0, 0, "Already logged in", user_rsp.user_data);
            return;
        }

        p.parsed_login = login;
        p.remote_ack_seq = hdr.ack_seq;
        p.target_grpid = static_cast<uint32_t>(grpid);
        p.callback_rsp_user_data = user_rsp.user_data;

        if (login.use_shm) {
            if (!start_shm_probe(p)) {
                prepare_login_rsp(p, 0, 2, 0, 0, "SHM probe init failed", p.callback_rsp_user_data);
            }
            return;
        }

        finish_login_and_reserve_slot(p);
    }

    bool start_shm_probe(PendingConn& p) noexcept {
        p.probe_token = make_probe_token(p.parsed_login.client_name);
        const uint64_t seq = probe_seq_.fetch_add(1, std::memory_order_relaxed);
        std::snprintf(p.probe_name,
                      sizeof(p.probe_name),
                      "/tcpshm_probe_%016llx_%llx",
                      static_cast<unsigned long long>(p.probe_token),
                      static_cast<unsigned long long>(seq));
        auto res = detail::mmap_create_or_open<detail::ShmProbeData>(p.probe_name, true, true);
        if (!res) {
            p.probe_name[0] = '\0';
            return false;
        }
        p.probe_ptr = *res;
        std::atomic_ref<uint64_t>(p.probe_ptr->token).store(p.probe_token, std::memory_order_relaxed);
        std::atomic_ref<uint64_t>(p.probe_ptr->ack).store(0, std::memory_order_release);

        FrameHeader hdr{static_cast<uint16_t>(kProbeReqWireSize), detail::kShmProbeReqType, 0};
        hdr.template convert_byte_order<Config::wire_endian>();

        detail::ShmProbeReqMsg req{};
        req.token = p.probe_token;
        detail::copy_cstr(req.shm_name, sizeof(req.shm_name), p.probe_name);
        req.template convert_byte_order<Config::wire_endian>();

        std::memcpy(p.tx_buf, &hdr, sizeof(FrameHeader));
        std::memcpy(p.tx_buf + sizeof(FrameHeader), &req, sizeof(detail::ShmProbeReqMsg));
        p.io_done = 0;
        p.io_total = kProbeReqWireSize;
        p.state = PendingState::SendingProbeReq;
        return true;
    }

    void on_probe_rsp_received(PendingConn& p) {
        FrameHeader hdr{};
        std::memcpy(&hdr, p.rx_buf, sizeof(FrameHeader));
        hdr.template convert_byte_order<Config::wire_endian>();
        detail::ShmProbeRspMsg rsp{};
        std::memcpy(&rsp, p.rx_buf + sizeof(FrameHeader), sizeof(detail::ShmProbeRspMsg));
        rsp.template convert_byte_order<Config::wire_endian>();

        const uint64_t expected_ack = p.probe_token ^ detail::kShmProbeAckMask;
        const bool verified =
            hdr.size == kProbeRspWireSize &&
            hdr.msg_type == detail::kShmProbeRspType &&
            rsp.ok != 0 &&
            rsp.ack == expected_ack &&
            p.probe_ptr != nullptr &&
            std::atomic_ref<uint64_t>(p.probe_ptr->ack).load(std::memory_order_acquire) == expected_ack;

        cleanup_probe(p);
        if (!verified) {
            prepare_login_rsp(p, 0, 2, 0, 0, "SHM probe failed", p.callback_rsp_user_data);
            return;
        }
        finish_login_and_reserve_slot(p);
    }

    void finish_login_and_reserve_slot(PendingConn& p) {
        if (is_client_name_live(p.parsed_login.client_name)) {
            prepare_login_rsp(p, 0, 2, 0, 0, "Already logged in", p.callback_rsp_user_data);
            return;
        }

        if (p.parsed_login.use_shm) {
            auto& grp = shm_grps_[p.target_grpid];
            int free_idx = -1;
            for (uint32_t i = 0; i < Config::max_shm_conns_per_group; ++i) {
                if (grp.slot_state[i].load(std::memory_order_acquire) == SlotFree) {
                    free_idx = static_cast<int>(i);
                    break;
                }
            }
            if (free_idx < 0) {
                prepare_login_rsp(p, 0, 2, 0, 0, "Max client count exceeded", p.callback_rsp_user_data);
                return;
            }
            const uint32_t s = static_cast<uint32_t>(free_idx);
            grp.slot_state[s].store(SlotHandshaking, std::memory_order_release);
            ShmChannelType& ch = *grp.conns[s];
            const uint32_t gen = bump_generation(grp.slot_gen[s]);
            ch.set_handle(ConnHandle::make(TransportKind::Shm, static_cast<uint16_t>(p.target_grpid),
                                           static_cast<uint16_t>(s), gen));
            ch.set_remote_name(p.parsed_login.client_name);

            if (auto open_res = ch.open_shm_files(); !open_res) {
                notify_client_file_error(ch, open_res.error());
                ch.release();
                grp.slot_state[s].store(SlotFree, std::memory_order_release);
                prepare_login_rsp(p, 0, 2, 0, 0, "System error", p.callback_rsp_user_data);
                return;
            }
            if (std::string_view(p.parsed_login.last_server_name) != std::string_view(server_name_)) {
                ch.reset_queues();
            }
            p.reserved_group = p.target_grpid;
            p.reserved_slot = free_idx;
            p.reserved_is_shm = true;
            prepare_login_rsp(p, 0, 0, 0, 0, "", p.callback_rsp_user_data);
            return;
        }

        // TCP channel reservation & PTCP sequence check
        auto& grp = tcp_grps_[p.target_grpid];
        int free_idx = -1;
        for (uint32_t i = 0; i < Config::max_tcp_conns_per_group; ++i) {
            if (grp.slot_state[i].load(std::memory_order_acquire) == SlotFree) {
                free_idx = static_cast<int>(i);
                break;
            }
        }
        if (free_idx < 0) {
            prepare_login_rsp(p, 0, 2, 0, 0, "Max client count exceeded", p.callback_rsp_user_data);
            return;
        }
        const uint32_t s = static_cast<uint32_t>(free_idx);
        grp.slot_state[s].store(SlotHandshaking, std::memory_order_release);
        TcpChannelType& ch = *grp.conns[s];
        const uint32_t gen = bump_generation(grp.slot_gen[s]);
        ch.set_handle(ConnHandle::make(TransportKind::Tcp, static_cast<uint16_t>(p.target_grpid),
                                       static_cast<uint16_t>(s), gen));
        ch.set_remote_name(p.parsed_login.client_name);

        if (auto open_res = ch.open_file(); !open_res) {
            notify_client_file_error(ch, open_res.error());
            ch.release();
            grp.slot_state[s].store(SlotFree, std::memory_order_release);
            prepare_login_rsp(p, 0, 2, 0, 0, "System error", p.callback_rsp_user_data);
            return;
        }

        uint32_t local_ack_seq = 0;
        uint32_t local_seq_start = 0;
        uint32_t local_seq_end = 0;
        uint32_t remote_ack_seq = p.remote_ack_seq;
        uint32_t remote_seq_start = p.parsed_login.client_seq_start;
        uint32_t remote_seq_end = p.parsed_login.client_seq_end;

        if (std::string_view(p.parsed_login.last_server_name) != std::string_view(server_name_)) {
            ch.reset_queue();
            remote_ack_seq = remote_seq_start = remote_seq_end = 0;
        } else {
            auto seq_res = ch.get_seq();
            if (!seq_res) {
                notify_client_file_error(ch, seq_res.error());
                ch.release();
                grp.slot_state[s].store(SlotFree, std::memory_order_release);
                prepare_login_rsp(p, 0, 2, 0, 0, "System error", p.callback_rsp_user_data);
                return;
            }
            local_ack_seq = seq_res->local_ack;
            local_seq_start = seq_res->seq_start;
            local_seq_end = seq_res->seq_end;
        }

        if (!detail::check_ack_in_range(remote_ack_seq, local_seq_start, local_seq_end) ||
            !detail::check_ack_in_range(local_ack_seq, remote_seq_start, remote_seq_end)) {
            notify_seq_mismatch(ch, SeqMismatchInfo{
                                        local_ack_seq,
                                        local_seq_start,
                                        local_seq_end,
                                        remote_ack_seq,
                                        remote_seq_start,
                                        remote_seq_end,
                                    });
            ch.release();
            grp.slot_state[s].store(SlotFree, std::memory_order_release);
            prepare_login_rsp(p, local_ack_seq, 1, local_seq_start, local_seq_end,
                              "Sequence number mismatch", p.callback_rsp_user_data);
            return;
        }

        p.remote_ack_seq = remote_ack_seq;
        p.reserved_group = p.target_grpid;
        p.reserved_slot = free_idx;
        p.reserved_is_shm = false;
        prepare_login_rsp(p, local_ack_seq, 0, local_seq_start, local_seq_end, "", p.callback_rsp_user_data);
    }

    void activate_reserved_slot(TimePoint now, PendingConn& p) {
        const uint32_t g = p.reserved_group;
        const uint32_t s = static_cast<uint32_t>(p.reserved_slot);
        const detail::socket_t fd = p.fd;
        p.fd = detail::kInvalidSocket; // Channel takes ownership
        p.reserved_slot = -1;
        p.state = PendingState::Empty;

        if (p.reserved_is_shm) {
            ShmChannelType& ch = *shm_grps_[g].conns[s];
            ch.open_keepalive(fd, now);
            notify_client_logon(p.addr, ch);
            if (stopping_.load(std::memory_order_acquire)) {
                ch.release();
                shm_grps_[g].slot_state[s].store(SlotFree, std::memory_order_release);
                return;
            }
            shm_grps_[g].slot_state[s].store(SlotPendingAdd, std::memory_order_release);
            shm_grps_[g].ctl_dirty.store(true, std::memory_order_release);
        } else {
            TcpChannelType& ch = *tcp_grps_[g].conns[s];
            ch.open(fd, p.remote_ack_seq, now);
            notify_client_logon(p.addr, ch);
            if (stopping_.load(std::memory_order_acquire)) {
                ch.release();
                tcp_grps_[g].slot_state[s].store(SlotFree, std::memory_order_release);
                return;
            }
            tcp_grps_[g].slot_state[s].store(SlotPendingAdd, std::memory_order_release);
            tcp_grps_[g].ctl_dirty.store(true, std::memory_order_release);
        }
    }

    void advance_pending(TimePoint now, PendingConn& p) {
        while (p.state != PendingState::Empty) {
            if (stopping_.load(std::memory_order_acquire)) {
                reset_pending_conn(p, true);
                return;
            }
            switch (p.state) {
                case PendingState::ReadingLogin: {
                    IoStep step = drive_nonblocking_recv(p, now);
                    if (step == IoStep::Again) return;
                    if (step == IoStep::Failed) {
                        reset_pending_conn(p, false);
                        return;
                    }
                    on_login_received(p);
                    break;
                }
                case PendingState::SendingProbeReq: {
                    IoStep step = drive_nonblocking_send(p, now);
                    if (step == IoStep::Again) return;
                    if (step == IoStep::Failed) {
                        reset_pending_conn(p, false);
                        return;
                    }
                    p.io_done = 0;
                    p.io_total = kProbeRspWireSize;
                    p.state = PendingState::ReadingProbeRsp;
                    break;
                }
                case PendingState::ReadingProbeRsp: {
                    IoStep step = drive_nonblocking_recv(p, now);
                    if (step == IoStep::Again) return;
                    if (step == IoStep::Failed) {
                        reset_pending_conn(p, false);
                        return;
                    }
                    on_probe_rsp_received(p);
                    break;
                }
                case PendingState::SendingLoginRsp: {
                    IoStep step = drive_nonblocking_send(p, now);
                    if (step == IoStep::Again) return;
                    if (step == IoStep::Failed || p.reserved_slot < 0) {
                        reset_pending_conn(p, true);
                        return;
                    }
                    activate_reserved_slot(now, p);
                    return;
                }
                case PendingState::Empty:
                    return;
            }
        }
    }

    Handler& handler_;
    // Declared before pools so channel destructors execute while server_name_ and ptcp_dir_ remain alive.
    char server_name_[Config::name_size]{};
    std::string ptcp_dir_;

    detail::socket_t listenfd_ = detail::kInvalidSocket;
    std::array<PendingConn, Config::max_pending_conns> pending_conns_{};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> in_stop_{false};
    std::atomic<uint64_t> probe_seq_{1};

    std::array<ShmChannelType, Config::max_shm_groups * Config::max_shm_conns_per_group> shm_pool_{};
    std::array<TcpChannelType, Config::max_tcp_groups * Config::max_tcp_conns_per_group> tcp_pool_{};
    std::array<ShmGroup<ShmChannelType, Config::max_shm_conns_per_group>, Config::max_shm_groups> shm_grps_{};
    std::array<TcpGroup<TcpChannelType, Config::max_tcp_conns_per_group, TcpPoller>, Config::max_tcp_groups> tcp_grps_{};
};

} // namespace tcpshm
