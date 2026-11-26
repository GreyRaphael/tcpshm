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
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include "os.h"
#include "tcpshm_conn.h"

namespace tcpshm {

template<class Derived, class Conf>
class TcpShmServer
{
public:
    using Connection = TcpShmConnection<Conf>;
    using LoginMsg = LoginMsgTpl<Conf>;
    using LoginRspMsg = LoginRspMsgTpl<Conf>;

    TcpShmServer(const TcpShmServer&) = delete;
    TcpShmServer& operator=(const TcpShmServer&) = delete;
    TcpShmServer(TcpShmServer&&) = delete;
    TcpShmServer& operator=(TcpShmServer&&) = delete;

protected:
    TcpShmServer(const std::string& server_name, const std::string& ptcp_dir)
        : ptcp_dir_(ptcp_dir) {
        tcp_copy_str(server_name_, sizeof(server_name_), server_name.c_str());
        tcp_mkdir(ptcp_dir_.c_str());
        for(auto& conn : conn_pool_) {
            conn.init(ptcp_dir_.c_str(), server_name_);
        }
        int cnt = 0;
        for(auto& grp : shm_grps_) {
            for(auto& conn : grp.conns) {
                conn = conn_pool_ + cnt++;
            }
        }
        for(auto& grp : tcp_grps_) {
            for(auto& conn : grp.conns) {
                conn = conn_pool_ + cnt++;
            }
        }
    }

    ~TcpShmServer() {
        Stop();
    }

    // start the server
    // return true if success
    bool Start(const char* listen_ipv4, uint16_t listen_port) {
        if(listenfd_ != INVALID_TCP_SOCKET) {
            static_cast<Derived*>(this)->OnSystemError("already started", 0);
            return false;
        }

        auto fail_cleanup = [&]() {
            if(listenfd_ != INVALID_TCP_SOCKET) {
                tcp_close(listenfd_);
                listenfd_ = INVALID_TCP_SOCKET;
            }
            return false;
        };

        if((listenfd_ = tcp_socket(AF_INET, SOCK_STREAM, 0)) == INVALID_TCP_SOCKET) {
            static_cast<Derived*>(this)->OnSystemError("socket", tcp_get_last_error());
            return false;
        }

        if(tcp_set_nonblocking(listenfd_) < 0) {
            static_cast<Derived*>(this)->OnSystemError("tcp_set_nonblocking", tcp_get_last_error());
            return fail_cleanup();
        }
        int yes = 1;
        if(tcp_setsockopt(listenfd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&yes, sizeof(yes)) < 0) {
            static_cast<Derived*>(this)->OnSystemError("setsockopt SO_REUSEADDR", tcp_get_last_error());
            return fail_cleanup();
        }
        if(Conf::TcpNoDelay && tcp_setsockopt(listenfd_, IPPROTO_TCP, TCP_NODELAY, (const char*)&yes, sizeof(yes)) < 0) {
            static_cast<Derived*>(this)->OnSystemError("setsockopt TCP_NODELAY", tcp_get_last_error());
            return fail_cleanup();
        }

        struct sockaddr_in local_addr;
        memset(&local_addr, 0, sizeof(local_addr));
        local_addr.sin_family = AF_INET;
        if(inet_pton(AF_INET, listen_ipv4, &(local_addr.sin_addr)) != 1) {
            static_cast<Derived*>(this)->OnSystemError("inet_pton", tcp_get_last_error());
            return fail_cleanup();
        }
        local_addr.sin_port = htons(listen_port);
        if(tcp_bind(listenfd_, (struct sockaddr*)&local_addr, sizeof(local_addr)) < 0) {
            static_cast<Derived*>(this)->OnSystemError("bind", tcp_get_last_error());
            return fail_cleanup();
        }
        if(tcp_listen(listenfd_, 5) < 0) {
            static_cast<Derived*>(this)->OnSystemError("listen", tcp_get_last_error());
            return fail_cleanup();
        }
        for(auto& grp : shm_grps_) {
            grp.ctl_dirty.store(false, std::memory_order_relaxed);
            grp.active_cnt = 0;
        }
        for(auto& grp : tcp_grps_) {
            grp.ctl_dirty.store(false, std::memory_order_relaxed);
            grp.active_cnt = 0;
        }
        stopping_.store(false, std::memory_order_release);
        return true;
    }

    // poll control for handling new connections and keep shm connections alive
    void PollCtl(int64_t now) {
        if(listenfd_ == INVALID_TCP_SOCKET || stopping_.load(std::memory_order_acquire)) return;
        // every poll we accept only one connection
        if(avail_idx_ != Conf::MaxNewConnections) {
            NewConn& conn = new_conns_[avail_idx_];
            socklen_t addr_len = sizeof(conn.addr);
            conn.fd = tcp_accept(listenfd_, (struct sockaddr*)&(conn.addr), &addr_len);
            // we ignore errors from accept as most errno should be treated like EAGAIN
            if(conn.fd != INVALID_TCP_SOCKET) {
                tcp_set_nonblocking(conn.fd);
                conn.time = now;
                conn.recv_len = 0;
                avail_idx_ = Conf::MaxNewConnections;
            }
        }
        // visit all new connections, trying to read LoginMsg
        for(uint32_t i = 0; i < Conf::MaxNewConnections; i++) {
            NewConn& conn = new_conns_[i];
            if(conn.fd == INVALID_TCP_SOCKET) {
                avail_idx_ = i;
                continue;
            }
            int need = static_cast<int>(sizeof(conn.recvbuf) - conn.recv_len);
            int ret = tcp_recv(conn.fd, reinterpret_cast<char*>(conn.recvbuf) + conn.recv_len, need, 0);
            if(ret < 0) {
                if(tcp_is_retryable_err(tcp_get_last_error()) && now - conn.time <= Conf::NewConnectionTimeout) {
                    continue;
                }
            }
            else if(ret > 0) {
                conn.recv_len += static_cast<size_t>(ret);
                if(conn.recv_len < sizeof(conn.recvbuf)) {
                    if(now - conn.time <= Conf::NewConnectionTimeout) {
                        continue;
                    }
                }
                else {
                    conn.recvbuf[0].template ConvertByteOrder<Conf::ToLittleEndian>();
                    if(conn.recvbuf[0].size == sizeof(MsgHeader) + sizeof(LoginMsg) &&
                       conn.recvbuf[0].msg_type == LoginMsg::msg_type) {
                        // looks like a valid login msg
                        LoginMsg* login = (LoginMsg*)(conn.recvbuf + 1);
                        login->ConvertByteOrder();
                        if(login->use_shm) {
                            HandleLogin(now, conn, shm_grps_, Conf::MaxShmGrps);
                        }
                        else {
                            HandleLogin(now, conn, tcp_grps_, Conf::MaxTcpGrps);
                        }
                    }
                }
            }

            if(conn.fd != INVALID_TCP_SOCKET) {
                tcp_close(conn.fd);
                conn.fd = INVALID_TCP_SOCKET;
            }
            conn.recv_len = 0;
            avail_idx_ = i;
            if(stopping_.load(std::memory_order_acquire)) return;
        }

        for(auto& grp : shm_grps_) {
            for(uint32_t i = 0; i < Conf::MaxShmConnsPerGrp; i++) {
                uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
                if(st == SLOT_FREE) continue;
                Connection& conn = *grp.conns[i];
                if(st == SLOT_PENDING_ADD || st == SLOT_LIVE) {
                    conn.TcpFront(now); // poll heartbeats, ignore return
                    if(conn.TryCloseFd()) {
                        uint8_t cur = st;
                        while(cur == SLOT_PENDING_ADD || cur == SLOT_LIVE) {
                            if(grp.slot_state[i].compare_exchange_weak(
                                   cur, SLOT_RETIRE_REQ, std::memory_order_acq_rel, std::memory_order_acquire)) {
                                grp.ctl_dirty.store(true, std::memory_order_release);
                                break;
                            }
                        }
                    }
                }
                else if(st == SLOT_RETIRED) {
                    int sys_errno;
                    const char* reason = conn.GetCloseReason(&sys_errno);
                    static_cast<Derived*>(this)->OnClientDisconnected(conn, reason, sys_errno);
                    if(stopping_.load(std::memory_order_acquire)) return;
                    conn.Release();
                    grp.slot_state[i].store(SLOT_FREE, std::memory_order_release);
                }
            }
        }

        for(auto& grp : tcp_grps_) {
            for(uint32_t i = 0; i < Conf::MaxTcpConnsPerGrp; i++) {
                uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
                if(st == SLOT_RETIRED) {
                    Connection& conn = *grp.conns[i];
                    int sys_errno;
                    const char* reason = conn.GetCloseReason(&sys_errno);
                    static_cast<Derived*>(this)->OnClientDisconnected(conn, reason, sys_errno);
                    if(stopping_.load(std::memory_order_acquire)) return;
                    conn.Release();
                    grp.slot_state[i].store(SLOT_FREE, std::memory_order_release);
                }
            }
        }
    }

    // poll tcp for serving tcp connections
    void PollTcp(int64_t now, int grpid) {
        auto& grp = tcp_grps_[grpid];
        if(stopping_.load(std::memory_order_relaxed)) [[unlikely]] {
            if(stopping_.load(std::memory_order_acquire)) {
                RetireActiveTcp(grp);
                return;
            }
        }
        if(grp.ctl_dirty.load(std::memory_order_acquire)) [[unlikely]] {
            SyncTcpGroup(grp);
        }
        for(uint32_t j = 0; j < grp.active_cnt;) {
            uint32_t slot = grp.active_slots[j];
            Connection& conn = *grp.active_conns[j];
            MsgHeader* head = conn.TcpFront(now);
            if(head) [[likely]] {
                static_cast<Derived*>(this)->OnClientMsg(conn, head);
                if(stopping_.load(std::memory_order_acquire)) [[unlikely]] {
                    RetireActiveTcp(grp);
                    return;
                }
            }
            if(conn.TryCloseFd()) [[unlikely]] {
                uint32_t last = --grp.active_cnt;
                grp.active_slots[j] = grp.active_slots[last];
                grp.active_conns[j] = grp.active_conns[last];
                grp.slot_state[slot].store(SLOT_RETIRED, std::memory_order_release);
            }
            else if(grp.slot_state[slot].load(std::memory_order_acquire) == SLOT_RETIRE_REQ) [[unlikely]] {
                conn.Close();
                conn.TryCloseFd();
                uint32_t last = --grp.active_cnt;
                grp.active_slots[j] = grp.active_slots[last];
                grp.active_conns[j] = grp.active_conns[last];
                grp.slot_state[slot].store(SLOT_RETIRED, std::memory_order_release);
            }
            else {
                j++;
            }
        }
    }

    // poll shm for serving shm connections
    void PollShm(int grpid) {
        auto& grp = shm_grps_[grpid];
        if(stopping_.load(std::memory_order_relaxed)) [[unlikely]] {
            if(stopping_.load(std::memory_order_acquire)) {
                RetireActiveShm(grp);
                return;
            }
        }
        if(grp.ctl_dirty.load(std::memory_order_acquire)) [[unlikely]] {
            SyncShmGroup(grp);
        }
        for(uint32_t j = 0; j < grp.active_cnt; j++) {
            Connection& conn = *grp.active_conns[j];
            MsgHeader* head = conn.ShmFront();
            if(head) [[likely]] {
                static_cast<Derived*>(this)->OnClientMsg(conn, head);
                if(stopping_.load(std::memory_order_acquire)) [[unlikely]] {
                    RetireActiveShm(grp);
                    return;
                }
            }
        }
    }

    void Stop() {
        if(in_stop_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        stopping_.store(true, std::memory_order_release);
        if(listenfd_ == INVALID_TCP_SOCKET) {
            in_stop_.store(false, std::memory_order_release);
            return;
        }
        tcp_close(listenfd_);
        listenfd_ = INVALID_TCP_SOCKET;
        for(uint32_t i = 0; i < Conf::MaxNewConnections; i++) {
            tcp_socket_t& fd = new_conns_[i].fd;
            if(fd != INVALID_TCP_SOCKET) {
                tcp_close(fd);
                fd = INVALID_TCP_SOCKET;
            }
            new_conns_[i].recv_len = 0;
        }
        avail_idx_ = 0;

        auto request_retire_all = [](auto* grps, uint32_t grp_cnt, uint32_t slots_per_grp) {
            bool any_pending = false;
            for(uint32_t g = 0; g < grp_cnt; g++) {
                auto& grp = grps[g];
                bool dirty = false;
                for(uint32_t i = 0; i < slots_per_grp; i++) {
                    uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
                    while(st == SLOT_PENDING_ADD || st == SLOT_LIVE) {
                        if(grp.slot_state[i].compare_exchange_weak(
                               st, SLOT_RETIRE_REQ, std::memory_order_acq_rel, std::memory_order_acquire)) {
                            dirty = true;
                            any_pending = true;
                            break;
                        }
                    }
                    if(st == SLOT_RETIRE_REQ) {
                        dirty = true;
                        any_pending = true;
                    }
                }
                if(dirty) grp.ctl_dirty.store(true, std::memory_order_release);
            }
            return any_pending;
        };

        bool pending_shm = request_retire_all(shm_grps_, Conf::MaxShmGrps, Conf::MaxShmConnsPerGrp);
        bool pending_tcp = request_retire_all(tcp_grps_, Conf::MaxTcpGrps, Conf::MaxTcpConnsPerGrp);

        if(pending_shm || pending_tcp) {
            auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
            while(std::chrono::steady_clock::now() < deadline) {
                auto has_unretired = [](auto* grps, uint32_t grp_cnt, uint32_t slots_per_grp) {
                    for(uint32_t g = 0; g < grp_cnt; g++) {
                        for(uint32_t i = 0; i < slots_per_grp; i++) {
                            uint8_t st = grps[g].slot_state[i].load(std::memory_order_acquire);
                            if(st != SLOT_FREE && st != SLOT_RETIRED) return true;
                        }
                    }
                    return false;
                };
                if(!has_unretired(shm_grps_, Conf::MaxShmGrps, Conf::MaxShmConnsPerGrp) &&
                   !has_unretired(tcp_grps_, Conf::MaxTcpGrps, Conf::MaxTcpConnsPerGrp)) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }

        auto drain_grps = [](auto* grps, uint32_t grp_cnt, uint32_t slots_per_grp) {
            for(uint32_t g = 0; g < grp_cnt; g++) {
                auto& grp = grps[g];
                for(uint32_t i = 0; i < slots_per_grp; i++) {
                    uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
                    if(st != SLOT_FREE) {
                        grp.conns[i]->Release();
                        grp.slot_state[i].store(SLOT_FREE, std::memory_order_release);
                    }
                }
                grp.ctl_dirty.store(false, std::memory_order_release);
            }
        };
        drain_grps(shm_grps_, Conf::MaxShmGrps, Conf::MaxShmConnsPerGrp);
        drain_grps(tcp_grps_, Conf::MaxTcpGrps, Conf::MaxTcpConnsPerGrp);
        in_stop_.store(false, std::memory_order_release);
    }

private:
    enum SlotState : uint8_t
    {
        SLOT_FREE = 0,
        SLOT_PENDING_ADD = 1,
        SLOT_LIVE = 2,
        SLOT_RETIRE_REQ = 3,
        SLOT_RETIRED = 4,
    };

    struct NewConn
    {
        int64_t time = 0;
        tcp_socket_t fd = INVALID_TCP_SOCKET;
        size_t recv_len = 0;
        struct sockaddr_in addr{};
        MsgHeader recvbuf[1 + (sizeof(LoginMsg) + 7) / 8]{};
    };
    template<uint32_t N>
    struct alignas(64) ConnectionGroup
    {
        Connection* conns[N]{};
        alignas(64) std::atomic<bool> ctl_dirty{false};
        std::atomic<uint8_t> slot_state[N]{};
        alignas(64) uint32_t active_cnt = 0;
        uint32_t active_slots[N]{};
        Connection* active_conns[N]{};
    };

    template<uint32_t N>
    void SyncTcpGroup(ConnectionGroup<N>& grp) {
        grp.ctl_dirty.exchange(false, std::memory_order_acq_rel);
        for(uint32_t i = 0; i < N; i++) {
            uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
            if(st == SLOT_PENDING_ADD) {
                uint8_t expected = SLOT_PENDING_ADD;
                if(grp.slot_state[i].compare_exchange_strong(
                       expected, SLOT_LIVE, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    uint32_t pos = grp.active_cnt++;
                    grp.active_slots[pos] = i;
                    grp.active_conns[pos] = grp.conns[i];
                }
                else if(expected == SLOT_RETIRE_REQ) {
                    grp.conns[i]->Close();
                    grp.conns[i]->TryCloseFd();
                    grp.slot_state[i].store(SLOT_RETIRED, std::memory_order_release);
                }
            }
            else if(st == SLOT_RETIRE_REQ) {
                for(uint32_t j = 0; j < grp.active_cnt; j++) {
                    if(grp.active_slots[j] == i) {
                        uint32_t last = --grp.active_cnt;
                        grp.active_slots[j] = grp.active_slots[last];
                        grp.active_conns[j] = grp.active_conns[last];
                        break;
                    }
                }
                grp.conns[i]->Close();
                grp.conns[i]->TryCloseFd();
                grp.slot_state[i].store(SLOT_RETIRED, std::memory_order_release);
            }
        }
    }

    template<uint32_t N>
    void SyncShmGroup(ConnectionGroup<N>& grp) {
        grp.ctl_dirty.exchange(false, std::memory_order_acq_rel);
        for(uint32_t i = 0; i < N; i++) {
            uint8_t st = grp.slot_state[i].load(std::memory_order_acquire);
            if(st == SLOT_PENDING_ADD) {
                uint8_t expected = SLOT_PENDING_ADD;
                if(grp.slot_state[i].compare_exchange_strong(
                       expected, SLOT_LIVE, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    uint32_t pos = grp.active_cnt++;
                    grp.active_slots[pos] = i;
                    grp.active_conns[pos] = grp.conns[i];
                }
                else if(expected == SLOT_RETIRE_REQ) {
                    grp.slot_state[i].store(SLOT_RETIRED, std::memory_order_release);
                }
            }
            else if(st == SLOT_RETIRE_REQ) {
                for(uint32_t j = 0; j < grp.active_cnt; j++) {
                    if(grp.active_slots[j] == i) {
                        uint32_t last = --grp.active_cnt;
                        grp.active_slots[j] = grp.active_slots[last];
                        grp.active_conns[j] = grp.active_conns[last];
                        break;
                    }
                }
                grp.slot_state[i].store(SLOT_RETIRED, std::memory_order_release);
            }
        }
    }

    template<uint32_t N>
    static void RetireActiveTcp(ConnectionGroup<N>& grp) {
        for(uint32_t j = 0; j < grp.active_cnt; j++) {
            uint32_t s = grp.active_slots[j];
            if(grp.slot_state[s].load(std::memory_order_acquire) == SLOT_RETIRE_REQ) {
                grp.conns[s]->Close();
                grp.conns[s]->TryCloseFd();
                grp.slot_state[s].store(SLOT_RETIRED, std::memory_order_release);
            }
        }
        grp.active_cnt = 0;
    }

    template<uint32_t N>
    static void RetireActiveShm(ConnectionGroup<N>& grp) {
        for(uint32_t j = 0; j < grp.active_cnt; j++) {
            uint32_t s = grp.active_slots[j];
            if(grp.slot_state[s].load(std::memory_order_acquire) == SLOT_RETIRE_REQ) {
                grp.slot_state[s].store(SLOT_RETIRED, std::memory_order_release);
            }
        }
        grp.active_cnt = 0;
    }

    static bool IsValidClientName(const char* name) {
        if(!name || name[0] == '\0') return false;
        if(strstr(name, "..") != nullptr) return false;
        for(const char* p = name; *p; ++p) {
            unsigned char c = static_cast<unsigned char>(*p);
            if(c == '/' || c == '\\' || c < 0x20 || c >= 0x7f) return false;
        }
        return true;
    }

    bool IsClientNameLive(const char* name) const {
        for(uint32_t g = 0; g < Conf::MaxShmGrps; g++) {
            for(uint32_t i = 0; i < Conf::MaxShmConnsPerGrp; i++) {
                if(shm_grps_[g].slot_state[i].load(std::memory_order_acquire) != SLOT_FREE &&
                   strncmp(shm_grps_[g].conns[i]->GetRemoteName(), name, Conf::NameSize) == 0) {
                    return true;
                }
            }
        }
        for(uint32_t g = 0; g < Conf::MaxTcpGrps; g++) {
            for(uint32_t i = 0; i < Conf::MaxTcpConnsPerGrp; i++) {
                if(tcp_grps_[g].slot_state[i].load(std::memory_order_acquire) != SLOT_FREE &&
                   strncmp(tcp_grps_[g].conns[i]->GetRemoteName(), name, Conf::NameSize) == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    template<uint32_t N>
    void HandleLogin(int64_t now, NewConn& conn, ConnectionGroup<N>* grps, uint32_t max_grps) {
        tcp_set_blocking(conn.fd);
        tcp_set_timeout(conn.fd, SOL_SOCKET, SO_SNDTIMEO, 300);

        MsgHeader sendbuf[1 + (sizeof(LoginRspMsg) + 7) / 8]{};
        sendbuf[0].size = sizeof(MsgHeader) + sizeof(LoginRspMsg);
        sendbuf[0].msg_type = LoginRspMsg::msg_type;
        sendbuf[0].ack_seq = 0;
        LoginRspMsg* login_rsp = (LoginRspMsg*)(sendbuf + 1);
        tcp_copy_str(login_rsp->server_name, sizeof(login_rsp->server_name), server_name_);
        login_rsp->status = 2;
        login_rsp->error_msg[0] = 0;

        LoginMsg* login = (LoginMsg*)(conn.recvbuf + 1);
        login->client_name[sizeof(login->client_name) - 1] = 0;
        login->last_server_name[sizeof(login->last_server_name) - 1] = 0;
        if(!IsValidClientName(login->client_name) ||
           strncmp(login->client_name, server_name_, sizeof(server_name_)) == 0) {
            tcp_copy_str(login_rsp->error_msg, sizeof(login_rsp->error_msg), "Invalid client name");
            SendLoginRsp(conn.fd, sendbuf, login_rsp);
            return;
        }

        int grpid = static_cast<Derived*>(this)->OnNewConnection(conn.addr, login, login_rsp);
        if(stopping_.load(std::memory_order_acquire)) {
            return;
        }
        if(grpid < 0 || static_cast<uint32_t>(grpid) >= max_grps) {
            if(login_rsp->error_msg[0] == 0) { // user didn't set error_msg? set a default one
                tcp_copy_str(login_rsp->error_msg, sizeof(login_rsp->error_msg), grpid < 0 ? "Login Reject" : "Invalid group id");
            }
            SendLoginRsp(conn.fd, sendbuf, login_rsp);
            return;
        }
        if(IsClientNameLive(login->client_name)) {
            tcp_copy_str(login_rsp->error_msg, sizeof(login_rsp->error_msg), "Already loggned on");
            SendLoginRsp(conn.fd, sendbuf, login_rsp);
            return;
        }
        if(login->use_shm && !VerifyShmProbe(conn.fd, login->client_name)) {
            tcp_copy_str(login_rsp->error_msg, sizeof(login_rsp->error_msg), "SHM probe failed");
            SendLoginRsp(conn.fd, sendbuf, login_rsp);
            return;
        }

        auto& grp = grps[grpid];
        int free_idx = -1;
        for(uint32_t i = 0; i < N; i++) {
            if(grp.slot_state[i].load(std::memory_order_acquire) == SLOT_FREE) {
                free_idx = static_cast<int>(i);
                break;
            }
        }
        if(free_idx < 0) {
            // no space for new remote name
            tcp_copy_str(login_rsp->error_msg, sizeof(login_rsp->error_msg), "Max client cnt exceeded");
            SendLoginRsp(conn.fd, sendbuf, login_rsp);
            return;
        }

        Connection& curconn = *grp.conns[free_idx];
        char* remote_name = curconn.GetRemoteName();
        tcp_copy_str(remote_name, Conf::NameSize, login->client_name);
        auto rollback_conn = [&]() {
            remote_name[0] = '\0';
            curconn.Release();
        };

        const char* error_msg = nullptr;
        if(!curconn.OpenFile(login->use_shm, &error_msg)) {
            // we can not mmap to ptcp or shm files with filenames related to local and remote name
            static_cast<Derived*>(this)->OnClientFileError(curconn, error_msg, tcp_get_last_error());
            rollback_conn();
            tcp_copy_str(login_rsp->error_msg, sizeof(login_rsp->error_msg), "System error");
            SendLoginRsp(conn.fd, sendbuf, login_rsp);
            return;
        }
        uint32_t local_ack_seq = 0;
        uint32_t local_seq_start = 0;
        uint32_t local_seq_end = 0;
        uint32_t remote_ack_seq = conn.recvbuf[0].ack_seq;
        uint32_t remote_seq_start = login->client_seq_start;
        uint32_t remote_seq_end = login->client_seq_end;
        // if server_name has changed, reset the ack_seq
        if(strncmp(login->last_server_name, server_name_, sizeof(server_name_)) != 0) {
            curconn.Reset();
            remote_ack_seq = remote_seq_start = remote_seq_end = 0;
        }
        else {
            if(!curconn.GetSeq(&local_ack_seq, &local_seq_start, &local_seq_end, &error_msg)) {
                static_cast<Derived*>(this)->OnClientFileError(curconn, error_msg, tcp_get_last_error());
                rollback_conn();
                tcp_copy_str(login_rsp->error_msg, sizeof(login_rsp->error_msg), "System error");
                SendLoginRsp(conn.fd, sendbuf, login_rsp);
                return;
            }
        }
        sendbuf[0].ack_seq = local_ack_seq;
        login_rsp->server_seq_start = local_seq_start;
        login_rsp->server_seq_end = local_seq_end;
        if(!CheckAckInQueue(remote_ack_seq, local_seq_start, local_seq_end) ||
           !CheckAckInQueue(local_ack_seq, remote_seq_start, remote_seq_end)) {
            static_cast<Derived*>(this)->OnSeqNumberMismatch(curconn,
                                                             local_ack_seq,
                                                             local_seq_start,
                                                             local_seq_end,
                                                             remote_ack_seq,
                                                             remote_seq_start,
                                                             remote_seq_end);
            rollback_conn();
            login_rsp->status = 1;
            SendLoginRsp(conn.fd, sendbuf, login_rsp);
            return;
        }

        // send Login OK
        login_rsp->status = 0;
        if(!SendLoginRsp(conn.fd, sendbuf, login_rsp)) {
            rollback_conn();
            return;
        }
        tcp_set_nonblocking(conn.fd);
        curconn.Open(conn.fd, remote_ack_seq, now);
        conn.fd = INVALID_TCP_SOCKET; // so it won't be closed by caller
        // Invoke OnClientLogon BEFORE publishing slot to worker threads
        static_cast<Derived*>(this)->OnClientLogon(conn.addr, curconn);
        if(stopping_.load(std::memory_order_acquire)) {
            curconn.Release();
            return;
        }
        grp.slot_state[free_idx].store(SLOT_PENDING_ADD, std::memory_order_release);
        grp.ctl_dirty.store(true, std::memory_order_release);
    }

    // check if seq_start <= ack_seq <= seq_end, considering uint32_t wrap around
    bool CheckAckInQueue(uint32_t ack_seq, uint32_t seq_start, uint32_t seq_end) {
        return static_cast<int32_t>(ack_seq - seq_start) >= 0 && static_cast<int32_t>(seq_end - ack_seq) >= 0;
    }

    static bool SendLoginRsp(tcp_socket_t fd, const MsgHeader* header, const LoginRspMsg* login_rsp) {
        if(fd == INVALID_TCP_SOCKET) return false;
        MsgHeader outbuf[1 + (sizeof(LoginRspMsg) + 7) / 8]{};
        outbuf[0] = *header;
        LoginRspMsg* out_rsp = (LoginRspMsg*)(outbuf + 1);
        *out_rsp = *login_rsp;
        outbuf[0].template ConvertByteOrder<Conf::ToLittleEndian>();
        out_rsp->ConvertByteOrder();
        int sys_errno = 0;
        return tcp_send_all(fd, outbuf, sizeof(outbuf), TCP_MSG_NOSIGNAL, &sys_errno);
    }

    bool VerifyShmProbe(tcp_socket_t fd, const char* client_name) {
        tcp_set_blocking(fd);
        tcp_set_timeout(fd, SOL_SOCKET, SO_RCVTIMEO, 300);
        tcp_set_timeout(fd, SOL_SOCKET, SO_SNDTIMEO, 300);

        uint64_t token = MakeShmProbeToken(client_name);
        uint64_t seq = probe_seq_.fetch_add(1, std::memory_order_relaxed);
        char probe_name[ShmProbeNameSize]{};
        std::snprintf(probe_name,
                      sizeof(probe_name),
                      "/tcpshm_probe_%016llx_%llx",
                      static_cast<unsigned long long>(token),
                      static_cast<unsigned long long>(seq));

        const char* error_msg = nullptr;
        ShmProbeData* probe = my_mmap<ShmProbeData>(probe_name, true, &error_msg, true);
        if(!probe) return false;
        std::atomic_ref<uint64_t>(probe->token).store(token, std::memory_order_relaxed);
        std::atomic_ref<uint64_t>(probe->ack).store(0, std::memory_order_release);

        bool ok = SendShmProbeReq(fd, probe_name, token) &&
                  RecvAndCheckShmProbeRsp(fd, token, probe);

        my_munmap<ShmProbeData>(probe);
        my_shm_unlink(probe_name);
        return ok;
    }

    bool SendShmProbeReq(tcp_socket_t fd, const char* probe_name, uint64_t token) {
        MsgHeader reqbuf[1 + (sizeof(ShmProbeReqMsg) + 7) / 8]{};
        reqbuf[0].size = sizeof(MsgHeader) + sizeof(ShmProbeReqMsg);
        reqbuf[0].msg_type = ShmProbeReqMsg::msg_type;
        reqbuf[0].ack_seq = 0;
        ShmProbeReqMsg* req = (ShmProbeReqMsg*)(reqbuf + 1);
        req->token = token;
        tcp_copy_str(req->shm_name, sizeof(req->shm_name), probe_name);
        reqbuf[0].template ConvertByteOrder<Conf::ToLittleEndian>();
        req->template ConvertByteOrder<Conf::ToLittleEndian>();
        int sys_errno = 0;
        return tcp_send_all(fd, reqbuf, sizeof(reqbuf), TCP_MSG_NOSIGNAL, &sys_errno);
    }

    bool RecvAndCheckShmProbeRsp(tcp_socket_t fd, uint64_t token, ShmProbeData* probe) {
        MsgHeader rspbuf[1 + (sizeof(ShmProbeRspMsg) + 7) / 8]{};
        int sys_errno = 0;
        if(!tcp_recv_all(fd, rspbuf, sizeof(rspbuf), &sys_errno)) return false;
        rspbuf[0].template ConvertByteOrder<Conf::ToLittleEndian>();
        if(rspbuf[0].size != sizeof(MsgHeader) + sizeof(ShmProbeRspMsg) ||
           rspbuf[0].msg_type != ShmProbeRspMsg::msg_type) {
            return false;
        }
        ShmProbeRspMsg* rsp = (ShmProbeRspMsg*)(rspbuf + 1);
        rsp->template ConvertByteOrder<Conf::ToLittleEndian>();
        uint64_t expected_ack = token ^ ShmProbeAckMask;
        return rsp->ok &&
               rsp->ack == expected_ack &&
               std::atomic_ref<uint64_t>(probe->ack).load(std::memory_order_acquire) == expected_ack;
    }

    uint64_t MakeShmProbeToken(const char* client_name) const {
        uint64_t now = static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        uint64_t salt = static_cast<uint64_t>(reinterpret_cast<std::uintptr_t>(this));
        for(const char* p = client_name; *p; ++p) {
            salt = (salt * 131) + static_cast<unsigned char>(*p);
        }
        return now ^ (salt << 17) ^ (salt >> 7);
    }

private:
    char server_name_[Conf::NameSize];
    std::string ptcp_dir_;
    tcp_socket_t listenfd_ = INVALID_TCP_SOCKET;

    NewConn new_conns_[Conf::MaxNewConnections];
    uint32_t avail_idx_ = 0;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> in_stop_{false};
    std::atomic<uint64_t> probe_seq_{1};

    Connection conn_pool_[Conf::MaxShmConnsPerGrp * Conf::MaxShmGrps + Conf::MaxTcpConnsPerGrp * Conf::MaxTcpGrps];
    ConnectionGroup<Conf::MaxShmConnsPerGrp> shm_grps_[Conf::MaxShmGrps];
    ConnectionGroup<Conf::MaxTcpConnsPerGrp> tcp_grps_[Conf::MaxTcpGrps];
};
} // namespace tcpshm
