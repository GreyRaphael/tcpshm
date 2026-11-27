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
#include "tcpshm/shm_channel.hpp"
#include "tcpshm/tcp_channel.hpp"
#include <array>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace tcpshm {

template<typename H>
concept ClientHandlerConcept =
    requires(H& h, MsgView msg) {
        h.on_server_msg(msg);
    };

namespace detail {

template<ConfigConcept Config, class Handler>
struct ClientCallbackBridge
{
    static void on_system_error(Handler& h, Error err) {
        if constexpr (requires { h.on_system_error(err); }) {
            h.on_system_error(err);
        } else {
            (void)h;
            (void)err;
        }
    }

    static void on_login_reject(Handler& h, const LoginRspMsg<Config>& rsp) {
        if constexpr (requires { h.on_login_reject(rsp); }) {
            h.on_login_reject(rsp);
        } else {
            (void)h;
            (void)rsp;
        }
    }

    static TimePoint on_login_success(Handler& h, const LoginRspMsg<Config>& rsp) {
        if constexpr (requires { { h.on_login_success(rsp) } -> std::same_as<TimePoint>; }) {
            return h.on_login_success(rsp);
        } else if constexpr (requires { h.on_login_success(rsp); }) {
            h.on_login_success(rsp);
            return steady_now();
        } else {
            (void)h;
            (void)rsp;
            return steady_now();
        }
    }

    static void on_seq_mismatch(Handler& h, const SeqMismatchInfo& info) {
        if constexpr (requires { h.on_seq_mismatch(info); }) {
            h.on_seq_mismatch(info);
        } else {
            (void)h;
            (void)info;
        }
    }

    static void on_disconnected(Handler& h, Error reason) {
        if constexpr (requires { h.on_disconnected(reason); }) {
            h.on_disconnected(reason);
        } else {
            (void)h;
            (void)reason;
        }
    }

    static bool on_server_msg(Handler& h, MsgView msg) {
        using Ret = decltype(h.on_server_msg(msg));
        if constexpr (std::same_as<Ret, bool>) {
            return h.on_server_msg(msg);
        } else {
            h.on_server_msg(msg);
            return true;
        }
    }
};

template<ConfigConcept Config>
inline bool handle_shm_probe_request(socket_t fd, const FrameHeader& hdr, Error& out_err) noexcept {
    if (hdr.size != sizeof(FrameHeader) + sizeof(ShmProbeReqMsg)) {
        out_err = Error{ErrorCode::ShmProbeFailed, 0};
        return false;
    }
    ShmProbeReqMsg req{};
    int sys_errno = 0;
    if (!recv_all(fd, &req, static_cast<int>(sizeof(req)), &sys_errno)) {
        out_err = Error{ErrorCode::RecvFailed, sys_errno};
        return false;
    }
    req.template convert_byte_order<Config::wire_endian>();
    req.shm_name[kShmProbeNameSize - 1] = '\0';

    uint64_t ack = 0;
    uint8_t ok = 0;
    if (is_valid_shm_probe_name(req.shm_name)) {
        auto probe_res = mmap_existing<ShmProbeData>(req.shm_name, true);
        if (probe_res) {
            ShmProbeData* probe = *probe_res;
            if (std::atomic_ref<uint64_t>(probe->token).load(std::memory_order_acquire) == req.token) {
                ack = req.token ^ kShmProbeAckMask;
                std::atomic_ref<uint64_t>(probe->ack).store(ack, std::memory_order_release);
                ok = 1;
            }
            munmap_region(probe);
        }
    }

    struct alignas(16) ProbeRspPacket
    {
        FrameHeader hdr;
        ShmProbeRspMsg body;
    } pkt{};
    pkt.hdr.size = static_cast<uint32_t>(sizeof(FrameHeader) + sizeof(ShmProbeRspMsg));
    pkt.hdr.msg_type = kShmProbeRspType;
    pkt.hdr.flags = 0;
    pkt.hdr.ack_seq = 0;
    pkt.hdr.reserved = 0;
    pkt.hdr.template convert_byte_order<Config::wire_endian>();
    pkt.body.ack = ack;
    pkt.body.ok = ok;
    pkt.body.template convert_byte_order<Config::wire_endian>();

    if (!send_all(fd, &pkt, static_cast<int>(sizeof(FrameHeader) + sizeof(ShmProbeRspMsg)),
                  kMsgNoSignal, &sys_errno)) {
        out_err = Error{ErrorCode::SendFailed, sys_errno};
        return false;
    }
    return true;
}

template<ConfigConcept Config>
inline bool recv_login_rsp(socket_t fd,
                           bool use_shm,
                           FrameHeader& out_hdr,
                           LoginRspMsg<Config>& out_rsp,
                           Error& out_err) noexcept {
    int sys_errno = 0;
    if (!recv_all(fd, &out_hdr, static_cast<int>(sizeof(FrameHeader)), &sys_errno)) {
        out_err = Error{ErrorCode::RecvFailed, sys_errno};
        return false;
    }
    out_hdr.template convert_byte_order<Config::wire_endian>();

    if (out_hdr.msg_type == kShmProbeReqType) {
        if (!use_shm) {
            out_err = Error{ErrorCode::ShmProbeFailed, 0};
            return false;
        }
        if (!handle_shm_probe_request<Config>(fd, out_hdr, out_err)) {
            return false;
        }
        if (!recv_all(fd, &out_hdr, static_cast<int>(sizeof(FrameHeader)), &sys_errno)) {
            out_err = Error{ErrorCode::RecvFailed, sys_errno};
            return false;
        }
        out_hdr.template convert_byte_order<Config::wire_endian>();
    }

    if (out_hdr.size != sizeof(FrameHeader) + sizeof(LoginRspMsg<Config>) ||
        out_hdr.msg_type != kLoginRspMsgType) {
        out_err = Error{ErrorCode::InvalidLoginRsp, 0};
        return false;
    }
    if (!recv_all(fd, &out_rsp, static_cast<int>(sizeof(LoginRspMsg<Config>)), &sys_errno)) {
        out_err = Error{ErrorCode::RecvFailed, sys_errno};
        return false;
    }
    out_rsp.convert_byte_order();
    out_rsp.server_name[Config::name_size - 1] = '\0';
    out_rsp.error_msg[kLoginErrorMsgSize - 1] = '\0';
    return true;
}

template<ConfigConcept Config>
[[nodiscard]] inline VoidResult open_last_server_file(const char* client_name,
                                                     const std::string& ptcp_dir,
                                                     std::array<char, Config::name_size>*& out_ptr) noexcept {
    if (out_ptr) return {};
    std::string path = ptcp_dir + "/" + client_name + ".lastserver";
    auto res = mmap_create_or_open<std::array<char, Config::name_size>>(path.c_str(), false, false);
    if (!res) return make_error(res.error());
    out_ptr = *res;
    return {};
}

template<ConfigConcept Config, class Handler>
[[nodiscard]] inline Result<socket_t> dial_and_handshake(
    Handler& handler,
    const char* client_name,
    std::array<char, Config::name_size>* server_name_buf,
    bool use_shm,
    std::string_view server_ipv4,
    uint16_t server_port,
    const typename Config::LoginUserData& login_user_data,
    uint32_t local_ack_seq,
    uint32_t local_seq_start,
    uint32_t local_seq_end,
    FrameHeader& rsp_hdr,
    LoginRspMsg<Config>& login_rsp) noexcept {
    struct sockaddr_in server_addr{};
    if (!parse_ipv4_endpoint(server_ipv4, server_port, server_addr)) {
        Error err{ErrorCode::InvalidAddress, last_socket_error()};
        ClientCallbackBridge<Config, Handler>::on_system_error(handler, err);
        return make_error(err);
    }

    socket_t fd = create_socket(AF_INET, SOCK_STREAM, 0);
    if (fd == kInvalidSocket) {
        Error err{ErrorCode::SocketCreateFailed, last_socket_error()};
        ClientCallbackBridge<Config, Handler>::on_system_error(handler, err);
        return make_error(err);
    }

    auto fail = [&](Error err) -> Result<socket_t> {
        close_socket(fd);
        ClientCallbackBridge<Config, Handler>::on_system_error(handler, err);
        return make_error(err);
    };

    const int timeout_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(Config::handshake_timeout).count());
    const int io_timeout_ms = timeout_ms > 10000 ? timeout_ms : 10000;
    if (set_timeout_ms(fd, SOL_SOCKET, SO_RCVTIMEO, io_timeout_ms) < 0 ||
        set_timeout_ms(fd, SOL_SOCKET, SO_SNDTIMEO, io_timeout_ms) < 0) {
        return fail(Error{ErrorCode::SocketOptionFailed, last_socket_error()});
    }
    if constexpr (Config::tcp_no_delay) {
        int yes = 1;
        if (set_sockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&yes), sizeof(yes)) < 0) {
            return fail(Error{ErrorCode::SocketOptionFailed, last_socket_error()});
        }
    }
    if (connect_socket(fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        return fail(Error{ErrorCode::ConnectFailed, last_socket_error()});
    }

    struct alignas(16) LoginPacket
    {
        FrameHeader hdr;
        LoginMsg<Config> body;
    } req_pkt{};
    req_pkt.hdr.size = static_cast<uint32_t>(sizeof(FrameHeader) + sizeof(LoginMsg<Config>));
    req_pkt.hdr.msg_type = kLoginMsgType;
    req_pkt.hdr.flags = 0;
    req_pkt.hdr.ack_seq = local_ack_seq;
    req_pkt.hdr.reserved = 0;
    req_pkt.hdr.template convert_byte_order<Config::wire_endian>();

    req_pkt.body.magic = kProtocolMagic;
    req_pkt.body.version = kProtocolVersion;
    req_pkt.body.cfg_fingerprint = config_fingerprint<Config>();
    req_pkt.body.client_seq_start = local_seq_start;
    req_pkt.body.client_seq_end = local_seq_end;
    req_pkt.body.user_data = login_user_data;
    req_pkt.body.use_shm = use_shm ? 1u : 0u;
    copy_cstr(req_pkt.body.client_name, sizeof(req_pkt.body.client_name), client_name);
    copy_cstr(req_pkt.body.last_server_name, sizeof(req_pkt.body.last_server_name), server_name_buf->data());
    req_pkt.body.convert_byte_order();

    // Send header + body explicitly to avoid struct trailing padding assumptions
    std::byte send_buf[sizeof(FrameHeader) + sizeof(LoginMsg<Config>)]{};
    std::memcpy(send_buf, &req_pkt.hdr, sizeof(FrameHeader));
    std::memcpy(send_buf + sizeof(FrameHeader), &req_pkt.body, sizeof(LoginMsg<Config>));

    int sys_errno = 0;
    if (!send_all(fd, send_buf, static_cast<int>(sizeof(send_buf)), kMsgNoSignal, &sys_errno)) {
        return fail(Error{ErrorCode::SendFailed, sys_errno});
    }

    Error rsp_err{};
    if (!recv_login_rsp<Config>(fd, use_shm, rsp_hdr, login_rsp, rsp_err)) {
        return fail(rsp_err);
    }

    if (login_rsp.magic != kProtocolMagic || login_rsp.version != kProtocolVersion) {
        close_socket(fd);
        Error err{ErrorCode::ProtocolMismatch, 0};
        ClientCallbackBridge<Config, Handler>::on_system_error(handler, err);
        return make_error(err);
    }
    if (login_rsp.server_name[0] == '\0') {
        close_socket(fd);
        Error err{ErrorCode::InvalidLoginRsp, 0};
        ClientCallbackBridge<Config, Handler>::on_system_error(handler, err);
        return make_error(err);
    }

    if (login_rsp.status != 0) {
        close_socket(fd);
        if (login_rsp.status == 1) {
            ClientCallbackBridge<Config, Handler>::on_seq_mismatch(
                handler,
                SeqMismatchInfo{
                    local_ack_seq,
                    local_seq_start,
                    local_seq_end,
                    rsp_hdr.ack_seq,
                    login_rsp.server_seq_start,
                    login_rsp.server_seq_end,
                });
            return make_error(ErrorCode::SeqMismatch);
        }
        ClientCallbackBridge<Config, Handler>::on_login_reject(handler, login_rsp);
        return make_error(ErrorCode::LoginRejected);
    }

    if (set_nonblocking(fd) < 0) {
        return fail(Error{ErrorCode::SocketOptionFailed, last_socket_error()});
    }
    return fd;
}

} // namespace detail

// Pure SHM client with zero PTCP state or queues.
template<class Handler, class Config = DefaultConfig>
class ShmClient
{
    static_assert(ConfigConcept<Config>, "Config must satisfy ConfigConcept");
public:
    using Channel = ShmChannel<Config>;
    using LoginUserData = typename Config::LoginUserData;
    using LoginRspMsg = detail::LoginRspMsg<Config>;

    ShmClient(Handler& handler, std::string_view client_name, std::string_view ptcp_dir)
        : handler_(handler), ptcp_dir_(ptcp_dir) {
        detail::copy_cstr(client_name_, sizeof(client_name_), client_name);
        detail::make_dir(ptcp_dir_.c_str());
        channel_.init(client_name_);
    }

    ~ShmClient() noexcept { stop(); }

    ShmClient(const ShmClient&) = delete;
    ShmClient& operator=(const ShmClient&) = delete;
    ShmClient(ShmClient&&) = delete;
    ShmClient& operator=(ShmClient&&) = delete;

    [[nodiscard]] VoidResult connect(std::string_view server_ipv4,
                                     uint16_t server_port,
                                     const LoginUserData& login_user_data = {}) noexcept {
        static_assert(ClientHandlerConcept<Handler>, "Handler must implement on_server_msg(MsgView)");
        if (!channel_.is_closed()) {
            Error err{ErrorCode::AlreadyConnected, 0};
            detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, err);
            return make_error(err);
        }
        channel_.try_close_fd();

        if (auto res = detail::open_last_server_file<Config>(client_name_, ptcp_dir_, last_server_name_); !res) {
            detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, res.error());
            return res;
        }

        FrameHeader rsp_hdr{};
        LoginRspMsg login_rsp{};
        auto fd_res = detail::dial_and_handshake<Config>(
            handler_, client_name_, last_server_name_, true,
            server_ipv4, server_port, login_user_data,
            0, 0, 0, rsp_hdr, login_rsp);
        if (!fd_res) {
            channel_.release();
            return make_error(fd_res.error());
        }

        detail::socket_t fd = *fd_res;
        const bool server_name_changed =
            std::string_view(last_server_name_->data()) != std::string_view(login_rsp.server_name);
        if (server_name_changed || channel_.remote_name()[0] == '\0') {
            channel_.release();
            channel_.set_remote_name(login_rsp.server_name);
            if (auto open_res = channel_.open_shm_files(); !open_res) {
                detail::close_socket(fd);
                channel_.release();
                detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, open_res.error());
                return open_res;
            }
            detail::copy_cstr(last_server_name_->data(), last_server_name_->size(), login_rsp.server_name);
        }

        const TimePoint now = detail::ClientCallbackBridge<Config, Handler>::on_login_success(handler_, login_rsp);
        channel_.open_keepalive(fd, now);
        return {};
    }

    uint32_t poll_shm(uint32_t max_batch = Config::poll_batch_size) {
        return channel_.drain(
            [this](MsgView msg) -> bool {
                return detail::ClientCallbackBridge<Config, Handler>::on_server_msg(handler_, msg);
            },
            max_batch);
    }

    void poll_keepalive(TimePoint now = steady_now()) {
        if (!channel_.is_closed()) {
            channel_.poll_keepalive(now);
        }
        if (channel_.try_close_fd()) {
            detail::ClientCallbackBridge<Config, Handler>::on_disconnected(handler_, channel_.close_error());
        }
    }

    void stop() noexcept {
        if (last_server_name_) {
            detail::munmap_region(last_server_name_);
            last_server_name_ = nullptr;
        }
        channel_.release();
    }

    [[nodiscard]] Channel& channel() noexcept { return channel_; }
    [[nodiscard]] const Channel& channel() const noexcept { return channel_; }

private:
    Handler& handler_;
    char client_name_[Config::name_size]{};
    std::string ptcp_dir_;
    std::array<char, Config::name_size>* last_server_name_ = nullptr;
    Channel channel_;
};

// Pure persistent-TCP client with zero SHM fields.
template<class Handler, class Config = DefaultConfig>
class TcpClient
{
    static_assert(ConfigConcept<Config>, "Config must satisfy ConfigConcept");
public:
    using Channel = TcpChannel<Config>;
    using LoginUserData = typename Config::LoginUserData;
    using LoginRspMsg = detail::LoginRspMsg<Config>;

    TcpClient(Handler& handler, std::string_view client_name, std::string_view ptcp_dir)
        : handler_(handler), ptcp_dir_(ptcp_dir) {
        detail::copy_cstr(client_name_, sizeof(client_name_), client_name);
        detail::make_dir(ptcp_dir_.c_str());
        channel_.init(ptcp_dir_.c_str(), client_name_);
    }

    ~TcpClient() noexcept { stop(); }

    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;
    TcpClient(TcpClient&&) = delete;
    TcpClient& operator=(TcpClient&&) = delete;

    [[nodiscard]] VoidResult connect(std::string_view server_ipv4,
                                     uint16_t server_port,
                                     const LoginUserData& login_user_data = {}) noexcept {
        static_assert(ClientHandlerConcept<Handler>, "Handler must implement on_server_msg(MsgView)");
        if (!channel_.is_closed()) {
            Error err{ErrorCode::AlreadyConnected, 0};
            detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, err);
            return make_error(err);
        }
        channel_.try_close_fd();

        if (auto res = detail::open_last_server_file<Config>(client_name_, ptcp_dir_, last_server_name_); !res) {
            detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, res.error());
            return res;
        }
        channel_.set_remote_name(last_server_name_->data());

        uint32_t local_ack = 0;
        uint32_t local_start = 0;
        uint32_t local_end = 0;
        bool opened_file = false;
        if ((*last_server_name_)[0] != '\0') {
            if (auto open_res = channel_.open_file(); !open_res) {
                channel_.release();
                detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, open_res.error());
                return open_res;
            }
            auto seq_res = channel_.get_seq();
            if (!seq_res) {
                channel_.release();
                detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, seq_res.error());
                return make_error(seq_res.error());
            }
            local_ack = seq_res->local_ack;
            local_start = seq_res->seq_start;
            local_end = seq_res->seq_end;
            opened_file = true;
        }

        FrameHeader rsp_hdr{};
        LoginRspMsg login_rsp{};
        auto fd_res = detail::dial_and_handshake<Config>(
            handler_, client_name_, last_server_name_, false,
            server_ipv4, server_port, login_user_data,
            local_ack, local_start, local_end, rsp_hdr, login_rsp);
        if (!fd_res) {
            channel_.release();
            return make_error(fd_res.error());
        }

        detail::socket_t fd = *fd_res;
        const bool server_name_changed =
            std::string_view(last_server_name_->data()) != std::string_view(login_rsp.server_name);
        if (server_name_changed) {
            channel_.release();
            channel_.set_remote_name(login_rsp.server_name);
            opened_file = false;
        }
        if (!opened_file) {
            if (auto open_res = channel_.open_file(); !open_res) {
                detail::close_socket(fd);
                channel_.release();
                detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, open_res.error());
                return open_res;
            }
        }
        if (server_name_changed) {
            channel_.reset_queue();
            detail::copy_cstr(last_server_name_->data(), last_server_name_->size(), login_rsp.server_name);
        }

        const TimePoint now = detail::ClientCallbackBridge<Config, Handler>::on_login_success(handler_, login_rsp);
        channel_.open(fd, rsp_hdr.ack_seq, now);
        return {};
    }

    uint32_t poll_tcp(TimePoint now = steady_now(), uint32_t max_batch = Config::poll_batch_size) {
        uint32_t count = 0;
        if (!channel_.is_closed()) {
            count = channel_.poll_and_drain(
                now,
                [this](MsgView msg) -> bool {
                    return detail::ClientCallbackBridge<Config, Handler>::on_server_msg(handler_, msg);
                },
                max_batch);
        }
        if (channel_.try_close_fd()) {
            detail::ClientCallbackBridge<Config, Handler>::on_disconnected(handler_, channel_.close_error());
        }
        return count;
    }

    void stop() noexcept {
        if (last_server_name_) {
            detail::munmap_region(last_server_name_);
            last_server_name_ = nullptr;
        }
        channel_.release();
    }

    [[nodiscard]] Channel& channel() noexcept { return channel_; }
    [[nodiscard]] const Channel& channel() const noexcept { return channel_; }

private:
    Handler& handler_;
    char client_name_[Config::name_size]{};
    std::string ptcp_dir_;
    std::array<char, Config::name_size>* last_server_name_ = nullptr;
    Channel channel_;
};

// Runtime-selectable client supporting TransportKind::Shm or TransportKind::Tcp over its dedicated channels.
template<class Handler, class Config = DefaultConfig>
class Client
{
    static_assert(ConfigConcept<Config>, "Config must satisfy ConfigConcept");
public:
    using ShmChannelType = ShmChannel<Config>;
    using TcpChannelType = TcpChannel<Config>;
    using LoginUserData = typename Config::LoginUserData;
    using LoginRspMsg = detail::LoginRspMsg<Config>;

    Client(Handler& handler, std::string_view client_name, std::string_view ptcp_dir)
        : handler_(handler), ptcp_dir_(ptcp_dir) {
        detail::copy_cstr(client_name_, sizeof(client_name_), client_name);
        detail::make_dir(ptcp_dir_.c_str());
        shm_ch_.init(client_name_);
        tcp_ch_.init(ptcp_dir_.c_str(), client_name_);
    }

    ~Client() noexcept { stop(); }

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) = delete;
    Client& operator=(Client&&) = delete;

    [[nodiscard]] VoidResult connect(TransportKind kind,
                                     std::string_view server_ipv4,
                                     uint16_t server_port,
                                     const LoginUserData& login_user_data = {}) noexcept {
        static_assert(ClientHandlerConcept<Handler>, "Handler must implement on_server_msg(MsgView)");
        if (!shm_ch_.is_closed() || !tcp_ch_.is_closed()) {
            Error err{ErrorCode::AlreadyConnected, 0};
            detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, err);
            return make_error(err);
        }
        shm_ch_.try_close_fd();
        tcp_ch_.try_close_fd();

        if (auto res = detail::open_last_server_file<Config>(client_name_, ptcp_dir_, last_server_name_); !res) {
            detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, res.error());
            return res;
        }

        kind_ = kind;
        const bool use_shm = (kind == TransportKind::Shm);
        uint32_t local_ack = 0;
        uint32_t local_start = 0;
        uint32_t local_end = 0;
        bool opened_tcp_file = false;
        if (!use_shm && (*last_server_name_)[0] != '\0') {
            tcp_ch_.set_remote_name(last_server_name_->data());
            if (auto open_res = tcp_ch_.open_file(); !open_res) {
                tcp_ch_.release();
                detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, open_res.error());
                return open_res;
            }
            auto seq_res = tcp_ch_.get_seq();
            if (!seq_res) {
                tcp_ch_.release();
                detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, seq_res.error());
                return make_error(seq_res.error());
            }
            local_ack = seq_res->local_ack;
            local_start = seq_res->seq_start;
            local_end = seq_res->seq_end;
            opened_tcp_file = true;
        }

        FrameHeader rsp_hdr{};
        LoginRspMsg login_rsp{};
        auto fd_res = detail::dial_and_handshake<Config>(
            handler_, client_name_, last_server_name_, use_shm,
            server_ipv4, server_port, login_user_data,
            local_ack, local_start, local_end, rsp_hdr, login_rsp);
        if (!fd_res) {
            if (use_shm) shm_ch_.release();
            else tcp_ch_.release();
            return make_error(fd_res.error());
        }

        detail::socket_t fd = *fd_res;
        const bool server_name_changed =
            std::string_view(last_server_name_->data()) != std::string_view(login_rsp.server_name);

        if (use_shm) {
            if (server_name_changed || shm_ch_.remote_name()[0] == '\0') {
                shm_ch_.release();
                shm_ch_.set_remote_name(login_rsp.server_name);
                if (auto open_res = shm_ch_.open_shm_files(); !open_res) {
                    detail::close_socket(fd);
                    shm_ch_.release();
                    detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, open_res.error());
                    return open_res;
                }
                detail::copy_cstr(last_server_name_->data(), last_server_name_->size(), login_rsp.server_name);
            }
            const TimePoint now = detail::ClientCallbackBridge<Config, Handler>::on_login_success(handler_, login_rsp);
            shm_ch_.open_keepalive(fd, now);
            return {};
        }

        if (server_name_changed) {
            tcp_ch_.release();
            tcp_ch_.set_remote_name(login_rsp.server_name);
            opened_tcp_file = false;
        }
        if (!opened_tcp_file) {
            if (auto open_res = tcp_ch_.open_file(); !open_res) {
                detail::close_socket(fd);
                tcp_ch_.release();
                detail::ClientCallbackBridge<Config, Handler>::on_system_error(handler_, open_res.error());
                return open_res;
            }
        }
        if (server_name_changed) {
            tcp_ch_.reset_queue();
            detail::copy_cstr(last_server_name_->data(), last_server_name_->size(), login_rsp.server_name);
        }
        const TimePoint now = detail::ClientCallbackBridge<Config, Handler>::on_login_success(handler_, login_rsp);
        tcp_ch_.open(fd, rsp_hdr.ack_seq, now);
        return {};
    }

    uint32_t poll_shm(uint32_t max_batch = Config::poll_batch_size) {
        return shm_ch_.drain(
            [this](MsgView msg) -> bool {
                return detail::ClientCallbackBridge<Config, Handler>::on_server_msg(handler_, msg);
            },
            max_batch);
    }

    uint32_t poll_tcp(TimePoint now = steady_now(), uint32_t max_batch = Config::poll_batch_size) {
        uint32_t count = 0;
        if (kind_ == TransportKind::Shm) {
            if (!shm_ch_.is_closed()) {
                shm_ch_.poll_keepalive(now);
            }
            if (shm_ch_.try_close_fd()) {
                detail::ClientCallbackBridge<Config, Handler>::on_disconnected(handler_, shm_ch_.close_error());
            }
            return 0;
        }
        if (!tcp_ch_.is_closed()) {
            count = tcp_ch_.poll_and_drain(
                now,
                [this](MsgView msg) -> bool {
                    return detail::ClientCallbackBridge<Config, Handler>::on_server_msg(handler_, msg);
                },
                max_batch);
        }
        if (tcp_ch_.try_close_fd()) {
            detail::ClientCallbackBridge<Config, Handler>::on_disconnected(handler_, tcp_ch_.close_error());
        }
        return count;
    }

    void stop() noexcept {
        if (last_server_name_) {
            detail::munmap_region(last_server_name_);
            last_server_name_ = nullptr;
        }
        shm_ch_.release();
        tcp_ch_.release();
    }

    [[nodiscard]] TransportKind transport() const noexcept { return kind_; }
    [[nodiscard]] ShmChannelType& shm_channel() noexcept { return shm_ch_; }
    [[nodiscard]] TcpChannelType& tcp_channel() noexcept { return tcp_ch_; }

    [[nodiscard]] bool is_closed() const noexcept {
        return (kind_ == TransportKind::Shm) ? shm_ch_.is_closed() : tcp_ch_.is_closed();
    }

    void close() noexcept {
        if (kind_ == TransportKind::Shm) shm_ch_.close();
        else tcp_ch_.close();
    }

    // Dispatches fn to active ShmChannel or TcpChannel cleanly without runtime branching on callers.
    template<typename Fn>
    void with_channel(Fn&& fn) {
        if (kind_ == TransportKind::Shm) {
            fn(shm_ch_);
        } else {
            fn(tcp_ch_);
        }
    }

private:
    Handler& handler_;
    char client_name_[Config::name_size]{};
    std::string ptcp_dir_;
    std::array<char, Config::name_size>* last_server_name_ = nullptr;
    TransportKind kind_ = TransportKind::Tcp;
    ShmChannelType shm_ch_;
    TcpChannelType tcp_ch_;
};

} // namespace tcpshm
