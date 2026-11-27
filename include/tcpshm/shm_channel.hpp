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
#include "tcpshm/detail/endian.hpp"
#include "tcpshm/detail/os_shm.hpp"
#include "tcpshm/detail/os_socket.hpp"
#include "tcpshm/error.hpp"
#include "tcpshm/frame.hpp"
#include "tcpshm/spsc_queue.hpp"
#include <atomic>
#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace tcpshm {

template<class Handler, class Config, template<uint32_t> class TcpPoller>
class Server;

template<class Handler, class Config>
class ShmClient;

template<class Handler, class Config>
class Client;

// Dedicated shared-memory channel with branchless SPSC queues and a lightweight TCP keepalive socket.
// Contains zero PTCP buffers or sequence-tracking state.
template<ConfigConcept Config>
class ShmChannel
{
public:
    static constexpr TransportKind transport = TransportKind::Shm;
    using Layout = SpscRingLayout<Config::shm_queue_bytes, Config::shm_block_bytes>;
    using Producer = SpscProducer<Config::shm_queue_bytes, Config::shm_block_bytes>;
    using Consumer = SpscConsumer<Config::shm_queue_bytes, Config::shm_block_bytes>;

    ShmChannel() noexcept {
        remote_name_[0] = '\0';
        hb_frame_.size = kFrameHeaderSize;
        hb_frame_.msg_type = kHeartbeatMsgType;
        hb_frame_.flags = 0;
        hb_frame_.ack_seq = 0;
        hb_frame_.reserved = 0;
        hb_frame_.template convert_byte_order<Config::wire_endian>();
    }

    ~ShmChannel() noexcept { release(); }

    ShmChannel(const ShmChannel&) = delete;
    ShmChannel& operator=(const ShmChannel&) = delete;
    ShmChannel(ShmChannel&&) = delete;
    ShmChannel& operator=(ShmChannel&&) = delete;

    [[nodiscard]] auto try_alloc(uint16_t msg_type, uint32_t payload_size) noexcept {
        return sendq_.try_alloc(msg_type, payload_size);
    }

    [[nodiscard]] auto try_alloc(uint32_t payload_size) noexcept {
        return sendq_.try_alloc(kFirstAppMsgType, payload_size);
    }

    void flush() noexcept { sendq_.flush(); }
    [[nodiscard]] bool has_staged() const noexcept { return sendq_.has_staged(); }

    [[nodiscard]] auto try_front() noexcept { return recvq_.try_front(); }

    template<typename Fn>
        requires std::invocable<Fn&, MsgView>
    uint32_t drain(Fn&& fn, uint32_t max_batch = Config::poll_batch_size) {
        return recvq_.drain(std::forward<Fn>(fn), max_batch);
    }

    // Thread-safe connection shutdown request.
    void close() noexcept {
        close_with(Error{ErrorCode::RequestClose, 0});
    }

    [[nodiscard]] bool is_closed() const noexcept {
        return sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket;
    }

    [[nodiscard]] Error close_error() const noexcept { return close_error_; }
    [[nodiscard]] const char* remote_name() const noexcept { return remote_name_; }
    [[nodiscard]] const char* local_name() const noexcept { return local_name_; }
    [[nodiscard]] ConnHandle handle() const noexcept { return handle_; }

    typename Config::ConnectionUserData user_data{};

private:
    template<class Handler, class Cfg, template<uint32_t> class TcpPoller>
    friend class Server;
    template<class Handler, class Cfg>
    friend class ShmClient;
    template<class Handler, class Cfg>
    friend class Client;

    void init(const char* local_name) noexcept {
        local_name_ = local_name;
    }

    void set_handle(ConnHandle h) noexcept { handle_ = h; }
    void set_remote_name(std::string_view name) noexcept {
        detail::copy_cstr(remote_name_, sizeof(remote_name_), name);
    }

    [[nodiscard]] VoidResult open_shm_files() noexcept {
        std::string send_name = std::string("/") + local_name_ + "_" + remote_name_ + ".shm";
        std::string recv_name = std::string("/") + remote_name_ + "_" + local_name_ + ".shm";

        if (!sendq_.is_attached()) {
            auto res = detail::mmap_create_or_open<Layout>(send_name.c_str(), true, false);
            if (!res) [[unlikely]] return make_error(res.error());
            sendq_.attach(*res);
        }
        if (!recvq_.is_attached()) {
            auto res = detail::mmap_create_or_open<Layout>(recv_name.c_str(), true, false);
            if (!res) [[unlikely]] {
                if (Layout* old_send = sendq_.detach()) {
                    detail::munmap_region(old_send);
                }
                return make_error(res.error());
            }
            recvq_.attach(*res);
        }
        return {};
    }

    void reset_queues() noexcept {
        sendq_.reset();
        recvq_.reset();
    }

    void open_keepalive(detail::socket_t sock_fd, TimePoint now) noexcept {
        close_error_ = Error{ErrorCode::Ok, 0};
        hb_send_off_ = 0;
        fd_to_close_ = sock_fd;
        sockfd_.store(sock_fd, std::memory_order_relaxed);
        close_claimed_.store(false, std::memory_order_release);
        recv_time_ = now;
        send_time_ = now;
        now_ = now;
    }

    void poll_keepalive(TimePoint now) noexcept {
        detail::socket_t fd = sockfd_.load(std::memory_order_acquire);
        if (fd == detail::kInvalidSocket) [[unlikely]] return;
        now_ = now;

        if (hb_send_off_ > 0 || (now_ - send_time_ >= Config::heartbeat_interval)) [[unlikely]] {
            send_hb_frame(fd);
            fd = sockfd_.load(std::memory_order_acquire);
            if (fd == detail::kInvalidSocket) [[unlikely]] return;
        }

        char hb_buf[256];
        while (true) {
            int ret = detail::recv_bytes(fd, hb_buf, static_cast<int>(sizeof(hb_buf)), 0);
            if (ret > 0) {
                recv_time_ = now_;
                if (ret < static_cast<int>(sizeof(hb_buf))) break;
                continue;
            }
            if (ret == 0) [[unlikely]] {
                close_with(Error{ErrorCode::PeerClosed, 0});
                return;
            }
            int err = detail::last_socket_error();
            if (detail::is_retryable_err(err)) [[likely]] {
                if (now_ - recv_time_ > Config::connection_timeout) [[unlikely]] {
                    close_with(Error{ErrorCode::Timeout, 0});
                }
            } else {
                close_with(Error{ErrorCode::RecvFailed, err});
            }
            return;
        }
    }

    bool try_close_fd() noexcept {
        if (sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket &&
            fd_to_close_ != detail::kInvalidSocket) {
            detail::close_socket(fd_to_close_);
            fd_to_close_ = detail::kInvalidSocket;
            return true;
        }
        return false;
    }

    void release() noexcept {
        close_with(Error{ErrorCode::Released, 0});
        try_close_fd();
        hb_send_off_ = 0;
        remote_name_[0] = '\0';
        handle_ = ConnHandle{};
        if (Layout* p = sendq_.detach()) {
            detail::munmap_region(p);
        }
        if (Layout* p = recvq_.detach()) {
            detail::munmap_region(p);
        }
    }

    void close_with(Error err) noexcept {
        if (close_claimed_.exchange(true, std::memory_order_acq_rel)) return;
        close_error_ = err;
        sockfd_.store(detail::kInvalidSocket, std::memory_order_release);
    }

    void send_hb_frame(detail::socket_t fd) noexcept {
        const char* base = reinterpret_cast<const char*>(&hb_frame_);
        while (hb_send_off_ < sizeof(FrameHeader)) {
            int remain = static_cast<int>(sizeof(FrameHeader) - hb_send_off_);
            int sent = detail::send_bytes(fd, base + hb_send_off_, remain, detail::kMsgNoSignal);
            if (sent < 0) {
                int err = detail::last_socket_error();
                if (detail::is_retryable_err(err)) return;
                close_with(Error{ErrorCode::SendFailed, err});
                return;
            }
            if (sent == 0) {
                close_with(Error{ErrorCode::SendFailed, 0});
                return;
            }
            hb_send_off_ = static_cast<uint8_t>(hb_send_off_ + static_cast<uint8_t>(sent));
        }
        hb_send_off_ = 0;
        send_time_ = now_;
    }

    const char* local_name_ = "";
    char remote_name_[Config::name_size]{};
    ConnHandle handle_{};

    std::atomic<detail::socket_t> sockfd_{detail::kInvalidSocket};
    std::atomic<bool> close_claimed_{true};
    detail::socket_t fd_to_close_ = detail::kInvalidSocket;
    Error close_error_{};
    TimePoint recv_time_{};
    TimePoint send_time_{};
    TimePoint now_{};
    FrameHeader hb_frame_{};
    uint8_t hb_send_off_ = 0;

    alignas(64) Producer sendq_;
    alignas(64) Consumer recvq_;
};

} // namespace tcpshm
