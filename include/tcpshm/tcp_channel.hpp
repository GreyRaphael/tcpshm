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
#include "tcpshm/ptcp_queue.hpp"
#include <algorithm>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace tcpshm {

template<class Handler, class Config, template<uint32_t> class TcpPoller>
class Server;

template<class Handler, class Config>
class TcpClient;

template<class Handler, class Config>
class Client;

// Dedicated persistent-TCP channel with mmap-backed sequence recovery and branchless TX/RX path.
template<ConfigConcept Config>
class TcpChannel
{
public:
    static constexpr TransportKind transport = TransportKind::Tcp;
    using Queue = PtcpQueue<Config::tcp_queue_bytes, Config::wire_endian>;
    using Guard = WriteGuard<TcpChannel>;

    TcpChannel() noexcept {
        remote_name_[0] = '\0';
        hb_frame_.size = static_cast<uint16_t>(sizeof(FrameHeader));
        hb_frame_.msg_type = kHeartbeatMsgType;
        hb_frame_.ack_seq = 0;
        hb_frame_.template convert_byte_order<Config::wire_endian>();
    }

    ~TcpChannel() noexcept { release(); }

    TcpChannel(const TcpChannel&) = delete;
    TcpChannel& operator=(const TcpChannel&) = delete;
    TcpChannel(TcpChannel&&) = delete;
    TcpChannel& operator=(TcpChannel&&) = delete;

    // Allocates an outbound frame in persistent queue. commit() pushes and flushes over TCP; stage() batches without send syscall.
    [[nodiscard]] std::optional<Guard> try_alloc(uint16_t msg_type, uint16_t payload_size) noexcept {
        if (!q_ || sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket) [[unlikely]] {
            return std::nullopt;
        }
        FrameHeader* hdr = q_->alloc_raw(msg_type, payload_size);
        if (!hdr) return std::nullopt;
        return std::optional<Guard>(std::in_place, this, hdr, payload_size);
    }

    [[nodiscard]] std::optional<Guard> try_alloc(uint16_t payload_size) noexcept {
        return try_alloc(kFirstAppMsgType, payload_size);
    }

    void commit_pending() noexcept {
        if (!q_) [[unlikely]] return;
        q_->push_pending();
        staged_ = false;
        flush();
    }

    void stage_pending() noexcept {
        if (!q_) [[unlikely]] return;
        q_->push_pending();
        staged_ = true;
    }

    void cancel_pending() noexcept {}

    bool flush() noexcept {
        staged_ = false;
        detail::socket_t fd = sockfd_.load(std::memory_order_acquire);
        if (fd == detail::kInvalidSocket || !q_) [[unlikely]] return false;
        return send_pending_with_fd(fd);
    }

    [[nodiscard]] bool has_pending_send() const noexcept {
        return send_byte_off_ > 0 || (q_ && q_->has_sendable());
    }

    [[nodiscard]] bool has_buffered_rx() const noexcept {
        return readidx_ != nextmsg_idx_;
    }

    // Thread-safe shutdown request.
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

    [[nodiscard]] std::string ptcp_file_path() const {
        return std::string(ptcp_dir_) + "/" + local_name_ + "_" + remote_name_ + ".ptcp";
    }

private:
    template<class Handler, class Cfg, template<uint32_t> class TcpPoller>
    friend class Server;
    template<class Handler, class Cfg>
    friend class TcpClient;
    template<class Handler, class Cfg>
    friend class Client;

    static constexpr uint32_t kOverflowCap =
        Config::tcp_recv_max_bytes < 65536u ? Config::tcp_recv_max_bytes : 65536u;

    void init(const char* ptcp_dir, const char* local_name) noexcept {
        ptcp_dir_ = ptcp_dir;
        local_name_ = local_name;
    }

    void set_handle(ConnHandle h) noexcept { handle_ = h; }
    void set_remote_name(std::string_view name) noexcept {
        detail::copy_cstr(remote_name_, sizeof(remote_name_), name);
    }

    [[nodiscard]] VoidResult open_file() noexcept {
        if (!q_) {
            std::string path = ptcp_file_path();
            auto res = detail::mmap_create_or_open<Queue>(path.c_str(), false, false);
            if (!res) [[unlikely]] return std::unexpected(res.error());
            q_ = *res;
        }
        return {};
    }

    [[nodiscard]] Result<PtcpSeqRange> get_seq() noexcept {
        if (!q_) return make_error(ErrorCode::NotConnected);
        return q_->validate_and_get_seq();
    }

    void reset_queue() noexcept {
        send_byte_off_ = 0;
        hb_send_off_ = 0;
        if (q_) q_->reset();
    }

    void open(detail::socket_t sock_fd, uint32_t remote_ack_seq, TimePoint now) noexcept {
        close_error_ = Error{ErrorCode::Ok, 0};
        send_byte_off_ = 0;
        hb_send_off_ = 0;
        fd_to_close_ = sock_fd;
        sockfd_.store(sock_fd, std::memory_order_relaxed);
        close_claimed_.store(false, std::memory_order_release);
        writeidx_ = 0;
        readidx_ = 0;
        nextmsg_idx_ = 0;
        recv_time_ = now;
        send_time_ = now;
        now_ = now;
        if (recvbuf_size_ == 0) {
            recvbuf_size_ = Config::tcp_recv_init_bytes;
            recvbuf_.reset(new std::byte[recvbuf_size_]);
        }
        if (q_) {
            q_->login_ack(remote_ack_seq);
            send_pending_with_fd(sock_fd);
        }
    }

    // Combined poll & batch drain: flushes pending TX/HB, performs at most one recv, and dispatches up to max_batch frames.
    template<typename Fn>
        requires std::invocable<Fn&, MsgView>
    uint32_t poll_and_drain(TimePoint now, Fn&& fn, uint32_t max_batch = Config::poll_batch_size) {
        detail::socket_t fd = sockfd_.load(std::memory_order_acquire);
        if (fd == detail::kInvalidSocket || !q_) [[unlikely]] return 0;
        now_ = now;

        bool tried_send = false;
        if (has_pending_send()) {
            tried_send = true;
            send_pending_with_fd(fd);
            fd = sockfd_.load(std::memory_order_acquire);
            if (fd == detail::kInvalidSocket) [[unlikely]] return 0;
        }
        if (hb_send_off_ > 0 || (!tried_send && (now_ - send_time_ >= Config::heartbeat_interval))) [[unlikely]] {
            send_hb_frame(fd);
            fd = sockfd_.load(std::memory_order_acquire);
            if (fd == detail::kInvalidSocket) [[unlikely]] return 0;
        }

        if (int len = do_recv(fd); len > 0) {
            const uint32_t old_writeidx = writeidx_;
            writeidx_ += static_cast<uint32_t>(len);
            while (writeidx_ - nextmsg_idx_ >= sizeof(FrameHeader)) {
                FrameHeader* hdr = std::launder(reinterpret_cast<FrameHeader*>(&recvbuf_[nextmsg_idx_]));
                if (old_writeidx < nextmsg_idx_ + static_cast<uint32_t>(sizeof(FrameHeader))) {
                    hdr->template convert_byte_order<Config::wire_endian>();
                }
                if (hdr->size < sizeof(FrameHeader) ||
                    (hdr->msg_type == kHeartbeatMsgType && hdr->size != sizeof(FrameHeader))) [[unlikely]] {
                    close_with(Error{ErrorCode::InvalidMsgHeader, 0});
                    return 0;
                }
                const uint32_t msg_bytes = (static_cast<uint32_t>(hdr->size) + 7u) & ~7u;
                if (msg_bytes > Config::tcp_recv_max_bytes) [[unlikely]] {
                    close_with(Error{ErrorCode::MsgTooLarge, 0});
                    return 0;
                }
                if (!q_->ack(hdr->ack_seq)) [[unlikely]] {
                    close_with(Error{ErrorCode::InvalidAckSeq, 0});
                    return 0;
                }
                if (writeidx_ - nextmsg_idx_ < msg_bytes) break;
                if (hdr->msg_type == kHeartbeatMsgType && readidx_ == nextmsg_idx_) {
                    readidx_ += msg_bytes;
                }
                nextmsg_idx_ += msg_bytes;
            }
        }

        if (sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket) [[unlikely]] {
            return 0;
        }
        return dispatch_buffered(std::forward<Fn>(fn), max_batch);
    }

    template<typename Fn>
        requires std::invocable<Fn&, MsgView>
    uint32_t drain_buffered(TimePoint now, Fn&& fn, uint32_t max_batch = Config::poll_batch_size) {
        if (!q_ || sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket) [[unlikely]] {
            return 0;
        }
        now_ = now;
        if (has_pending_send()) {
            flush();
            if (sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket) [[unlikely]] return 0;
        }
        return dispatch_buffered(std::forward<Fn>(fn), max_batch);
    }

    void maintain_idle(TimePoint now) noexcept {
        detail::socket_t fd = sockfd_.load(std::memory_order_acquire);
        if (fd == detail::kInvalidSocket) [[unlikely]] return;
        now_ = now;
        if (has_pending_send()) {
            send_pending_with_fd(fd);
            fd = sockfd_.load(std::memory_order_acquire);
            if (fd == detail::kInvalidSocket) [[unlikely]] return;
        }
        if (hb_send_off_ > 0 || (now_ - send_time_ >= Config::heartbeat_interval)) [[unlikely]] {
            send_hb_frame(fd);
            if (sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket) [[unlikely]] return;
        }
        if (now_ - recv_time_ > Config::connection_timeout) [[unlikely]] {
            close_with(Error{ErrorCode::Timeout, 0});
        }
    }

    [[nodiscard]] detail::socket_t socket_fd() const noexcept {
        return fd_to_close_;
    }

    [[nodiscard]] detail::socket_t pending_close_fd() const noexcept {
        if (sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket) {
            return fd_to_close_;
        }
        return detail::kInvalidSocket;
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
        send_byte_off_ = 0;
        hb_send_off_ = 0;
        overflow_buf_.reset();
        remote_name_[0] = '\0';
        handle_ = ConnHandle{};
        if (q_) {
            detail::munmap_region(q_);
            q_ = nullptr;
        }
    }

    typename Config::ConnectionUserData user_data{};

private:
    template<typename Fn>
        requires std::invocable<Fn&, MsgView>
    uint32_t dispatch_buffered(Fn&& fn, uint32_t max_batch) {
        uint32_t consumed = 0;
        while (readidx_ != nextmsg_idx_ && consumed < max_batch) {
            const FrameHeader* hdr = std::launder(reinterpret_cast<const FrameHeader*>(&recvbuf_[readidx_]));
            if (hdr->msg_type == kHeartbeatMsgType) {
                readidx_ += static_cast<uint32_t>(sizeof(FrameHeader));
                continue;
            }
            const uint32_t msg_bytes = (static_cast<uint32_t>(hdr->size) + 7u) & ~7u;
            MsgView view{hdr};
            using Ret = std::invoke_result_t<Fn&, MsgView>;
            if constexpr (std::same_as<Ret, bool>) {
                if (!fn(view)) {
                    break;
                }
            } else {
                fn(view);
            }
            readidx_ += msg_bytes;
            ++q_->my_ack();
            ++consumed;
            if (sockfd_.load(std::memory_order_acquire) == detail::kInvalidSocket) [[unlikely]] {
                staged_ = false;
                break;
            }
        }
        if (staged_) [[unlikely]] {
            flush();
        }
        return consumed;
    }

    void close_with(Error err) noexcept {
        if (close_claimed_.exchange(true, std::memory_order_acq_rel)) return;
        close_error_ = err;
        sockfd_.store(detail::kInvalidSocket, std::memory_order_release);
    }

    void send_hb_frame(detail::socket_t fd) noexcept {
        if (q_ && hb_send_off_ == 0) {
            hb_frame_.ack_seq = detail::to_wire<Config::wire_endian>(q_->my_ack());
        }
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

    bool send_pending_with_fd(detail::socket_t fd) noexcept {
        uint32_t blk_sz = 0;
        const char* base = reinterpret_cast<const char*>(q_->get_sendable(blk_sz));
        if (blk_sz == 0) {
            send_byte_off_ = 0;
            return false;
        }
        const uint32_t total_bytes = (blk_sz << 3u) - send_byte_off_;
        const char* p = base + send_byte_off_;
        uint32_t sent_total = 0;
        while (sent_total < total_bytes) {
            int sent = detail::send_bytes(fd, p + sent_total, static_cast<int>(total_bytes - sent_total), detail::kMsgNoSignal);
            if (sent <= 0) [[unlikely]] {
                if (sent == 0) {
                    close_with(Error{ErrorCode::SendFailed, 0});
                    return false;
                }
                int err = detail::last_socket_error();
                if (detail::is_retryable_err(err)) {
                    break;
                }
                close_with(Error{ErrorCode::SendFailed, err});
                return false;
            }
            sent_total += static_cast<uint32_t>(sent);
        }
        if (sent_total > 0) {
            send_time_ = now_;
            const uint32_t total_consumed = static_cast<uint32_t>(send_byte_off_) + sent_total;
            const uint32_t sent_blk = total_consumed >> 3u;
            send_byte_off_ = static_cast<uint8_t>(total_consumed & 7u);
            if (sent_blk > 0) {
                q_->on_sent_blocks(sent_blk);
            }
        }
        return true;
    }

    int do_recv(detail::socket_t fd) noexcept {
        if (readidx_ > 0 && readidx_ == writeidx_) {
            readidx_ = nextmsg_idx_ = writeidx_ = 0;
        }
        const uint32_t writable = recvbuf_size_ - writeidx_;
        const bool allow_expand = (writable + readidx_) * 2u < recvbuf_size_;
        const uint32_t extra_size = std::min(
            kOverflowCap,
            readidx_ + (allow_expand ? (Config::tcp_recv_max_bytes - recvbuf_size_) : 0u));
        if (writable + extra_size == 0) [[unlikely]] {
            if (now_ - recv_time_ > Config::connection_timeout) [[unlikely]] {
                close_with(Error{ErrorCode::Timeout, 0});
            }
            return 0;
        }
        int ret = 0;
        if (extra_size == 0) [[likely]] {
            ret = detail::recv_bytes(fd, reinterpret_cast<char*>(&recvbuf_[writeidx_]), static_cast<int>(writable), 0);
        } else {
            if (!overflow_buf_) [[unlikely]] {
                overflow_buf_.reset(new std::byte[kOverflowCap]);
            }
            detail::iovec vec[2]{};
            vec[0].iov_base = &recvbuf_[writeidx_];
            vec[0].iov_len = writable;
            vec[1].iov_base = overflow_buf_.get();
            vec[1].iov_len = extra_size;
            ret = detail::readv_bytes(fd, vec);
        }
        if (ret <= 0) {
            if (ret < 0) [[likely]] {
                int err = detail::last_socket_error();
                if (detail::is_retryable_err(err)) [[likely]] {
                    if (now_ - recv_time_ > Config::connection_timeout) [[unlikely]] {
                        close_with(Error{ErrorCode::Timeout, 0});
                    }
                } else {
                    close_with(Error{ErrorCode::RecvFailed, err});
                }
            } else {
                close_with(Error{ErrorCode::PeerClosed, 0});
            }
            return 0;
        }
        recv_time_ = now_;
        if (ret <= static_cast<int>(writable)) [[likely]] return ret;
        const uint32_t uret = static_cast<uint32_t>(ret);
        if (uret <= writable + readidx_) {
            std::memmove(&recvbuf_[0], &recvbuf_[readidx_], recvbuf_size_ - readidx_);
            std::memcpy(&recvbuf_[recvbuf_size_ - readidx_], overflow_buf_.get(), uret - writable);
        } else {
            const uint32_t needed = (writeidx_ - readidx_ + uret + 7u) & ~7u;
            const uint32_t newbufsize = std::min(Config::tcp_recv_max_bytes, std::max(recvbuf_size_ * 2u, needed));
            std::unique_ptr<std::byte[]> new_buf(new std::byte[newbufsize]);
            std::memcpy(&new_buf[0], &recvbuf_[readidx_], recvbuf_size_ - readidx_);
            std::memcpy(&new_buf[recvbuf_size_ - readidx_], overflow_buf_.get(), uret - writable);
            recvbuf_size_ = newbufsize;
            std::swap(recvbuf_, new_buf);
        }
        writeidx_ -= readidx_;
        nextmsg_idx_ -= readidx_;
        readidx_ = 0;
        return ret;
    }

    const char* ptcp_dir_ = "";
    const char* local_name_ = "";
    char remote_name_[Config::name_size]{};
    ConnHandle handle_{};

    Queue* q_ = nullptr;
    std::atomic<detail::socket_t> sockfd_{detail::kInvalidSocket};
    std::atomic<bool> close_claimed_{true};
    detail::socket_t fd_to_close_ = detail::kInvalidSocket;
    Error close_error_{};

    std::unique_ptr<std::byte[]> recvbuf_;
    std::unique_ptr<std::byte[]> overflow_buf_;
    uint32_t recvbuf_size_ = 0;
    uint32_t writeidx_ = 0;
    uint32_t nextmsg_idx_ = 0;
    uint32_t readidx_ = 0;
    TimePoint recv_time_{};
    TimePoint send_time_{};
    TimePoint now_{};
    FrameHeader hb_frame_{};
    uint8_t send_byte_off_ = 0;
    uint8_t hb_send_off_ = 0;
    bool staged_ = false;
};

} // namespace tcpshm
