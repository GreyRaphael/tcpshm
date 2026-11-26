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

#include "tcpshm/detail/os_socket.hpp"
#include "tcpshm/error.hpp"
#include <cerrno>
#include <cstdint>

#ifdef __linux__
#include <sys/epoll.h>
#include <unistd.h>
#endif

namespace tcpshm {

// Cross-platform default TCP poller: busy-polls active connections in-process for minimum tail latency.
template<uint32_t MaxConns>
class BusyPoller
{
public:
    static constexpr bool is_event_driven = false;

    [[nodiscard]] VoidResult init() noexcept { return {}; }
    void reset() noexcept {}
    [[nodiscard]] VoidResult on_add(detail::socket_t, uint32_t) noexcept { return {}; }
    void on_remove(detail::socket_t) noexcept {}
};

#ifdef __linux__
// Linux epoll-based event-driven TCP poller: avoids O(N) idle recv() syscalls as TCP connection count grows.
template<uint32_t MaxConns>
class EpollPoller
{
public:
    static constexpr bool is_event_driven = true;

    EpollPoller() = default;
    ~EpollPoller() noexcept { reset(); }

    EpollPoller(const EpollPoller&) = delete;
    EpollPoller& operator=(const EpollPoller&) = delete;
    EpollPoller(EpollPoller&&) = delete;
    EpollPoller& operator=(EpollPoller&&) = delete;

    [[nodiscard]] VoidResult init() noexcept {
        if (epoll_fd_ != -1) return {};
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ == -1) {
            return make_error(ErrorCode::EpollCreateFailed, errno);
        }
        return {};
    }

    void reset() noexcept {
        if (epoll_fd_ != -1) {
            ::close(epoll_fd_);
            epoll_fd_ = -1;
        }
    }

    [[nodiscard]] VoidResult on_add(detail::socket_t fd, uint32_t slot) noexcept {
        if (epoll_fd_ == -1 || fd == detail::kInvalidSocket) return {};
        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLRDHUP;
        ev.data.u32 = slot;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
            return make_error(ErrorCode::EpollCtlFailed, errno);
        }
        return {};
    }

    void on_remove(detail::socket_t fd) noexcept {
        if (epoll_fd_ != -1 && fd != detail::kInvalidSocket) {
            (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
    }

    template<typename Fn>
    [[nodiscard]] int poll_ready(Fn&& fn, int timeout_ms = 0) {
        if (epoll_fd_ == -1) return 0;
        int n = 0;
        do {
            n = ::epoll_wait(epoll_fd_, events_, static_cast<int>(MaxConns), timeout_ms);
        } while (n < 0 && errno == EINTR);
        if (n <= 0) return n;
        for (int i = 0; i < n; ++i) {
            fn(events_[i].data.u32);
        }
        return n;
    }

private:
    int epoll_fd_ = -1;
    struct epoll_event events_[MaxConns]{};
};
#endif

} // namespace tcpshm
