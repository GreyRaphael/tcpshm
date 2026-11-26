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
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <direct.h>
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#ifdef _MSC_VER
#pragma comment(lib, "ws2_32.lib")
#endif
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace tcpshm::detail {

#ifdef _WIN32
struct WsaInitializer
{
    WsaInitializer() noexcept {
        WSADATA wsa_data{};
        WSAStartup(MAKEWORD(2, 2), &wsa_data);
    }
    ~WsaInitializer() noexcept { WSACleanup(); }
};

inline void ensure_wsa_initialized() noexcept {
    static WsaInitializer init;
    (void)init;
}

using socket_t = SOCKET;
inline constexpr socket_t kInvalidSocket = INVALID_SOCKET;
inline constexpr int kMsgNoSignal = 0;

[[nodiscard]] inline int last_socket_error() noexcept {
    return WSAGetLastError();
}

[[nodiscard]] inline bool is_retryable_err(int err) noexcept {
    return err == WSAEWOULDBLOCK || err == WSAEINTR;
}

[[nodiscard]] inline socket_t create_socket(int af, int type, int proto) noexcept {
    ensure_wsa_initialized();
    return ::socket(af, type, proto);
}

inline int close_socket(socket_t s) noexcept {
    return ::closesocket(s);
}

inline int send_bytes(socket_t s, const char* buf, int len, int flags) noexcept {
    return ::send(s, buf, len, flags);
}

inline int recv_bytes(socket_t s, char* buf, int len, int flags) noexcept {
    return ::recv(s, buf, len, flags);
}

inline socket_t accept_socket(socket_t s, struct sockaddr* addr, socklen_t* addrlen) noexcept {
    return ::accept(s, addr, addrlen);
}

inline int bind_socket(socket_t s, const struct sockaddr* addr, socklen_t addrlen) noexcept {
    return ::bind(s, addr, static_cast<int>(addrlen));
}

inline int listen_socket(socket_t s, int backlog) noexcept {
    return ::listen(s, backlog);
}

inline int connect_socket(socket_t s, const struct sockaddr* addr, socklen_t addrlen) noexcept {
    return ::connect(s, addr, static_cast<int>(addrlen));
}

inline int set_sockopt(socket_t s, int level, int optname, const char* optval, int optlen) noexcept {
    return ::setsockopt(s, level, optname, optval, optlen);
}

inline int set_nonblocking(socket_t s) noexcept {
    u_long mode = 1;
    return ::ioctlsocket(s, FIONBIO, &mode);
}

inline int set_blocking(socket_t s) noexcept {
    u_long mode = 0;
    return ::ioctlsocket(s, FIONBIO, &mode);
}

inline int set_timeout_ms(socket_t s, int level, int optname, int timeout_ms) noexcept {
    if (timeout_ms < 0) return -1;
    DWORD timeout = static_cast<DWORD>(timeout_ms);
    return ::setsockopt(s, level, optname, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
}

struct iovec
{
    void* iov_base;
    size_t iov_len;
};

inline int readv_bytes(socket_t s, std::span<struct iovec> vecs) noexcept {
    WSABUF bufs[16]{};
    DWORD count = static_cast<DWORD>(vecs.size() > 16 ? 16 : vecs.size());
    for (DWORD i = 0; i < count; ++i) {
        bufs[i].buf = static_cast<char*>(vecs[i].iov_base);
        bufs[i].len = static_cast<ULONG>(vecs[i].iov_len);
    }
    DWORD received = 0;
    DWORD flags = 0;
    if (::WSARecv(s, bufs, count, &received, &flags, nullptr, nullptr) == 0) {
        return static_cast<int>(received);
    }
    return -1;
}

inline int make_dir(const char* path) noexcept {
    return ::_mkdir(path);
}

#else

using socket_t = int;
inline constexpr socket_t kInvalidSocket = -1;
inline constexpr int kMsgNoSignal = MSG_NOSIGNAL;

[[nodiscard]] inline int last_socket_error() noexcept {
    return errno;
}

[[nodiscard]] inline bool is_retryable_err(int err) noexcept {
    return err == EAGAIN || err == EWOULDBLOCK || err == EINTR;
}

[[nodiscard]] inline socket_t create_socket(int af, int type, int proto) noexcept {
    return ::socket(af, type, proto);
}

inline int close_socket(socket_t s) noexcept {
    return ::close(s);
}

inline int send_bytes(socket_t s, const char* buf, int len, int flags) noexcept {
    return static_cast<int>(::send(s, buf, static_cast<size_t>(len), flags));
}

inline int recv_bytes(socket_t s, char* buf, int len, int flags) noexcept {
    return static_cast<int>(::recv(s, buf, static_cast<size_t>(len), flags));
}

inline socket_t accept_socket(socket_t s, struct sockaddr* addr, socklen_t* addrlen) noexcept {
    return ::accept(s, addr, addrlen);
}

inline int bind_socket(socket_t s, const struct sockaddr* addr, socklen_t addrlen) noexcept {
    return ::bind(s, addr, addrlen);
}

inline int listen_socket(socket_t s, int backlog) noexcept {
    return ::listen(s, backlog);
}

inline int connect_socket(socket_t s, const struct sockaddr* addr, socklen_t addrlen) noexcept {
    return ::connect(s, addr, addrlen);
}

inline int set_sockopt(socket_t s, int level, int optname, const char* optval, int optlen) noexcept {
    return ::setsockopt(s, level, optname, static_cast<const void*>(optval), static_cast<socklen_t>(optlen));
}

inline int set_nonblocking(socket_t s) noexcept {
    int flags = ::fcntl(s, F_GETFL, 0);
    if (flags < 0) return flags;
    return ::fcntl(s, F_SETFL, flags | O_NONBLOCK);
}

inline int set_blocking(socket_t s) noexcept {
    int flags = ::fcntl(s, F_GETFL, 0);
    if (flags < 0) return flags;
    return ::fcntl(s, F_SETFL, flags & ~O_NONBLOCK);
}

inline int set_timeout_ms(socket_t s, int level, int optname, int timeout_ms) noexcept {
    if (timeout_ms < 0) return -1;
    struct timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    return ::setsockopt(s, level, optname, static_cast<const void*>(&tv), sizeof(tv));
}

inline int readv_bytes(socket_t s, std::span<struct iovec> vecs) noexcept {
    return static_cast<int>(::readv(s, vecs.data(), static_cast<int>(vecs.size())));
}

inline int make_dir(const char* path) noexcept {
    return ::mkdir(path, 0755);
}

#endif

[[nodiscard]] inline bool parse_ipv4_endpoint(std::string_view ipv4, uint16_t port, struct sockaddr_in& out) noexcept {
    if (ipv4.empty() || ipv4.size() >= INET_ADDRSTRLEN) return false;
    char buf[INET_ADDRSTRLEN]{};
    std::memcpy(buf, ipv4.data(), ipv4.size());
    out = {};
    out.sin_family = AF_INET;
    out.sin_port = htons(port);
    return ::inet_pton(AF_INET, buf, &out.sin_addr) == 1;
}

inline bool send_all(socket_t s, const void* buf, int len, int flags, int* sys_errno = nullptr) noexcept {
    if (len < 0) {
        if (sys_errno) *sys_errno = EINVAL;
        return false;
    }
    const char* p = static_cast<const char*>(buf);
    int sent_total = 0;
    while (sent_total < len) {
        int ret = send_bytes(s, p + sent_total, len - sent_total, flags);
        if (ret < 0) {
            int err = last_socket_error();
#ifdef _WIN32
            if (err == WSAEINTR) continue;
#else
            if (err == EINTR) continue;
#endif
            if (sys_errno) *sys_errno = err;
            return false;
        }
        if (ret == 0) {
            if (sys_errno) *sys_errno = 0;
            return false;
        }
        sent_total += ret;
    }
    return true;
}

inline bool recv_all(socket_t s, void* buf, int len, int* sys_errno = nullptr) noexcept {
    if (len < 0) {
        if (sys_errno) *sys_errno = EINVAL;
        return false;
    }
    char* p = static_cast<char*>(buf);
    int recv_total = 0;
    while (recv_total < len) {
        int ret = recv_bytes(s, p + recv_total, len - recv_total, 0);
        if (ret < 0) {
            int err = last_socket_error();
#ifdef _WIN32
            if (err == WSAEINTR) continue;
#else
            if (err == EINTR) continue;
#endif
            if (sys_errno) *sys_errno = err;
            return false;
        }
        if (ret == 0) {
            if (sys_errno) *sys_errno = 0;
            return false;
        }
        recv_total += ret;
    }
    return true;
}

inline void copy_cstr(char* dst, size_t dst_sz, std::string_view src) noexcept {
    if (dst_sz == 0) return;
    size_t n = src.size() < (dst_sz - 1) ? src.size() : (dst_sz - 1);
    if (n > 0) std::memcpy(dst, src.data(), n);
    dst[n] = '\0';
}

inline void copy_cstr(char* dst, size_t dst_sz, const char* src) noexcept {
    if (!src) {
        if (dst_sz > 0) dst[0] = '\0';
        return;
    }
    copy_cstr(dst, dst_sz, std::string_view{src});
}

[[nodiscard]] inline bool is_valid_peer_name(std::string_view name) noexcept {
    if (name.empty()) return false;
    if (name.find("..") != std::string_view::npos) return false;
    for (unsigned char c : std::span<const char>{name.data(), name.size()}) {
        if (c == '/' || c == '\\' || c < 0x20 || c >= 0x7f) return false;
    }
    return true;
}

[[nodiscard]] inline bool is_valid_shm_probe_name(std::string_view name) noexcept {
    constexpr std::string_view kPrefix = "/tcpshm_probe_";
    if (!name.starts_with(kPrefix)) return false;
    if (name.find("..") != std::string_view::npos) return false;
    std::string_view tail = name.substr(1);
    for (unsigned char c : std::span<const char>{tail.data(), tail.size()}) {
        if (c == '/' || c == '\\' || c < 0x20 || c >= 0x7f) return false;
    }
    return true;
}

inline void compiler_barrier() noexcept {
    std::atomic_signal_fence(std::memory_order_acq_rel);
}

} // namespace tcpshm::detail
