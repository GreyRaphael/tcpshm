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

#include <concepts>
#include <cstdint>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

namespace tcpshm {

enum class ErrorCode : uint16_t
{
    Ok = 0,
    AlreadyStarted,
    AlreadyConnected,
    NotConnected,
    InvalidAddress,
    SocketCreateFailed,
    SocketOptionFailed,
    BindFailed,
    ListenFailed,
    ConnectFailed,
    SendFailed,
    RecvFailed,
    PeerClosed,
    Timeout,
    HandshakeTimeout,
    InvalidClientName,
    InvalidLoginMsg,
    InvalidLoginRsp,
    ProtocolMismatch,
    ConfigMismatch,
    LoginRejected,
    AlreadyLoggedIn,
    MaxClientsExceeded,
    InvalidGroupId,
    ShmProbeFailed,
    SeqMismatch,
    CorruptPtcpFile,
    OpenFileFailed,
    StatFileFailed,
    TruncateFileFailed,
    MmapFailed,
    ShmAlreadyExists,
    InvalidMsgHeader,
    MsgTooLarge,
    InvalidAckSeq,
    QueueFull,
    EpollCreateFailed,
    EpollCtlFailed,
    EpollWaitFailed,
    RequestClose,
    Released,
};

struct Error
{
    ErrorCode code = ErrorCode::Ok;
    int sys_errno = 0;

    [[nodiscard]] constexpr bool ok() const noexcept { return code == ErrorCode::Ok; }
    [[nodiscard]] constexpr explicit operator bool() const noexcept { return ok(); }
    friend constexpr bool operator==(const Error&, const Error&) noexcept = default;

    [[nodiscard]] constexpr std::string_view message() const noexcept {
        switch (code) {
            case ErrorCode::Ok: return "Ok";
            case ErrorCode::AlreadyStarted: return "Already started";
            case ErrorCode::AlreadyConnected: return "Already connected";
            case ErrorCode::NotConnected: return "Not connected";
            case ErrorCode::InvalidAddress: return "Invalid IPv4 address";
            case ErrorCode::SocketCreateFailed: return "Socket creation failed";
            case ErrorCode::SocketOptionFailed: return "Socket option failed";
            case ErrorCode::BindFailed: return "Socket bind failed";
            case ErrorCode::ListenFailed: return "Socket listen failed";
            case ErrorCode::ConnectFailed: return "Socket connect failed";
            case ErrorCode::SendFailed: return "Socket send failed";
            case ErrorCode::RecvFailed: return "Socket recv failed";
            case ErrorCode::PeerClosed: return "Remote peer closed";
            case ErrorCode::Timeout: return "Connection timeout";
            case ErrorCode::HandshakeTimeout: return "Handshake timeout";
            case ErrorCode::InvalidClientName: return "Invalid client name";
            case ErrorCode::InvalidLoginMsg: return "Invalid login message";
            case ErrorCode::InvalidLoginRsp: return "Invalid login response";
            case ErrorCode::ProtocolMismatch: return "Protocol version mismatch";
            case ErrorCode::ConfigMismatch: return "Config layout mismatch";
            case ErrorCode::LoginRejected: return "Login rejected";
            case ErrorCode::AlreadyLoggedIn: return "Client already logged in";
            case ErrorCode::MaxClientsExceeded: return "Max client count exceeded";
            case ErrorCode::InvalidGroupId: return "Invalid group id";
            case ErrorCode::ShmProbeFailed: return "SHM probe verification failed";
            case ErrorCode::SeqMismatch: return "Sequence number mismatch";
            case ErrorCode::CorruptPtcpFile: return "Corrupt PTCP file";
            case ErrorCode::OpenFileFailed: return "Open file or SHM failed";
            case ErrorCode::StatFileFailed: return "Stat file failed";
            case ErrorCode::TruncateFileFailed: return "Truncate file failed";
            case ErrorCode::MmapFailed: return "Memory map failed";
            case ErrorCode::ShmAlreadyExists: return "SHM object already exists";
            case ErrorCode::InvalidMsgHeader: return "Invalid message header";
            case ErrorCode::MsgTooLarge: return "Message size exceeds limit";
            case ErrorCode::InvalidAckSeq: return "Invalid ack_seq";
            case ErrorCode::QueueFull: return "Queue is full";
            case ErrorCode::EpollCreateFailed: return "epoll_create1 failed";
            case ErrorCode::EpollCtlFailed: return "epoll_ctl failed";
            case ErrorCode::EpollWaitFailed: return "epoll_wait failed";
            case ErrorCode::RequestClose: return "Closed by request";
            case ErrorCode::Released: return "Released";
        }
        return "Unknown error";
    }
};

// Pure C++20 exception-free Unexpected/Expected implementation (replaces std::expected).
template<typename E>
class Unexpected
{
public:
    constexpr explicit Unexpected(const E& err) noexcept(std::is_nothrow_copy_constructible_v<E>)
        : err_(err) {}
    constexpr explicit Unexpected(E&& err) noexcept(std::is_nothrow_move_constructible_v<E>)
        : err_(std::move(err)) {}

    [[nodiscard]] constexpr const E& error() const& noexcept { return err_; }
    [[nodiscard]] constexpr E& error() & noexcept { return err_; }
    [[nodiscard]] constexpr E&& error() && noexcept { return std::move(err_); }

private:
    E err_;
};

template<typename T, typename E = Error>
class Expected
{
public:
    using value_type = T;
    using error_type = E;

    constexpr Expected() noexcept(std::is_nothrow_default_constructible_v<T>)
        requires std::is_default_constructible_v<T>
        : val_(), has_val_(true) {}

    template<typename U = T>
        requires (!std::same_as<std::decay_t<U>, Expected> &&
                  !std::same_as<std::decay_t<U>, Unexpected<E>> &&
                  std::convertible_to<U, T>)
    constexpr Expected(U&& val) noexcept(std::is_nothrow_constructible_v<T, U>)
        : val_(std::forward<U>(val)), has_val_(true) {}

    template<typename G>
        requires std::convertible_to<const G&, E>
    constexpr Expected(const Unexpected<G>& u) noexcept(std::is_nothrow_constructible_v<E, const G&>)
        : err_(u.error()), has_val_(false) {}

    template<typename G>
        requires std::convertible_to<G, E>
    constexpr Expected(Unexpected<G>&& u) noexcept(std::is_nothrow_constructible_v<E, G&&>)
        : err_(std::move(u.error())), has_val_(false) {}

    constexpr Expected(const Expected& other) noexcept(
        std::is_nothrow_copy_constructible_v<T> && std::is_nothrow_copy_constructible_v<E>)
        requires (std::is_trivially_copy_constructible_v<T> && std::is_trivially_copy_constructible_v<E>)
        = default;

    constexpr Expected(const Expected& other) noexcept(
        std::is_nothrow_copy_constructible_v<T> && std::is_nothrow_copy_constructible_v<E>)
        : has_val_(other.has_val_) {
        if (has_val_) std::construct_at(std::addressof(val_), other.val_);
        else std::construct_at(std::addressof(err_), other.err_);
    }

    constexpr Expected(Expected&& other) noexcept(
        std::is_nothrow_move_constructible_v<T> && std::is_nothrow_move_constructible_v<E>)
        requires (std::is_trivially_move_constructible_v<T> && std::is_trivially_move_constructible_v<E>)
        = default;

    constexpr Expected(Expected&& other) noexcept(
        std::is_nothrow_move_constructible_v<T> && std::is_nothrow_move_constructible_v<E>)
        : has_val_(other.has_val_) {
        if (has_val_) std::construct_at(std::addressof(val_), std::move(other.val_));
        else std::construct_at(std::addressof(err_), std::move(other.err_));
    }

    constexpr ~Expected() noexcept
        requires (std::is_trivially_destructible_v<T> && std::is_trivially_destructible_v<E>)
        = default;

    constexpr ~Expected() noexcept {
        if (has_val_) std::destroy_at(std::addressof(val_));
        else std::destroy_at(std::addressof(err_));
    }

    constexpr Expected& operator=(const Expected& other) noexcept(
        std::is_nothrow_copy_constructible_v<T> && std::is_nothrow_copy_assignable_v<T> &&
        std::is_nothrow_copy_constructible_v<E> && std::is_nothrow_copy_assignable_v<E>) {
        if (this == &other) return *this;
        if (has_val_ && other.has_val_) {
            val_ = other.val_;
        } else if (!has_val_ && !other.has_val_) {
            err_ = other.err_;
        } else if (has_val_) {
            std::destroy_at(std::addressof(val_));
            std::construct_at(std::addressof(err_), other.err_);
            has_val_ = false;
        } else {
            std::destroy_at(std::addressof(err_));
            std::construct_at(std::addressof(val_), other.val_);
            has_val_ = true;
        }
        return *this;
    }

    constexpr Expected& operator=(Expected&& other) noexcept(
        std::is_nothrow_move_constructible_v<T> && std::is_nothrow_move_assignable_v<T> &&
        std::is_nothrow_move_constructible_v<E> && std::is_nothrow_move_assignable_v<E>) {
        if (this == &other) return *this;
        if (has_val_ && other.has_val_) {
            val_ = std::move(other.val_);
        } else if (!has_val_ && !other.has_val_) {
            err_ = std::move(other.err_);
        } else if (has_val_) {
            std::destroy_at(std::addressof(val_));
            std::construct_at(std::addressof(err_), std::move(other.err_));
            has_val_ = false;
        } else {
            std::destroy_at(std::addressof(err_));
            std::construct_at(std::addressof(val_), std::move(other.val_));
            has_val_ = true;
        }
        return *this;
    }

    [[nodiscard]] constexpr bool has_value() const noexcept { return has_val_; }
    [[nodiscard]] constexpr explicit operator bool() const noexcept { return has_val_; }

    [[nodiscard]] constexpr T& operator*() & noexcept { return val_; }
    [[nodiscard]] constexpr const T& operator*() const& noexcept { return val_; }
    [[nodiscard]] constexpr T&& operator*() && noexcept { return std::move(val_); }

    [[nodiscard]] constexpr T* operator->() noexcept { return std::addressof(val_); }
    [[nodiscard]] constexpr const T* operator->() const noexcept { return std::addressof(val_); }

    [[nodiscard]] constexpr E& error() & noexcept { return err_; }
    [[nodiscard]] constexpr const E& error() const& noexcept { return err_; }
    [[nodiscard]] constexpr E&& error() && noexcept { return std::move(err_); }

    template<typename U>
    [[nodiscard]] constexpr T value_or(U&& default_val) const& {
        return has_val_ ? val_ : static_cast<T>(std::forward<U>(default_val));
    }

private:
    union
    {
        T val_;
        E err_;
    };
    bool has_val_;
};

template<typename E>
class Expected<void, E>
{
public:
    using value_type = void;
    using error_type = E;

    constexpr Expected() noexcept : dummy_(0), has_val_(true) {}

    template<typename G>
        requires std::convertible_to<const G&, E>
    constexpr Expected(const Unexpected<G>& u) noexcept(std::is_nothrow_constructible_v<E, const G&>)
        : err_(u.error()), has_val_(false) {}

    template<typename G>
        requires std::convertible_to<G, E>
    constexpr Expected(Unexpected<G>&& u) noexcept(std::is_nothrow_constructible_v<E, G&&>)
        : err_(std::move(u.error())), has_val_(false) {}

    constexpr Expected(const Expected&) noexcept(std::is_nothrow_copy_constructible_v<E>)
        requires std::is_trivially_copy_constructible_v<E> = default;

    constexpr Expected(const Expected& other) noexcept(std::is_nothrow_copy_constructible_v<E>)
        : dummy_(0), has_val_(other.has_val_) {
        if (!has_val_) std::construct_at(std::addressof(err_), other.err_);
    }

    constexpr Expected(Expected&&) noexcept(std::is_nothrow_move_constructible_v<E>)
        requires std::is_trivially_move_constructible_v<E> = default;

    constexpr Expected(Expected&& other) noexcept(std::is_nothrow_move_constructible_v<E>)
        : dummy_(0), has_val_(other.has_val_) {
        if (!has_val_) std::construct_at(std::addressof(err_), std::move(other.err_));
    }

    constexpr ~Expected() noexcept
        requires std::is_trivially_destructible_v<E> = default;

    constexpr ~Expected() noexcept {
        if (!has_val_) std::destroy_at(std::addressof(err_));
    }

    constexpr Expected& operator=(const Expected& other) noexcept(
        std::is_nothrow_copy_constructible_v<E> && std::is_nothrow_copy_assignable_v<E>) {
        if (this == &other) return *this;
        if (!has_val_ && !other.has_val_) {
            err_ = other.err_;
        } else if (!has_val_) {
            std::destroy_at(std::addressof(err_));
            dummy_ = 0;
            has_val_ = true;
        } else if (!other.has_val_) {
            std::construct_at(std::addressof(err_), other.err_);
            has_val_ = false;
        }
        return *this;
    }

    constexpr Expected& operator=(Expected&& other) noexcept(
        std::is_nothrow_move_constructible_v<E> && std::is_nothrow_move_assignable_v<E>) {
        if (this == &other) return *this;
        if (!has_val_ && !other.has_val_) {
            err_ = std::move(other.err_);
        } else if (!has_val_) {
            std::destroy_at(std::addressof(err_));
            dummy_ = 0;
            has_val_ = true;
        } else if (!other.has_val_) {
            std::construct_at(std::addressof(err_), std::move(other.err_));
            has_val_ = false;
        }
        return *this;
    }

    [[nodiscard]] constexpr bool has_value() const noexcept { return has_val_; }
    [[nodiscard]] constexpr explicit operator bool() const noexcept { return has_val_; }

    [[nodiscard]] constexpr E& error() & noexcept { return err_; }
    [[nodiscard]] constexpr const E& error() const& noexcept { return err_; }
    [[nodiscard]] constexpr E&& error() && noexcept { return std::move(err_); }

private:
    union
    {
        uint8_t dummy_;
        E err_;
    };
    bool has_val_;
};

template<typename T>
using Result = Expected<T, Error>;

using VoidResult = Expected<void, Error>;

[[nodiscard]] constexpr Unexpected<Error> make_error(Error err) noexcept {
    return Unexpected<Error>(err);
}

[[nodiscard]] constexpr Unexpected<Error> make_error(ErrorCode code, int sys_errno = 0) noexcept {
    return Unexpected<Error>(Error{code, sys_errno});
}

} // namespace tcpshm
