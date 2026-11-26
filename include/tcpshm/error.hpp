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

#include <cstdint>
#include <expected>
#include <string_view>

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

template<typename T>
using Result = std::expected<T, Error>;

using VoidResult = std::expected<void, Error>;

[[nodiscard]] inline std::unexpected<Error> make_error(ErrorCode code, int sys_errno = 0) noexcept {
    return std::unexpected<Error>(Error{code, sys_errno});
}

} // namespace tcpshm
