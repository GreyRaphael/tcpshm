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
#include "ptcp_queue.h"
#include "mmap.h"
#include "os.h"
#include <atomic>
#include <memory>

namespace tcpshm {

// HeartbeatMsg is special that it only has MsgHeader
struct HeartbeatMsg
{
    static const uint16_t msg_type = 0;
};

constexpr uint64_t ShmProbeAckMask = 0x5a5a5a5a5a5a5a5aULL;
constexpr uint32_t ShmProbeNameSize = 64;

struct ShmProbeData
{
    alignas(8) uint64_t token;
    alignas(8) uint64_t ack;
};
static_assert(std::atomic_ref<uint64_t>::is_always_lock_free, "atomic_ref<uint64_t> must be lock-free");

struct ShmProbeReqMsg
{
    static const uint16_t msg_type = 3;
    uint64_t token;
    char shm_name[ShmProbeNameSize];

    template<bool ToLittle>
    void ConvertByteOrder() {
        Endian<ToLittle> ed;
        ed.ConvertInPlace(token);
    }
};

struct ShmProbeRspMsg
{
    static const uint16_t msg_type = 4;
    uint64_t ack;
    char ok;

    template<bool ToLittle>
    void ConvertByteOrder() {
        Endian<ToLittle> ed;
        ed.ConvertInPlace(ack);
    }
};

// Note that we allow user to reuse msg_type 1 and 2
// Because LoginMsg and LoginRspMsg are expected only at the beginning of a connection
template<class Conf>
struct LoginMsgTpl
{
    static const uint16_t msg_type = 1;
    uint32_t client_seq_start;
    uint32_t client_seq_end;
    // user can put more information in user_data for auth, such as username, password...
    typename Conf::LoginUserData user_data;

    // below are all char types, no alignment requirement
    char use_shm;
    char client_name[Conf::NameSize];
    char last_server_name[Conf::NameSize];

    void ConvertByteOrder() {
        Endian<Conf::ToLittleEndian> ed;
        ed.ConvertInPlace(client_seq_start);
        ed.ConvertInPlace(client_seq_end);
    }
};

template<class Conf>
struct LoginRspMsgTpl
{
    static const uint16_t msg_type = 2;
    uint32_t server_seq_start;
    uint32_t server_seq_end;
    typename Conf::LoginRspUserData user_data;

    // below are all char types, no alignment requirement
    char status; // 0: OK, 1: seqnum mismatch, 2: other error
    char server_name[Conf::NameSize];
    char error_msg[32]; // empty error_msg means success

    void ConvertByteOrder() {
        Endian<Conf::ToLittleEndian> ed;
        ed.ConvertInPlace(server_seq_start);
        ed.ConvertInPlace(server_seq_end);
    }
};

// Single thread class except RequestClose()
template<class Conf>
class PTCPConnection
{
public:
    PTCPConnection() {
        hbmsg_.size = sizeof(MsgHeader);
        hbmsg_.msg_type = HeartbeatMsg::msg_type;
        hbmsg_.ack_seq = 0;
        hbmsg_.ConvertByteOrder<Conf::ToLittleEndian>();
    }

    ~PTCPConnection() {
        Release();
    }

    PTCPConnection(const PTCPConnection&) = delete;
    PTCPConnection& operator=(const PTCPConnection&) = delete;
    PTCPConnection(PTCPConnection&&) = delete;
    PTCPConnection& operator=(PTCPConnection&&) = delete;

    bool OpenFile(const char* ptcp_queue_file,
                  const char** error_msg) {
        if(!q_) {
            q_ = my_mmap<PTCPQ>(ptcp_queue_file, false, error_msg);
            if(!q_) return false;
        }
        return true;
    }

    bool GetSeq(uint32_t* local_ack_seq, uint32_t* local_seq_start, uint32_t* local_seq_end) {
        *local_ack_seq = q_->MyAck();
        return q_->SanityCheckAndGetSeq(local_seq_start, local_seq_end);
    }

    void Reset() {
        send_byte_off_ = 0;
        if(q_) q_->Reset();
    }

    void Release() {
        Close("Release", 0);
        TryCloseFd();
        send_byte_off_ = 0;
        if(q_) {
            my_munmap<PTCPQ>(q_);
            q_ = nullptr;
        }
    }

    // precondition: sockfd_ == fd_to_close_ == -1 (or INVALID_TCP_SOCKET)
    void Open(tcp_socket_t sock_fd, uint32_t remote_ack_seq, int64_t now) {
        close_reason_ = "nil";
        close_errno_ = 0;
        send_byte_off_ = 0;
        fd_to_close_ = sock_fd;
        sockfd_.store(sock_fd, std::memory_order_relaxed);
        close_claimed_.store(false, std::memory_order_release);
        writeidx_ = readidx_ = nextmsg_idx_ = 0;
        last_my_ack_ = q_ ? q_->MyAck() : 0;
        recv_time_ = send_time_ = now_ = now;
        if(recvbuf_size_ == 0) {
            recvbuf_size_ = Conf::TcpRecvBufInitSize;
            recvbuf_.reset(new char[recvbuf_size_]);
        }
        if(q_) {
            q_->LoginAck(remote_ack_seq);
            SendPending();
        }
    }

    MsgHeader* Alloc(uint16_t size) {
        if(!q_) return nullptr;
        return q_->Alloc(size);
    }

    void Push() {
        if(!q_) return;
        q_->Push();
        SendPending();
    }

    void PushMore() {
        if(!q_) return;
        q_->Push();
    }

    // safe if IsClosed
    MsgHeader* Front() {
        if(IsClosed()) return nullptr;
        if(UseShm()) { // for shm, we only expect HB in tcp channel so just read something and ignore
            DoRecv();
            return nullptr;
        }
        if(q_ && (send_byte_off_ > 0 || q_->HasSendable())) {
            SendPending();
            if(IsClosed()) return nullptr;
        }
        while(nextmsg_idx_ != readidx_) {
            MsgHeader* header = (MsgHeader*)&recvbuf_[readidx_];
            if(header->msg_type == HeartbeatMsg::msg_type) {
                readidx_ += sizeof(MsgHeader);
                continue;
            }
            // if user didn't pop last msg, we need to keep reading for updating ack_seq
            if(last_my_ack_ == q_->MyAck()) break;
            last_my_ack_ = q_->MyAck();
            return header;
        }

        if(int len = DoRecv()) {
            uint32_t old_writeidx = writeidx_;
            writeidx_ += static_cast<uint32_t>(len);
            while(writeidx_ - nextmsg_idx_ >= sizeof(MsgHeader)) {
                MsgHeader* header = (MsgHeader*)&recvbuf_[nextmsg_idx_];
                if(old_writeidx < nextmsg_idx_ + static_cast<uint32_t>(sizeof(MsgHeader))) { // we haven't converted this header
                    header->ConvertByteOrder<Conf::ToLittleEndian>();
                }
                if(header->size < sizeof(MsgHeader) ||
                   (header->msg_type == HeartbeatMsg::msg_type && header->size != sizeof(MsgHeader))) {
                    Close("Invalid msg header", 0);
                    return nullptr;
                }
                uint32_t msg_size = (static_cast<uint32_t>(header->size) + 7u) & ~7u;
                if(msg_size > Conf::TcpRecvBufMaxSize) {
                    Close("Msg size larger than recv buf max size", 0);
                    return nullptr;
                }
                if(!q_->Ack(header->ack_seq)) {
                    Close("Invalid ack_seq", 0);
                    return nullptr;
                }
                if(writeidx_ - nextmsg_idx_ < msg_size) break;
                // we have got a full msg
                if(header->msg_type == HeartbeatMsg::msg_type && readidx_ == nextmsg_idx_) {
                    readidx_ += msg_size;
                }
                nextmsg_idx_ += msg_size;
            }
        }
        if(readidx_ != nextmsg_idx_) {
            return (MsgHeader*)&recvbuf_[readidx_];
        }
        return nullptr;
    }

    // we have consumed the msg we got from Front()
    void Pop() {
        if(!q_ || readidx_ == nextmsg_idx_) return;
        MsgHeader* header = (MsgHeader*)&recvbuf_[readidx_];
        readidx_ += (static_cast<uint32_t>(header->size) + 7u) & ~7u;
        q_->MyAck()++;
    }

    // safe if IsClosed
    void SendHB(int64_t now) {
        tcp_socket_t fd = sockfd_.load(std::memory_order_acquire);
        if(fd == INVALID_TCP_SOCKET) return;
        now_ = now;
        if(q_ && (send_byte_off_ > 0 || q_->HasSendable())) {
            if(SendPending()) return;
        }
        if(now_ - send_time_ < Conf::HeartBeatInverval) return;
        if(q_) {
            hbmsg_.ack_seq = Endian<Conf::ToLittleEndian>::Convert(q_->MyAck());
        }
        int sent = tcp_send(fd, (const char*)&hbmsg_, sizeof(hbmsg_), TCP_MSG_NOSIGNAL);
        if(sent < 0 && tcp_is_retryable_err(tcp_get_last_error())) return;
        if(sent != (int)sizeof(MsgHeader)) { // for simplicity, we see partial sendout as error
            Close("Send error", sent < 0 ? tcp_get_last_error() : 0);
            return;
        }
        send_time_ = now_; // successfully sent
    }

    // return false only if no pending data to send (or closed)
    bool SendPending() {
        tcp_socket_t fd = sockfd_.load(std::memory_order_acquire);
        if(fd == INVALID_TCP_SOCKET || !q_) return false;
        int blk_sz;
        const char* base = reinterpret_cast<const char*>(q_->GetSendable(blk_sz));
        if(blk_sz <= 0) {
            send_byte_off_ = 0;
            return false;
        }
        uint32_t total_bytes = (static_cast<uint32_t>(blk_sz) << 3) - send_byte_off_;
        const char* p = base + send_byte_off_;
        uint32_t sent_total = 0;
        while(sent_total < total_bytes) {
            int sent = tcp_send(fd, p + sent_total, static_cast<int>(total_bytes - sent_total), TCP_MSG_NOSIGNAL);
            if(sent <= 0) {
                if(sent == 0) {
                    Close("Send error", 0);
                    return false;
                }
                int err = tcp_get_last_error();
                if(tcp_is_retryable_err(err)) {
                    break;
                }
                Close("Send error", err);
                return false;
            }
            sent_total += static_cast<uint32_t>(sent);
        }
        if(sent_total > 0) {
            send_time_ = now_;
            uint32_t total_consumed = static_cast<uint32_t>(send_byte_off_) + sent_total;
            uint32_t sent_blk = total_consumed >> 3;
            send_byte_off_ = static_cast<uint8_t>(total_consumed & 7u);
            if(sent_blk > 0) {
                q_->Sendout(static_cast<int>(sent_blk));
            }
        }
        return true;
    }

    bool IsClosed() const {
        return sockfd_.load(std::memory_order_acquire) == INVALID_TCP_SOCKET;
    }

    // not thread safe
    bool TryCloseFd() {
        if(sockfd_.load(std::memory_order_acquire) == INVALID_TCP_SOCKET && fd_to_close_ != INVALID_TCP_SOCKET) {
            tcp_close(fd_to_close_);
            fd_to_close_ = INVALID_TCP_SOCKET;
            return true;
        }
        return false;
    }

    const char* GetCloseReason(int* sys_errno) {
        *sys_errno = close_errno_;
        return close_reason_;
    }

    void RequestClose() {
        Close("Request close", 0);
    }

    bool UseShm() {
        return q_ == nullptr;
    }

private:
    // thread safe
    // need to call TryCloseFd to really close it
    void Close(const char* reason, int sys_errno) {
        if(close_claimed_.exchange(true, std::memory_order_acq_rel)) return;
        close_reason_ = reason;
        close_errno_ = sys_errno;
        sockfd_.store(INVALID_TCP_SOCKET, std::memory_order_release);
    }

    // For Windows: use WSARecv to simulate readv
    int DoRecv() {
        tcp_socket_t fd = sockfd_.load(std::memory_order_acquire);
        if(fd == INVALID_TCP_SOCKET) return 0;
        char stackbuf[65536];
        if(readidx_ > 0 && readidx_ == writeidx_) {
            readidx_ = nextmsg_idx_ = writeidx_ = 0;
        }
        uint32_t writable = recvbuf_size_ - writeidx_;
        // we should avoid buffer expansion
        // if total writable size is less than a half of recvbuf_size_, allow buffer expansion
        bool allow_expand = (writable + readidx_) * 2 < recvbuf_size_;
        uint32_t extra_size = std::min((uint32_t)sizeof(stackbuf),
                                       readidx_ + (allow_expand ? Conf::TcpRecvBufMaxSize - recvbuf_size_ : 0));
        if(writable + extra_size == 0) return 0;
        int ret;
        if(extra_size == 0){
            ret = tcp_recv(fd, &recvbuf_[writeidx_], static_cast<int>(writable), 0);
        }
        else {
            struct iovec vec[2];
            vec[0].iov_base = &recvbuf_[writeidx_];
            vec[0].iov_len = writable;
            vec[1].iov_base = stackbuf;
            vec[1].iov_len = extra_size;
            ret = tcp_readv(fd, vec);
        }
        if(ret <= 0) {
            if(ret < 0) {
                int err = tcp_get_last_error();
                if(tcp_is_retryable_err(err)) {
                    if(now_ - recv_time_ > Conf::ConnectionTimeout) {
                        Close("Timeout", 0);
                    }
                }
                else {
                    Close("Read error", err);
                }
            }
            else { // ret == 0;
                Close("Remote close", 0);
            }
            return 0;
        }
        recv_time_ = now_;
        if(ret <= static_cast<int>(writable)) return ret;
        uint32_t uret = static_cast<uint32_t>(ret);
        if(uret <= writable + readidx_) { // need to memmove
            memmove(&recvbuf_[0], &recvbuf_[readidx_], recvbuf_size_ - readidx_);
            memcpy(&recvbuf_[recvbuf_size_ - readidx_], stackbuf, uret - writable);
        }
        else { // need to expand buffer
            // newbufsize must be large enough to hold all data just read
            // and should be at least twice recvbuf_size_, but not larger than TcpRecvBufMaxSize
            uint32_t newbufsize = std::min(
                Conf::TcpRecvBufMaxSize,
                std::max(recvbuf_size_ * 2, (writeidx_ - readidx_ + uret + 7u) & ~7u));
            // std::cout << "expand: " << recvbuf_size_ << " -> " << newbufsize << std::endl;
            std::unique_ptr<char[]> new_buf(new char[newbufsize]);
            memcpy(&new_buf[0], &recvbuf_[readidx_], recvbuf_size_ - readidx_);
            memcpy(&new_buf[recvbuf_size_ - readidx_], stackbuf, uret - writable);
            recvbuf_size_ = newbufsize;
            std::swap(recvbuf_, new_buf);
        }
        writeidx_ -= readidx_; // let caller update writeidx_
        nextmsg_idx_ -= readidx_;
        readidx_ = 0;

        return ret;
    }

private:
    using PTCPQ = PTCPQueue<Conf::TcpQueueSize, Conf::ToLittleEndian>;
    PTCPQ* q_ = nullptr; // may be mmaped to file
    std::atomic<tcp_socket_t> sockfd_{INVALID_TCP_SOCKET};
    std::atomic<bool> close_claimed_{true};
    tcp_socket_t fd_to_close_ = INVALID_TCP_SOCKET;
    const char* close_reason_ = "nil";
    int close_errno_ = 0;
    static_assert(Conf::TcpRecvBufMaxSize >= Conf::TcpRecvBufInitSize, "Conf::TcpRecvBufMaxSize too small");
    static_assert((Conf::TcpRecvBufInitSize % 8) == 0, "Conf::TcpRecvBufInitSize must be a multiple of 8");
    static_assert((Conf::TcpRecvBufMaxSize % 8) == 0, "Conf::TcpRecvBufMaxSize must be a multiple of 8");
    std::unique_ptr<char[]> recvbuf_;
    uint32_t recvbuf_size_ = 0;
    uint32_t writeidx_ = 0;
    uint32_t nextmsg_idx_ = 0;
    uint32_t readidx_ = 0;
    int64_t recv_time_ = 0;
    int64_t send_time_ = 0;
    int64_t now_ = 0;
    MsgHeader hbmsg_;

    uint32_t last_my_ack_ = 0;
    uint8_t send_byte_off_ = 0;
};
} // namespace tcpshm
