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
#include <string>
#include <array>
#include "os.h"
#include "tcpshm_conn.h"

namespace tcpshm {

template<class Derived, class Conf>
class TcpShmClient
{
public:
    using Connection = TcpShmConnection<Conf>;
    using LoginMsg = LoginMsgTpl<Conf>;
    using LoginRspMsg = LoginRspMsgTpl<Conf>;

protected:
    TcpShmClient(const std::string& client_name, const std::string& ptcp_dir)
        : ptcp_dir_(ptcp_dir) {
        strncpy(client_name_, client_name.c_str(), sizeof(client_name_) - 1);
        tcp_mkdir(ptcp_dir_.c_str());
        client_name_[sizeof(client_name_) - 1] = 0;
        conn_.init(ptcp_dir.c_str(), client_name_);
    }

    ~TcpShmClient() {
        Stop();
    }

    // connect and login to server, may block for a short time
    // return true if success
    bool Connect(bool use_shm,
                 const char* server_ipv4,
                 uint16_t server_port,
                 const typename Conf::LoginUserData& login_user_data) {
        if(!conn_.IsClosed()) {
            static_cast<Derived*>(this)->OnSystemError("already connected", 0);
            return false;
        }
        conn_.TryCloseFd();
        const char* error_msg;
        if(!server_name_) {
            std::string last_server_name_file = std::string(ptcp_dir_) + "/" + client_name_ + ".lastserver";
            server_name_ = (char*)my_mmap<ServerName>(last_server_name_file.c_str(), false, &error_msg);
            if(!server_name_) {
                static_cast<Derived*>(this)->OnSystemError(error_msg, tcp_get_last_error());
                return false;
            }
            strncpy(conn_.GetRemoteName(), server_name_, sizeof(ServerName));
        }
        MsgHeader sendbuf[1 + (sizeof(LoginMsg) + 7) / 8];
        sendbuf[0].size = sizeof(MsgHeader) + sizeof(LoginMsg);
        sendbuf[0].msg_type = LoginMsg::msg_type;
        sendbuf[0].ack_seq = 0;
        LoginMsg* login = (LoginMsg*)(sendbuf + 1);
        strncpy(login->client_name, client_name_, sizeof(login->client_name));
        strncpy(login->last_server_name, server_name_, sizeof(login->last_server_name));
        login->use_shm = use_shm;
        login->client_seq_start = login->client_seq_end = 0;
        login->user_data = login_user_data;
        bool opened_conn_file = false;
        if(server_name_[0] && !use_shm &&
           (!conn_.OpenFile(use_shm, &error_msg) ||
            !conn_.GetSeq(&sendbuf[0].ack_seq, &login->client_seq_start, &login->client_seq_end, &error_msg))) {
            static_cast<Derived*>(this)->OnSystemError(error_msg, tcp_get_last_error());
            return false;
        }
        opened_conn_file = server_name_[0] && !use_shm;
        tcp_socket_t fd;
        if((fd = tcp_socket(AF_INET, SOCK_STREAM, 0)) == INVALID_TCP_SOCKET) {
            static_cast<Derived*>(this)->OnSystemError("socket", tcp_get_last_error());
            return false;
        }
        if(tcp_set_timeout(fd, SOL_SOCKET, SO_RCVTIMEO, 10000) < 0) {
            static_cast<Derived*>(this)->OnSystemError("tcp_set_timeout SO_RCVTIMEO", tcp_get_last_error());
            tcp_close(fd);
            return false;
        }

        if(tcp_set_timeout(fd, SOL_SOCKET, SO_SNDTIMEO, 10000) < 0) {
            static_cast<Derived*>(this)->OnSystemError("tcp_set_timeout SO_SNDTIMEO", tcp_get_last_error());
            tcp_close(fd);
            return false;
        }
        int yes = 1;
        if(Conf::TcpNoDelay && tcp_setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&yes, sizeof(yes)) < 0) {
            static_cast<Derived*>(this)->OnSystemError("setsockopt TCP_NODELAY", tcp_get_last_error());
            tcp_close(fd);
            return false;
        }

        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        inet_pton(AF_INET, server_ipv4, &(server_addr.sin_addr));
        server_addr.sin_port = htons(server_port);

        if(tcp_connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            static_cast<Derived*>(this)->OnSystemError("connect", tcp_get_last_error());
            tcp_close(fd);
            return false;
        }

        sendbuf[0].template ConvertByteOrder<Conf::ToLittleEndian>();
        login->ConvertByteOrder();
        int sys_errno = 0;
        if(!tcp_send_all(fd, sendbuf, sizeof(sendbuf), TCP_MSG_NOSIGNAL, &sys_errno)) {
            static_cast<Derived*>(this)->OnSystemError("send", sys_errno);
            tcp_close(fd);
            return false;
        }

        MsgHeader recvbuf[1 + (sizeof(LoginRspMsg) + 7) / 8];
        const char* recv_error = nullptr;
        if(!RecvLoginRsp(fd, recvbuf, &recv_error, &sys_errno)) {
            static_cast<Derived*>(this)->OnSystemError(recv_error, sys_errno);
            tcp_close(fd);
            return false;
        }
        LoginRspMsg* login_rsp = (LoginRspMsg*)(recvbuf + 1);
        if(recvbuf[0].size != sizeof(MsgHeader) + sizeof(LoginRspMsg) || recvbuf[0].msg_type != LoginRspMsg::msg_type ||
           login_rsp->server_name[0] == 0) {
            static_cast<Derived*>(this)->OnSystemError("Invalid LoginRsp", 0);
            tcp_close(fd);
            return false;
        }
        if(login_rsp->status != 0) {
            if(login_rsp->status == 1) { // seq number mismatch
                sendbuf[0].template ConvertByteOrder<Conf::ToLittleEndian>();
                login->ConvertByteOrder();
                static_cast<Derived*>(this)->OnSeqNumberMismatch(sendbuf[0].ack_seq,
                                                                 login->client_seq_start,
                                                                 login->client_seq_end,
                                                                 recvbuf[0].ack_seq,
                                                                 login_rsp->server_seq_start,
                                                                 login_rsp->server_seq_end);
            }
            else {
                static_cast<Derived*>(this)->OnLoginReject(login_rsp);
            }
            tcp_close(fd);
            return false;
        }
        login_rsp->server_name[sizeof(login_rsp->server_name) - 1] = 0;
        // check if server name has changed
        bool server_name_changed = strncmp(server_name_, login_rsp->server_name, sizeof(ServerName)) != 0;
        if(server_name_changed) {
            conn_.Release();
            strncpy(server_name_, login_rsp->server_name, sizeof(ServerName));
            strncpy(conn_.GetRemoteName(), server_name_, sizeof(ServerName));
            opened_conn_file = false;
        }
        if(!opened_conn_file) {
            if(!conn_.OpenFile(use_shm, &error_msg)) {
                static_cast<Derived*>(this)->OnSystemError(error_msg, tcp_get_last_error());
                tcp_close(fd);
                return false;
            }
        }
        if(server_name_changed) {
            conn_.Reset();
        }
        tcp_set_nonblocking(fd);
        int64_t now = static_cast<Derived*>(this)->OnLoginSuccess(login_rsp);

        conn_.Open(fd, recvbuf[0].ack_seq, now);
        return true;
    }

    // we need to PollTcp even if using shm
    void PollTcp(int64_t now) {
        if(!conn_.IsClosed()) {
            MsgHeader* head = conn_.TcpFront(now);
            if(head) static_cast<Derived*>(this)->OnServerMsg(head);
        }
        if(conn_.TryCloseFd()) {
            int sys_errno;
            const char* reason = conn_.GetCloseReason(&sys_errno);
            static_cast<Derived*>(this)->OnDisconnected(reason, sys_errno);
        }
    }

    // only for using shm
    void PollShm() {
        MsgHeader* head = conn_.ShmFront();
        if(head) static_cast<Derived*>(this)->OnServerMsg(head);
    }

    // stop the connection and close files
    void Stop() {
        if(server_name_) {
            my_munmap<ServerName>(server_name_);
            server_name_ = nullptr;
        }
        conn_.Release();
    }

    // get the connection reference which can be kept by user as long as TcpShmClient is not destructed
    Connection& GetConnection() {
        return conn_;
    }

private:
    static bool RecvLoginRsp(tcp_socket_t fd,
                             MsgHeader* recvbuf,
                             const char** error_msg,
                             int* sys_errno) {
        MsgHeader header;
        if(!RecvHeader(fd, &header, error_msg, sys_errno)) return false;

        if(header.msg_type == ShmProbeReqMsg::msg_type) {
            if(!HandleShmProbe(fd, header, error_msg, sys_errno)) return false;
            if(!RecvHeader(fd, &header, error_msg, sys_errno)) return false;
        }

        if(header.size != sizeof(MsgHeader) + sizeof(LoginRspMsg) ||
           header.msg_type != LoginRspMsg::msg_type) {
            *error_msg = "Invalid LoginRsp";
            if(sys_errno) *sys_errno = 0;
            return false;
        }

        recvbuf[0] = header;
        constexpr int BodyBufSize = ((sizeof(LoginRspMsg) + 7) / 8) * 8;
        if(!tcp_recv_all(fd, recvbuf + 1, BodyBufSize, sys_errno)) {
            *error_msg = "recv";
            return false;
        }
        LoginRspMsg* login_rsp = (LoginRspMsg*)(recvbuf + 1);
        login_rsp->ConvertByteOrder();
        return true;
    }

    static bool RecvHeader(tcp_socket_t fd,
                           MsgHeader* header,
                           const char** error_msg,
                           int* sys_errno) {
        if(!tcp_recv_all(fd, header, sizeof(*header), sys_errno)) {
            *error_msg = "recv";
            return false;
        }
        header->template ConvertByteOrder<Conf::ToLittleEndian>();
        return true;
    }

    static bool HandleShmProbe(tcp_socket_t fd,
                               const MsgHeader& header,
                               const char** error_msg,
                               int* sys_errno) {
        if(header.size != sizeof(MsgHeader) + sizeof(ShmProbeReqMsg)) {
            *error_msg = "Invalid ShmProbeReq";
            if(sys_errno) *sys_errno = 0;
            return false;
        }

        MsgHeader probebuf[1 + (sizeof(ShmProbeReqMsg) + 7) / 8];
        probebuf[0] = header;
        constexpr int ProbeBodyBufSize = ((sizeof(ShmProbeReqMsg) + 7) / 8) * 8;
        if(!tcp_recv_all(fd, probebuf + 1, ProbeBodyBufSize, sys_errno)) {
            *error_msg = "recv";
            return false;
        }

        ShmProbeReqMsg* req = (ShmProbeReqMsg*)(probebuf + 1);
        req->template ConvertByteOrder<Conf::ToLittleEndian>();
        req->shm_name[ShmProbeNameSize - 1] = 0;

        uint64_t ack = 0;
        char ok = 0;
        const char* mmap_error = nullptr;
        ShmProbeData* probe = my_mmap_existing<ShmProbeData>(req->shm_name, true, &mmap_error);
        if(probe) {
            if(probe->token == req->token) {
                ack = req->token ^ ShmProbeAckMask;
                tcp_volatile_write(probe->ack, ack);
                ok = 1;
            }
            my_munmap<ShmProbeData>(probe);
        }

        MsgHeader rspbuf[1 + (sizeof(ShmProbeRspMsg) + 7) / 8]{};
        rspbuf[0].size = sizeof(MsgHeader) + sizeof(ShmProbeRspMsg);
        rspbuf[0].msg_type = ShmProbeRspMsg::msg_type;
        rspbuf[0].ack_seq = 0;
        ShmProbeRspMsg* rsp = (ShmProbeRspMsg*)(rspbuf + 1);
        rsp->ack = ack;
        rsp->ok = ok;
        rspbuf[0].template ConvertByteOrder<Conf::ToLittleEndian>();
        rsp->template ConvertByteOrder<Conf::ToLittleEndian>();
        if(!tcp_send_all(fd, rspbuf, sizeof(rspbuf), TCP_MSG_NOSIGNAL, sys_errno)) {
            *error_msg = "send";
            return false;
        }
        return true;
    }

    char client_name_[Conf::NameSize];
    using ServerName = std::array<char, Conf::NameSize>;
    char* server_name_ = nullptr;
    std::string ptcp_dir_;
    Connection conn_;
};
} // namespace tcpshm
