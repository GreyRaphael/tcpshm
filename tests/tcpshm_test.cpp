#include "tcpshm/tcpshm_server.h"
#include "tcpshm/tcpshm_client.h"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

namespace {

using namespace tcpshm;

#define TEST_CHECK(cond)                                                                          \
    do {                                                                                          \
        if (!(cond)) {                                                                            \
            std::cerr << "TEST_CHECK failed at line " << __LINE__ << ": " #cond << std::endl;     \
            std::exit(1);                                                                         \
        }                                                                                         \
    } while (0)

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct TestConf {
    static constexpr uint32_t NameSize = 16;
    static constexpr uint32_t ShmQueueSize = 4096;
    static constexpr bool ToLittleEndian = true;

    static constexpr uint32_t MaxNewConnections = 4;
    static constexpr uint32_t MaxShmConnsPerGrp = 4;
    static constexpr uint32_t MaxShmGrps = 1;
    static constexpr uint32_t MaxTcpConnsPerGrp = 4;
    static constexpr uint32_t MaxTcpGrps = 1;

    static constexpr uint32_t TcpQueueSize = 65536;
    static constexpr uint32_t TcpRecvBufInitSize = 1024;
    static constexpr uint32_t TcpRecvBufMaxSize = 4096;
    static constexpr bool TcpNoDelay = true;

    static constexpr int64_t NewConnectionTimeout = 2000000000LL;
    static constexpr int64_t ConnectionTimeout = 5000000000LL;
    static constexpr int64_t HeartBeatInverval = 3000000000LL;

    using LoginUserData = char;
    using LoginRspUserData = char;
    using ConnectionUserData = uint64_t;
};

struct CleanupGuard {
    std::string dir;
    std::vector<std::string> shm_names;

    ~CleanupGuard() {
        for (const auto& s : shm_names) {
            my_shm_unlink(s.c_str());
        }
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void track_pair(const std::string& a, const std::string& b) {
        shm_names.push_back("/" + a + "_" + b + ".shm");
        shm_names.push_back("/" + b + "_" + a + ".shm");
    }
};

uint16_t pick_unused_port() {
    tcp_socket_t s = tcp_socket(AF_INET, SOCK_STREAM, 0);
    TEST_CHECK(s != INVALID_TCP_SOCKET);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    addr.sin_port = 0;
    TEST_CHECK(tcp_bind(s, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    socklen_t len = sizeof(addr);
    TEST_CHECK(::getsockname(s, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0);
    uint16_t port = ntohs(addr.sin_port);
    tcp_close(s);
    return port;
}

void create_loopback_pair(tcp_socket_t sv[2]) {
    tcp_socket_t lst = tcp_socket(AF_INET, SOCK_STREAM, 0);
    TEST_CHECK(lst != INVALID_TCP_SOCKET);
    int yes = 1;
    tcp_setsockopt(lst, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&yes), sizeof(yes));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    addr.sin_port = 0;
    TEST_CHECK(tcp_bind(lst, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    TEST_CHECK(tcp_listen(lst, 1) == 0);
    socklen_t len = sizeof(addr);
    TEST_CHECK(::getsockname(lst, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0);

    sv[1] = tcp_socket(AF_INET, SOCK_STREAM, 0);
    TEST_CHECK(sv[1] != INVALID_TCP_SOCKET);
    TEST_CHECK(tcp_connect(sv[1], reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    sv[0] = tcp_accept(lst, nullptr, nullptr);
    TEST_CHECK(sv[0] != INVALID_TCP_SOCKET);
    tcp_close(lst);
}

void test_alloc_overflow() {
    auto sq = std::make_unique<SPSCVarQueue<131072>>();
    auto pq = std::make_unique<PTCPQueue<131072, true>>();
    for (uint32_t sz = 65528; sz <= 65535; ++sz) {
        TEST_CHECK(sq->Alloc(static_cast<uint16_t>(sz)) == nullptr);
        TEST_CHECK(pq->Alloc(static_cast<uint16_t>(sz)) == nullptr);
    }
    MsgHeader* s_ok = sq->Alloc(65527);
    MsgHeader* p_ok = pq->Alloc(65527);
    TEST_CHECK(s_ok != nullptr && s_ok->size == 65535);
    TEST_CHECK(p_ok != nullptr && p_ok->size == 65535);
}

void test_spsc_wrap_and_stress() {
    {
        auto q = std::make_unique<SPSCVarQueue<256>>();
        for (int i = 0; i < 3; ++i) {
            MsgHeader* h = q->Alloc(100);
            TEST_CHECK(h != nullptr);
            h->msg_type = 1;
            q->Push();
            MsgHeader* r = q->Front();
            TEST_CHECK(r != nullptr && r->size == 108);
            q->Pop();
        }
        // Advance to slot 3 (padding_sz = 1)
        MsgHeader* h_step = q->Alloc(40);
        TEST_CHECK(h_step != nullptr);
        h_step->msg_type = 5;
        q->Push();
        TEST_CHECK(q->Front() != nullptr);
        q->Pop();

        // Cancel rewind Alloc without Push, then do non-rewinding Alloc
        TEST_CHECK(q->Alloc(180) != nullptr);
        MsgHeader* h_small = q->Alloc(40);
        TEST_CHECK(h_small != nullptr);
        h_small->msg_type = 7;
        q->Push();
        MsgHeader* r_small = q->Front();
        TEST_CHECK(r_small != nullptr && r_small->msg_type == 7 && r_small->size == 48);
        q->Pop();

        // Advance to slot 3 again and trigger real rewind across ring end
        MsgHeader* h_pad = q->Alloc(180);
        TEST_CHECK(h_pad != nullptr);
        h_pad->msg_type = 8;
        q->Push();
        TEST_CHECK(q->Front() != nullptr);
        q->Pop();

        MsgHeader* h_wrap = q->Alloc(180);
        TEST_CHECK(h_wrap != nullptr);
        h_wrap->msg_type = 9;
        std::memset(h_wrap + 1, 0xAB, 180);
        q->Push();
        MsgHeader* r_wrap = q->Front();
        TEST_CHECK(r_wrap != nullptr && r_wrap->msg_type == 9 && r_wrap->size == 188);
        const unsigned char* body = reinterpret_cast<const unsigned char*>(r_wrap + 1);
        for (size_t k = 0; k < 180; ++k) TEST_CHECK(body[k] == 0xAB);
        q->Pop();
        TEST_CHECK(q->Front() == nullptr);
    }

    auto stress_q = std::make_unique<SPSCVarQueue<4096>>();
    constexpr uint64_t kCount = 20000;
    std::thread prod([&]() {
        for (uint64_t i = 0; i < kCount; ++i) {
            uint16_t payload = static_cast<uint16_t>(sizeof(uint64_t) + (i % 192));
            MsgHeader* h = nullptr;
            while ((h = stress_q->Alloc(payload)) == nullptr) {
                std::this_thread::yield();
            }
            h->msg_type = 1;
            std::memcpy(h + 1, &i, sizeof(i));
            stress_q->Push();
        }
    });
    std::thread cons([&]() {
        for (uint64_t i = 0; i < kCount; ++i) {
            MsgHeader* h = nullptr;
            while ((h = stress_q->Front()) == nullptr) {
                std::this_thread::yield();
            }
            uint64_t seq = 0;
            std::memcpy(&seq, h + 1, sizeof(seq));
            TEST_CHECK(seq == i);
            stress_q->Pop();
        }
    });
    prod.join();
    cons.join();
}

void test_ptcp_ack_and_sanity() {
    auto pq = std::make_unique<PTCPQueue<256, true>>();
    for (int i = 0; i < 2; ++i) {
        MsgHeader* h = pq->Alloc(8);
        TEST_CHECK(h != nullptr);
        h->msg_type = 1;
        pq->Push();
    }
    TEST_CHECK(!pq->Ack(3));
    TEST_CHECK(!pq->Ack(0x7fffffffu));
    TEST_CHECK(pq->Ack(0));
    TEST_CHECK(pq->Ack(1));
    uint32_t s = 0, e = 0;
    TEST_CHECK(pq->SanityCheckAndGetSeq(&s, &e) && s == 1 && e == 2);
    TEST_CHECK(pq->Ack(2));
    TEST_CHECK(pq->SanityCheckAndGetSeq(&s, &e) && s == 2 && e == 2);

    // Corrupt size==0 and out-of-bounds write_idx_
    uint32_t meta[6]{};
    auto write_meta = [&]() {
        std::memcpy(reinterpret_cast<char*>(pq.get()) + 256, meta, sizeof(meta));
    };
    meta[0] = 1; // write_idx_ = 1 with zeroed buf (size == 0)
    write_meta();
    TEST_CHECK(!pq->SanityCheckAndGetSeq(&s, &e));
    meta[0] = PTCPQueue<256, true>::BLK_CNT + 1;
    write_meta();
    TEST_CHECK(!pq->SanityCheckAndGetSeq(&s, &e));

    // Wrap-around across UINT32_MAX -> 0
    pq->Reset();
    std::memset(meta, 0, sizeof(meta));
    meta[3] = 0xFFFFFFFEu; // read_seq_num_
    meta[4] = 0xFFFFFFFEu; // write_seq_num_
    write_meta();
    for (int i = 0; i < 4; ++i) {
        MsgHeader* h = pq->Alloc(8);
        TEST_CHECK(h != nullptr);
        h->msg_type = 1;
        pq->Push();
    }
    TEST_CHECK(pq->Ack(1u));
    TEST_CHECK(pq->Ack(2u));
    TEST_CHECK(!pq->Ack(3u));
}

void test_ptcp_conn_front_reject(const std::string& dir) {
    struct Case { uint16_t sz; uint16_t type; };
    const Case cases[] = {{0, 1}, {4, 1}, {16, 0}, {static_cast<uint16_t>(TestConf::TcpRecvBufMaxSize + 8), 1}};
    for (size_t idx = 0; idx < sizeof(cases) / sizeof(cases[0]); ++idx) {
        tcp_socket_t sv[2]{INVALID_TCP_SOCKET, INVALID_TCP_SOCKET};
        create_loopback_pair(sv);
        tcp_set_nonblocking(sv[0]);
        tcp_set_nonblocking(sv[1]);

        PTCPConnection<TestConf> pc;
        const char* err = nullptr;
        std::string f = dir + "/front_" + std::to_string(idx) + ".ptcp";
        TEST_CHECK(pc.OpenFile(f.c_str(), &err));
        pc.Reset();
        MsgHeader* out = pc.Alloc(8);
        TEST_CHECK(out != nullptr);
        out->msg_type = 1;
        pc.Push();
        pc.Open(sv[0], 0, now_ns());
        // Drain sv[1]
        char tmp[128];
        while (tcp_recv(sv[1], tmp, sizeof(tmp), 0) > 0) {}

        MsgHeader bad{cases[idx].sz, cases[idx].type, 1};
        bad.ConvertByteOrder<TestConf::ToLittleEndian>();
        int se = 0;
        TEST_CHECK(tcp_send_all(sv[1], &bad, sizeof(bad), TCP_MSG_NOSIGNAL, &se));
        for (int tries = 0; tries < 50 && !pc.IsClosed(); ++tries) {
            TEST_CHECK(pc.Front() == nullptr);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        TEST_CHECK(pc.IsClosed());
        uint32_t ack = 99, st = 99, en = 99;
        TEST_CHECK(pc.GetSeq(&ack, &st, &en) && st == 0 && en == 1);
        pc.Release();
        tcp_close(sv[1]);
    }
}

void test_partial_eagain_send_recovery(const std::string& dir) {
    tcp_socket_t sv[2]{INVALID_TCP_SOCKET, INVALID_TCP_SOCKET};
    create_loopback_pair(sv);
    int buf_sz = 4096;
    tcp_setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&buf_sz), sizeof(buf_sz));
    tcp_setsockopt(sv[1], SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&buf_sz), sizeof(buf_sz));
    tcp_set_nonblocking(sv[0]);
    tcp_set_nonblocking(sv[1]);

    PTCPConnection<TestConf> pc;
    const char* err = nullptr;
    std::string f = dir + "/eagain.ptcp";
    TEST_CHECK(pc.OpenFile(f.c_str(), &err));
    pc.Reset();
    pc.Open(sv[0], 0, now_ns());

    constexpr uint32_t kMsgs = 240;
    constexpr uint16_t kPayload = 96; // 104 bytes per frame
    constexpr size_t kTotalBytes = kMsgs * (sizeof(MsgHeader) + kPayload);
    std::vector<unsigned char> recv_data;
    recv_data.reserve(kTotalBytes);

    uint32_t pushed = 0;
    auto start = std::chrono::steady_clock::now();
    while (recv_data.size() < kTotalBytes) {
        TEST_CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds(5));
        while (pushed < kMsgs) {
            MsgHeader* h = pc.Alloc(kPayload);
            if (!h) break;
            h->msg_type = 5;
            std::memset(h + 1, static_cast<int>(pushed & 0xff), kPayload);
            pc.Push();
            TEST_CHECK(!pc.IsClosed());
            pushed++;
        }
        TEST_CHECK(pc.Front() == nullptr);
        TEST_CHECK(!pc.IsClosed());
        char chunk[37]; // odd chunk size forces non-8B-aligned TCP window updates
        int r = tcp_recv(sv[1], chunk, sizeof(chunk), 0);
        if (r > 0) {
            recv_data.insert(recv_data.end(), reinterpret_cast<unsigned char*>(chunk),
                             reinterpret_cast<unsigned char*>(chunk) + r);
        }
        else {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    TEST_CHECK(pushed == kMsgs && recv_data.size() == kTotalBytes);
    TEST_CHECK(!pc.SendPending() && !pc.IsClosed());
    for (uint32_t i = 0; i < kMsgs; ++i) {
        MsgHeader hdr;
        std::memcpy(&hdr, &recv_data[i * 104], sizeof(hdr));
        hdr.ConvertByteOrder<TestConf::ToLittleEndian>();
        TEST_CHECK(hdr.size == 104 && hdr.msg_type == 5);
        TEST_CHECK(recv_data[i * 104 + sizeof(MsgHeader)] == static_cast<unsigned char>(i & 0xff));
    }
    pc.Release();
    tcp_close(sv[1]);
}

class TestServer : public TcpShmServer<TestServer, TestConf> {
public:
    using TcpShmServer::Start;
    using TcpShmServer::PollCtl;
    using TcpShmServer::PollTcp;
    using TcpShmServer::PollShm;
    using TcpShmServer::Stop;

    std::atomic<uint32_t> logon_cnt{0};
    std::atomic<uint32_t> disc_cnt{0};
    bool stop_on_disc = false;
    std::atomic<bool> stop_triggered{false};

    TestServer(const std::string& name, const std::string& dir) : TcpShmServer(name, dir) {}

    void OnSystemError(const char*, int) {}
    int OnNewConnection(const struct sockaddr_in&, const LoginMsg* login, LoginRspMsg*) {
        if (std::strcmp(login->client_name, "badgrp") == 0) return 99;
        return 0;
    }
    void OnClientLogon(const struct sockaddr_in&, Connection&) { logon_cnt.fetch_add(1, std::memory_order_relaxed); }
    void OnClientDisconnected(Connection&, const char*, int) {
        disc_cnt.fetch_add(1, std::memory_order_relaxed);
        if (stop_on_disc) {
            stop_triggered.store(true, std::memory_order_release);
            this->Stop();
        }
    }
    void OnClientMsg(Connection& conn, MsgHeader* h) {
        uint16_t body_sz = static_cast<uint16_t>(h->size - sizeof(MsgHeader));
        uint16_t mtype = h->msg_type;
        std::vector<char> copy(body_sz);
        if (body_sz > 0) std::memcpy(copy.data(), h + 1, body_sz);
        conn.Pop();
        MsgHeader* rsp = conn.Alloc(body_sz);
        if (rsp) {
            rsp->msg_type = mtype;
            if (body_sz > 0) std::memcpy(rsp + 1, copy.data(), body_sz);
            conn.Push();
        }
    }
    void OnClientFileError(Connection&, const char*, int) {}
    void OnSeqNumberMismatch(Connection&, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t) {}
};

class TestClient : public TcpShmClient<TestClient, TestConf> {
public:
    using TcpShmClient::Connect;
    using TcpShmClient::PollTcp;
    using TcpShmClient::PollShm;
    using TcpShmClient::Stop;
    using TcpShmClient::GetConnection;

    std::vector<std::string> recvd;
    uint32_t seq_mismatch_cnt = 0;
    std::string last_reject;

    TestClient(const std::string& name, const std::string& dir) : TcpShmClient(name, dir) {}
    void OnSystemError(const char*, int) {}
    void OnLoginReject(const LoginRspMsg* r) { last_reject = r->error_msg; }
    int64_t OnLoginSuccess(const LoginRspMsg*) { return now_ns(); }
    void OnServerMsg(MsgHeader* h) {
        size_t body = h->size - sizeof(MsgHeader);
        recvd.emplace_back(reinterpret_cast<const char*>(h + 1), body);
        GetConnection().Pop();
    }
    void OnDisconnected(const char*, int) {}
    void OnSeqNumberMismatch(uint32_t, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t) { ++seq_mismatch_cnt; }
};

TestServer::LoginRspMsg raw_login_roundtrip(uint16_t port, const char* cli_name, char use_shm, bool fragmented) {
    tcp_socket_t fd = tcp_socket(AF_INET, SOCK_STREAM, 0);
    TEST_CHECK(fd != INVALID_TCP_SOCKET);
    tcp_set_timeout(fd, SOL_SOCKET, SO_RCVTIMEO, 3000);
    tcp_set_timeout(fd, SOL_SOCKET, SO_SNDTIMEO, 3000);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    addr.sin_port = htons(port);
    TEST_CHECK(tcp_connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);

    MsgHeader sbuf[1 + (sizeof(TestServer::LoginMsg) + 7) / 8]{};
    sbuf[0].size = sizeof(MsgHeader) + sizeof(TestServer::LoginMsg);
    sbuf[0].msg_type = TestServer::LoginMsg::msg_type;
    auto* lm = reinterpret_cast<TestServer::LoginMsg*>(sbuf + 1);
    lm->use_shm = use_shm;
    tcp_copy_str(lm->client_name, sizeof(lm->client_name), cli_name);
    sbuf[0].ConvertByteOrder<TestConf::ToLittleEndian>();
    lm->ConvertByteOrder();

    int se = 0;
    if (fragmented) {
        const char* p = reinterpret_cast<const char*>(sbuf);
        size_t off = 0;
        for (size_t step : {3u, 7u, 11u}) {
            TEST_CHECK(tcp_send_all(fd, p + off, static_cast<int>(step), TCP_MSG_NOSIGNAL, &se));
            off += step;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        TEST_CHECK(tcp_send_all(fd, p + off, static_cast<int>(sizeof(sbuf) - off), TCP_MSG_NOSIGNAL, &se));
    }
    else {
        TEST_CHECK(tcp_send_all(fd, sbuf, sizeof(sbuf), TCP_MSG_NOSIGNAL, &se));
    }

    MsgHeader rbuf[1 + (sizeof(TestServer::LoginRspMsg) + 7) / 8]{};
    TEST_CHECK(tcp_recv_all(fd, rbuf, sizeof(rbuf), &se));
    rbuf[0].ConvertByteOrder<TestConf::ToLittleEndian>();
    auto* rsp = reinterpret_cast<TestServer::LoginRspMsg*>(rbuf + 1);
    rsp->ConvertByteOrder();
    tcp_close(fd);
    return *rsp;
}

void roundtrip_msg(TestClient& cli, bool use_shm, const std::string& payload) {
    size_t before = cli.recvd.size();
    MsgHeader* h = cli.GetConnection().Alloc(static_cast<uint16_t>(payload.size()));
    TEST_CHECK(h != nullptr);
    h->msg_type = 1;
    std::memcpy(h + 1, payload.data(), payload.size());
    cli.GetConnection().Push();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (cli.recvd.size() == before) {
        TEST_CHECK(std::chrono::steady_clock::now() < deadline);
        if (use_shm) cli.PollShm();
        cli.PollTcp(now_ns());
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    TEST_CHECK(cli.recvd.back() == payload);
}

void test_server_integration(const std::string& dir, CleanupGuard& guard) {
    uint32_t pid_tag = static_cast<uint32_t>(::getpid() % 10000);
    std::string srv_name = "srv" + std::to_string(pid_tag);
    uint16_t port = pick_unused_port();

    TestServer srv(srv_name, dir);
    TEST_CHECK(srv.Start("127.0.0.1", port));
    std::atomic<bool> stop_thr{false};
    std::thread t_ctl([&]() {
        while (!stop_thr.load(std::memory_order_acquire)) {
            srv.PollCtl(now_ns());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::thread t_tcp([&]() {
        while (!stop_thr.load(std::memory_order_acquire)) {
            srv.PollTcp(now_ns(), 0);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });
    std::thread t_shm([&]() {
        while (!stop_thr.load(std::memory_order_acquire)) {
            srv.PollShm(0);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    // Fragmented LoginMsg
    {
        auto rsp = raw_login_roundtrip(port, "drip_cli", 0, true);
        TEST_CHECK(rsp.status == 0);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (srv.disc_cnt.load(std::memory_order_acquire) < 1) {
            TEST_CHECK(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // Security rejections
    TEST_CHECK(raw_login_roundtrip(port, "bad..1", 0, false).status == 2);
    TEST_CHECK(raw_login_roundtrip(port, "a/b", 0, false).status == 2);
    TEST_CHECK(raw_login_roundtrip(port, srv_name.c_str(), 0, false).status == 2);
    TEST_CHECK(raw_login_roundtrip(port, "badgrp", 0, false).status == 2);

    // TCP round-trip + reconnect recovery + failed Connect(false)->Connect(true) retry + duplicate name rejection
    std::string c1_name = "c1_" + std::to_string(pid_tag);
    guard.track_pair(srv_name, c1_name);
    {
        TestClient cli(c1_name, dir);
        TEST_CHECK(cli.Connect(false, "127.0.0.1", port, 0));
        roundtrip_msg(cli, false, "tcp-one");
        roundtrip_msg(cli, false, "tcp-two");
        // Duplicate live client_name across SHM while TCP is live
        auto dup_rsp = raw_login_roundtrip(port, c1_name.c_str(), 1, false);
        TEST_CHECK(dup_rsp.status == 2);
        cli.Stop();

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (srv.disc_cnt.load(std::memory_order_acquire) < 2) {
            TEST_CHECK(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // TCP reconnect recovery
        TEST_CHECK(cli.Connect(false, "127.0.0.1", port, 0));
        TEST_CHECK(cli.seq_mismatch_cnt == 0);
        roundtrip_msg(cli, false, "tcp-three");
        cli.Stop();

        deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (srv.disc_cnt.load(std::memory_order_acquire) < 3) {
            TEST_CHECK(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Failed Connect(false) followed by Connect(true) SHM retry
        uint16_t dead_port = pick_unused_port();
        TEST_CHECK(!cli.Connect(false, "127.0.0.1", dead_port, 0));
        TEST_CHECK(cli.Connect(true, "127.0.0.1", port, 0));
        roundtrip_msg(cli, true, "shm-after-tcp-fail");
        cli.Stop();
    }

    // Full SHM probe + SHM round-trip
    std::string c2_name = "c2_" + std::to_string(pid_tag);
    guard.track_pair(srv_name, c2_name);
    {
        TestClient cli2(c2_name, dir);
        TEST_CHECK(cli2.Connect(true, "127.0.0.1", port, 0));
        for (int i = 0; i < 10; ++i) {
            roundtrip_msg(cli2, true, "shm-msg-" + std::to_string(i));
        }
        cli2.Stop();
    }

    stop_thr.store(true, std::memory_order_release);
    t_ctl.join();
    t_tcp.join();
    t_shm.join();
    srv.Stop();

    // OnClientDisconnected calling Stop() reentrancy test
    std::string r_srv_name = "rs" + std::to_string(pid_tag);
    std::string rc1 = "rc1_" + std::to_string(pid_tag);
    std::string rc2 = "rc2_" + std::to_string(pid_tag);
    guard.track_pair(r_srv_name, rc1);
    guard.track_pair(r_srv_name, rc2);
    uint16_t rport = pick_unused_port();
    TestServer rsrv(r_srv_name, dir);
    rsrv.stop_on_disc = true;
    TEST_CHECK(rsrv.Start("127.0.0.1", rport));

    std::atomic<bool> rstop{false};
    std::thread rt_ctl([&]() {
        while (!rstop.load(std::memory_order_acquire)) {
            rsrv.PollCtl(now_ns());
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::thread rt_shm([&]() {
        while (!rstop.load(std::memory_order_acquire)) {
            rsrv.PollShm(0);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    TestClient rcli1(rc1, dir), rcli2(rc2, dir);
    TEST_CHECK(rcli1.Connect(true, "127.0.0.1", rport, 0));
    TEST_CHECK(rcli2.Connect(true, "127.0.0.1", rport, 0));
    roundtrip_msg(rcli2, true, "before-reentrant-stop");
    rcli1.Stop();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!rsrv.stop_triggered.load(std::memory_order_acquire)) {
        TEST_CHECK(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    rstop.store(true, std::memory_order_release);
    rt_ctl.join();
    rt_shm.join();
    rcli2.Stop();
    rsrv.Stop();
}

} // namespace

int main() {
    std::string dir = (std::filesystem::temp_directory_path() / ("tcpshm_test_" + std::to_string(::getpid()))).string();
    CleanupGuard guard{dir, {}};
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    test_alloc_overflow();
    test_spsc_wrap_and_stress();
    test_ptcp_ack_and_sanity();
    test_ptcp_conn_front_reject(dir);
    test_partial_eagain_send_recovery(dir);
    test_server_integration(dir, guard);

    std::cout << "All tcpshm_test checks passed." << std::endl;
    return 0;
}
