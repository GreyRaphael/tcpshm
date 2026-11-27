#include "tcpshm/detail/os_shm.hpp"
#include "tcpshm/ptcp_queue.hpp"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#ifndef _WIN32
#include <unistd.h>
#endif

namespace {

using namespace tcpshm;

#define CHECK(cond)                                                                      \
    do {                                                                                 \
        if (!(cond)) {                                                                   \
            std::cerr << "CHECK failed at line " << __LINE__ << ": " #cond << std::endl; \
            std::exit(1);                                                                \
        }                                                                                \
    } while (0)

void test_ptcp_basic_alloc_ack_and_wrap() {
    auto q = std::make_unique<PtcpQueue<131072, std::endian::little>>();
    q->reset();

    // Overflow & capacity guards
    CHECK(!q->try_alloc(1, kMaxPayloadSize + 1u).has_value());
    CHECK(!q->try_alloc(1, 0xFFFFFFFFu).has_value());
    CHECK(!q->try_alloc(1, 131072u).has_value());
    {
        auto tx = q->try_alloc(1, 131072u - kFrameHeaderSize);
        CHECK(tx.has_value());
        tx->cancel();
    }
    CHECK(!q->has_sendable());

    // Commit 10 frames (16B header + 24B payload = 40B -> 3 x 16B blocks each)
    for (uint32_t i = 0; i < 10; ++i) {
        q->my_ack() = i;
        auto tx = q->try_alloc(static_cast<uint16_t>(i + 1), 24);
        CHECK(tx.has_value());
        tx->set_flags(static_cast<uint16_t>(100u + i));
        CHECK(tx->write_pod(i));
        tx->commit();
    }
    CHECK(q->has_sendable());
    uint32_t blks = 0;
    const std::byte* ptr = q->get_sendable(blks);
    CHECK(ptr != nullptr && blks == 10 * 3);
    q->on_sent_blocks(blks);
    CHECK(!q->has_sendable());

    auto seq_res = q->validate_and_get_seq();
    CHECK(seq_res.has_value());
    CHECK(seq_res->local_ack == 9);
    CHECK(seq_res->seq_start == 0);
    CHECK(seq_res->seq_end == 10);

    CHECK(q->ack(5));
    CHECK(!q->ack(11));
    CHECK(q->ack(10));
    auto seq_after = q->validate_and_get_seq();
    CHECK(seq_after.has_value() && seq_after->seq_start == 10 && seq_after->seq_end == 10);
}

void test_ptcp_seq_uint32_wraparound() {
    auto q = std::make_unique<PtcpQueue<4096, std::endian::big>>();
    q->reset();
    q->read_seq_num_ = 0xFFFFFFFDu;
    q->write_seq_num_ = 0xFFFFFFFDu;
    q->my_ack() = 0xFFFFFFFEu;

    for (uint32_t i = 0; i < 6; ++i) {
        auto tx = q->try_alloc(1, 16);
        CHECK(tx.has_value());
        tx->commit();
    }
    auto seq_res = q->validate_and_get_seq();
    CHECK(seq_res.has_value());
    CHECK(seq_res->seq_start == 0xFFFFFFFDu);
    CHECK(seq_res->seq_end == 3u);

    CHECK(q->ack(0xFFFFFFFFu));
    CHECK(q->ack(0u));
    CHECK(q->ack(3u));
    CHECK(!q->ack(4u));
}

void test_ptcp_mmap_persistence_and_corruption() {
    namespace fs = std::filesystem;
    std::string dir = (fs::temp_directory_path() / ("tcpshm_v2_ptcp_test_" + std::to_string(::getpid()))).string();
    fs::remove_all(dir);
    fs::create_directories(dir);
    std::string path = dir + "/peer.ptcp";

    using QSmall = PtcpQueue<1024, std::endian::little>;
    using QLarge = PtcpQueue<2048, std::endian::little>;

    // 1. Fresh mmap auto-initializes magic/version/capacity/header_size
    {
        auto map_res = detail::mmap_create_or_open<QSmall>(path.c_str(), false);
        CHECK(map_res.has_value());
        QSmall* q = *map_res;
        auto seq = q->validate_and_get_seq();
        CHECK(seq.has_value() && seq->seq_start == 0 && seq->seq_end == 0);
        for (uint32_t i = 0; i < 4; ++i) {
            auto tx = q->try_alloc(2, 16); // (16 + 16) / 16 = 2 blocks each
            CHECK(tx.has_value());
            tx->commit();
        }
        detail::munmap_region(q);
    }

    // 2. Reopen with same config recovers 4 unacked messages
    {
        auto map_res = detail::mmap_create_or_open<QSmall>(path.c_str(), false);
        CHECK(map_res.has_value());
        QSmall* q = *map_res;
        auto seq = q->validate_and_get_seq();
        CHECK(seq.has_value() && seq->seq_start == 0 && seq->seq_end == 4);
        q->login_ack(2);
        uint32_t blks = 0;
        (void)q->get_sendable(blks);
        CHECK(blks == 2 * 2); // 2 frames * 2 blocks
        detail::munmap_region(q);
    }

    // 3. Reopen with different header_size detects ConfigMismatch
    {
        auto map_res = detail::mmap_create_or_open<QSmall>(path.c_str(), false);
        CHECK(map_res.has_value());
        QSmall* q = *map_res;
        q->header_size_ = 8;
        auto seq = q->validate_and_get_seq();
        CHECK(!seq.has_value() && seq.error().code == ErrorCode::ConfigMismatch);
        q->header_size_ = QSmall::kHdrSz;
        detail::munmap_region(q);
    }

    // 4. Reopen with different capacity detects ConfigMismatch
    {
        auto map_res = detail::mmap_create_or_open<QLarge>(path.c_str(), false);
        CHECK(map_res.has_value());
        QLarge* q = *map_res;
        auto seq = q->validate_and_get_seq();
        CHECK(!seq.has_value() && seq.error().code == ErrorCode::ConfigMismatch);
        detail::munmap_region(q);
    }

    // 5. Corrupt magic detects CorruptPtcpFile
    {
        auto map_res = detail::mmap_create_or_open<QSmall>(path.c_str(), false);
        CHECK(map_res.has_value());
        QSmall* q = *map_res;
        q->magic_ = 0xDEADBEEFULL;
        auto seq = q->validate_and_get_seq();
        CHECK(!seq.has_value() && seq.error().code == ErrorCode::CorruptPtcpFile);
        detail::munmap_region(q);
    }

    // 6. Persistent >64 KiB frame survives mmap close/reopen
    {
        std::string large_path = dir + "/large.ptcp";
        using QBig = PtcpQueue<512u * 1024u, std::endian::little>;
        constexpr uint32_t kBigPayload = 128u * 1024u;
        {
            auto map_res = detail::mmap_create_or_open<QBig>(large_path.c_str(), false);
            CHECK(map_res.has_value());
            QBig* q = *map_res;
            CHECK(q->validate_and_get_seq().has_value());
            auto tx = q->try_alloc(5, kBigPayload);
            CHECK(tx.has_value());
            std::memset(tx->data(), 0x7E, kBigPayload);
            tx->commit();
            detail::munmap_region(q);
        }
        {
            auto map_res = detail::mmap_create_or_open<QBig>(large_path.c_str(), false);
            CHECK(map_res.has_value());
            QBig* q = *map_res;
            auto seq = q->validate_and_get_seq();
            CHECK(seq.has_value() && seq->seq_start == 0 && seq->seq_end == 1);
            uint32_t blks = 0;
            const std::byte* ptr = q->get_sendable(blks);
            CHECK(ptr != nullptr && blks == (kBigPayload + kFrameHeaderSize) / 16u);
            CHECK(q->ack(1));
            detail::munmap_region(q);
        }
    }

    fs::remove_all(dir);
}

} // namespace

int main() {
    test_ptcp_basic_alloc_ack_and_wrap();
    test_ptcp_seq_uint32_wraparound();
    test_ptcp_mmap_persistence_and_corruption();
    std::cout << "unit_ptcp_test passed\n";
    return 0;
}
