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

    // Overflow guard
    for (uint32_t sz = kMaxPayloadSize + 1u; sz <= 65535u; ++sz) {
        CHECK(!q->try_alloc(1, static_cast<uint16_t>(sz)).has_value());
    }
    {
        auto tx = q->try_alloc(1, kMaxPayloadSize);
        CHECK(tx.has_value());
        tx->cancel();
    }
    CHECK(!q->has_sendable());

    // Commit 10 frames and verify sendable + ack progression
    for (uint32_t i = 0; i < 10; ++i) {
        q->my_ack() = i;
        auto tx = q->try_alloc(static_cast<uint16_t>(i + 1), 24);
        CHECK(tx.has_value());
        CHECK(tx->write_pod(i));
        tx->commit();
    }
    CHECK(q->has_sendable());
    uint32_t blks = 0;
    const std::byte* ptr = q->get_sendable(blks);
    CHECK(ptr != nullptr && blks == 10 * 4); // (8 + 24) / 8 == 4 blocks each
    q->on_sent_blocks(blks);
    CHECK(!q->has_sendable());

    auto seq_res = q->validate_and_get_seq();
    CHECK(seq_res.has_value());
    CHECK(seq_res->local_ack == 9);
    CHECK(seq_res->seq_start == 0);
    CHECK(seq_res->seq_end == 10);

    // Partial ack & invalid future ack
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

    // Ack across uint32 wrap boundary
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

    // 1. Fresh mmap auto-initializes magic/version/capacity
    {
        auto map_res = detail::mmap_create_or_open<QSmall>(path.c_str(), false);
        CHECK(map_res.has_value());
        QSmall* q = *map_res;
        auto seq = q->validate_and_get_seq();
        CHECK(seq.has_value() && seq->seq_start == 0 && seq->seq_end == 0);
        for (uint32_t i = 0; i < 4; ++i) {
            auto tx = q->try_alloc(2, 16);
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
        // Login ack of 2 rewinds send_idx to read_idx so messages 2,3 are resendable
        q->login_ack(2);
        uint32_t blks = 0;
        (void)q->get_sendable(blks);
        CHECK(blks == 2 * 3); // 2 frames * ((8+16)/8 = 3 blocks)
        detail::munmap_region(q);
    }

    // 3. Reopen with different capacity detects ConfigMismatch
    {
        auto map_res = detail::mmap_create_or_open<QLarge>(path.c_str(), false);
        CHECK(map_res.has_value());
        QLarge* q = *map_res;
        auto seq = q->validate_and_get_seq();
        CHECK(!seq.has_value() && seq.error().code == ErrorCode::ConfigMismatch);
        detail::munmap_region(q);
    }

    // 4. Corrupt magic detects CorruptPtcpFile
    {
        auto map_res = detail::mmap_create_or_open<QSmall>(path.c_str(), false);
        CHECK(map_res.has_value());
        QSmall* q = *map_res;
        q->magic_ = 0xDEADBEEFULL;
        auto seq = q->validate_and_get_seq();
        CHECK(!seq.has_value() && seq.error().code == ErrorCode::CorruptPtcpFile);
        detail::munmap_region(q);
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
