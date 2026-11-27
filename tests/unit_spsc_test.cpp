#include "tcpshm/spsc_queue.hpp"
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace tcpshm;

#define CHECK(cond)                                                                      \
    do {                                                                                 \
        if (!(cond)) {                                                                   \
            std::cerr << "CHECK failed at line " << __LINE__ << ": " #cond << std::endl; \
            std::exit(1);                                                                \
        }                                                                                \
    } while (0)

struct alignas(16) Vec16
{
    uint64_t hi = 0;
    uint64_t lo = 0;
};
static_assert(alignof(Vec16) == 16 && std::is_trivially_copyable_v<Vec16>);

void test_payload_limit_and_guard_raii() {
    auto q = std::make_unique<SpscQueue<131072>>();
    CHECK(!q->try_alloc(1, kMaxPayloadSize + 1u).has_value());
    CHECK(!q->try_alloc(1, 0xFFFFFFFFu).has_value());
    CHECK(!q->try_alloc(1, 131072u).has_value()); // exceeds ring capacity once 16B header is added

    // Uncommitted WriteGuard cancels automatically on scope exit
    {
        auto tx = q->try_alloc(3, 131072u - kFrameHeaderSize);
        CHECK(tx.has_value());
        CHECK(tx->size() == 131072u - kFrameHeaderSize);
    }
    CHECK(!q->try_front().has_value());

    // Committed WriteGuard is readable (including flags/meta/reserved) and ReadGuard auto-pops
    {
        auto tx = q->try_alloc(42, sizeof(Vec16));
        CHECK(tx.has_value());
        tx->set_flags(0x1234u);
        tx->set_meta(12345u);
        tx->set_reserved(0xABCDEF01u);
        auto* v = tx->emplace<Vec16>(0x1122334455667788ULL, 0x99AABBCCDDEEFF00ULL);
        CHECK(v != nullptr);
        tx->commit();
    }
    {
        auto rx = q->try_front();
        CHECK(rx.has_value());
        CHECK(rx->type() == 42);
        CHECK(rx->flags() == 0x1234u);
        CHECK(rx->meta() == 12345u);
        CHECK(rx->reserved() == 0xABCDEF01u);
        CHECK(rx->payload_size() == sizeof(Vec16));
        const Vec16* ptr = rx->view().as_ptr<Vec16>();
        CHECK(ptr != nullptr);
        CHECK(reinterpret_cast<std::uintptr_t>(ptr) % 16u == 0u);
        CHECK(ptr->hi == 0x1122334455667788ULL && ptr->lo == 0x99AABBCCDDEEFF00ULL);
        rx->defer();
    }
    {
        auto rx = q->try_front();
        CHECK(rx.has_value() && rx->type() == 42);
    }
    CHECK(!q->try_front().has_value());
}

void test_wrap_cancel_and_blk_sizes() {
    auto run_for_blk = []<uint32_t BlkSz>() {
        auto q = std::make_unique<SpscQueue<512, BlkSz>>();
        // Advance write cursor close to ring end (400B out of 512B -> remaining padding < 196B for BlkSz=16/32/64)
        {
            auto tx = q->try_alloc(1, 384);
            CHECK(tx.has_value());
            tx->commit();
            auto rx = q->try_front();
            CHECK(rx.has_value() && rx->payload_size() == 384);
        }
        // Trigger rewind alloc (196B > remaining padding), then cancel without committing
        {
            auto tx_cancel = q->try_alloc(2, 180);
            CHECK(tx_cancel.has_value());
            tx_cancel->cancel();
        }
        // Small message fits in remaining space before wrap
        {
            auto tx_small = q->try_alloc(7, 20);
            CHECK(tx_small.has_value());
            tx_small->write_str("hello_wrap_cancel_ok");
            tx_small->commit();
        }
        {
            auto rx = q->try_front();
            CHECK(rx.has_value() && rx->type() == 7 && rx->payload_str() == "hello_wrap_cancel_ok");
        }
        // Consume filler or advance to near end and verify real rewind commit
        {
            auto tx_pad = q->try_alloc(8, 300);
            CHECK(tx_pad.has_value());
            tx_pad->commit();
            auto rx_pad = q->try_front();
            CHECK(rx_pad.has_value() && rx_pad->type() == 8);
        }
        {
            auto tx_wrap = q->try_alloc(9, 180);
            CHECK(tx_wrap.has_value());
            std::memset(tx_wrap->data(), 0xCD, 180);
            tx_wrap->commit();
        }
        {
            auto rx = q->try_front();
            CHECK(rx.has_value() && rx->type() == 9 && rx->payload_size() == 180);
            for (std::byte b : rx->payload()) {
                CHECK(b == std::byte{0xCD});
            }
        }
        CHECK(!q->try_front().has_value());
    };

    run_for_blk.template operator()<16>();
    run_for_blk.template operator()<32>();
    run_for_blk.template operator()<64>();
}

void test_large_over_64k_frame() {
    auto q = std::make_unique<SpscQueue<512u * 1024u, 64>>();
    constexpr uint32_t kLargeSize = 128u * 1024u; // 128 KiB (> 64 KiB)
    for (int iter = 0; iter < 5; ++iter) {
        auto tx = q->try_alloc(77, kLargeSize);
        CHECK(tx.has_value());
        tx->set_flags(static_cast<uint16_t>(iter + 1));
        std::memset(tx->data(), 0x5A + iter, kLargeSize);
        tx->commit();

        auto rx = q->try_front();
        CHECK(rx.has_value());
        CHECK(rx->type() == 77);
        CHECK(rx->flags() == static_cast<uint16_t>(iter + 1));
        CHECK(rx->payload_size() == kLargeSize);
        CHECK(rx->view().data()[0] == std::byte{static_cast<uint8_t>(0x5A + iter)});
        CHECK(rx->view().data()[kLargeSize - 1] == std::byte{static_cast<uint8_t>(0x5A + iter)});
    }
    CHECK(!q->try_front().has_value());
}

void test_batch_stage_flush_and_drain_backpressure() {
    auto q = std::make_unique<SpscQueue<4096, 32>>();

    for (uint32_t i = 0; i < 20; ++i) {
        auto tx = q->try_alloc(10, sizeof(uint32_t));
        CHECK(tx.has_value());
        CHECK(tx->write_pod(i));
        tx->stage();
    }
    CHECK(q->producer().has_staged());
    CHECK(q->drain([](MsgView) {}, 64) == 0);

    q->flush();
    CHECK(!q->producer().has_staged());

    std::vector<uint32_t> received;
    uint32_t n1 = q->drain(
        [&](MsgView msg) -> bool {
            auto val = msg.read_as<uint32_t>();
            CHECK(val.has_value());
            if (*val == 7u) return false;
            received.push_back(*val);
            return true;
        },
        64);
    CHECK(n1 == 7);
    CHECK(received.size() == 7);

    uint32_t n2 = q->drain(
        [&](MsgView msg) {
            auto val = msg.read_as<uint32_t>();
            CHECK(val.has_value());
            received.push_back(*val);
        },
        64);
    CHECK(n2 == 13);
    CHECK(received.size() == 20);
    for (uint32_t i = 0; i < 20; ++i) {
        CHECK(received[i] == i);
    }
    CHECK(q->drain([](MsgView) {}, 64) == 0);
}

void test_multithreaded_spsc_batch_stress() {
    auto q = std::make_unique<SpscQueue<4096, 64>>();
    constexpr uint64_t kTotal = 40000;
    constexpr uint32_t kBatch = 16;

    std::thread producer([&]() {
        uint64_t sent = 0;
        while (sent < kTotal) {
            uint32_t in_batch = 0;
            while (sent < kTotal && in_batch < kBatch) {
                uint32_t sz = static_cast<uint32_t>(sizeof(uint64_t) + (sent % 160));
                auto tx = q->try_alloc(1, sz);
                if (!tx) {
                    q->flush();
                    std::this_thread::yield();
                    continue;
                }
                CHECK(tx->write_pod(sent));
                if (sz > sizeof(uint64_t)) {
                    std::memset(tx->data() + sizeof(uint64_t), static_cast<int>(sent & 0xFFu), sz - sizeof(uint64_t));
                }
                tx->stage();
                ++sent;
                ++in_batch;
            }
            q->flush();
        }
    });

    std::thread consumer([&]() {
        uint64_t expected = 0;
        while (expected < kTotal) {
            uint32_t n = q->drain(
                [&](MsgView msg) {
                    uint32_t expected_sz = static_cast<uint32_t>(sizeof(uint64_t) + (expected % 160));
                    CHECK(msg.type() == 1);
                    CHECK(msg.payload_size() == expected_sz);
                    uint64_t seq = 0;
                    std::memcpy(&seq, msg.data(), sizeof(seq));
                    CHECK(seq == expected);
                    if (expected_sz > sizeof(uint64_t)) {
                        CHECK(static_cast<uint8_t>(msg.data()[expected_sz - 1]) == static_cast<uint8_t>(expected & 0xFFu));
                    }
                    ++expected;
                },
                32);
            if (n == 0) std::this_thread::yield();
        }
    });

    producer.join();
    consumer.join();
    CHECK(!q->try_front().has_value());
}

} // namespace

int main() {
    test_payload_limit_and_guard_raii();
    test_wrap_cancel_and_blk_sizes();
    test_large_over_64k_frame();
    test_batch_stage_flush_and_drain_backpressure();
    test_multithreaded_spsc_batch_stress();
    std::cout << "unit_spsc_test passed\n";
    return 0;
}
