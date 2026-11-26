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

void test_payload_limit_and_guard_raii() {
    auto q = std::make_unique<SpscQueue<131072>>();
    for (uint32_t sz = kMaxPayloadSize + 1u; sz <= 65535u; ++sz) {
        CHECK(!q->try_alloc(1, static_cast<uint16_t>(sz)).has_value());
    }
    // Uncommitted WriteGuard cancels automatically on scope exit
    {
        auto tx = q->try_alloc(3, kMaxPayloadSize);
        CHECK(tx.has_value());
        CHECK(tx->size() == kMaxPayloadSize);
    }
    CHECK(!q->try_front().has_value());

    // Committed WriteGuard is readable and ReadGuard auto-pops
    {
        auto tx = q->try_alloc(42, 16);
        CHECK(tx.has_value());
        tx->set_meta(12345u);
        uint64_t v1 = 0x1122334455667788ULL;
        CHECK(tx->write_pod(v1));
        tx->commit();
    }
    {
        auto rx = q->try_front();
        CHECK(rx.has_value());
        CHECK(rx->type() == 42);
        CHECK(rx->meta() == 12345u);
        CHECK(rx->payload_size() == 16);
        // Defer prevents pop
        rx->defer();
    }
    {
        auto rx = q->try_front();
        CHECK(rx.has_value());
        CHECK(rx->type() == 42);
    }
    CHECK(!q->try_front().has_value());
}

void test_wrap_cancel_and_blk_sizes() {
    auto run_for_blk = []<uint32_t BlkSz>() {
        auto q = std::make_unique<SpscQueue<256, BlkSz>>();
        for (int i = 0; i < 3; ++i) {
            auto tx = q->try_alloc(1, 50);
            CHECK(tx.has_value());
            tx->commit();
            auto rx = q->try_front();
            CHECK(rx.has_value() && rx->payload_size() == 50);
        }
        // Trigger rewind alloc, then cancel without committing
        {
            auto tx_cancel = q->try_alloc(2, 120);
            CHECK(tx_cancel.has_value());
            tx_cancel->cancel();
        }
        // Small message fits before wrap
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
        // Now do real rewind commit
        {
            auto tx_wrap = q->try_alloc(9, 140);
            CHECK(tx_wrap.has_value());
            std::memset(tx_wrap->data(), 0xCD, 140);
            tx_wrap->commit();
        }
        {
            auto rx = q->try_front();
            CHECK(rx.has_value() && rx->type() == 9 && rx->payload_size() == 140);
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

void test_batch_stage_flush_and_drain_backpressure() {
    auto q = std::make_unique<SpscQueue<4096, 32>>();

    // Stage 20 messages before flushing -> consumer must see 0 until flush()
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

    // Drain with backpressure: accept first 7, reject 8th (return false)
    std::vector<uint32_t> received;
    uint32_t n1 = q->drain(
        [&](MsgView msg) -> bool {
            auto val = msg.read_as<uint32_t>();
            CHECK(val.has_value());
            if (*val == 7u) return false; // backpressure!
            received.push_back(*val);
            return true;
        },
        64);
    CHECK(n1 == 7);
    CHECK(received.size() == 7);

    // Next drain must re-deliver 7 and consume the remaining 13
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
                uint16_t sz = static_cast<uint16_t>(sizeof(uint64_t) + (sent % 160));
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
                    uint16_t expected_sz = static_cast<uint16_t>(sizeof(uint64_t) + (expected % 160));
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
    test_batch_stage_flush_and_drain_backpressure();
    test_multithreaded_spsc_batch_stress();
    std::cout << "unit_spsc_test passed\n";
    return 0;
}
