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

#include "tcpshm/frame.hpp"
#include <atomic>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

namespace tcpshm {

// Pure trivial & standard-layout shared-memory ring buffer layout (implicit-lifetime POD).
// Holds only the data buffer and shared atomic_ref indices; process-private caches stay in handles.
template<uint32_t Bytes, uint32_t BlkSz = 64>
struct SpscRingLayout
{
    static constexpr uint32_t kBlkSz = BlkSz;
    static constexpr uint32_t kHdrSz = static_cast<uint32_t>(sizeof(FrameHeader));
    static_assert(kBlkSz >= kHdrSz && (kBlkSz % kHdrSz == 0) && std::has_single_bit(kBlkSz),
                  "BlkSz must be a power-of-2 multiple of 8");
    static_assert(Bytes >= kBlkSz && Bytes % kBlkSz == 0, "Bytes must be a positive multiple of BlkSz");
    static constexpr uint32_t kBlkCnt = Bytes / kBlkSz;
    static_assert(kBlkCnt > 0 && std::has_single_bit(kBlkCnt), "kBlkCnt must be a power of 2");
    static_assert(std::atomic_ref<uint32_t>::is_always_lock_free,
                  "std::atomic_ref<uint32_t> must be lock-free");

    alignas(64) std::byte buf[Bytes];
    alignas(128) uint32_t write_idx;
    alignas(128) uint32_t read_idx;
};

// Single-cache-line producer handle with process-private write/read caches and batch staging.
template<uint32_t Bytes, uint32_t BlkSz = 64>
class alignas(64) SpscProducer
{
public:
    using Layout = SpscRingLayout<Bytes, BlkSz>;
    using Guard = WriteGuard<SpscProducer>;
    static constexpr uint32_t kBlkSz = Layout::kBlkSz;
    static constexpr uint32_t kBlkCnt = Layout::kBlkCnt;
    static constexpr uint32_t kHdrSz = Layout::kHdrSz;
    static_assert(std::is_trivial_v<Layout> && std::is_standard_layout_v<Layout>,
                  "SpscRingLayout must be trivial and standard-layout");

    SpscProducer() = default;
    ~SpscProducer() = default;
    SpscProducer(const SpscProducer&) = delete;
    SpscProducer& operator=(const SpscProducer&) = delete;
    SpscProducer(SpscProducer&&) = delete;
    SpscProducer& operator=(SpscProducer&&) = delete;

    void attach(Layout* layout) noexcept {
        layout_ = layout;
        pending_write_idx_ = 0;
        pending_blk_sz_ = 0;
        staged_ = false;
        if (layout_) {
            local_write_idx_ = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_relaxed);
            read_idx_cache_ = std::atomic_ref<uint32_t>(layout_->read_idx).load(std::memory_order_acquire);
        } else {
            local_write_idx_ = 0;
            read_idx_cache_ = 0;
        }
    }

    Layout* detach() noexcept {
        if (staged_) flush();
        Layout* old = layout_;
        layout_ = nullptr;
        local_write_idx_ = 0;
        read_idx_cache_ = 0;
        pending_write_idx_ = 0;
        pending_blk_sz_ = 0;
        staged_ = false;
        return old;
    }

    [[nodiscard]] bool is_attached() const noexcept { return layout_ != nullptr; }
    [[nodiscard]] Layout* layout() const noexcept { return layout_; }
    [[nodiscard]] bool has_staged() const noexcept { return staged_; }

    // Must only be called when no concurrent producer/consumer accesses the layout.
    void reset() noexcept {
        if (!layout_) return;
        std::memset(layout_->buf, 0, sizeof(layout_->buf));
        std::atomic_ref<uint32_t>(layout_->write_idx).store(0, std::memory_order_relaxed);
        std::atomic_ref<uint32_t>(layout_->read_idx).store(0, std::memory_order_relaxed);
        local_write_idx_ = 0;
        read_idx_cache_ = 0;
        pending_write_idx_ = 0;
        pending_blk_sz_ = 0;
        staged_ = false;
    }

    [[nodiscard]] std::optional<Guard> try_alloc(uint16_t msg_type, uint16_t payload_size) noexcept {
        pending_blk_sz_ = 0;
        if (!layout_ || payload_size > kMaxPayloadSize) [[unlikely]] {
            return std::nullopt;
        }
        const uint32_t total_size = static_cast<uint32_t>(payload_size) + kHdrSz;
        const uint32_t blk_sz = (total_size + kBlkSz - 1u) / kBlkSz;
        if (blk_sz > kBlkCnt) [[unlikely]] {
            return std::nullopt;
        }
        uint32_t local_write = local_write_idx_;
        const uint32_t padding_sz = kBlkCnt - (local_write & (kBlkCnt - 1u));
        const bool rewind = blk_sz > padding_sz;
        const uint32_t min_read_idx = local_write + blk_sz + (rewind ? padding_sz : 0u) - kBlkCnt;
        if (static_cast<int32_t>(read_idx_cache_ - min_read_idx) < 0) [[unlikely]] {
            read_idx_cache_ = std::atomic_ref<uint32_t>(layout_->read_idx).load(std::memory_order_acquire);
            if (static_cast<int32_t>(read_idx_cache_ - min_read_idx) < 0) {
                if (staged_) flush();
                return std::nullopt;
            }
        }
        if (rewind) [[unlikely]] {
            new (layout_->buf + static_cast<size_t>(local_write & (kBlkCnt - 1u)) * kBlkSz) FrameHeader{0, 0, 0};
            local_write += padding_sz;
        }
        FrameHeader* hdr =
            new (layout_->buf + static_cast<size_t>(local_write & (kBlkCnt - 1u)) * kBlkSz)
                FrameHeader{static_cast<uint16_t>(total_size), msg_type, 0};
        pending_write_idx_ = local_write + blk_sz;
        pending_blk_sz_ = blk_sz;
        return std::optional<Guard>(std::in_place, this, hdr, payload_size);
    }

    [[nodiscard]] std::optional<Guard> try_alloc(uint16_t payload_size) noexcept {
        return try_alloc(kFirstAppMsgType, payload_size);
    }

    // Publish all staged messages in a single release store.
    void flush() noexcept {
        if (!layout_ || !staged_) [[unlikely]] return;
        std::atomic_ref<uint32_t>(layout_->write_idx).store(local_write_idx_, std::memory_order_release);
        staged_ = false;
    }

    void commit_pending() noexcept {
        if (!layout_ || pending_blk_sz_ == 0) [[unlikely]] return;
        local_write_idx_ = pending_write_idx_;
        pending_blk_sz_ = 0;
        staged_ = false;
        std::atomic_ref<uint32_t>(layout_->write_idx).store(local_write_idx_, std::memory_order_release);
    }

    void stage_pending() noexcept {
        if (!layout_ || pending_blk_sz_ == 0) [[unlikely]] return;
        local_write_idx_ = pending_write_idx_;
        pending_blk_sz_ = 0;
        staged_ = true;
    }

    void cancel_pending() noexcept {
        pending_blk_sz_ = 0;
    }

private:
    Layout* layout_ = nullptr;
    uint32_t local_write_idx_ = 0;
    uint32_t read_idx_cache_ = 0;
    uint32_t pending_write_idx_ = 0;
    uint32_t pending_blk_sz_ = 0;
    bool staged_ = false;
};

// Single-cache-line consumer handle with process-private read/write caches and batched drain().
template<uint32_t Bytes, uint32_t BlkSz = 64>
class alignas(64) SpscConsumer
{
public:
    using Layout = SpscRingLayout<Bytes, BlkSz>;
    using Guard = ReadGuard<SpscConsumer>;
    static constexpr uint32_t kBlkSz = Layout::kBlkSz;
    static constexpr uint32_t kBlkCnt = Layout::kBlkCnt;
    static constexpr uint32_t kHdrSz = Layout::kHdrSz;
    static_assert(std::is_trivial_v<Layout> && std::is_standard_layout_v<Layout>,
                  "SpscRingLayout must be trivial and standard-layout");

    SpscConsumer() = default;
    ~SpscConsumer() = default;
    SpscConsumer(const SpscConsumer&) = delete;
    SpscConsumer& operator=(const SpscConsumer&) = delete;
    SpscConsumer(SpscConsumer&&) = delete;
    SpscConsumer& operator=(SpscConsumer&&) = delete;

    void attach(Layout* layout) noexcept {
        layout_ = layout;
        front_blk_sz_ = 0;
        if (layout_) {
            local_read_idx_ = std::atomic_ref<uint32_t>(layout_->read_idx).load(std::memory_order_relaxed);
            write_idx_cache_ = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_acquire);
        } else {
            local_read_idx_ = 0;
            write_idx_cache_ = 0;
        }
    }

    Layout* detach() noexcept {
        Layout* old = layout_;
        layout_ = nullptr;
        local_read_idx_ = 0;
        write_idx_cache_ = 0;
        front_blk_sz_ = 0;
        return old;
    }

    [[nodiscard]] bool is_attached() const noexcept { return layout_ != nullptr; }
    [[nodiscard]] Layout* layout() const noexcept { return layout_; }

    // Must only be called when no concurrent producer/consumer accesses the layout.
    void reset() noexcept {
        if (!layout_) return;
        std::memset(layout_->buf, 0, sizeof(layout_->buf));
        std::atomic_ref<uint32_t>(layout_->write_idx).store(0, std::memory_order_relaxed);
        std::atomic_ref<uint32_t>(layout_->read_idx).store(0, std::memory_order_relaxed);
        local_read_idx_ = 0;
        write_idx_cache_ = 0;
        front_blk_sz_ = 0;
    }

    // Pull a single message with RAII auto-pop guard.
    [[nodiscard]] std::optional<Guard> try_front() noexcept {
        const FrameHeader* hdr = front_raw();
        if (!hdr) return std::nullopt;
        return std::optional<Guard>(std::in_place, this, hdr);
    }

    void pop() noexcept {
        if (!layout_ || front_blk_sz_ == 0) [[unlikely]] return;
        local_read_idx_ += front_blk_sz_;
        std::atomic_ref<uint32_t>(layout_->read_idx).store(local_read_idx_, std::memory_order_release);
        front_blk_sz_ = 0;
    }

    // Batch-consume up to max_batch frames with a single release store of read_idx at batch end.
    // Callback fn(MsgView) may return void (always consume) or bool (false = backpressure, stop and keep frame).
    template<typename Fn>
        requires std::invocable<Fn&, MsgView>
    uint32_t drain(Fn&& fn, uint32_t max_batch = 64) {
        if (!layout_ || max_batch == 0) [[unlikely]] return 0;

        uint32_t local_read = local_read_idx_;
        uint32_t write_cache = write_idx_cache_;
        uint32_t consumed = 0;

        while (consumed < max_batch) {
            if (local_read == write_cache) {
                write_cache = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_acquire);
                if (local_read == write_cache) {
                    break;
                }
            }
            const uint32_t slot = local_read & (kBlkCnt - 1u);
            FrameHeader hdr;
            std::memcpy(&hdr, layout_->buf + static_cast<size_t>(slot) * kBlkSz, sizeof(FrameHeader));
            if (hdr.size == 0) [[unlikely]] {
                local_read += kBlkCnt - slot;
                continue;
            }
            if (hdr.size < kHdrSz) [[unlikely]] {
                break;
            }
            const uint32_t blk_sz = (static_cast<uint32_t>(hdr.size) + kBlkSz - 1u) / kBlkSz;
            const uint32_t avail_blks = write_cache - local_read;
            if (blk_sz > avail_blks || blk_sz > kBlkCnt - slot) [[unlikely]] {
                break;
            }
            const FrameHeader* fptr = std::launder(
                reinterpret_cast<const FrameHeader*>(layout_->buf + static_cast<size_t>(slot) * kBlkSz));
            MsgView view{fptr};
            using Ret = std::invoke_result_t<Fn&, MsgView>;
            if constexpr (std::same_as<Ret, bool>) {
                if (!fn(view)) {
                    break;
                }
            } else {
                fn(view);
            }
            local_read += blk_sz;
            ++consumed;
        }

        write_idx_cache_ = write_cache;
        if (local_read != local_read_idx_) {
            local_read_idx_ = local_read;
            std::atomic_ref<uint32_t>(layout_->read_idx).store(local_read_idx_, std::memory_order_release);
        }
        return consumed;
    }

private:
    const FrameHeader* front_raw() noexcept {
        if (!layout_) [[unlikely]] return nullptr;
        uint32_t local_read = local_read_idx_;
        if (local_read == write_idx_cache_) {
            write_idx_cache_ = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_acquire);
            if (local_read == write_idx_cache_) {
                return nullptr;
            }
        }
        uint32_t slot = local_read & (kBlkCnt - 1u);
        FrameHeader hdr;
        std::memcpy(&hdr, layout_->buf + static_cast<size_t>(slot) * kBlkSz, sizeof(FrameHeader));
        if (hdr.size == 0) [[unlikely]] {
            local_read += kBlkCnt - slot;
            local_read_idx_ = local_read;
            std::atomic_ref<uint32_t>(layout_->read_idx).store(local_read, std::memory_order_release);
            if (local_read == write_idx_cache_) {
                write_idx_cache_ = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_acquire);
                if (local_read == write_idx_cache_) {
                    return nullptr;
                }
            }
            slot = local_read & (kBlkCnt - 1u);
            std::memcpy(&hdr, layout_->buf + static_cast<size_t>(slot) * kBlkSz, sizeof(FrameHeader));
        }
        if (hdr.size < kHdrSz) [[unlikely]] {
            return nullptr;
        }
        const uint32_t blk_sz = (static_cast<uint32_t>(hdr.size) + kBlkSz - 1u) / kBlkSz;
        const uint32_t avail_blks = write_idx_cache_ - local_read;
        if (blk_sz > avail_blks || blk_sz > kBlkCnt - slot) [[unlikely]] {
            return nullptr;
        }
        front_blk_sz_ = blk_sz;
        return std::launder(
            reinterpret_cast<const FrameHeader*>(layout_->buf + static_cast<size_t>(slot) * kBlkSz));
    }

    Layout* layout_ = nullptr;
    uint32_t local_read_idx_ = 0;
    uint32_t write_idx_cache_ = 0;
    uint32_t front_blk_sz_ = 0;
};

// Self-contained in-process SPSC queue combining a zero-initialized layout and separate producer/consumer handles.
template<uint32_t Bytes, uint32_t BlkSz = 64>
class SpscQueue
{
public:
    using Layout = SpscRingLayout<Bytes, BlkSz>;
    using Producer = SpscProducer<Bytes, BlkSz>;
    using Consumer = SpscConsumer<Bytes, BlkSz>;
    static constexpr uint32_t kBlkSz = Layout::kBlkSz;
    static constexpr uint32_t kBlkCnt = Layout::kBlkCnt;
    static constexpr uint32_t kHdrSz = Layout::kHdrSz;

    SpscQueue() noexcept {
        prod_.attach(&layout_);
        cons_.attach(&layout_);
    }

    ~SpscQueue() = default;
    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;
    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    void reset() noexcept {
        prod_.reset();
        cons_.attach(&layout_);
    }

    [[nodiscard]] auto try_alloc(uint16_t msg_type, uint16_t payload_size) noexcept {
        return prod_.try_alloc(msg_type, payload_size);
    }

    [[nodiscard]] auto try_alloc(uint16_t payload_size) noexcept {
        return prod_.try_alloc(payload_size);
    }

    void flush() noexcept { prod_.flush(); }

    [[nodiscard]] auto try_front() noexcept { return cons_.try_front(); }

    template<typename Fn>
        requires std::invocable<Fn&, MsgView>
    uint32_t drain(Fn&& fn, uint32_t max_batch = 64) {
        return cons_.drain(std::forward<Fn>(fn), max_batch);
    }

    [[nodiscard]] Producer& producer() noexcept { return prod_; }
    [[nodiscard]] Consumer& consumer() noexcept { return cons_; }

private:
    Layout layout_{};
    Producer prod_;
    Consumer cons_;
};

} // namespace tcpshm
