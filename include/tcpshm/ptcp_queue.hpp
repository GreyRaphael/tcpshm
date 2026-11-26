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

#include "tcpshm/detail/endian.hpp"
#include "tcpshm/error.hpp"
#include "tcpshm/frame.hpp"
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <optional>
#include <type_traits>

namespace tcpshm {

struct PtcpSeqRange
{
    uint32_t local_ack = 0;
    uint32_t seq_start = 0;
    uint32_t seq_end = 0;
};

// Pure trivial & standard-layout mmap-able persistent TCP outbound queue.
// Includes magic, version, and capacity metadata to detect corruption or config mismatch on reopen.
template<uint32_t Bytes, std::endian WireEndian = std::endian::little>
struct PtcpQueue
{
    static constexpr uint64_t kMagic = 0x5453484D50544350ULL; // "TSHMPTCP"
    static constexpr uint32_t kVersion = 2u;
    static constexpr uint32_t kHdrSz = static_cast<uint32_t>(sizeof(FrameHeader));
    static_assert(Bytes > 0 && Bytes % kHdrSz == 0, "Bytes must be a positive multiple of 8");
    static constexpr uint32_t kBlkCnt = Bytes / kHdrSz;
    static_assert(kBlkCnt <= static_cast<uint32_t>(std::numeric_limits<int>::max()), "kBlkCnt exceeds int max");

    using Guard = WriteGuard<PtcpQueue>;

    void reset() noexcept {
        std::memset(buf_, 0, sizeof(buf_));
        magic_ = kMagic;
        version_ = kVersion;
        capacity_bytes_ = Bytes;
        write_idx_ = 0;
        read_idx_ = 0;
        send_idx_ = 0;
        read_seq_num_ = 0;
        write_seq_num_ = 0;
        ack_seq_num_ = 0;
        reserved_ = 0;
    }

    [[nodiscard]] FrameHeader* alloc_raw(uint16_t msg_type, uint16_t payload_size) noexcept {
        if (payload_size > kMaxPayloadSize) [[unlikely]] return nullptr;
        const uint32_t total_size = static_cast<uint32_t>(payload_size) + kHdrSz;
        const uint32_t blk_sz = (total_size + kHdrSz - 1u) / kHdrSz;
        if (blk_sz > kBlkCnt) [[unlikely]] return nullptr;
        const uint32_t avail_sz = kBlkCnt - write_idx_;
        if (blk_sz > avail_sz) {
            if (blk_sz > avail_sz + read_idx_) return nullptr;
            std::memmove(buf_,
                         buf_ + static_cast<size_t>(read_idx_) * kHdrSz,
                         static_cast<size_t>(write_idx_ - read_idx_) * kHdrSz);
            write_idx_ -= read_idx_;
            send_idx_ -= read_idx_;
            read_idx_ = 0;
        }
        return new (buf_ + static_cast<size_t>(write_idx_) * kHdrSz)
            FrameHeader{static_cast<uint16_t>(total_size), msg_type, 0};
    }

    [[nodiscard]] std::optional<Guard> try_alloc(uint16_t msg_type, uint16_t payload_size) noexcept {
        FrameHeader* hdr = alloc_raw(msg_type, payload_size);
        if (!hdr) return std::nullopt;
        return std::optional<Guard>(std::in_place, this, hdr, payload_size);
    }

    [[nodiscard]] std::optional<Guard> try_alloc(uint16_t payload_size) noexcept {
        return try_alloc(kFirstAppMsgType, payload_size);
    }

    void push_pending() noexcept {
        FrameHeader* hdr = blk_at(write_idx_);
        const uint32_t blk_sz = (static_cast<uint32_t>(hdr->size) + kHdrSz - 1u) / kHdrSz;
        hdr->ack_seq = ack_seq_num_;
        hdr->template convert_byte_order<WireEndian>();
        write_idx_ += blk_sz;
        ++write_seq_num_;
    }

    void commit_pending() noexcept { push_pending(); }
    void stage_pending() noexcept { push_pending(); }
    void cancel_pending() noexcept {}

    [[nodiscard]] const std::byte* get_sendable(uint32_t& blk_sz) const noexcept {
        blk_sz = write_idx_ - send_idx_;
        return buf_ + static_cast<size_t>(send_idx_) * kHdrSz;
    }

    [[nodiscard]] bool has_sendable() const noexcept {
        return send_idx_ < write_idx_;
    }

    void on_sent_blocks(uint32_t blk_sz) noexcept {
        send_idx_ += blk_sz;
    }

    void login_ack(uint32_t ack_seq) noexcept {
        (void)ack(ack_seq);
        send_idx_ = read_idx_;
    }

    // Acknowledge messages up to peer's next expected seq_num.
    [[nodiscard]] bool ack(uint32_t ack_seq) noexcept {
        if (static_cast<int32_t>(ack_seq - read_seq_num_) <= 0) return true;
        if (static_cast<int32_t>(ack_seq - write_seq_num_) > 0) return false;
        uint32_t idx = read_idx_;
        uint32_t seq = read_seq_num_;
        while (seq != ack_seq) {
            if (idx >= write_idx_) return false;
            FrameHeader hdr;
            std::memcpy(&hdr, buf_ + static_cast<size_t>(idx) * kHdrSz, kHdrSz);
            const uint16_t msg_size = detail::from_wire<WireEndian>(hdr.size);
            if (msg_size < kHdrSz) return false;
            const uint32_t step = (static_cast<uint32_t>(msg_size) + kHdrSz - 1u) / kHdrSz;
            if (idx + step > write_idx_) return false;
            idx += step;
            ++seq;
        }
        read_idx_ = idx;
        read_seq_num_ = seq;
        if (send_idx_ < read_idx_) send_idx_ = read_idx_;
        if (read_idx_ == write_idx_) {
            read_idx_ = write_idx_ = send_idx_ = 0;
        }
        return true;
    }

    [[nodiscard]] uint32_t& my_ack() noexcept { return ack_seq_num_; }
    [[nodiscard]] uint32_t my_ack() const noexcept { return ack_seq_num_; }

    [[nodiscard]] Result<PtcpSeqRange> validate_and_get_seq() noexcept {
        if (magic_ == 0 && version_ == 0 && capacity_bytes_ == 0 &&
            write_idx_ == 0 && read_idx_ == 0 && send_idx_ == 0 &&
            read_seq_num_ == 0 && write_seq_num_ == 0 && ack_seq_num_ == 0) {
            magic_ = kMagic;
            version_ = kVersion;
            capacity_bytes_ = Bytes;
            return PtcpSeqRange{0, 0, 0};
        }
        if (magic_ != kMagic || version_ != kVersion) {
            return make_error(ErrorCode::CorruptPtcpFile);
        }
        if (capacity_bytes_ != Bytes) {
            return make_error(ErrorCode::ConfigMismatch);
        }
        if (write_idx_ > kBlkCnt || read_idx_ > write_idx_ ||
            send_idx_ < read_idx_ || send_idx_ > write_idx_) {
            return make_error(ErrorCode::CorruptPtcpFile);
        }
        uint32_t end = read_seq_num_;
        uint32_t idx = read_idx_;
        while (idx < write_idx_) {
            FrameHeader hdr;
            std::memcpy(&hdr, buf_ + static_cast<size_t>(idx) * kHdrSz, kHdrSz);
            hdr.template convert_byte_order<WireEndian>();
            if (hdr.size < kHdrSz) {
                return make_error(ErrorCode::CorruptPtcpFile);
            }
            if (static_cast<int32_t>(ack_seq_num_ - hdr.ack_seq) < 0) {
                return make_error(ErrorCode::CorruptPtcpFile);
            }
            const uint32_t step = (static_cast<uint32_t>(hdr.size) + kHdrSz - 1u) / kHdrSz;
            if (idx + step > write_idx_) {
                return make_error(ErrorCode::CorruptPtcpFile);
            }
            idx += step;
            ++end;
        }
        if (idx != write_idx_) {
            return make_error(ErrorCode::CorruptPtcpFile);
        }
        write_seq_num_ = end;
        return PtcpSeqRange{ack_seq_num_, read_seq_num_, end};
    }

    [[nodiscard]] FrameHeader* blk_at(uint32_t idx) noexcept {
        return std::launder(reinterpret_cast<FrameHeader*>(buf_ + static_cast<size_t>(idx) * kHdrSz));
    }

    // Persistent header fields (no default member initializers so PtcpQueue remains trivial & standard-layout).
    uint64_t magic_;
    uint32_t version_;
    uint32_t capacity_bytes_;
    uint32_t write_idx_;
    uint32_t read_idx_;
    uint32_t send_idx_;
    uint32_t read_seq_num_;
    uint32_t write_seq_num_;
    uint32_t ack_seq_num_;
    uint32_t reserved_;
    alignas(64) std::byte buf_[Bytes];
};

static_assert(std::is_trivial_v<PtcpQueue<1024>> && std::is_standard_layout_v<PtcpQueue<1024>>,
              "PtcpQueue must be trivial and standard-layout for safe mmap");

} // namespace tcpshm
