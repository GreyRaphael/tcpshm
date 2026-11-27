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
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

namespace tcpshm {

inline constexpr uint16_t kHeartbeatMsgType = 0;
inline constexpr uint16_t kFirstAppMsgType = 1;

// 16-byte aligned wire & ring-buffer frame header supporting >64 KiB payloads, 16B SIMD payload alignment, and future flags.
struct alignas(16) FrameHeader
{
    uint32_t size;     // Total frame size in bytes including this 16B header (0 marks SPSC rewind sentinel)
    uint16_t msg_type; // Application message type (!= 0 for user frames; 0 reserved for heartbeat)
    uint16_t flags;    // Protocol/application flags (0 by default)
    uint32_t ack_seq;  // PTCP peer ack sequence, or routing metadata in local SPSC queues
    uint32_t reserved; // Reserved extension slot (checksum / custom metadata)

    template<std::endian WireEndian>
    void convert_byte_order() noexcept {
        detail::convert_in_place<WireEndian>(size);
        detail::convert_in_place<WireEndian>(msg_type);
        detail::convert_in_place<WireEndian>(flags);
        detail::convert_in_place<WireEndian>(ack_seq);
        detail::convert_in_place<WireEndian>(reserved);
    }
};

static_assert(sizeof(FrameHeader) == 16 && alignof(FrameHeader) == 16 && std::is_trivially_copyable_v<FrameHeader>);

inline constexpr uint32_t kFrameHeaderSize = static_cast<uint32_t>(sizeof(FrameHeader));
inline constexpr uint32_t kFrameAlign = static_cast<uint32_t>(alignof(FrameHeader));
inline constexpr uint32_t kFrameAlignMask = kFrameAlign - 1u;
inline constexpr uint32_t kFrameAlignShift = 4u;
// Hard upper bound prevents uint32_t wrap in block rounding and keeps TCP transfers well within signed int range.
inline constexpr uint32_t kMaxFrameSize = 64u * 1024u * 1024u;
inline constexpr uint32_t kMaxPayloadSize = kMaxFrameSize - kFrameHeaderSize;

[[nodiscard]] constexpr uint32_t align_frame_size(uint32_t bytes) noexcept {
    return (bytes + kFrameAlignMask) & ~kFrameAlignMask;
}

// Lightweight immutable view over a received message frame.
class MsgView
{
public:
    constexpr explicit MsgView(const FrameHeader* hdr) noexcept : hdr_(hdr) {}

    [[nodiscard]] uint16_t type() const noexcept { return hdr_->msg_type; }
    [[nodiscard]] uint16_t flags() const noexcept { return hdr_->flags; }
    [[nodiscard]] uint32_t total_size() const noexcept { return hdr_->size; }
    [[nodiscard]] uint32_t payload_size() const noexcept { return hdr_->size - kFrameHeaderSize; }
    [[nodiscard]] uint32_t meta() const noexcept { return hdr_->ack_seq; }
    [[nodiscard]] uint32_t reserved() const noexcept { return hdr_->reserved; }

    [[nodiscard]] const std::byte* data() const noexcept {
        return reinterpret_cast<const std::byte*>(hdr_ + 1);
    }

    [[nodiscard]] std::span<const std::byte> payload() const noexcept {
        return {data(), payload_size()};
    }

    [[nodiscard]] std::string_view payload_str() const noexcept {
        return {reinterpret_cast<const char*>(hdr_ + 1), payload_size()};
    }

    // Strict-aliasing and alignment safe typed copy for trivially copyable payloads.
    template<typename T>
        requires std::is_trivially_copyable_v<T>
    [[nodiscard]] std::optional<T> read_as() const noexcept {
        if (payload_size() != sizeof(T)) return std::nullopt;
        T out;
        std::memcpy(&out, data(), sizeof(T));
        return out;
    }

    // Zero-copy typed pointer access (supports up to 16-byte-aligned types).
    template<typename T>
        requires (std::is_trivially_copyable_v<T> && alignof(T) <= alignof(FrameHeader))
    [[nodiscard]] const T* as_ptr() const noexcept {
        if (payload_size() != sizeof(T)) return nullptr;
        return std::launder(reinterpret_cast<const T*>(hdr_ + 1));
    }

private:
    const FrameHeader* hdr_ = nullptr;
};

// Zero-overhead RAII TX frame writer guard. Automatically cancels if dropped without commit()/stage().
template<class Producer>
class WriteGuard
{
public:
    WriteGuard(Producer* prod, FrameHeader* hdr, uint32_t payload_size) noexcept
        : prod_(prod), hdr_(hdr), payload_size_(payload_size) {}

    ~WriteGuard() noexcept {
        if (prod_) cancel();
    }

    WriteGuard(const WriteGuard&) = delete;
    WriteGuard& operator=(const WriteGuard&) = delete;

    WriteGuard(WriteGuard&& other) noexcept
        : prod_(std::exchange(other.prod_, nullptr)),
          hdr_(std::exchange(other.hdr_, nullptr)),
          payload_size_(std::exchange(other.payload_size_, 0)) {}

    WriteGuard& operator=(WriteGuard&& other) noexcept {
        if (this == &other) return *this;
        if (prod_) cancel();
        prod_ = std::exchange(other.prod_, nullptr);
        hdr_ = std::exchange(other.hdr_, nullptr);
        payload_size_ = std::exchange(other.payload_size_, 0);
        return *this;
    }

    [[nodiscard]] std::byte* data() noexcept {
        return reinterpret_cast<std::byte*>(hdr_ + 1);
    }

    [[nodiscard]] std::span<std::byte> payload() noexcept {
        return {data(), payload_size_};
    }

    [[nodiscard]] constexpr uint32_t size() const noexcept { return payload_size_; }

    void set_type(uint16_t msg_type) noexcept { hdr_->msg_type = msg_type; }
    void set_flags(uint16_t flags) noexcept { hdr_->flags = flags; }
    void set_meta(uint32_t meta) noexcept { hdr_->ack_seq = meta; }
    void set_reserved(uint32_t reserved) noexcept { hdr_->reserved = reserved; }

    template<typename T, typename... Args>
        requires (std::is_trivially_copyable_v<T> && alignof(T) <= alignof(FrameHeader))
    T* emplace(Args&&... args) noexcept {
        if (payload_size_ < sizeof(T)) return nullptr;
        return std::construct_at(reinterpret_cast<T*>(data()), std::forward<Args>(args)...);
    }

    template<typename T>
        requires std::is_trivially_copyable_v<T>
    bool write_pod(const T& val) noexcept {
        if (payload_size_ < sizeof(T)) return false;
        std::memcpy(data(), &val, sizeof(T));
        return true;
    }

    bool write_bytes(std::span<const std::byte> src) noexcept {
        if (src.size() > payload_size_) return false;
        if (!src.empty()) std::memcpy(data(), src.data(), src.size());
        return true;
    }

    bool write_str(std::string_view sv) noexcept {
        if (sv.size() > payload_size_) return false;
        if (!sv.empty()) std::memcpy(data(), sv.data(), sv.size());
        return true;
    }

    void commit() noexcept {
        if (!prod_) return;
        Producer* p = std::exchange(prod_, nullptr);
        hdr_ = nullptr;
        payload_size_ = 0;
        p->commit_pending();
    }

    void stage() noexcept {
        if (!prod_) return;
        Producer* p = std::exchange(prod_, nullptr);
        hdr_ = nullptr;
        payload_size_ = 0;
        p->stage_pending();
    }

    void cancel() noexcept {
        if (!prod_) return;
        Producer* p = std::exchange(prod_, nullptr);
        hdr_ = nullptr;
        payload_size_ = 0;
        p->cancel_pending();
    }

private:
    Producer* prod_ = nullptr;
    FrameHeader* hdr_ = nullptr;
    uint32_t payload_size_ = 0;
};

// Zero-overhead RAII RX frame reader guard. Automatically pops on destruction unless defer() is called.
template<class Consumer>
class ReadGuard
{
public:
    ReadGuard(Consumer* cons, const FrameHeader* hdr) noexcept
        : cons_(cons), hdr_(hdr) {}

    ~ReadGuard() noexcept {
        if (cons_) cons_->pop();
    }

    ReadGuard(const ReadGuard&) = delete;
    ReadGuard& operator=(const ReadGuard&) = delete;

    ReadGuard(ReadGuard&& other) noexcept
        : cons_(std::exchange(other.cons_, nullptr)),
          hdr_(std::exchange(other.hdr_, nullptr)) {}

    ReadGuard& operator=(ReadGuard&& other) noexcept {
        if (this == &other) return *this;
        if (cons_) cons_->pop();
        cons_ = std::exchange(other.cons_, nullptr);
        hdr_ = std::exchange(other.hdr_, nullptr);
        return *this;
    }

    [[nodiscard]] MsgView view() const noexcept { return MsgView{hdr_}; }
    [[nodiscard]] uint16_t type() const noexcept { return view().type(); }
    [[nodiscard]] uint16_t flags() const noexcept { return view().flags(); }
    [[nodiscard]] uint32_t payload_size() const noexcept { return view().payload_size(); }
    [[nodiscard]] uint32_t meta() const noexcept { return view().meta(); }
    [[nodiscard]] uint32_t reserved() const noexcept { return view().reserved(); }
    [[nodiscard]] std::span<const std::byte> payload() const noexcept { return view().payload(); }
    [[nodiscard]] std::string_view payload_str() const noexcept { return view().payload_str(); }

    template<typename T>
        requires std::is_trivially_copyable_v<T>
    [[nodiscard]] std::optional<T> read_as() const noexcept {
        return view().template read_as<T>();
    }

    void consume() noexcept {
        if (!cons_) return;
        Consumer* c = std::exchange(cons_, nullptr);
        hdr_ = nullptr;
        c->pop();
    }

    void defer() noexcept {
        cons_ = nullptr;
        hdr_ = nullptr;
    }

private:
    Consumer* cons_ = nullptr;
    const FrameHeader* hdr_ = nullptr;
};

} // namespace tcpshm
