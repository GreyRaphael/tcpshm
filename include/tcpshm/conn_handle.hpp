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

#include <cstdint>
#include <type_traits>

namespace tcpshm {

enum class TransportKind : uint8_t
{
    Shm = 0,
    Tcp = 1,
};

// 8-byte generational connection handle preventing ABA bugs when slots are reused after disconnect.
struct ConnHandle
{
    static constexpr uint32_t kKindBitMask = 0x80000000u;
    static constexpr uint32_t kGenMask = 0x7FFFFFFFu;

    uint16_t group_id = 0;
    uint16_t slot_id = 0;
    uint32_t gen_and_kind = 0; // Bit 31: TransportKind, Bits 0..30: non-zero generation

    [[nodiscard]] static constexpr ConnHandle make(TransportKind kind,
                                                   uint16_t group_id,
                                                   uint16_t slot_id,
                                                   uint32_t generation) noexcept {
        const uint32_t gen = generation & kGenMask;
        if (gen == 0) return ConnHandle{};
        const uint32_t kind_bit = (kind == TransportKind::Tcp) ? kKindBitMask : 0u;
        return ConnHandle{group_id, slot_id, kind_bit | gen};
    }

    [[nodiscard]] constexpr bool valid() const noexcept {
        return generation() != 0u;
    }

    [[nodiscard]] constexpr explicit operator bool() const noexcept {
        return valid();
    }

    [[nodiscard]] constexpr TransportKind transport() const noexcept {
        return (gen_and_kind & kKindBitMask) != 0u ? TransportKind::Tcp : TransportKind::Shm;
    }

    [[nodiscard]] constexpr uint32_t generation() const noexcept {
        return gen_and_kind & kGenMask;
    }

    [[nodiscard]] constexpr uint64_t to_u64() const noexcept {
        return (static_cast<uint64_t>(gen_and_kind) << 32) |
               (static_cast<uint64_t>(group_id) << 16) |
               static_cast<uint64_t>(slot_id);
    }

    [[nodiscard]] static constexpr ConnHandle from_u64(uint64_t raw) noexcept {
        return ConnHandle{
            static_cast<uint16_t>((raw >> 16) & 0xFFFFu),
            static_cast<uint16_t>(raw & 0xFFFFu),
            static_cast<uint32_t>(raw >> 32),
        };
    }

    friend constexpr bool operator==(const ConnHandle&, const ConnHandle&) noexcept = default;
};

static_assert(sizeof(ConnHandle) == 8 && std::is_trivially_copyable_v<ConnHandle>);

} // namespace tcpshm
