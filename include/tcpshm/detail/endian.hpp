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

#include <bit>
#include <cstdint>
#include <type_traits>

namespace tcpshm::detail {

static_assert(std::endian::native == std::endian::little || std::endian::native == std::endian::big,
              "Mixed-endian platforms are not supported");

[[nodiscard]] constexpr uint16_t bswap16(uint16_t v) noexcept {
    return static_cast<uint16_t>((v >> 8u) | (v << 8u));
}

[[nodiscard]] constexpr uint32_t bswap32(uint32_t v) noexcept {
    return ((v & 0x000000FFu) << 24u) |
           ((v & 0x0000FF00u) << 8u) |
           ((v & 0x00FF0000u) >> 8u) |
           ((v & 0xFF000000u) >> 24u);
}

[[nodiscard]] constexpr uint64_t bswap64(uint64_t v) noexcept {
    return ((v & 0x00000000000000FFULL) << 56u) |
           ((v & 0x000000000000FF00ULL) << 40u) |
           ((v & 0x0000000000FF0000ULL) << 24u) |
           ((v & 0x00000000FF000000ULL) << 8u) |
           ((v & 0x000000FF00000000ULL) >> 8u) |
           ((v & 0x0000FF0000000000ULL) >> 24u) |
           ((v & 0x00FF000000000000ULL) >> 40u) |
           ((v & 0xFF00000000000000ULL) >> 56u);
}

template<std::endian WireEndian, typename T>
    requires std::is_trivially_copyable_v<T>
[[nodiscard]] constexpr T convert_endian(T value) noexcept {
    if constexpr (WireEndian == std::endian::native || sizeof(T) == 1) {
        return value;
    } else if constexpr (sizeof(T) == 2) {
        return std::bit_cast<T>(bswap16(std::bit_cast<uint16_t>(value)));
    } else if constexpr (sizeof(T) == 4) {
        return std::bit_cast<T>(bswap32(std::bit_cast<uint32_t>(value)));
    } else if constexpr (sizeof(T) == 8) {
        return std::bit_cast<T>(bswap64(std::bit_cast<uint64_t>(value)));
    } else {
        static_assert(sizeof(T) == 0, "Unsupported type size for endian conversion");
    }
}

template<std::endian WireEndian, typename T>
    requires std::is_trivially_copyable_v<T>
[[nodiscard]] constexpr T to_wire(T value) noexcept {
    return convert_endian<WireEndian>(value);
}

template<std::endian WireEndian, typename T>
    requires std::is_trivially_copyable_v<T>
[[nodiscard]] constexpr T from_wire(T value) noexcept {
    return convert_endian<WireEndian>(value);
}

template<std::endian WireEndian, typename T>
    requires std::is_trivially_copyable_v<T>
constexpr void convert_in_place(T& value) noexcept {
    value = convert_endian<WireEndian>(value);
}

} // namespace tcpshm::detail
