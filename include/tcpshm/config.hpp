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
#include <bit>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <type_traits>

namespace tcpshm {

using TimePoint = std::chrono::steady_clock::time_point;
using Duration = std::chrono::nanoseconds;

[[nodiscard]] inline TimePoint steady_now() noexcept {
    return std::chrono::steady_clock::now();
}

struct EmptyUserData
{
    uint8_t reserved = 0;
};
static_assert(std::is_trivially_copyable_v<EmptyUserData>);

struct DefaultConfig
{
    static constexpr uint32_t name_size = 32;
    static constexpr uint32_t shm_queue_bytes = 1u << 20;     // 1 MiB, blk_cnt must be power of 2
    static constexpr uint32_t shm_block_bytes = 64;           // power-of-2 multiple of 16
    static constexpr uint32_t tcp_queue_bytes = 1u << 20;     // 1 MiB, multiple of 16
    static constexpr uint32_t tcp_recv_init_bytes = 4096;     // multiple of 16
    static constexpr uint32_t tcp_recv_max_bytes = 1u << 20;  // 1 MiB, multiple of 16

    static constexpr uint32_t max_shm_groups = 1;
    static constexpr uint32_t max_shm_conns_per_group = 4;
    static constexpr uint32_t max_tcp_groups = 1;
    static constexpr uint32_t max_tcp_conns_per_group = 16;
    static constexpr uint32_t max_pending_conns = 16;
    static constexpr uint32_t poll_batch_size = 64;

    static constexpr bool tcp_no_delay = true;
    static constexpr std::endian wire_endian = std::endian::little;

    static constexpr std::chrono::nanoseconds handshake_timeout = std::chrono::seconds(3);
    static constexpr std::chrono::nanoseconds connection_timeout = std::chrono::seconds(10);
    static constexpr std::chrono::nanoseconds heartbeat_interval = std::chrono::seconds(3);

    using LoginUserData = EmptyUserData;
    using LoginRspUserData = EmptyUserData;
    using ConnectionUserData = EmptyUserData;
};

template<typename C>
concept ConfigConcept =
    requires {
        { C::name_size } -> std::convertible_to<uint32_t>;
        { C::shm_queue_bytes } -> std::convertible_to<uint32_t>;
        { C::shm_block_bytes } -> std::convertible_to<uint32_t>;
        { C::tcp_queue_bytes } -> std::convertible_to<uint32_t>;
        { C::tcp_recv_init_bytes } -> std::convertible_to<uint32_t>;
        { C::tcp_recv_max_bytes } -> std::convertible_to<uint32_t>;
        { C::max_shm_groups } -> std::convertible_to<uint32_t>;
        { C::max_shm_conns_per_group } -> std::convertible_to<uint32_t>;
        { C::max_tcp_groups } -> std::convertible_to<uint32_t>;
        { C::max_tcp_conns_per_group } -> std::convertible_to<uint32_t>;
        { C::max_pending_conns } -> std::convertible_to<uint32_t>;
        { C::poll_batch_size } -> std::convertible_to<uint32_t>;
        { C::tcp_no_delay } -> std::convertible_to<bool>;
        { C::wire_endian } -> std::convertible_to<std::endian>;
        { C::handshake_timeout } -> std::convertible_to<std::chrono::nanoseconds>;
        { C::connection_timeout } -> std::convertible_to<std::chrono::nanoseconds>;
        { C::heartbeat_interval } -> std::convertible_to<std::chrono::nanoseconds>;
        typename C::LoginUserData;
        typename C::LoginRspUserData;
        typename C::ConnectionUserData;
    } &&
    (C::name_size >= 8u) &&
    (C::shm_block_bytes >= kFrameAlign) &&
    (C::shm_block_bytes % kFrameAlign == 0u) &&
    std::has_single_bit(C::shm_block_bytes) &&
    (C::shm_queue_bytes >= C::shm_block_bytes) &&
    (C::shm_queue_bytes % C::shm_block_bytes == 0u) &&
    std::has_single_bit(C::shm_queue_bytes / C::shm_block_bytes) &&
    (C::tcp_queue_bytes >= kFrameAlign && C::tcp_queue_bytes % kFrameAlign == 0u) &&
    (C::tcp_recv_init_bytes >= kFrameAlign && C::tcp_recv_init_bytes % kFrameAlign == 0u) &&
    (C::tcp_recv_max_bytes >= C::tcp_recv_init_bytes &&
     C::tcp_recv_max_bytes % kFrameAlign == 0u &&
     C::tcp_recv_max_bytes <= kMaxFrameSize) &&
    (C::max_shm_groups >= 1u && C::max_shm_conns_per_group >= 1u) &&
    (C::max_tcp_groups >= 1u && C::max_tcp_conns_per_group >= 1u) &&
    (C::max_pending_conns >= 1u && C::poll_batch_size >= 1u) &&
    (C::wire_endian == std::endian::little || C::wire_endian == std::endian::big) &&
    (std::chrono::nanoseconds(C::handshake_timeout).count() > 0) &&
    (std::chrono::nanoseconds(C::connection_timeout).count() > 0) &&
    (std::chrono::nanoseconds(C::heartbeat_interval).count() > 0) &&
    std::is_trivially_copyable_v<typename C::LoginUserData> &&
    std::is_trivially_copyable_v<typename C::LoginRspUserData> &&
    std::is_default_constructible_v<typename C::ConnectionUserData>;

static_assert(ConfigConcept<DefaultConfig>, "DefaultConfig must satisfy ConfigConcept");

template<ConfigConcept Config>
[[nodiscard]] consteval uint64_t config_fingerprint() noexcept {
    constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
    constexpr uint64_t kFnvPrime = 1099511628211ULL;
    auto mix_u64 = [](uint64_t h, uint64_t v) consteval -> uint64_t {
        for (int i = 0; i < 8; ++i) {
            h ^= (v & 0xFFULL);
            h *= kFnvPrime;
            v >>= 8;
        }
        return h;
    };
    uint64_t h = kFnvOffset;
    h = mix_u64(h, Config::wire_endian == std::endian::little ? 1ULL : 2ULL);
    h = mix_u64(h, kFrameHeaderSize); // 16-byte FrameHeader
    h = mix_u64(h, Config::name_size);
    h = mix_u64(h, Config::shm_queue_bytes);
    h = mix_u64(h, Config::shm_block_bytes);
    h = mix_u64(h, sizeof(typename Config::LoginUserData));
    h = mix_u64(h, sizeof(typename Config::LoginRspUserData));
    return h;
}

} // namespace tcpshm
