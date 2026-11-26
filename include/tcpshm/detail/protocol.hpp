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

#include "tcpshm/config.hpp"
#include "tcpshm/detail/endian.hpp"
#include <atomic>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace tcpshm::detail {

inline constexpr uint32_t kProtocolMagic = 0x5453484Du; // "TSHM"
inline constexpr uint32_t kProtocolVersion = 2u;
inline constexpr uint64_t kShmProbeAckMask = 0x5a5a5a5a5a5a5a5aULL;
inline constexpr uint32_t kShmProbeNameSize = 64u;
inline constexpr uint32_t kLoginErrorMsgSize = 64u;

inline constexpr uint16_t kLoginMsgType = 1u;
inline constexpr uint16_t kLoginRspMsgType = 2u;
inline constexpr uint16_t kShmProbeReqType = 3u;
inline constexpr uint16_t kShmProbeRspType = 4u;

struct ShmProbeData
{
    alignas(8) uint64_t token;
    alignas(8) uint64_t ack;
};
static_assert(std::is_trivial_v<ShmProbeData> && std::is_standard_layout_v<ShmProbeData>);
static_assert(std::atomic_ref<uint64_t>::is_always_lock_free, "atomic_ref<uint64_t> must be lock-free");

struct ShmProbeReqMsg
{
    uint64_t token;
    char shm_name[kShmProbeNameSize];

    template<std::endian WireEndian>
    void convert_byte_order() noexcept {
        convert_in_place<WireEndian>(token);
    }
};
static_assert(std::is_trivially_copyable_v<ShmProbeReqMsg>);

struct ShmProbeRspMsg
{
    uint64_t ack;
    uint8_t ok;
    uint8_t reserved[7];

    template<std::endian WireEndian>
    void convert_byte_order() noexcept {
        convert_in_place<WireEndian>(ack);
    }
};
static_assert(std::is_trivially_copyable_v<ShmProbeRspMsg>);

template<ConfigConcept Config>
struct LoginMsg
{
    uint32_t magic;
    uint32_t version;
    uint64_t cfg_fingerprint;
    uint32_t client_seq_start;
    uint32_t client_seq_end;
    typename Config::LoginUserData user_data;
    uint8_t use_shm;
    char client_name[Config::name_size];
    char last_server_name[Config::name_size];

    void convert_byte_order() noexcept {
        convert_in_place<Config::wire_endian>(magic);
        convert_in_place<Config::wire_endian>(version);
        convert_in_place<Config::wire_endian>(cfg_fingerprint);
        convert_in_place<Config::wire_endian>(client_seq_start);
        convert_in_place<Config::wire_endian>(client_seq_end);
    }
};

template<ConfigConcept Config>
struct LoginRspMsg
{
    uint32_t magic;
    uint32_t version;
    uint32_t server_seq_start;
    uint32_t server_seq_end;
    typename Config::LoginRspUserData user_data;
    uint8_t status; // 0: ok, 1: seq_mismatch, 2: error/rejected
    char server_name[Config::name_size];
    char error_msg[kLoginErrorMsgSize];

    void convert_byte_order() noexcept {
        convert_in_place<Config::wire_endian>(magic);
        convert_in_place<Config::wire_endian>(version);
        convert_in_place<Config::wire_endian>(server_seq_start);
        convert_in_place<Config::wire_endian>(server_seq_end);
    }
};

[[nodiscard]] constexpr bool check_ack_in_range(uint32_t ack_seq, uint32_t seq_start, uint32_t seq_end) noexcept {
    return static_cast<int32_t>(ack_seq - seq_start) >= 0 &&
           static_cast<int32_t>(seq_end - ack_seq) >= 0;
}

} // namespace tcpshm::detail

namespace tcpshm {

struct SeqMismatchInfo
{
    uint32_t local_ack = 0;
    uint32_t local_start = 0;
    uint32_t local_end = 0;
    uint32_t remote_ack = 0;
    uint32_t remote_start = 0;
    uint32_t remote_end = 0;
};

} // namespace tcpshm
