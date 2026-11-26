#pragma once

#include "tcpshm/config.hpp"
#include "tcpshm/conn_handle.hpp"
#include <cstdint>
#include <string>
#include <variant>

struct ExampleConfig : tcpshm::DefaultConfig
{
    static constexpr uint32_t name_size = 16;
    static constexpr uint32_t shm_queue_bytes = 1024u * 1024u;
    static constexpr uint32_t shm_block_bytes = 64u;
    static constexpr uint32_t tcp_queue_bytes = 64u * 1024u;
    static constexpr uint32_t tcp_recv_init_bytes = 4096u;
    static constexpr uint32_t tcp_recv_max_bytes = 64u * 1024u;
    static constexpr uint32_t max_shm_groups = 1u;
    static constexpr uint32_t max_shm_conns_per_group = 4u;
    static constexpr uint32_t max_tcp_groups = 1u;
    static constexpr uint32_t max_tcp_conns_per_group = 4u;
    static constexpr uint32_t max_pending_conns = 8u;
};

static_assert(tcpshm::ConfigConcept<ExampleConfig>);

template<class... Ts>
struct overload : Ts...
{
    using Ts::operator()...;
};

struct Order
{
    int32_t id = 0;
    double price = 0.0;
    int64_t volume = 0;
};

struct Trade
{
    int32_t order_id = 0;
    int32_t trade_id = 0;
    double price = 0.0;
    int64_t volume = 0;
};

struct Login
{
    std::string name;
    std::string password;
};

struct LoginResponse
{
    int32_t error_code = 0;
    std::string error_message;
};

struct Request
{
    std::variant<Login, Order> message_type;
};

struct Response
{
    std::variant<LoginResponse, Trade> message_type;
};
