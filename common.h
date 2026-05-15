#pragma once
#include <chrono>
#include <cstdint>
#include <string>
#include <variant>

enum class Platform : uint8_t {
    Windows = 1,
    Linux = 2,
};

struct CommonConf {
    static const uint32_t NameSize = 16;
    static const uint32_t ShmQueueSize = 1024 * 1024;  // must be power of 2
    static const bool ToLittleEndian = true;           // set to the endian of majority of the hosts

    using LoginUserData = Platform;
    using LoginRspUserData = char;
};

// 必须用steady_clock, 否则windows timeout会有问题
inline int64_t get_timestamp() {
    return std::chrono::steady_clock::now().time_since_epoch().count();
}

// for variant dispatch
template <class... Ts>
struct overload : Ts... {
    using Ts::operator()...;
};

struct Order {
    int32_t id;
    double price;
    int64_t volume;
};

struct Trade {
    int32_t order_id;
    int32_t trade_id;
    double price;
    int64_t volume;
};

struct Login {
    std::string name;
    std::string password;
};

struct LoginResponse {
    int32_t error_code;
    std::string error_message;
};

struct Request {
    std::variant<Login, Order> message_type;
};

struct Response {
    std::variant<LoginResponse, Trade> message_type;
};