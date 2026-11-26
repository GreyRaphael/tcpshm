#include "tcpshm/tcpshm.hpp"

namespace {

struct NoExceptHandler
{
    void on_message(auto&, tcpshm::MsgView) noexcept {}
    void on_server_msg(tcpshm::MsgView) noexcept {}
};

void verify_noexcept_instantiations() {
    NoExceptHandler h;
    tcpshm::SpscQueue<1024> sq;
    tcpshm::PtcpQueue<1024> pq;
    (void)sq;
    (void)pq;

    tcpshm::Server<NoExceptHandler, tcpshm::DefaultConfig, tcpshm::BusyPoller> srv_busy(h, "srv1", "/tmp");
#ifdef __linux__
    tcpshm::Server<NoExceptHandler, tcpshm::DefaultConfig, tcpshm::EpollPoller> srv_ep(h, "srv2", "/tmp");
    (void)srv_ep;
#endif
    tcpshm::ShmClient<NoExceptHandler> sc(h, "sc", "/tmp");
    tcpshm::TcpClient<NoExceptHandler> tc(h, "tc", "/tmp");
    tcpshm::Client<NoExceptHandler> uc(h, "uc", "/tmp");
    (void)srv_busy;
    (void)sc;
    (void)tc;
    (void)uc;
}

} // namespace

int tcpshm_noexcept_check_anchor() {
    (void)&verify_noexcept_instantiations;
    return 0;
}
