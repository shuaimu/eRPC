#pragma once

#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "logger.h"

namespace erpc {

/// Basic UDP server class that supports receiving messages.
/// Uses a blocking POSIX IPv4 datagram socket bound to the requested port
/// with SO_REUSEADDR enabled.
template <class T>
class UDPServer {
 public:
  UDPServer(uint16_t port, size_t timeout_ms)
      : timeout_ms_(timeout_ms), sock_fd_(-1) {
    sock_fd_ = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock_fd_ < 0) {
      ERPC_ERROR("eRPC: UDPServer socket() failed: %s\n", strerror(errno));
      return;
    }

    int reuse = 1;
    if (::setsockopt(sock_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse,
                     sizeof(reuse)) < 0) {
      ERPC_ERROR("eRPC: UDPServer setsockopt(SO_REUSEADDR) failed: %s\n",
                 strerror(errno));
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(sock_fd_, reinterpret_cast<struct sockaddr *>(&addr),
               sizeof(addr)) < 0) {
      ERPC_ERROR("eRPC: UDPServer bind(port=%u) failed: %s\n",
                 static_cast<unsigned>(port), strerror(errno));
      ::close(sock_fd_);
      sock_fd_ = -1;
    }
  }

  UDPServer() : timeout_ms_(0), sock_fd_(-1) {}
  UDPServer(const UDPServer &) = delete;

  ~UDPServer() {
    if (sock_fd_ >= 0) ::close(sock_fd_);
  }

  size_t recv_blocking(T &msg) {
    if (sock_fd_ < 0) return 0;
    ssize_t n = ::recv(sock_fd_, &msg, sizeof(T), 0);
    if (n < 0) {
      ERPC_ERROR("eRPC: UDPServer recv() failed: %s\n", strerror(errno));
      return 0;
    }
    return static_cast<size_t>(n);
  }

 private:
  size_t timeout_ms_;
  int sock_fd_;
};

}  // namespace erpc
