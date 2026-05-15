#pragma once

#include <netdb.h>
#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <vector>
#include "logger.h"

namespace erpc {

/// Basic UDP client class that supports sending messages.
/// Uses blocking POSIX sockets — one connectionless IPv4 datagram socket
/// is created lazily and reused for every send().
template <class T>
class UDPClient {
 public:
  UDPClient() : sock_fd_(-1) {
    sock_fd_ = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock_fd_ < 0) {
      ERPC_ERROR("eRPC: UDPClient socket() failed: %s\n", strerror(errno));
    }
  }

  UDPClient(const UDPClient &) = delete;

  ~UDPClient() {
    if (sock_fd_ >= 0) ::close(sock_fd_);
  }

  /**
   * @brief Send a UDP message to a remote host
   *
   * @param rem_hostname DNS-resolvable name of the remote host
   * @param rem_port Destination UDP port to send the message to
   * @param msg Contents of the message
   *
   * @return Number of bytes sent on success, SIZE_MAX on failure
   */
  size_t send(const std::string rem_hostname, uint16_t rem_port, const T &msg) {
    if (sock_fd_ < 0) return SIZE_MAX;

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;

    struct addrinfo *results = nullptr;
    const std::string port_str = std::to_string(rem_port);
    int rc = ::getaddrinfo(rem_hostname.c_str(), port_str.c_str(), &hints,
                           &results);
    if (rc != 0 || results == nullptr) {
      ERPC_ERROR("eRPC: Failed to resolve %s, getaddrinfo error = %s.\n",
                 rem_hostname.c_str(), gai_strerror(rc));
      if (results != nullptr) ::freeaddrinfo(results);
      return SIZE_MAX;
    }

    // Pick the first IPv4 endpoint
    size_t ret = SIZE_MAX;
    size_t non_v4_count = 0;
    for (struct addrinfo *p = results; p != nullptr; p = p->ai_next) {
      if (p->ai_family != AF_INET) {
        non_v4_count++;
        continue;
      }
      ssize_t n = ::sendto(sock_fd_, &msg, sizeof(T), 0, p->ai_addr,
                           p->ai_addrlen);
      if (n < 0) {
        ERPC_ERROR("eRPC: sendto() failed to %s, error: %s\n",
                   rem_hostname.c_str(), strerror(errno));
        ret = SIZE_MAX;
      } else {
        if (enable_recording_flag_) sent_vec_.push_back(msg);
        ret = static_cast<size_t>(n);
      }
      break;
    }

    if (ret == SIZE_MAX && non_v4_count > 0) {
      ERPC_ERROR(
          "eRPC: Failed to find an IPv4 endpoint to %s. Found %zu non-IPv4 "
          "endpoints to %s though.\n",
          rem_hostname.c_str(), non_v4_count, rem_hostname.c_str());
    }

    ::freeaddrinfo(results);
    return ret;
  }

  /// Maintain a all packets sent by this client
  void enable_recording() { enable_recording_flag_ = true; }

 private:
  int sock_fd_;

  /// The list of all packets sent, maintained if recording is enabled
  std::vector<T> sent_vec_;
  bool enable_recording_flag_ = false;  /// Flag to enable recording for testing
};

}  // namespace erpc
