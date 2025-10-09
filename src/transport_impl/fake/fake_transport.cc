#ifdef ERPC_FAKE
#include "fake_transport.h"
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <chrono>
#include <stdexcept>

namespace erpc {

constexpr size_t FakeTransport::kMaxDataPerPkt;

FakeTransport::FakeTransport(uint16_t sm_udp_port, uint8_t rpc_id, 
                            uint8_t phy_port, size_t numa_node, 
                            FILE *trace_file)
    : Transport(TransportType::kFake, rpc_id, phy_port, numa_node, trace_file),
      socket_fd_(-1), local_port_(sm_udp_port + 10000 + rpc_id), rx_thread_(nullptr), 
      stop_rx_thread_(false), rx_ring_(nullptr), rx_tail_(0) {
  
  // Resolve local IP address for socket communication
  resolve_local_ip_address();
  
  // Create UDP socket
  socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
  if (socket_fd_ < 0) {
    throw std::runtime_error("FakeTransport: Failed to create socket: " + 
                           std::string(strerror(errno)));
  }

  // Set socket options
  int reuse = 1;
  if (setsockopt(socket_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    close(socket_fd_);
    throw std::runtime_error("FakeTransport: Failed to set SO_REUSEADDR: " + 
                           std::string(strerror(errno)));
  }

  // Bind socket to local address
  memset(&local_addr_, 0, sizeof(local_addr_));
  local_addr_.sin_family = AF_INET;
  local_addr_.sin_addr.s_addr = INADDR_ANY;
  local_addr_.sin_port = htons(local_port_);

  if (bind(socket_fd_, reinterpret_cast<struct sockaddr*>(&local_addr_), 
           sizeof(local_addr_)) < 0) {
    close(socket_fd_);
    throw std::runtime_error("FakeTransport: Failed to bind socket: " + 
                           std::string(strerror(errno)));
  }

  // Set non-blocking mode
  int flags = fcntl(socket_fd_, F_GETFL, 0);
  if (fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
    close(socket_fd_);
    throw std::runtime_error("FakeTransport: Failed to set non-blocking: " + 
                           std::string(strerror(errno)));
  }

  // Initialize memory registration functions
  init_mem_reg_funcs();
}

FakeTransport::~FakeTransport() {
  cleanup_rx_thread();
  
  if (socket_fd_ >= 0) {
    close(socket_fd_);
  }

  // Clean up any remaining packets in queue
  std::lock_guard<std::mutex> lock(rx_queue_mutex_);
  while (!rx_packet_queue_.empty()) {
    free(rx_packet_queue_.front().first);
    rx_packet_queue_.pop();
  }
}

void FakeTransport::init_mem_reg_funcs() {
  // For fake transport using UDP sockets, we don't need actual memory registration
  // Provide dummy functions that return valid but unused values
  reg_mr_func_ = [](void* /* buf */, size_t /* size */) -> Transport::mem_reg_info {
    return Transport::mem_reg_info(nullptr, 0);
  };
  
  dereg_mr_func_ = [](Transport::mem_reg_info /* mr_info */) {
    // Nothing to do for fake transport
  };
}

void FakeTransport::init_hugepage_structures(HugeAlloc *huge_alloc, 
                                            uint8_t **rx_ring) {
  huge_alloc_ = huge_alloc;
  
  // Store pointer to eRPC's rx_ring array for direct access
  rx_ring_ = rx_ring;
  
  // Initialize all ring entries to null
  for (size_t i = 0; i < kNumRxRingEntries; i++) {
    rx_ring_[i] = nullptr;
  }
  
  // Start receive thread
  stop_rx_thread_ = false;
  rx_thread_ = new std::thread(&FakeTransport::rx_thread_func, this);
}

void FakeTransport::fill_local_routing_info(routing_info_t *routing_info) const {
  auto *socket_ri = reinterpret_cast<socket_routing_info_t*>(routing_info->buf_);
  // Use the resolved local IP address
  socket_ri->ipv4_addr = local_ipv4_addr_;
  socket_ri->udp_port = local_port_;
  socket_ri->padding = 0;
  printf("DEBUG: fill_local_routing_info: ipv4_addr=0x%x, udp_port=%u\n", 
         socket_ri->ipv4_addr, socket_ri->udp_port);
  fflush(stdout);
}

// Resolve the port information from the remote server you plan to send to
bool FakeTransport::resolve_remote_routing_info(routing_info_t *routing_info) {
  // Socket routing info is already in the correct format
  // Just validate that we can parse it
  auto *socket_ri = reinterpret_cast<socket_routing_info_t*>(routing_info->buf_);
  bool result = (socket_ri->ipv4_addr != 0 && socket_ri->udp_port != 0);
  printf("DEBUG: resolve_remote_routing_info: ipv4_addr=0x%x, udp_port=%u, result=%d\n", 
         socket_ri->ipv4_addr, socket_ri->udp_port, result);
  fflush(stdout);
  return result;
}

size_t FakeTransport::get_bandwidth() const {
  return 0; // Not implemented for socket
}

std::string FakeTransport::routing_info_str(routing_info_t *routing_info) {
  auto *socket_ri = reinterpret_cast<socket_routing_info_t*>(routing_info->buf_);
  struct in_addr addr;
  addr.s_addr = socket_ri->ipv4_addr;
  return std::string(inet_ntoa(addr)) + ":" + std::to_string(socket_ri->udp_port);
}

void FakeTransport::tx_burst(const tx_burst_item_t *tx_burst_arr, size_t num_pkts) {
  for (size_t i = 0; i < num_pkts; i++) {
    const auto &item = tx_burst_arr[i];
    
    if (item.drop_) continue; // Skip dropped packets

    // Get routing info
    auto *socket_ri = reinterpret_cast<socket_routing_info_t*>(item.routing_info_->buf_);
    
    // Set up destination address
    struct sockaddr_in dest_addr;
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_addr.s_addr = socket_ri->ipv4_addr;
    dest_addr.sin_port = htons(socket_ri->udp_port);
    
    // Get packet data from MsgBuffer
    pkthdr_t *pkthdr;
    if (item.pkt_idx_ == 0) {
      pkthdr = item.msg_buffer_->get_pkthdr_0();
    } else {
      pkthdr = item.msg_buffer_->get_pkthdr_n(item.pkt_idx_);
    }
    uint8_t *pkt_buf = reinterpret_cast<uint8_t*>(pkthdr);
    size_t pkt_size = item.msg_buffer_->get_pkt_size<kMaxDataPerPkt>(item.pkt_idx_);
    
    // Send packet
    ssize_t bytes_sent = sendto(socket_fd_, pkt_buf, pkt_size, MSG_DONTWAIT,
                               reinterpret_cast<struct sockaddr*>(&dest_addr),
                               sizeof(dest_addr));
    
    if (bytes_sent < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        if (trace_file_ != nullptr) {
          fprintf(trace_file_, "FakeTransport: Send error: %s\n", strerror(errno));
        }
      }
    }
  }
}

void FakeTransport::tx_flush() {
  // Nothing to do for UDP sockets - packets are sent immediately
}

size_t FakeTransport::rx_burst() {
  std::lock_guard<std::mutex> lock(rx_queue_mutex_);
  
  size_t packets_processed = 0;
  //printf("DEBUG: rx_burst called, queue_size=%zu\n", rx_packet_queue_.size());
  //fflush(stdout);
  
  while (!rx_packet_queue_.empty() && packets_processed < kPostlist) {
    auto pkt_info = rx_packet_queue_.front();
    rx_packet_queue_.pop();
    
    uint8_t *pkt_data = pkt_info.first;
    size_t pkt_size = pkt_info.second;
    
    // Store packet pointer directly in eRPC's RX ring
    size_t ring_index = rx_tail_ % kNumRxRingEntries;
    rx_ring_[ring_index] = pkt_data;
    rx_tail_++;
    
    packets_processed++;
  }
  
  return packets_processed;
}

void FakeTransport::post_recvs(size_t /* num_recvs */) {
  // Nothing to do - receive thread handles this automatically
}

void FakeTransport::rx_thread_func() {
  uint8_t buffer[kMTU];
  struct sockaddr_in sender_addr;
  socklen_t sender_len = sizeof(sender_addr);
  
  while (!stop_rx_thread_) {
    ssize_t bytes_received = recvfrom(socket_fd_, buffer, sizeof(buffer),
                                     MSG_DONTWAIT, 
                                     reinterpret_cast<struct sockaddr*>(&sender_addr),
                                     &sender_len);
    
    if (bytes_received > 0) {
      // Allocate memory for packet copy
      uint8_t *pkt_copy = static_cast<uint8_t*>(malloc(bytes_received));
      if (pkt_copy != nullptr) {
        memcpy(pkt_copy, buffer, bytes_received);
        
        // Add to receive queue
        std::lock_guard<std::mutex> lock(rx_queue_mutex_);
        rx_packet_queue_.push(std::make_pair(pkt_copy, bytes_received));
      }
    } else if (bytes_received < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        if (trace_file_ != nullptr) {
          fprintf(trace_file_, "FakeTransport: Receive error: %s\n", strerror(errno));
        }
      }
    }
    
    // Small delay to avoid busy waiting
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
}

void FakeTransport::cleanup_rx_thread() {
  if (rx_thread_ != nullptr) {
    stop_rx_thread_ = true;
    rx_thread_->join();
    delete rx_thread_;
    rx_thread_ = nullptr;
  }
  
  if (rx_ring_ != nullptr) {
    // Free any remaining packets in the ring
    for (size_t i = 0; i < kNumRxRingEntries; i++) {
      if (rx_ring_[i] != nullptr) {
        free(rx_ring_[i]);
      }
    }
    rx_ring_ = nullptr;
  }
}

void FakeTransport::resolve_local_ip_address() {
  // For socket-based transport, we determine local IP by connecting to a remote address
  // This lets the OS choose the best local interface/IP automatically
  int temp_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (temp_sock < 0) {
    local_ipv4_addr_ = inet_addr("127.0.0.1");
    return;
  }
  
  // Connect to a public DNS server (doesn't actually send data for UDP)
  struct sockaddr_in remote_addr;
  memset(&remote_addr, 0, sizeof(remote_addr));
  remote_addr.sin_family = AF_INET;
  remote_addr.sin_addr.s_addr = inet_addr("8.8.8.8");
  remote_addr.sin_port = htons(53);
  
  if (connect(temp_sock, (struct sockaddr*)&remote_addr, sizeof(remote_addr)) < 0) {
    close(temp_sock);
    local_ipv4_addr_ = inet_addr("127.0.0.1");
    return;
  }
  
  // Get the local address chosen by the OS
  struct sockaddr_in local_addr;
  socklen_t addr_len = sizeof(local_addr);
  if (getsockname(temp_sock, (struct sockaddr*)&local_addr, &addr_len) < 0) {
    close(temp_sock);
    local_ipv4_addr_ = inet_addr("127.0.0.1");
    return;
  }
  
  close(temp_sock);
  local_ipv4_addr_ = local_addr.sin_addr.s_addr;
}

}  // namespace erpc

#endif
