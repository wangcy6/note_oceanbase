#pragma once
#include <sys/socket.h>
#include <arpa/inet.h>

typedef struct addr_t {
  uint32_t ip;
  uint16_t port;
  uint16_t tid;
} addr_t;
extern const char* addr_str(format_t* f, addr_t addr);
extern addr_t addr_build(const char* ip, int port);
extern addr_t* addr_init(addr_t* addr, const char* ip, int port);
extern addr_t* addr_set(addr_t* addr, uint32_t ip, uint16_t port, uint16_t tid);

extern struct sockaddr_in* make_sockaddr(struct sockaddr_in *sin, addr_t addr);
extern addr_t get_remote_addr(int fd);
extern addr_t get_local_addr(int fd);
