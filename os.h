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

#include <cstdint>
#include <cstring>
#include <cerrno>
#include <cstdlib>
#include <unordered_map>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#include <direct.h>
#include <intrin.h>
#pragma comment(lib, "ws2_32.lib")
#pragma intrinsic(_ReadWriteBarrier)
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <strings.h>
#endif

namespace tcpshm {

// ============================================================
// WSAStartup (Windows only)
// ============================================================
#ifdef _WIN32
struct WSAInitializer {
  WSAInitializer() {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
  }
  ~WSAInitializer() { WSACleanup(); }
};
static WSAInitializer wsa_init_;
#endif

// ============================================================
// Socket type abstraction
// ============================================================
#ifdef _WIN32
using tcp_socket_t = SOCKET;
constexpr tcp_socket_t INVALID_TCP_SOCKET = INVALID_SOCKET;
constexpr int TCP_MSG_NOSIGNAL = 0;
#else
using tcp_socket_t = int;
constexpr tcp_socket_t INVALID_TCP_SOCKET = -1;
constexpr int TCP_MSG_NOSIGNAL = MSG_NOSIGNAL;
#endif

// ============================================================
// Error handling
// ============================================================
#ifdef _WIN32
inline int tcp_get_last_error() { return WSAGetLastError(); }
constexpr int TCP_ERR_AGAIN = WSAEWOULDBLOCK;
#else
inline int tcp_get_last_error() { return errno; }
constexpr int TCP_ERR_AGAIN = EAGAIN;
#endif

// ============================================================
// Socket operations
// ============================================================
#ifdef _WIN32
inline tcp_socket_t tcp_socket(int af, int type, int proto) {
  return ::socket(af, type, proto);
}
inline int tcp_close(tcp_socket_t s) {
  return ::closesocket(s);
}
inline int tcp_send(tcp_socket_t s, const char* buf, int len, int flags) {
  return ::send(s, buf, len, flags);
}
inline int tcp_recv(tcp_socket_t s, char* buf, int len, int flags) {
  return ::recv(s, buf, len, flags);
}
inline tcp_socket_t tcp_accept(tcp_socket_t s, struct sockaddr* addr, socklen_t* addrlen) {
  return ::accept(s, addr, addrlen);
}
inline int tcp_bind(tcp_socket_t s, const struct sockaddr* addr, socklen_t addrlen) {
  return ::bind(s, addr, (int)addrlen);
}
inline int tcp_listen(tcp_socket_t s, int backlog) {
  return ::listen(s, backlog);
}
inline int tcp_connect(tcp_socket_t s, const struct sockaddr* addr, socklen_t addrlen) {
  return ::connect(s, addr, (int)addrlen);
}
inline int tcp_setsockopt(tcp_socket_t s, int level, int optname, const char* optval, int optlen) {
  return ::setsockopt(s, level, optname, optval, optlen);
}
inline int tcp_set_nonblocking(tcp_socket_t s) {
  u_long mode = 1;
  return ::ioctlsocket(s, FIONBIO, &mode);
}
#else
inline tcp_socket_t tcp_socket(int af, int type, int proto) {
  return ::socket(af, type, proto);
}
inline int tcp_close(tcp_socket_t s) {
  return ::close(s);
}
inline int tcp_send(tcp_socket_t s, const char* buf, int len, int flags) {
  return ::send(s, buf, len, flags);
}
inline int tcp_recv(tcp_socket_t s, char* buf, int len, int flags) {
  return ::recv(s, buf, len, flags);
}
inline tcp_socket_t tcp_accept(tcp_socket_t s, struct sockaddr* addr, socklen_t* addrlen) {
  return ::accept(s, addr, addrlen);
}
inline int tcp_bind(tcp_socket_t s, const struct sockaddr* addr, socklen_t addrlen) {
  return ::bind(s, addr, addrlen);
}
inline int tcp_listen(tcp_socket_t s, int backlog) {
  return ::listen(s, backlog);
}
inline int tcp_connect(tcp_socket_t s, const struct sockaddr* addr, socklen_t addrlen) {
  return ::connect(s, addr, addrlen);
}
inline int tcp_setsockopt(tcp_socket_t s, int level, int optname, const char* optval, int optlen) {
  return ::setsockopt(s, level, optname, (const void*)optval, (socklen_t)optlen);
}
inline int tcp_set_nonblocking(tcp_socket_t s) {
  return ::fcntl(s, F_SETFL, O_NONBLOCK);
}
#endif

// ============================================================
// Endian conversion
// ============================================================
#ifdef _WIN32
inline uint16_t tcp_bswap16(uint16_t x) { return _byteswap_ushort(x); }
inline uint32_t tcp_bswap32(uint32_t x) { return _byteswap_ulong(x); }
inline uint64_t tcp_bswap64(uint64_t x) { return _byteswap_uint64(x); }
#else
inline uint16_t tcp_bswap16(uint16_t x) { return __builtin_bswap16(x); }
inline uint32_t tcp_bswap32(uint32_t x) { return __builtin_bswap32(x); }
inline uint64_t tcp_bswap64(uint64_t x) { return __builtin_bswap64(x); }
#endif

// ============================================================
// Compiler barriers (volatile + compiler fence)
// ============================================================

// Force the compiler to read a variable from memory
template<class T>
inline T tcp_volatile_read(T& x) {
  T val;
#ifdef _WIN32
  _ReadWriteBarrier();
  val = *(const volatile T*)&x;
#else
  asm volatile("" : "=m"(val) : "m"(x) :);
  val = x;
#endif
  return val;
}

// Force the compiler to write a variable to memory
template<class T>
inline void tcp_volatile_write(T& x, const T& val) {
#ifdef _WIN32
  *(volatile T*)&x = val;
  _ReadWriteBarrier();
#else
  asm volatile("" : : "m"(x), "m"(val) :);
  x = val;
  asm volatile("" : : "m"(x) :);
#endif
}

#ifdef _WIN32
inline void tcp_compiler_barrier() { _ReadWriteBarrier(); }
#else
inline void tcp_compiler_barrier() {
  asm volatile("" : : : "memory");
}
#endif

// ============================================================
// Memory mapping
// ============================================================

// On Windows we need to store the file mapping handle for each mmap
#ifdef _WIN32
inline std::unordered_map<void*, HANDLE>& get_mmap_handles() {
  static std::unordered_map<void*, HANDLE> handles;
  return handles;
}

// Strip leading '/' from SHM names on Windows
inline const char* shm_name_win(const char* name) {
  return (*name == '/') ? name + 1 : name;
}
#endif

template<class T>
T* my_mmap(const char* filename, bool use_shm, const char** error_msg) {
#ifdef _WIN32
  HANDLE hMap;
  if (use_shm) {
    hMap = CreateFileMappingA(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE,
                              0, sizeof(T), shm_name_win(filename));
  }
  else {
    HANDLE hFile = CreateFileA(filename, GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE,
                                NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
      *error_msg = "CreateFile";
      return nullptr;
    }
    hMap = CreateFileMappingA(hFile, NULL, PAGE_READWRITE, 0, sizeof(T), NULL);
    CloseHandle(hFile);
  }
  if (hMap == NULL) {
    *error_msg = "CreateFileMapping";
    return nullptr;
  }
  T* ret = (T*)MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(T));
  if (ret == NULL) {
    *error_msg = "MapViewOfFile";
    CloseHandle(hMap);
    return nullptr;
  }
  get_mmap_handles()[ret] = hMap;
  return ret;
#else
  int fd = -1;
  if (use_shm) {
    fd = shm_open(filename, O_CREAT | O_RDWR, 0666);
  }
  else {
    fd = open(filename, O_CREAT | O_RDWR, 0644);
  }
  if (fd == -1) {
    *error_msg = "open";
    return nullptr;
  }
  if (ftruncate(fd, sizeof(T))) {
    *error_msg = "ftruncate";
    close(fd);
    return nullptr;
  }
  T* ret = (T*)mmap(0, sizeof(T), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (ret == MAP_FAILED) {
    *error_msg = "mmap";
    return nullptr;
  }
  return ret;
#endif
}

template<class T>
void my_munmap(void* addr) {
#ifdef _WIN32
  if (addr) {
    UnmapViewOfFile(addr);
    auto& handles = get_mmap_handles();
    auto it = handles.find(addr);
    if (it != handles.end()) {
      CloseHandle(it->second);
      handles.erase(it);
    }
  }
#else
  munmap(addr, sizeof(T));
#endif
}

// ============================================================
// Directory creation
// ============================================================
#ifdef _WIN32
inline int tcp_mkdir(const char* path) {
  return ::_mkdir(path);
}
#else
inline int tcp_mkdir(const char* path) {
  return ::mkdir(path, 0755);
}
#endif

} // namespace tcpshm
