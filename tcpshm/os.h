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
#include <atomic>
#include <span>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#include <windows.h>
#include <direct.h>
#include <intrin.h>
#include <unordered_map>
#include <mutex>
#pragma comment(lib, "ws2_32.lib")
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

inline void ensure_wsa_initialized() {
  static WSAInitializer wsa_init;
  (void)wsa_init;
}
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
inline bool tcp_is_again(int err) { return err == WSAEWOULDBLOCK; }
inline bool tcp_is_retryable_err(int err) { return err == WSAEWOULDBLOCK || err == WSAEINTR; }
#else
inline int tcp_get_last_error() { return errno; }
constexpr int TCP_ERR_AGAIN = EAGAIN;
inline bool tcp_is_again(int err) { return err == EAGAIN || err == EWOULDBLOCK; }
inline bool tcp_is_retryable_err(int err) { return err == EAGAIN || err == EWOULDBLOCK || err == EINTR; }
#endif

// ============================================================
// Socket operations
// ============================================================
#ifdef _WIN32
inline tcp_socket_t tcp_socket(int af, int type, int proto) {
  ensure_wsa_initialized();
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

inline int tcp_set_blocking(tcp_socket_t s) {
  u_long mode = 0;
  return ::ioctlsocket(s, FIONBIO, &mode);
}

inline int tcp_set_timeout(tcp_socket_t s, int level, int optname, int timeout_ms) {
  if (timeout_ms < 0) return -1;
  DWORD dwTimeout = (DWORD)timeout_ms;
  return ::setsockopt(s, level, optname, (const char*)&dwTimeout, sizeof(dwTimeout));
}

#else
inline tcp_socket_t tcp_socket(int af, int type, int proto) {
  return ::socket(af, type, proto);
}
inline int tcp_close(tcp_socket_t s) {
  return ::close(s);
}
inline int tcp_send(tcp_socket_t s, const char* buf, int len, int flags) {
  return static_cast<int>(::send(s, buf, static_cast<size_t>(len), flags));
}
inline int tcp_recv(tcp_socket_t s, char* buf, int len, int flags) {
  return static_cast<int>(::recv(s, buf, static_cast<size_t>(len), flags));
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
  int flags = ::fcntl(s, F_GETFL, 0);
  if (flags < 0) return flags;
  return ::fcntl(s, F_SETFL, flags | O_NONBLOCK);
}

inline int tcp_set_blocking(tcp_socket_t s) {
  int flags = ::fcntl(s, F_GETFL, 0);
  if (flags < 0) return flags;
  return ::fcntl(s, F_SETFL, flags & ~O_NONBLOCK);
}

inline int tcp_set_timeout(tcp_socket_t s, int level, int optname, int timeout_ms) {
  if (timeout_ms < 0) return -1;
  struct timeval tv;
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  return ::setsockopt(s, level, optname, (const void*)&tv, sizeof(tv));
}
#endif

#ifdef _WIN32
struct iovec {
    void* iov_base;
    size_t iov_len;
};

inline int tcp_readv(tcp_socket_t s, std::span<struct iovec> vecs) {
  WSABUF bufs[16];
  int iovcnt = (int)vecs.size();
  if (iovcnt > 16) iovcnt = 16;
  for (int i = 0; i < iovcnt; ++i) {
    bufs[i].buf = (char*)vecs[i].iov_base;
    bufs[i].len = (ULONG)vecs[i].iov_len;
  }
  DWORD received = 0;
  DWORD flags = 0;
  int ret = WSARecv(s, bufs, iovcnt, &received, &flags, NULL, NULL);
  if (ret == 0) return (int)received;
  return -1;
}
#else
inline int tcp_readv(tcp_socket_t s, std::span<struct iovec> vecs) {
  return static_cast<int>(::readv(s, vecs.data(), static_cast<int>(vecs.size())));
}
#endif

inline bool tcp_send_all(tcp_socket_t s, const void* buf, int len, int flags, int* sys_errno) {
  if (len < 0) {
    if (sys_errno) *sys_errno = EINVAL;
    return false;
  }
  const char* p = static_cast<const char*>(buf);
  int sent_total = 0;
  while (sent_total < len) {
    int ret = tcp_send(s, p + sent_total, len - sent_total, flags);
    if (ret < 0) {
      int err = tcp_get_last_error();
#ifndef _WIN32
      if (err == EINTR) continue;
#else
      if (err == WSAEINTR) continue;
#endif
      if (sys_errno) *sys_errno = err;
      return false;
    }
    if (ret == 0) {
      if (sys_errno) *sys_errno = 0;
      return false;
    }
    sent_total += ret;
  }
  return true;
}

inline bool tcp_recv_all(tcp_socket_t s, void* buf, int len, int* sys_errno) {
  if (len < 0) {
    if (sys_errno) *sys_errno = EINVAL;
    return false;
  }
  char* p = static_cast<char*>(buf);
  int recv_total = 0;
  while (recv_total < len) {
    int ret = tcp_recv(s, p + recv_total, len - recv_total, 0);
    if (ret < 0) {
      int err = tcp_get_last_error();
#ifndef _WIN32
      if (err == EINTR) continue;
#else
      if (err == WSAEINTR) continue;
#endif
      if (sys_errno) *sys_errno = err;
      return false;
    }
    if (ret == 0) {
      if (sys_errno) *sys_errno = 0;
      return false;
    }
    recv_total += ret;
  }
  return true;
}

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
// Synchronization primitives
// ============================================================

inline void tcp_compiler_barrier() {
  std::atomic_signal_fence(std::memory_order_acq_rel);
}

// ============================================================
// Memory mapping
// ============================================================

// On Windows we need to store the file mapping handle for each mmap
#ifdef _WIN32
struct MMapManager {
  std::unordered_map<void*, HANDLE> handles;
  std::mutex mutex;

  void add(void* addr, HANDLE h) {
    std::lock_guard<std::mutex> lock(mutex);
    handles[addr] = h;
  }

  HANDLE remove(void* addr) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = handles.find(addr);
    if (it != handles.end()) {
      HANDLE h = it->second;
      handles.erase(it);
      return h;
    }
    return NULL;
  }
};

inline MMapManager& get_mmap_manager() {
  static auto* manager = new MMapManager();
  return *manager;
}

// Strip leading '/' from SHM names on Windows
inline const char* shm_name_win(const char* name) {
  if (!name) return "";
  return (*name == '/') ? name + 1 : name;
}
#endif

template<class T>
T* my_mmap(const char* filename, bool use_shm, const char** error_msg, bool excl = false) {
#ifdef _WIN32
  constexpr uint64_t map_size = sizeof(T);
  const DWORD size_high = static_cast<DWORD>(map_size >> 32);
  const DWORD size_low = static_cast<DWORD>(map_size & 0xFFFFFFFFULL);
  HANDLE hMap;
  if (use_shm) {
    hMap = CreateFileMappingA(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE,
                              size_high, size_low, shm_name_win(filename));
    if (hMap != NULL && excl && GetLastError() == ERROR_ALREADY_EXISTS) {
      CloseHandle(hMap);
      if (error_msg) *error_msg = "CreateFileMapping already exists";
      SetLastError(ERROR_ALREADY_EXISTS);
      return nullptr;
    }
  }
  else {
    HANDLE hFile = CreateFileA(filename, GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE,
                                NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
      if (error_msg) *error_msg = "CreateFile";
      return nullptr;
    }
    hMap = CreateFileMappingA(hFile, NULL, PAGE_READWRITE, size_high, size_low, NULL);
    DWORD map_err = GetLastError();
    CloseHandle(hFile);
    if (hMap == NULL) SetLastError(map_err);
  }
  if (hMap == NULL) {
    if (error_msg) *error_msg = "CreateFileMapping";
    return nullptr;
  }
  T* ret = (T*)MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(T));
  if (ret == NULL) {
    DWORD view_err = GetLastError();
    if (error_msg) *error_msg = "MapViewOfFile";
    CloseHandle(hMap);
    SetLastError(view_err);
    return nullptr;
  }
  get_mmap_manager().add(ret, hMap);
  return ret;
#else
  int fd = -1;
  bool created = false;
  if (use_shm) {
    fd = shm_open(filename, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd != -1) {
      created = true;
    }
    else if (!excl && errno == EEXIST) {
      fd = shm_open(filename, O_RDWR, 0600);
    }
  }
  else {
    fd = open(filename, O_CREAT | O_RDWR, 0600);
  }
  if (fd == -1) {
    if (error_msg) *error_msg = "open";
    return nullptr;
  }
  bool need_truncate = !use_shm || created;
  if (!need_truncate) {
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
      int saved_errno = errno;
      if (error_msg) *error_msg = "fstat";
      close(fd);
      errno = saved_errno;
      return nullptr;
    }
    need_truncate = static_cast<uint64_t>(st.st_size) < sizeof(T);
  }
  if (need_truncate && ftruncate(fd, static_cast<off_t>(sizeof(T))) != 0) {
    int saved_errno = errno;
    if (error_msg) *error_msg = "ftruncate";
    close(fd);
    if (use_shm && created) shm_unlink(filename);
    errno = saved_errno;
    return nullptr;
  }
  T* ret = (T*)mmap(0, sizeof(T), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  int mmap_errno = errno;
  close(fd);
  if (ret == MAP_FAILED) {
    if (error_msg) *error_msg = "mmap";
    if (use_shm && created) shm_unlink(filename);
    errno = mmap_errno;
    return nullptr;
  }
  return ret;
#endif
}

template<class T>
T* my_mmap_existing(const char* filename, bool use_shm, const char** error_msg) {
#ifdef _WIN32
  constexpr uint64_t map_size = sizeof(T);
  const DWORD size_high = static_cast<DWORD>(map_size >> 32);
  const DWORD size_low = static_cast<DWORD>(map_size & 0xFFFFFFFFULL);
  HANDLE hMap;
  if (use_shm) {
    hMap = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, shm_name_win(filename));
  }
  else {
    HANDLE hFile = CreateFileA(filename, GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE,
                                NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
      if (error_msg) *error_msg = "CreateFile";
      return nullptr;
    }
    LARGE_INTEGER file_size{};
    if (!GetFileSizeEx(hFile, &file_size) || file_size.QuadPart < 0 ||
        static_cast<uint64_t>(file_size.QuadPart) < sizeof(T)) {
      DWORD sz_err = GetLastError();
      if (error_msg) *error_msg = "GetFileSizeEx";
      CloseHandle(hFile);
      SetLastError(sz_err ? sz_err : ERROR_HANDLE_EOF);
      return nullptr;
    }
    hMap = CreateFileMappingA(hFile, NULL, PAGE_READWRITE, size_high, size_low, NULL);
    DWORD map_err = GetLastError();
    CloseHandle(hFile);
    if (hMap == NULL) SetLastError(map_err);
  }
  if (hMap == NULL) {
    if (error_msg) *error_msg = "OpenFileMapping";
    return nullptr;
  }
  T* ret = (T*)MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(T));
  if (ret == NULL) {
    DWORD view_err = GetLastError();
    if (error_msg) *error_msg = "MapViewOfFile";
    CloseHandle(hMap);
    SetLastError(view_err);
    return nullptr;
  }
  get_mmap_manager().add(ret, hMap);
  return ret;
#else
  int fd = -1;
  if (use_shm) {
    fd = shm_open(filename, O_RDWR, 0600);
  }
  else {
    fd = open(filename, O_RDWR, 0600);
  }
  if (fd == -1) {
    if (error_msg) *error_msg = "open";
    return nullptr;
  }
  struct stat st{};
  if (::fstat(fd, &st) != 0) {
    int saved_errno = errno;
    if (error_msg) *error_msg = "fstat";
    close(fd);
    errno = saved_errno;
    return nullptr;
  }
  if (st.st_size < 0 || static_cast<uint64_t>(st.st_size) < sizeof(T)) {
    if (error_msg) *error_msg = "fstat";
    close(fd);
    errno = EINVAL;
    return nullptr;
  }
  T* ret = (T*)mmap(0, sizeof(T), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  int mmap_errno = errno;
  close(fd);
  if (ret == MAP_FAILED) {
    if (error_msg) *error_msg = "mmap";
    errno = mmap_errno;
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
    HANDLE h = get_mmap_manager().remove(addr);
    if (h) CloseHandle(h);
  }
#else
  if (addr && addr != MAP_FAILED) {
    munmap(addr, sizeof(T));
  }
#endif
}

inline int my_shm_unlink(const char* filename) {
#ifdef _WIN32
  (void)filename;
  return 0;
#else
  return shm_unlink(filename);
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
