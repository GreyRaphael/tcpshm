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

#include "tcpshm/detail/os_socket.hpp"
#include "tcpshm/error.hpp"
#include <cerrno>
#include <cstdint>
#include <type_traits>

#ifdef _WIN32
#include <mutex>
#include <unordered_map>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace tcpshm::detail {

#ifdef _WIN32
struct MMapManager
{
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
        return nullptr;
    }
};

inline MMapManager& get_mmap_manager() {
    static auto* manager = new MMapManager();
    return *manager;
}

inline const char* shm_name_win(const char* name) noexcept {
    if (!name) return "";
    return (*name == '/') ? name + 1 : name;
}
#else
inline void* posix_mmap_fd(int fd, size_t len) noexcept {
#ifdef MAP_POPULATE
    void* ret = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, 0);
    if (ret == MAP_FAILED) {
        ret = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    }
    return ret;
#else
    return ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
#endif
}
#endif

template<class T>
    requires (std::is_trivial_v<T> && std::is_standard_layout_v<T>)
[[nodiscard]] Result<T*> mmap_create_or_open(const char* filename, bool use_shm, bool excl = false) noexcept {
#ifdef _WIN32
    constexpr uint64_t map_size = sizeof(T);
    const DWORD size_high = static_cast<DWORD>(map_size >> 32);
    const DWORD size_low = static_cast<DWORD>(map_size & 0xFFFFFFFFULL);
    HANDLE h_map = nullptr;
    if (use_shm) {
        h_map = ::CreateFileMappingA(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
                                     size_high, size_low, shm_name_win(filename));
        if (h_map != nullptr && excl && ::GetLastError() == ERROR_ALREADY_EXISTS) {
            ::CloseHandle(h_map);
            return make_error(ErrorCode::ShmAlreadyExists, static_cast<int>(ERROR_ALREADY_EXISTS));
        }
        if (h_map == nullptr) {
            return make_error(ErrorCode::OpenFileFailed, static_cast<int>(::GetLastError()));
        }
    } else {
        HANDLE h_file = ::CreateFileA(filename, GENERIC_READ | GENERIC_WRITE,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE,
                                      nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h_file == INVALID_HANDLE_VALUE) {
            return make_error(ErrorCode::OpenFileFailed, static_cast<int>(::GetLastError()));
        }
        h_map = ::CreateFileMappingA(h_file, nullptr, PAGE_READWRITE, size_high, size_low, nullptr);
        DWORD map_err = ::GetLastError();
        ::CloseHandle(h_file);
        if (h_map == nullptr) {
            return make_error(ErrorCode::MmapFailed, static_cast<int>(map_err));
        }
    }
    void* raw = ::MapViewOfFile(h_map, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(T));
    if (raw == nullptr) {
        DWORD view_err = ::GetLastError();
        ::CloseHandle(h_map);
        return make_error(ErrorCode::MmapFailed, static_cast<int>(view_err));
    }
    get_mmap_manager().add(raw, h_map);
    return static_cast<T*>(raw);
#else
    int fd = -1;
    bool created = false;
    if (use_shm) {
        fd = ::shm_open(filename, O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd != -1) {
            created = true;
        } else if (excl && errno == EEXIST) {
            return make_error(ErrorCode::ShmAlreadyExists, EEXIST);
        } else if (!excl && errno == EEXIST) {
            fd = ::shm_open(filename, O_RDWR, 0600);
        }
    } else {
        fd = ::open(filename, O_CREAT | O_RDWR, 0600);
    }
    if (fd == -1) {
        return make_error(ErrorCode::OpenFileFailed, errno);
    }

    bool need_truncate = !use_shm || created;
    if (!need_truncate) {
        struct stat st{};
        if (::fstat(fd, &st) != 0) {
            int saved = errno;
            ::close(fd);
            return make_error(ErrorCode::StatFileFailed, saved);
        }
        need_truncate = static_cast<uint64_t>(st.st_size) < sizeof(T);
    }
    if (need_truncate && ::ftruncate(fd, static_cast<off_t>(sizeof(T))) != 0) {
        int saved = errno;
        ::close(fd);
        if (use_shm && created) ::shm_unlink(filename);
        return make_error(ErrorCode::TruncateFileFailed, saved);
    }
    void* raw = posix_mmap_fd(fd, sizeof(T));
    int mmap_err = errno;
    ::close(fd);
    if (raw == MAP_FAILED) {
        if (use_shm && created) ::shm_unlink(filename);
        return make_error(ErrorCode::MmapFailed, mmap_err);
    }
    return static_cast<T*>(raw);
#endif
}

template<class T>
    requires (std::is_trivial_v<T> && std::is_standard_layout_v<T>)
[[nodiscard]] Result<T*> mmap_existing(const char* filename, bool use_shm) noexcept {
#ifdef _WIN32
    constexpr uint64_t map_size = sizeof(T);
    const DWORD size_high = static_cast<DWORD>(map_size >> 32);
    const DWORD size_low = static_cast<DWORD>(map_size & 0xFFFFFFFFULL);
    HANDLE h_map = nullptr;
    if (use_shm) {
        h_map = ::OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, shm_name_win(filename));
        if (h_map == nullptr) {
            return make_error(ErrorCode::OpenFileFailed, static_cast<int>(::GetLastError()));
        }
    } else {
        HANDLE h_file = ::CreateFileA(filename, GENERIC_READ | GENERIC_WRITE,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE,
                                      nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h_file == INVALID_HANDLE_VALUE) {
            return make_error(ErrorCode::OpenFileFailed, static_cast<int>(::GetLastError()));
        }
        LARGE_INTEGER file_size{};
        if (!::GetFileSizeEx(h_file, &file_size) || file_size.QuadPart < 0 ||
            static_cast<uint64_t>(file_size.QuadPart) < sizeof(T)) {
            DWORD sz_err = ::GetLastError();
            ::CloseHandle(h_file);
            return make_error(ErrorCode::StatFileFailed, static_cast<int>(sz_err ? sz_err : ERROR_HANDLE_EOF));
        }
        h_map = ::CreateFileMappingA(h_file, nullptr, PAGE_READWRITE, size_high, size_low, nullptr);
        DWORD map_err = ::GetLastError();
        ::CloseHandle(h_file);
        if (h_map == nullptr) {
            return make_error(ErrorCode::MmapFailed, static_cast<int>(map_err));
        }
    }
    void* raw = ::MapViewOfFile(h_map, FILE_MAP_ALL_ACCESS, 0, 0, sizeof(T));
    if (raw == nullptr) {
        DWORD view_err = ::GetLastError();
        ::CloseHandle(h_map);
        return make_error(ErrorCode::MmapFailed, static_cast<int>(view_err));
    }
    get_mmap_manager().add(raw, h_map);
    return static_cast<T*>(raw);
#else
    int fd = use_shm ? ::shm_open(filename, O_RDWR, 0600) : ::open(filename, O_RDWR, 0600);
    if (fd == -1) {
        return make_error(ErrorCode::OpenFileFailed, errno);
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        int saved = errno;
        ::close(fd);
        return make_error(ErrorCode::StatFileFailed, saved);
    }
    if (st.st_size < 0 || static_cast<uint64_t>(st.st_size) < sizeof(T)) {
        ::close(fd);
        return make_error(ErrorCode::StatFileFailed, EINVAL);
    }
    void* raw = posix_mmap_fd(fd, sizeof(T));
    int mmap_err = errno;
    ::close(fd);
    if (raw == MAP_FAILED) {
        return make_error(ErrorCode::MmapFailed, mmap_err);
    }
    return static_cast<T*>(raw);
#endif
}

template<class T>
void munmap_region(T* addr) noexcept {
#ifdef _WIN32
    if (addr) {
        ::UnmapViewOfFile(addr);
        HANDLE h = get_mmap_manager().remove(addr);
        if (h) ::CloseHandle(h);
    }
#else
    if (addr && addr != reinterpret_cast<T*>(MAP_FAILED)) {
        ::munmap(addr, sizeof(T));
    }
#endif
}

inline int shm_unlink_name(const char* filename) noexcept {
#ifdef _WIN32
    (void)filename;
    return 0;
#else
    return ::shm_unlink(filename);
#endif
}

} // namespace tcpshm::detail
