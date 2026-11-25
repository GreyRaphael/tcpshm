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
#include "msg_header.h"
#include "os.h"
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>

namespace tcpshm {

template<uint32_t Bytes>
class SPSCVarQueue
{
public:
  static constexpr uint32_t BLK_SZ = 64;
  static constexpr uint32_t BLK_CNT = Bytes / BLK_SZ;
  static constexpr uint32_t HDR_SZ = static_cast<uint32_t>(sizeof(MsgHeader));
  static_assert(Bytes % BLK_SZ == 0, "Bytes must be a multiple of 64");
  static_assert(BLK_CNT && std::has_single_bit(BLK_CNT), "BLK_CNT must be a power of 2");
  static_assert(std::atomic<uint32_t>::is_always_lock_free, "std::atomic<uint32_t> must be lock-free");

  SPSCVarQueue() = default;
  ~SPSCVarQueue() = default;
  SPSCVarQueue(const SPSCVarQueue&) = delete;
  SPSCVarQueue& operator=(const SPSCVarQueue&) = delete;
  SPSCVarQueue(SPSCVarQueue&&) = delete;
  SPSCVarQueue& operator=(SPSCVarQueue&&) = delete;

  MsgHeader* Alloc(uint16_t size) {
    pending_blk_sz_ = 0;
    if (size > 65535u - HDR_SZ) return nullptr;
    uint32_t total_size = static_cast<uint32_t>(size) + HDR_SZ;
    uint32_t blk_sz = (total_size + BLK_SZ - 1) / BLK_SZ;
    if (blk_sz > BLK_CNT) return nullptr;
    uint32_t local_write_idx = write_idx.load(std::memory_order_relaxed);
    uint32_t padding_sz = BLK_CNT - (local_write_idx & (BLK_CNT - 1));
    bool rewind = blk_sz > padding_sz;
    // min_read_idx could be a negtive value which results in a large unsigned int
    uint32_t min_read_idx = local_write_idx + blk_sz + (rewind ? padding_sz : 0) - BLK_CNT;
    if (static_cast<int32_t>(read_idx_cach - min_read_idx) < 0) {
      read_idx_cach = read_idx.load(std::memory_order_acquire);
      if (static_cast<int32_t>(read_idx_cach - min_read_idx) < 0) { // no enough space
        return nullptr;
      }
    }
    if (rewind) {
      new (buf_ + static_cast<size_t>(local_write_idx & (BLK_CNT - 1)) * BLK_SZ) MsgHeader{0, 0, 0};
      local_write_idx += padding_sz;
    }
    MsgHeader* header =
        new (buf_ + static_cast<size_t>(local_write_idx & (BLK_CNT - 1)) * BLK_SZ)
            MsgHeader{static_cast<uint16_t>(total_size), 0, 0};
    pending_write_idx_ = local_write_idx + blk_sz;
    pending_blk_sz_ = blk_sz;
    return header;
  }

  void Push() {
    if (pending_blk_sz_ == 0) return;
    write_idx.store(pending_write_idx_, std::memory_order_release);
    pending_blk_sz_ = 0;
  }

  MsgHeader* Front() {
    uint32_t local_write_idx = write_idx.load(std::memory_order_acquire);
    uint32_t local_read_idx = read_idx.load(std::memory_order_relaxed);
    if (local_read_idx == local_write_idx) {
      return nullptr;
    }
    uint32_t slot = local_read_idx & (BLK_CNT - 1);
    MsgHeader hdr;
    std::memcpy(&hdr, buf_ + static_cast<size_t>(slot) * BLK_SZ, sizeof(MsgHeader));
    if (hdr.size == 0) { // rewind
      local_read_idx += BLK_CNT - slot;
      read_idx.store(local_read_idx, std::memory_order_release);
      if (local_read_idx == local_write_idx) {
        return nullptr;
      }
      slot = local_read_idx & (BLK_CNT - 1);
      std::memcpy(&hdr, buf_ + static_cast<size_t>(slot) * BLK_SZ, sizeof(MsgHeader));
    }
    if (hdr.size < sizeof(MsgHeader)) {
      return nullptr;
    }
    uint32_t blk_sz = (static_cast<uint32_t>(hdr.size) + BLK_SZ - 1) / BLK_SZ;
    uint32_t avail_blks = local_write_idx - local_read_idx;
    if (blk_sz > avail_blks || blk_sz > BLK_CNT - slot) {
      return nullptr;
    }
    front_blk_sz_ = blk_sz;
    return std::launder(reinterpret_cast<MsgHeader*>(buf_ + static_cast<size_t>(slot) * BLK_SZ));
  }

  void Pop() {
    if (front_blk_sz_ == 0) return;
    uint32_t local_read_idx = read_idx.load(std::memory_order_relaxed);
    read_idx.store(local_read_idx + front_blk_sz_, std::memory_order_release);
    front_blk_sz_ = 0;
  }

private:
  alignas(64) std::byte buf_[Bytes]{};

  alignas(128) std::atomic<uint32_t> write_idx{0};
  uint32_t read_idx_cach = 0; // used only by writing thread
  uint32_t pending_write_idx_ = 0;
  uint32_t pending_blk_sz_ = 0;

  alignas(128) std::atomic<uint32_t> read_idx{0};
  uint32_t front_blk_sz_ = 0;
};
} // namespace tcpshm
