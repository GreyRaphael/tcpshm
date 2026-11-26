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
#include <type_traits>

namespace tcpshm {

// Pure trivial / standard-layout shared-memory ring buffer layout (implicit-lifetime POD).
// Holds only the data buffer and shared atomic-ref indices; private caches stay in SPSCVarQueueHandle.
template<uint32_t Bytes, uint32_t BlkSz = 64>
struct SPSCVarQueueLayout
{
  static constexpr uint32_t BLK_SZ = BlkSz;
  static constexpr uint32_t HDR_SZ = static_cast<uint32_t>(sizeof(MsgHeader));
  static_assert(BLK_SZ >= HDR_SZ && (BLK_SZ % HDR_SZ == 0) && std::has_single_bit(BLK_SZ),
                "BlkSz must be a power-of-2 multiple of 8");
  static_assert(Bytes % BLK_SZ == 0, "Bytes must be a multiple of BlkSz");
  static constexpr uint32_t BLK_CNT = Bytes / BLK_SZ;
  static_assert(BLK_CNT && std::has_single_bit(BLK_CNT), "BLK_CNT must be a power of 2");
  static_assert(std::atomic_ref<uint32_t>::is_always_lock_free,
                "std::atomic_ref<uint32_t> must be lock-free");

  alignas(64) std::byte buf[Bytes];
  alignas(128) uint32_t write_idx;
  alignas(128) uint32_t read_idx;
};

// Process-private SPSC handle attached to a shared SPSCVarQueueLayout using C++20 std::atomic_ref.
template<uint32_t Bytes, uint32_t BlkSz = 64>
class SPSCVarQueueHandle
{
public:
  using Layout = SPSCVarQueueLayout<Bytes, BlkSz>;
  static constexpr uint32_t BLK_SZ = Layout::BLK_SZ;
  static constexpr uint32_t BLK_CNT = Layout::BLK_CNT;
  static constexpr uint32_t HDR_SZ = Layout::HDR_SZ;
  static_assert(std::is_trivial_v<Layout> && std::is_standard_layout_v<Layout>,
                "SPSCVarQueueLayout must be trivial and standard-layout");

  SPSCVarQueueHandle() = default;
  ~SPSCVarQueueHandle() = default;
  SPSCVarQueueHandle(const SPSCVarQueueHandle&) = delete;
  SPSCVarQueueHandle& operator=(const SPSCVarQueueHandle&) = delete;
  SPSCVarQueueHandle(SPSCVarQueueHandle&&) = delete;
  SPSCVarQueueHandle& operator=(SPSCVarQueueHandle&&) = delete;

  void Attach(Layout* layout) noexcept {
    layout_ = layout;
    pending_write_idx_ = 0;
    pending_blk_sz_ = 0;
    front_blk_sz_ = 0;
    if (layout_) {
      read_idx_cach_ = std::atomic_ref<uint32_t>(layout_->read_idx).load(std::memory_order_acquire);
      write_idx_cach_ = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_acquire);
    } else {
      read_idx_cach_ = 0;
      write_idx_cach_ = 0;
    }
  }

  Layout* Detach() noexcept {
    Layout* old = layout_;
    layout_ = nullptr;
    read_idx_cach_ = 0;
    pending_write_idx_ = 0;
    pending_blk_sz_ = 0;
    write_idx_cach_ = 0;
    front_blk_sz_ = 0;
    return old;
  }

  bool IsAttached() const noexcept {
    return layout_ != nullptr;
  }

  Layout* GetLayout() const noexcept {
    return layout_;
  }

  // Must only be called when no concurrent producer/consumer accesses the attached layout.
  void Reset() noexcept {
    if (!layout_) return;
    std::memset(layout_->buf, 0, sizeof(layout_->buf));
    std::atomic_ref<uint32_t>(layout_->write_idx).store(0, std::memory_order_relaxed);
    std::atomic_ref<uint32_t>(layout_->read_idx).store(0, std::memory_order_relaxed);
    read_idx_cach_ = 0;
    pending_write_idx_ = 0;
    pending_blk_sz_ = 0;
    write_idx_cach_ = 0;
    front_blk_sz_ = 0;
  }

  MsgHeader* Alloc(uint16_t size) {
    pending_blk_sz_ = 0;
    if (!layout_ || size > 65535u - HDR_SZ) return nullptr;
    uint32_t total_size = static_cast<uint32_t>(size) + HDR_SZ;
    uint32_t blk_sz = (total_size + BLK_SZ - 1) / BLK_SZ;
    if (blk_sz > BLK_CNT) return nullptr;
    uint32_t local_write_idx =
        std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_relaxed);
    uint32_t padding_sz = BLK_CNT - (local_write_idx & (BLK_CNT - 1));
    bool rewind = blk_sz > padding_sz;
    // min_read_idx could be a negative value which results in a large unsigned int
    uint32_t min_read_idx = local_write_idx + blk_sz + (rewind ? padding_sz : 0) - BLK_CNT;
    if (static_cast<int32_t>(read_idx_cach_ - min_read_idx) < 0) {
      read_idx_cach_ = std::atomic_ref<uint32_t>(layout_->read_idx).load(std::memory_order_acquire);
      if (static_cast<int32_t>(read_idx_cach_ - min_read_idx) < 0) { // no enough space
        return nullptr;
      }
    }
    if (rewind) {
      new (layout_->buf + static_cast<size_t>(local_write_idx & (BLK_CNT - 1)) * BLK_SZ) MsgHeader{0, 0, 0};
      local_write_idx += padding_sz;
    }
    MsgHeader* header =
        new (layout_->buf + static_cast<size_t>(local_write_idx & (BLK_CNT - 1)) * BLK_SZ)
            MsgHeader{static_cast<uint16_t>(total_size), 0, 0};
    pending_write_idx_ = local_write_idx + blk_sz;
    pending_blk_sz_ = blk_sz;
    return header;
  }

  void Push() {
    if (!layout_ || pending_blk_sz_ == 0) return;
    std::atomic_ref<uint32_t>(layout_->write_idx).store(pending_write_idx_, std::memory_order_release);
    pending_blk_sz_ = 0;
  }

  MsgHeader* Front() {
    if (!layout_) return nullptr;
    uint32_t local_read_idx =
        std::atomic_ref<uint32_t>(layout_->read_idx).load(std::memory_order_relaxed);
    if (local_read_idx == write_idx_cach_) {
      write_idx_cach_ = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_acquire);
      if (local_read_idx == write_idx_cach_) {
        return nullptr;
      }
    }
    uint32_t slot = local_read_idx & (BLK_CNT - 1);
    MsgHeader hdr;
    std::memcpy(&hdr, layout_->buf + static_cast<size_t>(slot) * BLK_SZ, sizeof(MsgHeader));
    if (hdr.size == 0) { // rewind
      local_read_idx += BLK_CNT - slot;
      std::atomic_ref<uint32_t>(layout_->read_idx).store(local_read_idx, std::memory_order_release);
      if (local_read_idx == write_idx_cach_) {
        write_idx_cach_ = std::atomic_ref<uint32_t>(layout_->write_idx).load(std::memory_order_acquire);
        if (local_read_idx == write_idx_cach_) {
          return nullptr;
        }
      }
      slot = local_read_idx & (BLK_CNT - 1);
      std::memcpy(&hdr, layout_->buf + static_cast<size_t>(slot) * BLK_SZ, sizeof(MsgHeader));
    }
    if (hdr.size < HDR_SZ) {
      return nullptr;
    }
    uint32_t blk_sz = (static_cast<uint32_t>(hdr.size) + BLK_SZ - 1) / BLK_SZ;
    uint32_t avail_blks = write_idx_cach_ - local_read_idx;
    if (blk_sz > avail_blks || blk_sz > BLK_CNT - slot) {
      return nullptr;
    }
    front_blk_sz_ = blk_sz;
    return std::launder(
        reinterpret_cast<MsgHeader*>(layout_->buf + static_cast<size_t>(slot) * BLK_SZ));
  }

  void Pop() {
    if (!layout_ || front_blk_sz_ == 0) return;
    uint32_t local_read_idx =
        std::atomic_ref<uint32_t>(layout_->read_idx).load(std::memory_order_relaxed);
    std::atomic_ref<uint32_t>(layout_->read_idx)
        .store(local_read_idx + front_blk_sz_, std::memory_order_release);
    front_blk_sz_ = 0;
  }

private:
  alignas(64) Layout* layout_ = nullptr;

  alignas(64) uint32_t read_idx_cach_ = 0; // used only by writing thread
  uint32_t pending_write_idx_ = 0;
  uint32_t pending_blk_sz_ = 0;

  alignas(64) uint32_t write_idx_cach_ = 0; // used only by reading thread
  uint32_t front_blk_sz_ = 0;
};

// Self-contained in-process wrapper combining a zero-initialized Layout and its Handle.
template<uint32_t Bytes, uint32_t BlkSz = 64>
class SPSCVarQueue
{
public:
  using Layout = SPSCVarQueueLayout<Bytes, BlkSz>;
  using Handle = SPSCVarQueueHandle<Bytes, BlkSz>;
  static constexpr uint32_t BLK_SZ = Layout::BLK_SZ;
  static constexpr uint32_t BLK_CNT = Layout::BLK_CNT;
  static constexpr uint32_t HDR_SZ = Layout::HDR_SZ;

  SPSCVarQueue() noexcept {
    handle_.Attach(&layout_);
  }

  ~SPSCVarQueue() = default;
  SPSCVarQueue(const SPSCVarQueue&) = delete;
  SPSCVarQueue& operator=(const SPSCVarQueue&) = delete;
  SPSCVarQueue(SPSCVarQueue&&) = delete;
  SPSCVarQueue& operator=(SPSCVarQueue&&) = delete;

  void Reset() noexcept {
    handle_.Reset();
  }

  MsgHeader* Alloc(uint16_t size) {
    return handle_.Alloc(size);
  }

  void Push() {
    handle_.Push();
  }

  MsgHeader* Front() {
    return handle_.Front();
  }

  void Pop() {
    handle_.Pop();
  }

private:
  Layout layout_{};
  Handle handle_;
};
} // namespace tcpshm
