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
#include <bit>
#include <atomic>

namespace tcpshm {

template<uint32_t Bytes>
class SPSCVarQueue
{
public:
  static constexpr uint32_t BLK_CNT = Bytes / 64;
  static_assert(BLK_CNT && std::has_single_bit(BLK_CNT), "BLK_CNT must be a power of 2");

  MsgHeader* Alloc(uint16_t size) {
    size += sizeof(MsgHeader);
    uint32_t blk_sz = (size + sizeof(Block) - 1) / sizeof(Block);
    uint32_t local_write_idx = write_idx.load(std::memory_order_relaxed);
    uint32_t padding_sz = BLK_CNT - (local_write_idx % BLK_CNT);
    bool rewind = blk_sz > padding_sz;
    // min_read_idx could be a negtive value which results in a large unsigned int
    uint32_t min_read_idx = local_write_idx + blk_sz + (rewind ? padding_sz : 0) - BLK_CNT;
    if ((int)(read_idx_cach - min_read_idx) < 0) {
      read_idx_cach = read_idx.load(std::memory_order_acquire);
      if ((int)(read_idx_cach - min_read_idx) < 0) { // no enough space
        return nullptr;
      }
    }
    if (rewind) {
      blk[local_write_idx % BLK_CNT].header.size = 0;
      local_write_idx += padding_sz;
      write_idx.store(local_write_idx, std::memory_order_relaxed);
    }
    MsgHeader& header = blk[local_write_idx % BLK_CNT].header;
    header.size = size;
    return &header;
    }

    void Push() {
        uint32_t local_write_idx = write_idx.load(std::memory_order_relaxed);
        uint32_t blk_sz = (blk[local_write_idx % BLK_CNT].header.size + sizeof(Block) - 1) / sizeof(Block);
        local_write_idx += blk_sz;
        write_idx.store(local_write_idx, std::memory_order_release);
        write_idx.notify_one();
    }

    MsgHeader* Front() {
        uint32_t local_write_idx = write_idx.load(std::memory_order_acquire);
        uint32_t local_read_idx = read_idx.load(std::memory_order_relaxed);
        if(local_read_idx == local_write_idx) {
            return nullptr;
        }
        uint16_t size = blk[local_read_idx % BLK_CNT].header.size;
        if(size == 0) { // rewind
            local_read_idx += BLK_CNT - (local_read_idx % BLK_CNT);
            read_idx.store(local_read_idx, std::memory_order_relaxed);
            if(local_read_idx == local_write_idx) {
                return nullptr;
            }
        }
        return &blk[local_read_idx % BLK_CNT].header;
    }

    void Pop() {
        uint32_t local_read_idx = read_idx.load(std::memory_order_relaxed);
        uint32_t blk_sz = (blk[local_read_idx % BLK_CNT].header.size + sizeof(Block) - 1) / sizeof(Block);
        local_read_idx += blk_sz;
        read_idx.store(local_read_idx, std::memory_order_release);
        read_idx.notify_one();
    }

    // C++20 efficient wait
    void WaitForData() {
        uint32_t curr_w = write_idx.load(std::memory_order_acquire);
        while (curr_w == read_idx.load(std::memory_order_relaxed)) {
            write_idx.wait(curr_w);
            curr_w = write_idx.load(std::memory_order_acquire);
        }
    }

private:
  struct Block // size of 64, same as cache line
  {
    alignas(64) MsgHeader header;
  } blk[BLK_CNT];

  alignas(128) std::atomic<uint32_t> write_idx{0};
  uint32_t read_idx_cach = 0; // used only by writing thread

  alignas(128) std::atomic<uint32_t> read_idx{0};
};
} // namespace tcpshm
