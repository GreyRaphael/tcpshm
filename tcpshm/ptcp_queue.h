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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>

namespace tcpshm {

// Simple single thread persist Queue that can be mmap-ed to a file
template<uint32_t Bytes, bool ToLittleEndian>
class PTCPQueue
{
public:
    static_assert(Bytes > 0 && Bytes % sizeof(MsgHeader) == 0, "Bytes must be positive multiple of 8");
    static constexpr uint32_t BLK_CNT = Bytes / sizeof(MsgHeader);
    static_assert(BLK_CNT <= static_cast<uint32_t>(std::numeric_limits<int>::max()), "BLK_CNT exceeds int max");

    void Reset() {
        std::memset(buf_, 0, sizeof(buf_));
        write_idx_ = 0;
        read_idx_ = 0;
        send_idx_ = 0;
        read_seq_num_ = 0;
        write_seq_num_ = 0;
        ack_seq_num_ = 0;
    }

    MsgHeader* Alloc(uint16_t size) {
        if(size > 65535 - sizeof(MsgHeader)) return nullptr;
        uint32_t total_size = static_cast<uint32_t>(size) + sizeof(MsgHeader);
        uint32_t blk_sz = (total_size + sizeof(MsgHeader) - 1) / sizeof(MsgHeader);
        if(blk_sz > BLK_CNT) return nullptr;
        uint32_t avail_sz = BLK_CNT - write_idx_;
        if(blk_sz > avail_sz) {
            if(blk_sz > avail_sz + read_idx_) return nullptr;
            std::memmove(buf_,
                         buf_ + static_cast<size_t>(read_idx_) * sizeof(MsgHeader),
                         static_cast<size_t>(write_idx_ - read_idx_) * sizeof(MsgHeader));
            write_idx_ -= read_idx_;
            send_idx_ -= read_idx_;
            read_idx_ = 0;
        }
        MsgHeader* header = new (buf_ + static_cast<size_t>(write_idx_) * sizeof(MsgHeader)) MsgHeader;
        header->size = static_cast<uint16_t>(total_size);
        return header;
    }

    void Push() {
        MsgHeader* header = BlkAt(write_idx_);
        uint32_t blk_sz = (static_cast<uint32_t>(header->size) + sizeof(MsgHeader) - 1) / sizeof(MsgHeader);
        header->ack_seq = ack_seq_num_;
        header->ConvertByteOrder<ToLittleEndian>();
        write_idx_ += blk_sz;
        write_seq_num_++;
    }

    MsgHeader* GetSendable(int& blk_sz) {
        blk_sz = static_cast<int>(write_idx_ - send_idx_);
        return BlkAt(send_idx_);
    }

    void Sendout(int blk_sz) {
        send_idx_ += static_cast<uint32_t>(blk_sz);
    }

    void LoginAck(uint32_t ack_seq) {
        Ack(ack_seq);
        send_idx_ = read_idx_;
    }

    // the next seq_num peer side expect
    bool Ack(uint32_t ack_seq) {
        if(static_cast<int32_t>(ack_seq - read_seq_num_) <= 0) return true; // not newer than read_seq_num_
        if(static_cast<int32_t>(ack_seq - write_seq_num_) > 0) return false;
        uint32_t idx = read_idx_;
        uint32_t seq = read_seq_num_;
        while(seq != ack_seq) {
            if(idx >= write_idx_) return false;
            MsgHeader hdr;
            std::memcpy(&hdr, buf_ + static_cast<size_t>(idx) * sizeof(MsgHeader), sizeof(MsgHeader));
            uint16_t msg_size = Endian<ToLittleEndian>::Convert(hdr.size);
            if(msg_size < sizeof(MsgHeader)) return false;
            uint32_t step = (static_cast<uint32_t>(msg_size) + sizeof(MsgHeader) - 1) / sizeof(MsgHeader);
            if(idx + step > write_idx_) return false;
            idx += step;
            seq++;
        }
        read_idx_ = idx;
        read_seq_num_ = seq;
        if(send_idx_ < read_idx_) send_idx_ = read_idx_;
        if(read_idx_ == write_idx_) {
            read_idx_ = write_idx_ = send_idx_ = 0;
        }
        return true;
    }

    uint32_t& MyAck() {
        return ack_seq_num_;
    }

    bool SanityCheckAndGetSeq(uint32_t* seq_start, uint32_t* seq_end) {
        if(write_idx_ > BLK_CNT || read_idx_ > write_idx_ || send_idx_ < read_idx_ || send_idx_ > write_idx_) {
            return false;
        }
        uint32_t end = read_seq_num_;
        uint32_t idx = read_idx_;
        while(idx < write_idx_) {
            MsgHeader header;
            std::memcpy(&header, buf_ + static_cast<size_t>(idx) * sizeof(MsgHeader), sizeof(MsgHeader));
            header.ConvertByteOrder<ToLittleEndian>();
            if(header.size < sizeof(MsgHeader)) return false;
            if(static_cast<int32_t>(ack_seq_num_ - header.ack_seq) < 0) return false; // ack_seq in this msg is too new
            uint32_t step = (static_cast<uint32_t>(header.size) + sizeof(MsgHeader) - 1) / sizeof(MsgHeader);
            if(idx + step > write_idx_) return false;
            idx += step;
            end++;
        }
        if(idx != write_idx_) return false;
        write_seq_num_ = end;
        *seq_start = read_seq_num_;
        *seq_end = end;
        return true;
    }

private:
    MsgHeader* BlkAt(uint32_t idx) {
        return reinterpret_cast<MsgHeader*>(buf_ + static_cast<size_t>(idx) * sizeof(MsgHeader));
    }

    alignas(64) std::byte buf_[Bytes]{};
    // invariant: read_idx_ <= send_idx_ <= write_idx_
    // where send_idx_ may point to the middle of a msg
    uint32_t write_idx_ = 0;
    uint32_t read_idx_ = 0;
    uint32_t send_idx_ = 0;
    uint32_t read_seq_num_ = 0; // the seq_num_ of msg read_idx_ points to
    uint32_t write_seq_num_ = 0;
    uint32_t ack_seq_num_ = 0;
};
} // namespace tcpshm
