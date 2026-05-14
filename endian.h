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
#include "os.h"
#include <bit>

namespace tcpshm {

template<bool ToLittle>
class Endian
{
public:
    static constexpr bool IsLittle = std::endian::native == std::endian::little;

    template<class T>
    static T Convert(T t) {
        if constexpr (ToLittle == IsLittle) return t;

        if constexpr (sizeof(T) == 2)
            return tcp_bswap16((uint16_t)t);
        else if constexpr (sizeof(T) == 4)
            return tcp_bswap32((uint32_t)t);
        else if constexpr (sizeof(T) == 8)
            return tcp_bswap64((uint64_t)t);
        else
            return t;
    }

    template<class T>
    static void ConvertInPlace(T& t) {
        t = Convert(t);
    }
};
} // namespace tcpshm

