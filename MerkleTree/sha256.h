/*
    MIT License

    Copyright (c) 2017-2021, 2023-2024, 2024 by Walton Wormsely, Zenon Evans

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

#include <string>
#include <vector>
#include <array>
#include <cstdint>
#include <iomanip>
#include <sstream>

class SHA256 {
public:
    SHA256();
    void update(const uint8_t* data, size_t length);
    void update(const std::string& data);
    std::array<uint8_t, 32> digest();
    static std::string toString(const std::array<uint8_t, 32>& digest);

private:
    void pad();
    void processBlock(const uint8_t* data);

    uint32_t m_h[8];
    uint64_t m_len;
    std::vector<uint8_t> m_unprocessed;
};

inline SHA256::SHA256() {
    m_h[0] = 0x6a09e667;
    m_h[1] = 0xbb67ae85;
    m_h[2] = 0x3c6ef372;
    m_h[3] = 0xa54ff53a;
    m_h[4] = 0x510e527f;
    m_h[5] = 0x9b05688c;
    m_h[6] = 0x1f83d9ab;
    m_h[7] = 0x5be0cd19;
    m_len = 0;
}

inline void SHA256::update(const uint8_t* data, size_t length) {
    m_len += length;
    size_t pos = 0;
    if (!m_unprocessed.empty()) {
        size_t to_fill = 64 - m_unprocessed.size();
        if (length >= to_fill) {
            m_unprocessed.insert(m_unprocessed.end(), data, data + to_fill);
            processBlock(m_unprocessed.data());
            m_unprocessed.clear();
            pos += to_fill;
        }
        else {
            m_unprocessed.insert(m_unprocessed.end(), data, data + length);
            return;
        }
    }
    while (length - pos >= 64) {
        processBlock(data + pos);
        pos += 64;
    }
    if (pos < length) {
        m_unprocessed.insert(m_unprocessed.end(), data + pos, data + length);
    }
}

inline void SHA256::update(const std::string& data) {
    update(reinterpret_cast<const uint8_t*>(data.c_str()), data.length());
}

inline std::array<uint8_t, 32> SHA256::digest() {
    pad();
    for (size_t i = 0; i < m_unprocessed.size(); i += 64) {
        processBlock(m_unprocessed.data() + i);
    }

    std::array<uint8_t, 32> hash;
    for (int i = 0; i < 8; ++i) {
        hash[i * 4] = (m_h[i] >> 24) & 0xff;
        hash[i * 4 + 1] = (m_h[i] >> 16) & 0xff;
        hash[i * 4 + 2] = (m_h[i] >> 8) & 0xff;
        hash[i * 4 + 3] = m_h[i] & 0xff;
    }
    return hash;
}

inline std::string SHA256::toString(const std::array<uint8_t, 32>& digest) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto& byte : digest) {
        ss << std::setw(2) << static_cast<int>(byte);
    }
    return ss.str();
}

inline void SHA256::pad() {
    size_t unproc_len = m_unprocessed.size();
    size_t len_bits = m_len * 8;
    m_unprocessed.push_back(0x80);
    size_t pad_len = (unproc_len < 56) ? (56 - 1 - unproc_len) : (120 - 1 - unproc_len);
    m_unprocessed.insert(m_unprocessed.end(), pad_len, 0x00);
    for (int i = 7; i >= 0; --i) {
        m_unprocessed.push_back((len_bits >> (i * 8)) & 0xff);
    }
}

inline void SHA256::processBlock(const uint8_t* data) {
    uint32_t w[64];
    for (int i = 0; i < 16; ++i) {
        w[i] = (static_cast<uint32_t>(data[i * 4]) << 24) |
            (static_cast<uint32_t>(data[i * 4 + 1]) << 16) |
            (static_cast<uint32_t>(data[i * 4 + 2]) << 8) |
            (static_cast<uint32_t>(data[i * 4 + 3]));
    }
    for (int i = 16; i < 64; ++i) {
        uint32_t s0 = (w[i - 15] >> 7 | w[i - 15] << 25) ^ (w[i - 15] >> 18 | w[i - 15] << 14) ^ (w[i - 15] >> 3);
        uint32_t s1 = (w[i - 2] >> 17 | w[i - 2] << 15) ^ (w[i - 2] >> 19 | w[i - 2] << 13) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }

    uint32_t a = m_h[0], b = m_h[1], c = m_h[2], d = m_h[3], e = m_h[4], f = m_h[5], g = m_h[6], h = m_h[7];

    static const uint32_t k[] = {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
    };

    for (int i = 0; i < 64; ++i) {
        uint32_t s1 = (e >> 6 | e << 26) ^ (e >> 11 | e << 21) ^ (e >> 25 | e << 7);
        uint32_t ch = (e & f) ^ (~e & g);
        uint32_t temp1 = h + s1 + ch + k[i] + w[i];
        uint32_t s0 = (a >> 2 | a << 30) ^ (a >> 13 | a << 19) ^ (a >> 22 | a << 10);
        uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
        uint32_t temp2 = s0 + maj;

        h = g;
        g = f;
        f = e;
        e = d + temp1;
        d = c;
        c = b;
        b = a;
        a = temp1 + temp2;
    }

    m_h[0] += a; m_h[1] += b; m_h[2] += c; m_h[3] += d;
    m_h[4] += e; m_h[5] += f; m_h[6] += g; m_h[7] += h;
}