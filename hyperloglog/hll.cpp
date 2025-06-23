#include "hll.h"
#include "../MurmurHash3/MurmurHash3.h"

#include <vector>
#include <cmath>
#include <algorithm>
#include <cstring>
#include <stdexcept>

/*
 * Format zapisa:
 * P (preciznost) - 1 bajt, uint8_t
 * M (broj registara) - 4 bajta, unsigned int
 * Reg (vrednosti registara) - M bajtova (1 bajt po registru)
*/

using namespace std;

HyperLogLog::HyperLogLog(uint8_t p) {
    if (p < 4 || p > 16) {
        throw invalid_argument("Preciznost (p) mora biti izmedju 4 i 16");
    }

    P = p;
    M = static_cast<uint64_t>(1u) << p;
    Reg.resize(M, 0);
}

uint32_t HyperLogLog::getHash(const string& word) const {
    uint32_t hash[1];
    MurmurHash3_x86_32(word.c_str(), static_cast<int>(word.size()), 0, hash);
    return hash[0];
}

void HyperLogLog::add(const string& word) {
    uint32_t hashValue = getHash(word);
    uint32_t key = hashValue >> (32 - P);

    uint8_t trailingZeros = 1;
    uint32_t mask = 1;

    while ((hashValue & mask) == 0 && trailingZeros < 255) {
        trailingZeros++;
        mask <<= 1;
    }

    if (key < M) {
        Reg[key] = max(Reg[key], trailingZeros);
    }
}

double HyperLogLog::estimate() const {
    long double sum = 0.0;
    uint64_t emptyRegs = 0;

    for (uint8_t val : Reg) {
        if (val == 0) emptyRegs++;
        sum += 1.0 / (1ull << val);
    }

    double alpha;
    if (P == 4) alpha = 0.673;
    else if (P == 5) alpha = 0.697;
    else if (P == 6) alpha = 0.709;
    else alpha = 0.7213 / (1 + 1.079 / M);

    double estimation = alpha * M * M / sum;

    if (emptyRegs > 0 && estimation <= 5 * M) {
        estimation = M * log(static_cast<double>(M) / emptyRegs);
    }
    else if (estimation > pow(2, 32) / 30) {
        estimation = -pow(2, 32) * log1p(-estimation / pow(2, 32));
    }

    return estimation;
}

vector<byte> HyperLogLog::serialize() const {
    vector<byte> data;
    data.reserve(1 + sizeof(uint32_t) + Reg.size());

    // P
    data.push_back(static_cast<byte>(P));

    // M
    data.push_back(static_cast<byte>((M >> 0) & 0xFF));
    data.push_back(static_cast<byte>((M >> 8) & 0xFF));
    data.push_back(static_cast<byte>((M >> 16) & 0xFF));
    data.push_back(static_cast<byte>((M >> 24) & 0xFF));

    // Reg
    for (uint8_t val : Reg) {
        data.push_back(static_cast<byte>(val));
    }

    return data;
}

HyperLogLog HyperLogLog::deserialize(const vector<byte>& data) {
    if (data.size() < 5) {
        throw runtime_error("Podaci su previse mali za deserijalizaciju");
    }

    uint8_t p = static_cast<uint8_t>(data[0]);

    uint32_t m = 0;
    m |= static_cast<uint32_t>(data[1]) << 0;
    m |= static_cast<uint32_t>(data[2]) << 8;
    m |= static_cast<uint32_t>(data[3]) << 16;
    m |= static_cast<uint32_t>(data[4]) << 24;

    size_t expectedSize = 5 + m;
    if (data.size() != expectedSize) {
        throw runtime_error("Nevalidna velicina serijalizovanih podataka: ocekivano " +
            to_string(expectedSize) + ", dobijeno " +
            to_string(data.size()));
    }

    HyperLogLog hll(p);

    for (uint32_t i = 0; i < m; ++i) {
        hll.Reg[i] = static_cast<uint8_t>(data[5 + i]);
    }

    return hll;
}
