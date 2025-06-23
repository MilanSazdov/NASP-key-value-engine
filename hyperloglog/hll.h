#pragma once

#include <string>
#include <vector>
#include <cmath>
#include <cstdint>
#include <cstddef>


class HyperLogLog {
private:
    uint64_t M;
    uint8_t P;
    std::vector<uint8_t> Reg;

public:
    HyperLogLog(uint8_t p);
    void add(const std::string& word);
    double estimate() const;

    std::vector<std::byte> serialize() const;
    static HyperLogLog deserialize(const std::vector<std::byte>& data);

    uint32_t getHash(const std::string& text) const;
    std::vector<uint8_t> getReg() const {
        return Reg;
    }
    uint8_t getPrecision() const {
        return P;
    }
};
