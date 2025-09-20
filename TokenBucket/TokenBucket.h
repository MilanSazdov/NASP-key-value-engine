#pragma once
#include <vector>
#include <cstddef> 
#include <cstdint>

class TokenBucket {
public:
    TokenBucket(uint32_t maxTokens, uint64_t refillIntervalSec, uint64_t lastRefillTimestamp = 0, uint32_t tokens = 0);

    bool allowRequest(bool* isRefilled);
    bool refillIfNeeded();

    std::vector<std::byte> serialize() const;
    static TokenBucket deserialize(const std::vector<std::byte>& data);

    uint32_t getTokens() const { return tokens; }
    uint32_t getMaxTokens() const { return maxTokens; }
    uint64_t getRefillIntervalSec() const { return refillIntervalSec; }

private:
    uint32_t maxTokens;
    uint64_t refillIntervalSec;
    uint32_t tokens;
    uint64_t lastRefillTimestamp;

    static uint64_t currentTimeSec();
};
