#include "TokenBucket.h"
#include <chrono>
#include <cstring>
#include <algorithm>

using namespace std;

TokenBucket::TokenBucket(uint32_t maxTokens, uint64_t refillIntervalSec, uint64_t lastRefillTimestamp, uint32_t tokens)
    : maxTokens(maxTokens), refillIntervalSec(refillIntervalSec), lastRefillTimestamp(lastRefillTimestamp), tokens(tokens) {
    if (this->lastRefillTimestamp == 0) {
        this->lastRefillTimestamp = currentTimeSec();
        this->tokens = maxTokens;
    }
}

bool TokenBucket::allowRequest(bool* isRefilled) {
    bool refilled = refillIfNeeded();
    if (isRefilled != nullptr) {
        *isRefilled = refilled;
    }

    if (tokens > 0) {
        --tokens;
        return true;
    }
    return false;
}

bool TokenBucket::refillIfNeeded() {
    uint64_t now = currentTimeSec();
    if (now - lastRefillTimestamp >= refillIntervalSec) {
        tokens = maxTokens;
        lastRefillTimestamp = now;
        return true;
    }
    return false;
}

vector<byte> TokenBucket::serialize() const {
    vector<byte> data;

    data.insert(data.end(), reinterpret_cast<const byte*>(&maxTokens), reinterpret_cast<const byte*>(&maxTokens) + sizeof(maxTokens));
    data.insert(data.end(), reinterpret_cast<const byte*>(&refillIntervalSec), reinterpret_cast<const byte*>(&refillIntervalSec) + sizeof(refillIntervalSec));
    data.insert(data.end(), reinterpret_cast<const byte*>(&tokens), reinterpret_cast<const byte*>(&tokens) + sizeof(tokens));
    data.insert(data.end(), reinterpret_cast<const byte*>(&lastRefillTimestamp), reinterpret_cast<const byte*>(&lastRefillTimestamp) + sizeof(lastRefillTimestamp));

    return data;
}

TokenBucket TokenBucket::deserialize(const vector<byte>& data) {
    size_t offset = 0;

    uint32_t maxTokens;
    uint64_t refillIntervalSec;
    uint32_t tokens;
    uint64_t lastRefillTimestamp;

    memcpy(&maxTokens, &data[offset], sizeof(maxTokens));
    offset += sizeof(maxTokens);

    memcpy(&refillIntervalSec, &data[offset], sizeof(refillIntervalSec));
    offset += sizeof(refillIntervalSec);

    memcpy(&tokens, &data[offset], sizeof(tokens));
    offset += sizeof(tokens);

    memcpy(&lastRefillTimestamp, &data[offset], sizeof(lastRefillTimestamp));
    offset += sizeof(lastRefillTimestamp);

    return TokenBucket(maxTokens, refillIntervalSec, lastRefillTimestamp, tokens);
}

uint64_t TokenBucket::currentTimeSec() {
    return chrono::duration_cast<chrono::seconds>(chrono::system_clock::now().time_since_epoch()).count();
}
