#pragma once
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <algorithm>
#include <cstdint>

class SimHash {
private:
    std::map<std::string, bool> stopWords;
    uint64_t hashWord(const std::string& word) const;
    uint64_t computeFingerprint(const std::vector<std::string>& words) const;
public:
    std::vector<std::string> parseText(const std::string& text) const;
    SimHash();
    int hammingDistance(const std::string& text1, const std::string& text2) const;
    uint64_t getFingerprint(const std::string& text) const;
};