#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include <vector>
#include <cstdint>
#include <optional> // Add this for std::optional

class System;

#include "../BloomFilter/BloomFilter.h"
#include "../Cms/cms.h"
#include "../HyperLogLog/hll.h"
#include "../SimHash/simhash.h"

/**
 * TypesManager handles all probabilistic data structures for the System.
 * It manages creation, deletion, and operations on Bloom Filters, Count-Min Sketches,
 * HyperLogLog, and SimHash data structures.
 */
class TypesManager {
private:
    System* system;

    // Storage keys for different data structures
    static const std::string BF_KEY_PREFIX;
    static const std::string CMS_KEY_PREFIX;
    static const std::string HLL_KEY_PREFIX;
    static const std::string SH_KEY_PREFIX;

    // Helper methods
    std::string generateStorageKey(const std::string& prefix, const std::string& userKey);
    //bool keyExists(const std::string& storageKey);

public:
    TypesManager(System* system);
    ~TypesManager();

    // Bloom Filter operations
    bool createBloomFilter(const std::string& key, int n, double p);
    bool deleteBloomFilter(const std::string& key);
    bool addToBloomFilter(const std::string& key, const std::string& value);
    bool hasKeyInBloomFilter(const std::string& key, const std::string& value);

    // Count-Min Sketch operations
    bool createCountMinSketch(const std::string& key, double epsilon, double delta);
    bool deleteCountMinSketch(const std::string& key);
    bool addToCountMinSketch(const std::string& key, const std::string& value);
    std::optional<int> getFromCountMinSketch(const std::string& key, const std::string& value);

    // HyperLogLog operations
    bool createHyperLogLog(const std::string& key, int p);
    bool deleteHyperLogLog(const std::string& key);
    bool addToHyperLogLog(const std::string& key, const std::string& value);
    std::optional<size_t> estimateHyperLogLog(const std::string& key);

    // SimHash operations
    bool addSimHashFingerprint(const std::string& key, const std::string& text);
    bool deleteSimHashFingerprint(const std::string& key);
    std::optional<int> getHammingDistance(const std::string& key1, const std::string& key2);

};