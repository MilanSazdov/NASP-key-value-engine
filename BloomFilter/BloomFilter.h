#pragma once

#include <vector>
#include <string>
#include <cmath>
#include <functional>
#include <ctime>
#include <cstring>
#include <stdexcept>
#include <random>

class BloomFilter {
private:
    unsigned int m; // Size of the bit set/array
    unsigned int k; // Number of hash functions
    double p;       // False-positive probability
    std::vector<bool> bitSet; // Bit set/array
    std::vector<std::function<size_t(const std::string&)>> hashFunctions; // Hash functions
    unsigned int timeConst; // Seed for generating hash functions
    size_t h2_seed;         // Seed for the second hash function

public:
    // Constructor
    BloomFilter();
    BloomFilter(unsigned int n, double falsePositiveRate);

    // Add an element to the Bloom Filter
    void add(const std::string& elem);

    // Check if an element is present
    bool possiblyContains(const std::string& elem) const;

    // Serialize Bloom Filter to a vector of bytes
    std::vector<std::byte> serialize() const;

    // Deserialize Bloom Filter from a vector of bytes
    static BloomFilter deserialize(const std::vector<std::byte>& data);

    // Helper functions
    static unsigned int calculateSizeOfBitSet(unsigned int expectedElements, double falsePositiveRate);
    static unsigned int calculateNumberOfHashFunctions(unsigned int expectedElements, unsigned int m);
};
