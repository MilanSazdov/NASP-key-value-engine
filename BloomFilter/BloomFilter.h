#pragma once

#include <vector>
#include <string>
#include <cmath>
#include <functional>
#include <bitset>
#include <ctime>
#include <iostream>
#include <CustomHash.h>

using namespace std;

class BloomFilter
{
private:
	unsigned int m; // Size of the bit set/array
	unsigned int k; // Number of hash functions
	double p; // false - positive probability
	vector<bool> bitSet; // Bit set/array
    vector<function<size_t(const string&)>> hashFunctions; // Hash functions
	unsigned int timeConst; // Seed for generating hash functions
	size_t h2_seed; // Seed for the second hash function 

public:

    BloomFilter(unsigned int n, double falsePositiveRate);

    // Add an element to the Bloom Filter
    void add(const string& elem);

    // Check if an element is present
    bool possiblyContains(const string& elem) const;

    // Serialize Bloom Filter to a file
    vector<uint8_t> serialize() const;

    // Deserialize Bloom Filter from a file
    static BloomFilter deserialize(const vector<uint8_t>& data);

    // Helper functions
    static unsigned int calculateSizeOfBitSet(int expectedElements, double falsePositiveRate);

    static unsigned int calculateNumberOfHashFunctions(int expectedElements, unsigned int m);

};