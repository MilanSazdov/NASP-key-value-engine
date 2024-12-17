#pragma once

#include "BloomFilter.h"
#include "IMemtable.h"
#include "MerkleTree.h"
#include <string>
#include <vector>
#include <fstream>

using namespace std;

class SSTable {
public:
    SSTable(const string& baseFilename);
    void createFromMemtable(const IMemtable& memtable);

private:
    string baseFilename;
    string dataFile, indexFile, summaryFile, filterFile, tocFile, metadataFile;

    void writeData(const std::vector<std::pair<std::string, std::string>>& entries, std::vector<std::string>& values);
    void writeIndex(const vector<string>& keys, const vector<uint64_t>& offsets);
    void writeSummary(const vector<string>& keys, const vector<uint64_t>& offsets);
    void writeBloomFilter(const vector<string>& keys);
    void writeMetadata(const vector<string>& values);
    void writeTOC();

    
};