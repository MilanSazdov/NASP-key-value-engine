#include "SSTable.h"

SSTable::SSTable(const std::string& baseFilename)
    : baseFilename(baseFilename),
    dataFile(baseFilename + "-Data.db"),
    indexFile(baseFilename + "-Index.db"),
    summaryFile(baseFilename + "-Summary.db"),
    filterFile(baseFilename + "-Filter.db"),
    tocFile(baseFilename + "-TOC.txt"),
    metadataFile(baseFilename + "-Metadata.db") {}

void SSTable::createFromMemtable(const IMemtable& memtable) {
    // Korak 1: Dohvati sve podatke iz Memtable
    std::vector<std::pair<std::string, std::string>> entries = memtable.getAllKeyValuePairs();

    // Kreiraj potrebne strukture
    std::vector<std::string> values;
    std::vector<std::string> keys;
    std::vector<uint64_t> offsets;

    // Step 1: Write Data
    writeData(entries, values);

    // Step 2: Generate Index
    uint64_t offset = 0;
    for (const auto& [key, value] : entries) {
        keys.push_back(key);
        offsets.push_back(offset);

        // Racunanje offseta na osnovu strukture podataka
        offset += sizeof(uint32_t) + sizeof(uint64_t) * 2 + key.size() + value.size();
    }
    writeIndex(keys, offsets);

    // Step 3: Write Summary
    writeSummary(keys, offsets);

    // Step 4: Write Bloom Filter
    writeBloomFilter(keys);

    // Step 5: Write Metadata (Merkle Tree)
    writeMetadata(values);

    // Step 6: Write TOC
    writeTOC();
}



void SSTable::writeData(const std::vector<std::pair<std::string, std::string>>& entries, std::vector<std::string>& values) {
    std::ofstream file(dataFile, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error opening Data file: " << dataFile << "\n";
        return;
    }

    for (const auto& [key, value] : entries) {
        std::hash<std::string> hasher;
        uint32_t hash = static_cast<uint32_t>(hasher(value));
        uint64_t keyLength = key.size();
        uint64_t valueLength = value.size();

        file.write(reinterpret_cast<const char*>(&hash), sizeof(hash));
        file.write(reinterpret_cast<const char*>(&keyLength), sizeof(keyLength));
        file.write(reinterpret_cast<const char*>(&valueLength), sizeof(valueLength));
        file.write(key.data(), keyLength);
        file.write(value.data(), valueLength);

        values.push_back(value);
    }
    file.close();
}

void SSTable::writeIndex(const std::vector<std::string>& keys, const std::vector<uint64_t>& offsets) {
    std::ofstream file(indexFile, std::ios::binary);
    for (size_t i = 0; i < keys.size(); i++) {
        uint64_t offset = offsets[i];
        file.write(keys[i].c_str(), keys[i].size());
        file.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }
    file.close();
}

void SSTable::writeSummary(const std::vector<std::string>& keys, const std::vector<uint64_t>& offsets) {
    std::ofstream file(summaryFile, std::ios::binary);
    size_t step = 5;

    for (size_t i = 0; i < keys.size(); i += step) {
        file.write(keys[i].c_str(), keys[i].size());
        file.write(reinterpret_cast<const char*>(&offsets[i]), sizeof(offsets[i]));
    }
    file.close();
}

void SSTable::writeBloomFilter(const std::vector<std::string>& keys) {
    BloomFilter filter(keys.size(), 0.01);
    for (const auto& key : keys) {
        filter.add(key);
    }

    auto serialized = filter.serialize();
    std::ofstream file(filterFile, std::ios::binary);
    file.write(reinterpret_cast<const char*>(serialized.data()), serialized.size());
    file.close();
}

void SSTable::writeMetadata(const std::vector<std::string>& values) {
    MerkleTree tree(values);
    std::string rootHash = tree.getRootHash();

    std::ofstream file(metadataFile, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error writing Metadata file: " << metadataFile << "\n";
        return;
    }

    file.write(rootHash.c_str(), rootHash.size());
    file.close();
}

void SSTable::writeTOC() {
    std::ofstream file(tocFile);
    file << dataFile << "\n" << indexFile << "\n" << summaryFile << "\n"
        << filterFile << "\n" << metadataFile;
    file.close();
}
