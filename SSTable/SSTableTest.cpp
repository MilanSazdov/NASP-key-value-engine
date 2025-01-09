#include "SSTable.h"
#include "MemtableHashMap.h"
#include <iostream>
#include <cassert>

class MockMemtable : public IMemtable {
private:
    std::unordered_map<std::string, std::string> table;
    size_t maxSize_;

public:
    MockMemtable() : maxSize_(1000) {}

    void put(const std::string& key, const std::string& value) override {
        table[key] = value;
    }

    void remove(const std::string& key) override {
        table.erase(key);
    }

    std::optional<std::string> get(const std::string& key) const override {
        auto it = table.find(key);
        if (it != table.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    size_t size() const override {
        return table.size();
    }

    void setMaxSize(size_t maxSize) override {
        maxSize_ = maxSize;
    }

    void loadFromWal(const std::string& wal_file) override {
        std::cout << "Mock load from WAL: " << wal_file << "\n";
    }

    std::vector<std::pair<std::string, std::string>> getAllKeyValuePairs() const override {
        std::vector<std::pair<std::string, std::string>> result;
        for (const auto& [key, value] : table) {
            result.push_back({ key, value });
        }
        return result;
    }
};

void testSSTableCreation() {
    std::cout << "Running SSTable creation test...\n";

    // Koristimo MockMemtable kao ulaz
    MockMemtable memtable;

    // Kreiramo SSTable instancu
    SSTable sstable("testSSTable");

    // Pozivamo createFromMemtable
    sstable.createFromMemtable(memtable);

    // Proveravamo da li su fajlovi kreirani
    std::ifstream dataFile("testSSTable-Data.db");
    assert(dataFile.is_open() && "Data file was not created!");

    std::ifstream indexFile("testSSTable-Index.db");
    assert(indexFile.is_open() && "Index file was not created!");

    std::ifstream summaryFile("testSSTable-Summary.db");
    assert(summaryFile.is_open() && "Summary file was not created!");

    std::ifstream filterFile("testSSTable-Filter.db");
    assert(filterFile.is_open() && "Filter file was not created!");

    std::ifstream metadataFile("testSSTable-Metadata.db");
    assert(metadataFile.is_open() && "Metadata file was not created!");

    std::ifstream tocFile("testSSTable-TOC.txt");
    assert(tocFile.is_open() && "TOC file was not created!");

    std::cout << "All SSTable files created successfully!\n";

    // Zatvaramo sve fajlove
    dataFile.close();
    indexFile.close();
    summaryFile.close();
    filterFile.close();
    metadataFile.close();
    tocFile.close();

    std::cout << "SSTable creation test passed!\n";
}

int main() {
    
    testSSTableCreation();

    return 0;
}
