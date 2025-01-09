
#include "MemtableSkipList.h"
#include <fstream>
#include <iostream>


MemtableSkipList::MemtableSkipList()
    : maxSize_(1000)
{}

void MemtableSkipList::put(const std::string& key, const std::string& value) {
    // Ako je dostignuta max velicina a kljuc nije vec prisutan, ne mozemo dodati novi par
	uint64_t timestamp = currentTime();
    if (skiplist_.Size() >= maxSize_ && !skiplist_.get(key).has_value()) {
        std::cerr << "[MemtableSkipList] Dostignut maxSize, ne moze se ubaciti novi kljuc: " << key << "\n";
        return;
    }
    skiplist_.insert(key, value, false, timestamp);
}

void MemtableSkipList::remove(const std::string& key) {
    uint64_t timestamp = currentTime();
    skiplist_.insert(key, "", true, timestamp);  // Prazan value, tombstone postavljen na true
}

// NOVO: Koristi novu metodu getNode iz SkipList-e 
std::optional<std::string> MemtableSkipList::get(const std::string& key) const {
    auto node = skiplist_.getNode(key); // Koristi novu metodu getNode iz SkipList-a
    if (node && !node->tombstone) {
        return node->value;
    }
    return std::nullopt;
}

size_t MemtableSkipList::size() const {
    return skiplist_.Size();
}

void MemtableSkipList::setMaxSize(size_t maxSize) {
    maxSize_ = maxSize;
}

void MemtableSkipList::loadFromWal(const std::string& wal_file) {
    std::ifstream file(wal_file);
    if (!file.is_open()) {
        std::cerr << "[MemtableSkipList] Ne mogu da otvorim WAL fajl: " << wal_file << "\n";
        return;
    }

    std::string op, key, value;
    while (file >> op >> key) {
        if (op == "INSERT") {
            file >> value;
            put(key, value);
        }
        else if (op == "REMOVE") {
            remove(key);
        }
        // Ignorisemo sve ostale linije koje ne odgovaraju INSERT ili REMOVE
    }
    file.close();
}

std::vector<MemtableEntry> MemtableSkipList::getAllMemtableEntries() const {
    std::vector<MemtableEntry> entries;
    auto pairs = skiplist_.getAllKeyValuePairs();
    for (const auto& [key, value] : pairs) {
        auto node = skiplist_.getNode(key);
        if (node) {
            entries.push_back({ node->key, node->value, node->tombstone, node->timestamp });
        }
    }
    return entries;
}

/*
std::vector<std::pair<std::string, std::string>> MemtableSkipList::getAllKeyValuePairs() const {
    return skiplist_.getAllKeyValuePairs();
}
*/