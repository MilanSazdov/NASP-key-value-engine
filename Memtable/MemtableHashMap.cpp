#include <fstream>
#include <optional>
#include <string>
#include <vector>
#include <algorithm> // Za sort
#include <iostream>
#include "MemtableHashMap.h"

MemtableHashMap::MemtableHashMap()
    : maxSize(Config::memtable_max_size)
{}

// put => upis (key, value), tombstone= false, timestamp = currentTime
void MemtableHashMap::put(const string& key, const string& value) {
    // Ako smo premašili maxSize, a ključ ne postoji, nećemo ubaciti (ili flush?)
    if (table_.size() >= maxSize && table_.find(key) == table_.end()) {
        std::cerr << "[MemtableHashMap] Max size reached, cannot insert new key: " << key << "\n";
        return;
    }
    Entry e{ value, false, currentTime() };
    table_[key] = e;
}

// remove => postavimo tombstone = true, value = ""
void MemtableHashMap::remove(const std::string& key) {
    auto it = table_.find(key);
    // Kljuc ne postoji, provera da li mozemo da ga insertujemo
    if (it == table_.end() && table_.size() >= maxSize) {
        std::cerr << "[MemtableHashMap] Max size reached, cannot insert new key: " << key << "\n";
        return;
    }

    // Kljuc postoji, ili ima mesta da ga dodamo. Gazimo stari entry / upisujemo novi
    Entry e{ "", true, currentTime() };
    table_[key] = e;
}

// get => vraća value ako tombstone=false, inače nullopt
optional<string> MemtableHashMap::get(const string& key, bool& deleted) const {
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullopt;
    }
    if (it->second.tombstone) {
        // obrisan
        deleted = true;
        return nullopt;
    }
    deleted = false;
    return it->second.value;
}

size_t MemtableHashMap::size() const {
    return table_.size();
}

void MemtableHashMap::setMaxSize(size_t maxSize) {
    this->maxSize = maxSize;
}

vector<MemtableEntry> MemtableHashMap::getAllMemtableEntries() const {
    vector<MemtableEntry> result;
    result.reserve(table_.size());
    for (const auto& [k, e] : table_) {
        MemtableEntry me;
        me.key = k;
        me.value = e.value;
        me.tombstone = e.tombstone;
        me.timestamp = e.timestamp;
        result.push_back(me);
    }

    sort(result.begin(), result.end());
    return result;
}

std::optional<MemtableEntry> MemtableHashMap::getEntry(const string& key) const {
	auto it = table_.find(key);
	if (it == table_.end()) {
        return std::nullopt;
	}
	return MemtableEntry{ key, it->second.value, it->second.tombstone, it->second.timestamp };
}

void MemtableHashMap::updateEntry(const string& key, const MemtableEntry& entry) {
    auto it = table_.find(key);
    if (it == table_.end()) {
        throw std::runtime_error("Key not found for update: " + key);
    }
    table_[key] = { entry.value, entry.tombstone, entry.timestamp };
}

std::vector<MemtableEntry> MemtableHashMap::getSortedEntries() const {
    std::vector<MemtableEntry> sorted;
    for (const auto& [key, entry] : table_) {
        MemtableEntry e;
        e.key = key;
        e.value = entry.value;
        e.tombstone = entry.tombstone;
        e.timestamp = entry.timestamp;
        sorted.push_back(e);
    }

    std::sort(sorted.begin(), sorted.end());
    return sorted;
}
