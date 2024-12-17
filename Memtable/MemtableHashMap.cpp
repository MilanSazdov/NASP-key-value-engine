#include "MemtableHashMap.h"
#include <fstream>
#include <iostream>

using namespace std;

MemtableHashMap::MemtableHashMap() : maxSize(1000) {}

void MemtableHashMap::put(const string& key, const string& value) {
	if (table.size() >= maxSize && table.find(key) == table.end()) {
		cerr << "[MemtableHashMap] Max size reached, cannot insert new key: " << key << "\n";
		return;
	}
	table[key] = value;
}

void MemtableHashMap::remove(const string& key) {
	table.erase(key);
}

optional<string> MemtableHashMap::get(const string& key) const {
	auto it = table.find(key);
	if (it == table.end()) {
		return nullopt;
	}
	return it->second;
}

size_t MemtableHashMap::size() const {
	return table.size();
}

void MemtableHashMap::setMaxSize(size_t maxSize) {
	this->maxSize = maxSize;
}

void MemtableHashMap::loadFromWal(const string& wal_file) {
	ifstream file(wal_file);
	if (!file.is_open()) {
		cerr << "[MemtableHashMap] Could not open WAL file: " << wal_file << "\n";
		return;
	}
	string key, value;
	while (file >> key >> value) {
		table[key] = value;
	}
	file.close();
}

vector<pair<string, string>> MemtableHashMap::getAllKeyValuePairs() const {
	vector<pair<string, string>> result;
	for (const auto& [key, value] : table) {
		result.push_back({ key, value });
	}
	return result;
}