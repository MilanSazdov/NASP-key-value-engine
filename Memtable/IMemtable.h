#pragma once

#include <string>
#include <optional>
#include "wal.h"
#include <vector>

using namespace std;

/**
 * Struktura koja enkapsulira sve sto cuvamo u memtable-u.
 * (key, value, tombstone, timestamp)
 *
*/

struct MemtableEntry {
	string key;
	string value;
	bool tombstone;
	uint64_t timestamp; // vreme poslednje izmene

	bool operator<(const MemtableEntry& other) const {
		return key < other.key;
	}
};

class IMemtable {

public:

	virtual ~IMemtable() = default;

	virtual void put(const string& key, const string& value) = 0;
	virtual void remove(const string& key) = 0;
	virtual optional<string> get(const string& key, bool& deleted) const = 0;

	virtual size_t size() const = 0;
	virtual void setMaxSize(size_t maxSize) = 0;
	
	// Ova funkcija mi vraca sve rekorde iz svih wal (segmenata) fajlova
	// virtual void loadFromRecords(const vector<Record>& records) = 0;

	// Za Flush - uzimamo sve key, value parove iz memtable-a (mozemo ih i sortirati)
	// virtual vector<pair<string, string>> getAllKeyValuePairs() const = 0;

	// NOVA metoda: vraća *sve* zapise, uključujući tombstone i timestamp.
	virtual std::vector<MemtableEntry> getAllMemtableEntries() const = 0;

	virtual std::optional<MemtableEntry> getEntry(const std::string& key) const = 0;
	virtual void updateEntry(const std::string& key, const MemtableEntry& entry) = 0;

	// Vraca sve MemtableEntry zapise sortirane po kljucu
	virtual std::vector<MemtableEntry> getSortedEntries() const = 0;
};