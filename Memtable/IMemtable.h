#pragma once

#include <string>
#include <optional>

using namespace std;

// Interface for Memtable

class IMemtable {

public:

	virtual ~IMemtable() = default;

	virtual void put(const string& key, const string& value) = 0;

	virtual void remove(const string& key) = 0;

	virtual optional<string> get(const string& key) const = 0;

	virtual size_t size() const = 0;

	virtual void setMaxSize(size_t maxSize) = 0;

	virtual void loadFromWal(const string& wal_file) = 0;

};
