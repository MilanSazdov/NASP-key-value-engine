#pragma once

#include "IMemtable.h"
#include <unordered_map>
#include <string>
#include <optional>

class MemtableHashMap : public IMemtable {

public:
	MemtableHashMap();

	~MemtableHashMap() = default;

	void put(const string& key, const string& value) override;

	void remove(const string& key) override;

	optional<string> get(const string& key) const override;

	size_t size() const override;

	void setMaxSize(size_t maxSize) override;

	void loadFromWal(const string& wal_file) override;

private:
	unordered_map<string, string> table;
	size_t maxSize;
};