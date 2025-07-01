#pragma once


#include "wal.h"
#include "MemtableManager.h"
#include "Config.h"

class System {

private:
	Wal* wal;
	MemtableManager* memtable;
	SSTManager* sstable;
	Cache<string>* cache;

public:
	System();

	void put(std::string key, std::string value, bool tombstone);
	string get(std::string key, bool& deleted);

	void debugWal() const;
	void debugMemtable() const;

};