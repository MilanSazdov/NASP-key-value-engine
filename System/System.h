#pragma once


#include "wal.h"
#include "MemtableManager.h"
#include "Config.h"

class System {
	
public:
	Config* config;
	Wal* wal;
	MemtableManager* memtable;

public:

	System();

	void put(std::string key, std::string value, bool tombstone);

	void debugWal() const;
	void debugMemtable() const;

};