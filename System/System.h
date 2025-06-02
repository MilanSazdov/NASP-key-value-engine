#pragma once

#include <iostream>
#include "wal.h"
#include "MemtableManager.h"


class System {
	
public:

	Wal* wal;
	MemtableManager* memtable;

public:

	System();

	void put(std::string key, std::string value, bool tombstone);

	void debugWal() const;
	void debugMemtable() const;

};