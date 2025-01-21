#pragma once

#include "wal.h"
#include "MemtableManager.h"
#include <iostream>

class System {
	
public:

	Wal* wal;
	MemtableManager* memtable;

public:

	System();

	void put(std::string key, std::string value, bool tombstone);

};