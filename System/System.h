#pragma once

#include "wal.h"
#include "MemtableManager.h"

using namespace std;

class System {
	
private:
	Wal* wal;
	MemtableManager* memtable;

public:

	System();

	void put(string key, string value, bool tombstone);
	// string get(string key, bool& found);
	// bool Delete(string key, bool& found);

};