#include "System.h"

System::System() {
	wal = new Wal();
	memtable = new MemtableManager("hash", 5, 15);
}

void System::put(string key, string value, bool tombstone) {
	wal->put(key, value);
	memtable->put(key, value);
}