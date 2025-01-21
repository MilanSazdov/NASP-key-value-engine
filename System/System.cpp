#include "System.h"

System::System() {

    std::cout << "[Debug] Initializing WAL...\n";
    wal = new Wal(15);
    std::cout << "[Debug] Initializing MemtableManager...\n";
    memtable = new MemtableManager("hash", 5, 15);

    std::cout << "[Debug] Retrieving records from WAL...\n";
    std::vector<Record> records = wal->get_all_records();

    std::cout << "[Debug] Loading records into Memtable...\n";
    memtable->loadFromWal(records);
	memtable->printAllData();

    std::cout << "[Debug] System initialization completed.\n";


	std::cout << "[System] Data from WAL loaded into the Memory table.\n";
}

void System::put(std::string key, std::string value, bool tombstone) {
	wal->put(key, value);

	memtable->put(key, value);
}
