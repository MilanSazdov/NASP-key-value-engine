#include "System.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;

void ensureDirectory(const std::string& path) {
    if (!fs::exists(path)) {
        try {
            if (fs::create_directory(path)) {
                std::cout << "[SYSTEM INFO] Created missing folder: " << path << "\n";
            }
            else {
                std::cerr << "[SYSTEM ERROR] Failed to create folder (unknown reason): " << path << "\n";
            }
        }
        catch (const fs::filesystem_error& e) {
            std::cerr << "[SYSTEM ERROR] Exception while creating folder '" << path << "': " << e.what() << "\n";
        }
    }
}

System::System() {
    std::cout << "[SYSTEM] Starting initialization... \n";

    std::cout << "Reading Config file ... \n";
	Config::load_init_configuration();

    // --- Folder checks ---
    ensureDirectory(Config::data_directory);

    // --- WAL setup ---
    std::cout << "[Debug] Initializing WAL...\n";
    wal = new Wal();

    // --- Cache setup ---
    cout << "[Debug] Initializing Cache...\n";
    cache = new Cache<string>();
    cout << "Cache initialized\n";

    // --- Memtable setup ---
    std::cout << "[Debug] Initializing MemtableManager...\n";
    //TODO: Ove putanje do direktorijuma sam ja (vedran) samo lupio, treba popraviti
    memtable = new MemtableManager("hash", 2, 2, "../data/.", "../Config/config.json/.");

    // --- Load from WAL ---
    std::cout << "[Debug] Retrieving records from WAL...\n";
    std::vector<Record> records = wal->get_all_records();

    /*std::cout << "[Debug] WAL Records:\n";
    for (const Record& r : records) {
        std::cout << "-----------------------------------\n";
        std::cout << "Key       : " << r.key << "\n";
        std::cout << "Value     : " << r.value << "\n";
        std::cout << "Tombstone : " << static_cast<int>(r.tombstone) << "\n";
        std::cout << "Timestamp : " << r.timestamp << "\n";
        std::cout << "CRC       : " << r.crc << "\n";
    }
    std::cout << "-----------------------------------\n";*/

    std::cout << "[Debug] Loading records into Memtable...\n";
    memtable->loadFromWal(records);
	//memtable->printAllData();

    std::cout << "[Debug] System initialization completed.\n";


	//std::cout << "[System] Data from WAL loaded into the Memory table.\n";
}

System::~System() {
	std::cout << "[SYSTEM] Shutting down system and freeing resources...\n";
	delete wal;
	delete memtable;
	std::cout << "[SYSTEM] System shutdown complete.\n";
}

void System::put(std::string key, std::string value, bool tombstone) {
    //TODO: treba updateovati cache (bar mislim) A U PICKU MATERINU... KO ZNA GDE TAJ CACHE TREBA DA STOJI
    if (tombstone) {
        wal->del(key);
        memtable->remove(key);
    }
    else {
        cout << "Put to wal\n";
        wal->put(key, value);
        cout << "Put to memtable\n";
        memtable->put(key, value);
    }
}

// NIJE TESTIRANO!!
string System::get(std::string key, bool& deleted) {
    // searching memtable
    deleted = false;
    auto value = memtable->get(key, deleted);
    if (deleted) {
        return "";
    }
    else if (value.has_value()) {
        return value.value();
    }
    
    // searching cache
    bool exists;
    vector<byte> bytes = cache->get(key, exists);
    if (exists) {
        // converting from vector<byte> to string ret
        string ret(bytes.size(), '\0');
        memcpy(ret.data(), bytes.data(), bytes.size());

        deleted = false;
        return ret;
    }

    // searching sstable (disc)
    string retValue = 
}

void System::debugWal() const {
    std::vector<Record> records = wal->get_all_records();
    std::cout << "[Debug] WAL Records: \n";

    for (const auto& r : records) {
        std::cout << "-----------------------------------\n";
        std::cout << "\033[31m" << "Key       : " << r.key << "\033[0m\n";
        std::cout << "\033[32m" << "Value     : " << r.value << "\033[0m\n";
        std::cout << "Tombstone : " << (int)r.tombstone << "\n";
        std::cout << "Timestamp : " << r.timestamp << "\n";
        std::cout << "CRC       : " << r.crc << "\n";
    }
    std::cout << "-----------------------------------\n";
    // red: << "\033[31m" << "This text is red!" << "\033[0m" <<
    // greem" "\033[32m" << "This text is green!" << "\033[0m"
}

void System::debugMemtable() const {
    memtable->printAllData();
}