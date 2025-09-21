#include "System.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = filesystem;

const string System::RATE_LIMIT_KEY = "system_tokenbucket";

void ensureDirectory(const string& path) {
    if (!fs::exists(path)) {
        try {
            if (fs::create_directory(path)) {
                cout << "[SYSTEM INFO] Created missing folder: " << path << "\n";
            }
            else {
                cerr << "[SYSTEM ERROR] Failed to create folder (unknown reason): " << path << "\n";
            }
        }
        catch (const fs::filesystem_error& e) {
            cerr << "[SYSTEM ERROR] Exception while creating folder '" << path << "': " << e.what() << "\n";
        }
    }
}

//TODO: sstable is uninitialized. fix?
System::System() : requestCounter(0) {
    cout << "[SYSTEM] Starting initialization... \n";

    cout << "Reading Config file ... \n";
    bool new_system = Config::load_init_configuration();
    if (new_system) {
        cout << "[Debug] Reseting system.\n";
        resetSystem(Config::data_directory);
        resetSystem(Config::wal_directory);
    }
    else {
        cout << "[Debug] Old configuration is in use.\n";
    }

    // ovo stoji dok radim testiranje, posle treba samo ovo od gore
    resetSystem(Config::data_directory);
    resetSystem(Config::wal_directory);

    sharedInstanceBM = new Block_manager();

    // --- Folder checks ---
    ensureDirectory(Config::data_directory);

    // --- WAL setup ---
    cout << "[Debug] Initializing WAL...\n";
    wal = new Wal(*sharedInstanceBM);

    // --- Cache setup ---

    cout << "[Debug] Initializing Cache...\n";
    cache = new Cache<string>();
    cout << "Cache initialized\n";

    // --- SSTManager setup ---
    sstable = new SSTManager(sharedInstanceBM);

    // --- SSTCursor setup TODO: THIS IS FOR TESTING REMOVE LATER---
    sstCursor = new SSTableCursor(sstable);

    // --- Memtable setup ---
    cout << "[Debug] Initializing MemtableManager...\n";
    memtable = new MemtableManager(sstable);


    cout << "[Debug] Initializing LSMManager...\n";
    lsmManager_ = new LSMManager(sstable);
    //cout << "[Debug] Printing existing sstables.\n";
    //memtable->printSSTables(1);

    // --- Load from WAL ---
    cout << "[Debug] Retrieving records from WAL...\n";
    vector<Record> records = wal->get_all_records();

    cout << "[Debug] Loading records into Memtable...\n";
    memtable->loadFromWal(records);
    //memtable->printAllData();

    // --- Load Rate Limiter state after memtable is ready ---
    cout << "[Debug] Loading Rate Limiter state...\n";
    loadTokenBucket();

    // --- TypesManager setup ---
    cout << "[Debug] Initializing TypesManager...\n";
    typesManager = new TypesManager(this);

    cout << "[Debug] System initialization completed.\n";
}

System::~System() {
    cout << "[SYSTEM] Shutting down system and freeing resources...\n";

    // Save rate limiter state before shutdown
    //saveTokenBucket();

    delete lsmManager_;
    delete wal;
    delete memtable;
    delete tokenBucket;
    delete sstCursor;


    cout << "[SYSTEM] System shutdown complete.\n";
}

TypesManager* System::getTypesManager() {
    return typesManager;
}

bool System::checkRateLimit() {
    bool isRefilled;
    if (!tokenBucket->allowRequest(&isRefilled)) {
        cout << "\033[31m[RATE LIMIT] Request denied - rate limit exceeded. Try again later.\033[0m\n";
        return false;
    }

    requestCounter++;

    // Save state every SAVE_INTERVAL requests or when refill is done
    if (requestCounter % SAVE_INTERVAL == 0 || isRefilled) {
        cout << "[RATE LIMIT] Saving rate limiter state (request #" << requestCounter <<
            ", avaliable:" << tokenBucket->getTokens() << "/" << tokenBucket->getMaxTokens() << ")\n";
        saveTokenBucket();
    }

    return true;
}

void System::saveTokenBucket() {
    try {
        vector<byte> serializedData = tokenBucket->serialize();

        // Convert byte vector to string for storage
        //string serializedString(serializedData.begin(), serializedData.end());
        string serializedString(
            reinterpret_cast<const char*>(serializedData.data()),
            serializedData.size()
        );

        // Store directly in the key-value engine using internal storage
        // We need to directly access WAL and memtable to avoid recursive rate limiting
        wal->put(RATE_LIMIT_KEY, serializedString);
        memtable->put(RATE_LIMIT_KEY, serializedString);

        if (memtable->checkFlushIfNeeded()) {
            vector<Record> records = memtable->getRecordsFromOldest();
            add_records_to_cache(records);

            memtable->flushMemtable();

            cout << "[SYSTEM] Triggering compaction check...\n";
            lsmManager_->triggerCompactionCheck();
            cout << "[SYSTEM] Compaction check finished.\n";

            debugMemtable();
        }

        cout << "[RATE LIMIT] State saved successfully to key: " << RATE_LIMIT_KEY << "\n";

    }
    catch (const exception& e) {
        cerr << "[RATE LIMIT ERROR] Failed to save state: " << e.what() << "\n";
    }
}

void System::loadTokenBucket() {
    try {
        // Try to load from memtable first (no rate limiting on system operations)
        bool deleted = false;
        auto value = memtable->get(RATE_LIMIT_KEY, deleted);

        if (!deleted && value.has_value()) {
            // Convert string back to byte vector
            string serializedString = value.value();
            //vector<byte> data(serializedString.begin(), serializedString.end());
            vector<byte> data(
                reinterpret_cast<const byte*>(serializedString.data()),
                reinterpret_cast<const byte*>(serializedString.data() + serializedString.size())
            );

            // Deserialize and replace current rate limiter
            TokenBucket loadedBucket = TokenBucket::deserialize(data);
            delete tokenBucket;
            tokenBucket = new TokenBucket(loadedBucket);

            cout << "[RATE LIMIT] State loaded successfully from key: " << RATE_LIMIT_KEY << "\n";
        }
        else {
            tokenBucket = new TokenBucket(Config::max_tokens, Config::refill_interval);

            cout << "[RATE LIMIT] No saved state found, using default configuration\n";
        }

    }
    catch (const exception& e) {
        cerr << "[RATE LIMIT ERROR] Failed to load state: " << e.what() << "\n";
        cout << "[RATE LIMIT] Continuing with default configuration\n";
    }
}

void System::del(const string& key) {
    if (!checkRateLimit()) {
        return; // Request denied by rate limiter
    }

    cout << "Deleted from wal\n";
    wal->del(key);

    cout << "Deleted from memtable\n";
    memtable->remove(key);
}

// NIJE TESTIRANO
void System::add_records_to_cache(vector<Record> records) {
    int lenght;
    for (Record r : records) {
        if (r.tombstone == (byte)1) {
            cache->del(r.key);
        }
        else {
            lenght = r.value.size();
            vector<byte> valueInBytes(lenght);
            memcpy(valueInBytes.data(), r.value.data(), lenght);

            cache->put(r.key, valueInBytes);
        }
    }
}

void System::put(const string& key, const string& value) {
    if (!checkRateLimit()) {
        return; // Request denied by rate limiter
    }

    cout << "Put to wal\n";
    wal->put(key, value);

    cout << "Put to memtable\n";
    memtable->put(key, value);

    if (memtable->checkFlushIfNeeded()) {
        //prvo ubacujem sve recorde iz najstarijeg memtablea u cache.
        vector<Record> records = memtable->getRecordsFromOldest();
        add_records_to_cache(records);

        //onda mogu da flushujem, i oslobodim prostor
        memtable->flushMemtable();

        cout << "[SYSTEM] Triggering compaction check...\n";
        lsmManager_->triggerCompactionCheck();
        cout << "[SYSTEM] Compaction check finished.\n";

        debugMemtable();
    }

}

// NIJE TESTIRANO!!
optional<string> System::get(const string& key) {
    /*
        VRACA NULLOPT AKO KLJUC NE POSTOJI
        VRACA STRING VALUE AKO KLJUC POSTOJI
    */

    if (!checkRateLimit()) {
        cout << "Premasili ste broj upita po jedinici vremena, probajte kasnije\n OVO BI MOGLO BOLJE";
        return nullopt; // Request denied by rate limiter
    }

    bool deleted;
    // searching memtable
    auto value = memtable->get(key, deleted);

    // key exists in memtable, but is deleted
    if (deleted) {
        return nullopt;
    }

    // key exists in memtable, return value
    else if (value.has_value()) {
        return value.value();
    }

    // key doesnt exists in memtable. Read path goes forward

    // searching cache
    bool exists;
    vector<byte> bytes = cache->get(key, exists);
    // key exists in cache, return it
    if (exists) {
        // converting from vector<byte> to string ret
        string ret(bytes.size(), '\0');
        memcpy(ret.data(), bytes.data(), bytes.size());

        deleted = false;
        return ret;
    }

    // searching sstable (disc)
    value = sstable->get(key);

    // update cache
    if (value != nullopt) {
        int lenght = value.value().size();

        vector<byte> valueInBytes(lenght);
        memcpy(valueInBytes.data(), value.value().data(), lenght);

        cache->put(key, valueInBytes);
    }

    return value;
}

void System::debugWal() const {
    vector<Record> records = wal->get_all_records();
    cout << "[Debug] WAL Records: \n";

    for (const auto& r : records) {
        cout << "-----------------------------------\n";
        cout << "\033[31m" << "Key       : " << r.key << "\033[0m\n";
        cout << "\033[32m" << "Value     : " << r.value << "\033[0m\n";
        cout << "Tombstone : " << (int)r.tombstone << "\n";
        cout << "Timestamp : " << r.timestamp << "\n";
        cout << "CRC       : " << r.crc << "\n";
    }
    cout << "-----------------------------------\n";
    // red: << "\033[31m" << "This text is red!" << "\033[0m" <<
    // greem" "\033[32m" << "This text is green!" << "\033[0m"
}

void System::debugMemtable() const {
    memtable->printAllData();
}

void System::resetSystem(string dataFolder) {
    try {
        // Proveri da li folder postoji
        if (fs::exists(dataFolder) && fs::is_directory(dataFolder)) {
            // Brisanje svih fajlova i podfoldera
            fs::remove_all(dataFolder);
            // Ponovo kreiraj prazan folder
            fs::create_directory(dataFolder);
        }
        cout << "Folder 'data' je sada prazan." << endl;
    }
    catch (const fs::filesystem_error& e) {
        cerr << "Gre�ka: " << e.what() << endl;
    }
}

// sluzi za testiranje, ne znam jel treba system ovo da poziva...?
void System::removeSSTables() {
    vector<unique_ptr<SSTable>> tablesToRemove = sstable->getTablesFromLevel(1);
    if (tablesToRemove.size() >= 2) {
        tablesToRemove.resize(tablesToRemove.size() - 2);
    }

    sstable->removeSSTables(tablesToRemove);
}

void System::prefixScan(const std::string& prefix, int page_size, bool& end) {
    vector<Record> page = sstCursor->prefix_scan(prefix, 10, end);
    cout << "Done scan." << endl;
}