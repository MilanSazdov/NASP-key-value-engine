#include "System.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;

const std::string System::RATE_LIMIT_KEY = "system_tokenbucket";

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

//TODO: sstable is uninitialized. fix?
System::System() : requestCounter(0) {
    std::cout << "[SYSTEM] Starting initialization... \n";

    std::cout << "Reading Config file ... \n";
    bool new_system = Config::load_init_configuration();
    if (new_system) {
        std::cout << "[Debug] Reseting system.\n";
        resetSystem(Config::data_directory);
        resetSystem(Config::wal_directory);
    }
    else {
        std::cout << "[Debug] Old configuration is in use.\n";
    }

    // ovo stoji dok radim testiranje, posle treba samo ovo od gore
    resetSystem(Config::data_directory);
    resetSystem(Config::wal_directory);

    sharedInstanceBM = new Block_manager();

    // --- Folder checks ---
    ensureDirectory(Config::data_directory);

    // --- WAL setup ---
    std::cout << "[Debug] Initializing WAL...\n";
    wal = new Wal(*sharedInstanceBM);

    // --- Cache setup ---

    cout << "[Debug] Initializing Cache...\n";
    cache = new Cache<string>();
    cout << "Cache initialized\n";

    // --- SSTManager setup ---
    sstable = new SSTManager(sharedInstanceBM);

    // --- Memtable setup ---
    std::cout << "[Debug] Initializing MemtableManager...\n";
    memtable = new MemtableManager(sstable);

	//std::cout << "[Debug] Printing existing sstables.\n";
    //memtable->printSSTables(1);

    // --- Load from WAL ---
    std::cout << "[Debug] Retrieving records from WAL...\n";
    std::vector<Record> records = wal->get_all_records();

    std::cout << "[Debug] Loading records into Memtable...\n";
    memtable->loadFromWal(records);
	//memtable->printAllData();

    // --- Load Rate Limiter state after memtable is ready ---
    std::cout << "[Debug] Loading Rate Limiter state...\n";
    loadTokenBucket();

	// --- TypesManager setup ---
	std::cout << "[Debug] Initializing TypesManager...\n";
	typesManager = new TypesManager(this);

    std::cout << "[Debug] System initialization completed.\n";
}

System::~System() {
	std::cout << "[SYSTEM] Shutting down system and freeing resources...\n";

    // Save rate limiter state before shutdown
    //saveTokenBucket();

	delete wal;
	delete memtable;
    delete tokenBucket;



	std::cout << "[SYSTEM] System shutdown complete.\n";
}

TypesManager* System::getTypesManager() {
    return typesManager;
}

bool System::checkRateLimit() {
    if (!tokenBucket->allowRequest()) {
        std::cout << "\033[31m[RATE LIMIT] Request denied - rate limit exceeded. Try again later.\033[0m\n";
        return false;
    }

    requestCounter++;

    // Save state every SAVE_INTERVAL requests
    if (requestCounter % SAVE_INTERVAL == 0) {
        std::cout << "[RATE LIMIT] Saving rate limiter state (request #" << requestCounter << ")\n";
        saveTokenBucket();
    }

    return true;
}

void System::saveTokenBucket() {
    try {
        std::vector<byte> serializedData = tokenBucket->serialize();

        // Convert byte vector to string for storage
        //std::string serializedString(serializedData.begin(), serializedData.end());
        std::string serializedString(
            reinterpret_cast<const char*>(serializedData.data()),
            serializedData.size()
        );

        // Store directly in the key-value engine using internal storage
        // We need to directly access WAL and memtable to avoid recursive rate limiting
        wal->put(RATE_LIMIT_KEY, serializedString);
        memtable->put(RATE_LIMIT_KEY, serializedString);

        std::cout << "[RATE LIMIT] State saved successfully to key: " << RATE_LIMIT_KEY << "\n";

    }
    catch (const std::exception& e) {
        std::cerr << "[RATE LIMIT ERROR] Failed to save state: " << e.what() << "\n";
    }
}

void System::loadTokenBucket() {
    try {
        // Try to load from memtable first (no rate limiting on system operations)
        bool deleted = false;
        auto value = memtable->get(RATE_LIMIT_KEY, deleted);

        if (!deleted && value.has_value()) {
            // Convert string back to byte vector
            std::string serializedString = value.value();
            //std::vector<byte> data(serializedString.begin(), serializedString.end());
            std::vector<std::byte> data(
                reinterpret_cast<const std::byte*>(serializedString.data()),
                reinterpret_cast<const std::byte*>(serializedString.data() + serializedString.size())
            );

            // Deserialize and replace current rate limiter
            TokenBucket loadedBucket = TokenBucket::deserialize(data);
            delete tokenBucket;
            tokenBucket = new TokenBucket(loadedBucket);

            std::cout << "[RATE LIMIT] State loaded successfully from key: " << RATE_LIMIT_KEY << "\n";
        }
        else {
            tokenBucket = new TokenBucket(Config::max_tokens, Config::refill_interval);

            std::cout << "[RATE LIMIT] No saved state found, using default configuration\n";
        }

    }
    catch (const std::exception& e) {
        std::cerr << "[RATE LIMIT ERROR] Failed to load state: " << e.what() << "\n";
        std::cout << "[RATE LIMIT] Continuing with default configuration\n";
    }
}

void System::del(const std::string& key) {
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
        if (r.tombstone == (byte)0) {
            cache->del(r.key);
        }
        else {
            lenght = r.value.size();
            vector<byte> valueInBytes(lenght);
            memcpy(valueInBytes.data(), r.key.data(), lenght);

            cache->put(r.key, valueInBytes);
        }
    }
}

void System::put(const std::string& key, const std::string& value) {
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

        debugMemtable();
    }
    
}

// NIJE TESTIRANO!!
std::optional<std::string> System::get(const std::string& key) {
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

void System::resetSystem(std::string dataFolder) {
    try {
        // Proveri da li folder postoji
        if (fs::exists(dataFolder) && fs::is_directory(dataFolder)) {
            // Brisanje svih fajlova i podfoldera
            fs::remove_all(dataFolder);
            // Ponovo kreiraj prazan folder
            fs::create_directory(dataFolder);
        }
        std::cout << "Folder 'data' je sada prazan." << std::endl;
    }
    catch (const fs::filesystem_error& e) {
        std::cerr << "Greška: " << e.what() << std::endl;
    }
}