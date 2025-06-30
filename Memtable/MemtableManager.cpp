
#include <memory>
#include <iostream>
#include <map>
#include <fstream>
#include <stdexcept>
#include "MemtableManager.h"
#include "MemtableFactory.h"
#include "SizeTieredCompaction.h"
#include "LeveledCompaction.h"

// pomocna funkcija za citanje konfiguracije
std::map<std::string, std::string> read_config(const std::string& config_path) {
    std::cout << "[Config] Reading configuration from: " << config_path << std::endl;
    std::map<std::string, std::string> config;
    std::ifstream config_file(config_path);

    if (!config_file.is_open()) {
        std::cerr << "[Config] WARNING: Could not open config file '" << config_path << "'. Using default values." << std::endl;
        // postavi podrazumevane vrednosti ako fajl ne postoji
        config["compaction_strategy"] = "leveled";
        config["max_levels"] = "7";
        config["l0_compaction_trigger"] = "4";
        config["target_file_size_base"] = "2097152"; // 2MB
        config["level_size_multiplier"] = "10";
        config["min_threshold"] = "4";  // Default za size_tiered
        config["max_threshold"] = "32"; // Default za size_tiered
        return config;
    }

    std::string line;
    while (std::getline(config_file, line)) {
        if (line.empty() || line[0] == '#') continue;
        size_t delimiter_pos = line.find('=');
        if (delimiter_pos != std::string::npos) {
            std::string key = line.substr(0, delimiter_pos);
            std::string value = line.substr(delimiter_pos + 1);
            key.erase(key.find_last_not_of(" \t\n\r") + 1);
            key.erase(0, key.find_first_not_of(" \t\n\r"));
            value.erase(value.find_last_not_of(" \t\n\r") + 1);
            value.erase(0, value.find_first_not_of(" \t\n\r"));
            config[key] = value;
        }
    }
    std::cout << "[Config] Reading config done\n";
    return config;
}

MemtableManager::MemtableManager(
    std::string type,
    size_t N,
    size_t maxSizePerTable,
    std::string directory,
    std::string config_path): 
    type_(type),
    N_(N),
    maxSize_(maxSizePerTable),
    directory_(directory),
    activeIndex_(0)
{
    sstManager_ = std::make_unique<SSTManager>(directory);

    auto config = read_config(config_path);

    std::unique_ptr<CompactionStrategy> strategy;
    int max_levels = std::stoi(config.at("max_levels"));

    if (config.at("compaction_strategy") == "leveled") {
        std::cout << "[MemtableManager] Using Leveled Compaction Strategy." << std::endl;
        strategy = std::make_unique<LeveledCompactionStrategy>(
            std::stoi(config.at("l0_compaction_trigger")),
            max_levels,
            std::stoull(config.at("target_file_size_base")),
            std::stoi(config.at("level_size_multiplier"))
        );
    }
    else if (config.at("compaction_strategy") == "size_tiered") {
        std::cout << "[MemtableManager] Using Size-Tiered Compaction Strategy." << std::endl;
        strategy = std::make_unique<SizeTieredCompactionStrategy>(
            std::stoi(config.at("min_threshold")),
            std::stoi(config.at("max_threshold"))
        );
    }
    else {
        throw std::runtime_error("Unknown compaction strategy in config: " + config.at("compaction_strategy"));
    }

    lsmManager_ = std::make_unique<LSMManager>(directory, std::move(strategy), max_levels);

    memtables_.reserve(N_);
    auto first = std::unique_ptr<IMemtable>(createNewMemtable());
    first->setMaxSize(maxSizePerTable);
    memtables_.push_back(std::move(first));

    std::cout << "Initializing lsm manager\n";

    lsmManager_->initialize();

}

MemtableManager::~MemtableManager() {
    // flushujemo sve preostale memtable pre gasenja da ne izgubimo podatke
    while (!memtables_.empty()){
        flushOldest();
    }
}

void MemtableManager::put(const std::string& key, const std::string& value) {
    memtables_[activeIndex_]->put(key, value);
    std::cout << "[MemtableManager] Key '" << key << "' added.\n";
    checkAndFlushIfNeeded();
}

void MemtableManager::remove(const std::string& key) {
    memtables_[activeIndex_]->remove(key);
    std::cout << "[MemtableManager] Key '" << key << "' marked for deletion.\n";
    checkAndFlushIfNeeded();
}

void MemtableManager::checkAndFlushIfNeeded() {
    if (memtables_[activeIndex_]->size() >= maxSize_) {
        if (memtables_.size() < N_) {
            std::cout << "[MemtableManager] Active memtable is full. Switching to a new one.\n";
            switchToNewMemtable();
        }
        else {
            std::cout << "[MemtableManager] All memtable slots are full. Flushing the oldest one to make space.\n";
            flushOldest();
            switchToNewMemtable();
        }
    }
}

void MemtableManager::switchToNewMemtable() {
    auto newMem = std::unique_ptr<IMemtable>(createNewMemtable());
    newMem->setMaxSize(maxSize_);
    memtables_.push_back(std::move(newMem));
    activeIndex_ = memtables_.size() - 1; // Nova aktivna tabela je poslednja dodata
}

void MemtableManager::flushOldest() {
    if (memtables_.empty()) {
        return;
    }

    // uzimamo najstariju memtable (ona koja je na pocetku vektora)
    auto& oldestMemtable = memtables_.front();
    std::vector<MemtableEntry> entries = oldestMemtable->getSortedEntries();

    if (entries.empty()) {
        std::cout << "[MemtableManager] Oldest memtable is empty, removing it without flushing." << std::endl;
    }
    else {
        std::vector<Record> records;
        records.reserve(entries.size());
        for (const auto& entry : entries) {
            Record r;
            r.key = entry.key;
            r.key_size = entry.key.size();
            r.value = entry.value;
            r.value_size = entry.value.size();
            r.tombstone = entry.tombstone ? std::byte{ 1 } : std::byte{ 0 };
            r.timestamp = entry.timestamp;
            records.push_back(r);
        }
        std::cout << "[MemtableManager] Flushing " << records.size() << " records to Level 0...\n";
        lsmManager_->flushToLevel0(records);
    }

    // brisemo najstariju memtable iz memorije
    memtables_.erase(memtables_.begin());

    // Posto smo obrisali element sa pocetka, svi indeksi su se pomerili ulevo
    if (activeIndex_ > 0) {
        activeIndex_--;
    }
    else {
        // ako je bila samo jedna tabela, sada je vektor prazan
        // Naredni put ce kreirati novu tabelu
        if (memtables_.empty()) {
            activeIndex_ = 0; // Reset
        }
    }
}

// NAPOMENA: OVO TREBA PREBACITI U LSM MANAGER DEO
std::optional<std::string> MemtableManager::get(const std::string& key) const {
    // prvo pretrazujemo memtable, od najnovije ka najstarijoj
    for (int i = static_cast<int>(memtables_.size()) - 1; i >= 0; i--) {
        auto val = memtables_[i]->get(key);
        if (val.has_value()) {
            return val;
        }
    }

    // Ako nije u memtable, pretrazujemo LSM stablo
    // TODO: LSMManager treba da ima svoju get metodu koja pretrazuje nivoe
    // za sada, pozivamo SSTManager direktno, sto nije idealno
    auto result = sstManager_->get(key);
    if (!result.empty()) {
        return result;
    }

    return std::nullopt;
}


/*
void MemtableManager::flushAll() {
    cout << "[DEBUG] Memtable -> flushAll\n";
    if (memtables_.empty()) {
        return; // Nema aktivnih memtables za flush
    }

    // Uzimamo najstariju memtable (prva u vektoru)
    auto& oldestMemtable = memtables_.front();

    std::vector<MemtableEntry> entries = oldestMemtable->getSortedEntries();

    // Pretvaramo njene unose u zapise za SSTable
    std::vector<Record> records;
    for (const auto& entry : entries) {
        Record r;
        //TODO: CRC treba dodati
        r.key = entry.key;
        r.key_size = entry.key.size();
        r.value = entry.value;
        r.value_size = entry.value.size();
        r.tombstone = (entry.tombstone ? std::byte{ 1 } : std::byte{ 0 });
        r.timestamp = entry.timestamp;
        records.push_back(r);
    }

    // Pišemo ove zapise u SSTable koristeći sstManager
    std::cout << "[MemtableManager] Flushing to SSTable...\n";
    lsmManager.flushToLevel0(records);

    // Brišemo najstariju memtable iz memorije
    memtables_.erase(memtables_.begin());
    std::cout << "[MemtableManager] Flushed and removed the oldest Memtable.\n";
}
*/

IMemtable* MemtableManager::createNewMemtable() const {
    return MemtableFactory::createMemtable(type_);
}

/*
// Trazenje kljuca u aktivnoj tabeli, ako ga ne nadjemo => dodajemo
// Ako ga nadjem, menjam tombstone i timestamp
// TODO: LANMI MAJAMI
void MemtableManager::loadFromWal(const std::vector<Record>& records) {
    for (const auto& record : records) {

        // Proveravamo da li kljuc vec postoji u aktivnoj memtable
        auto existingValue = memtables_[activeIndex_]->get(record.key);

        if (existingValue.has_value()) {
            // Dobijamo trenutni zapis iz opcionalnog rezultata
            MemtableEntry currentEntry = memtables_[activeIndex_]->getEntry(record.key).value();
            currentEntry.tombstone = static_cast<bool>(record.tombstone);
            currentEntry.timestamp = record.timestamp;

            // Čuvamo ažurirani zapis nazad u Memtable
            memtables_[activeIndex_]->updateEntry(record.key, currentEntry);
            std::cout << "[MemtableManager] Key '" << record.key << "' updated with new tombstone and timestamp.\n";
        }
        else {
            // Ako ključ ne postoji, dodajemo ga
            std::string compositeValue = record.value;
            memtables_[activeIndex_]->put(record.key, compositeValue);
            std::cout << "[MemtableManager] Key '" << record.key << "' added.\n";
        }

        // Provera da li je aktivna Memtable popunjena
        if (memtables_[activeIndex_]->size() >= maxSize_) {
            if (memtables_.size() < N_) {
                switchToNewMemtable();
				std::cout << "[MemtableManager] Switched to new Memtable.\n";
            }
            else {
                // Ako su sve Memtable popunjene, flushujemo najstariju i kreiramo novu
				std::cout << "[MemtableManager] Flushing all Memtables.\n";
                flushAll();
                auto newMemtable = std::unique_ptr<IMemtable>(createNewMemtable());
                newMemtable->setMaxSize(maxSize_);
                memtables_.push_back(std::move(newMemtable));
                activeIndex_ = memtables_.size() - 1;
            }
        }
    }

    std::cout << "[MemtableManager] Records from WAL loaded into Memtable.\n";
}
*/

void MemtableManager::loadFromWal(const std::vector<Record>& records) {
    for (const auto& record : records) {
        if (static_cast<bool>(record.tombstone)) {
            memtables_[activeIndex_]->remove(record.key);
        }
        else {
            memtables_[activeIndex_]->put(record.key, record.value);
        }
        checkAndFlushIfNeeded();
    }
    std::cout << "[MemtableManager] " << records.size() << " records from WAL loaded into Memtable.\n";
}

void MemtableManager::printAllData() const {
    for (size_t i = 0; i < memtables_.size(); ++i) {
        std::cout << "Memtable " << i << (i == activeIndex_ ? " (READ WRITE)" : " (READ ONLY)") << ":\n";
        std::vector<MemtableEntry> entries = memtables_[i]->getAllMemtableEntries();
        for (const auto& entry : entries) {
            std::cout << "\033[31m" << "  Key: " << entry.key
                << "\033[0m" <<", " << "\033[32m" << "Value: " << entry.value
                << "\033[0m" << ", Tombstone: " << entry.tombstone
                << ", Timestamp: " << entry.timestamp << "\n";
        }
    }
}

// red: << "\033[31m" << "This text is red!" << "\033[0m" <<
// greem" "\033[32m" << "This text is green!" << "\033[0m"