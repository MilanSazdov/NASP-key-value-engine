
#include <memory>
#include <iostream>
#include <map>
#include <fstream>
#include <stdexcept>
#include "MemtableManager.h"
#include "MemtableFactory.h"
#include "SizeTieredCompaction.h"
#include "LeveledCompaction.h"

MemtableManager::MemtableManager()
    : type_(Config::memtable_type),
    N_(Config::memtable_instances),
    maxSize_(Config::memtable_max_size),
    directory_(Config::data_directory),
    activeIndex_(0)
{
    // Kreiraj SSTManager
    sstManager_ = std::make_unique<SSTManager>();

    // Kreiraj odabranu strategiju kompakcije na osnovu vrednosti iz Config klase
    std::unique_ptr<CompactionStrategy> strategy;

    if (Config::compaction_strategy == "leveled") {
        std::cout << "[MemtableManager] Using Leveled Compaction Strategy (from Config)." << std::endl;
        strategy = std::make_unique<LeveledCompactionStrategy>(
            Config::l0_compaction_trigger,
            Config::max_levels,
            Config::target_file_size_base,
            Config::level_size_multiplier
        );
    }
    else if (Config::compaction_strategy == "size_tiered") {
        std::cout << "[MemtableManager] Using Size-Tiered Compaction Strategy (from Config)." << std::endl;
        strategy = std::make_unique<SizeTieredCompactionStrategy>(
            Config::min_threshold,
            Config::max_threshold
        );
    }
    else {
        throw std::runtime_error("Unknown compaction strategy in config: " + Config::compaction_strategy);
    }

    // Kreiraj LSMManager i prosledi mu kreiranu strategiju
    lsmManager_ = std::make_unique<LSMManager>(std::move(strategy), Config::max_levels);

    // Inicijalizuj prvu (aktivnu) Memtable
    memtables_.reserve(N_);
    auto first = std::unique_ptr<IMemtable>(createNewMemtable());
    first->setMaxSize(maxSize_);
    memtables_.push_back(std::move(first));

    // Pokreni pozadinsku nit u LSMManager-u
    lsmManager_->initialize();
}

MemtableManager::~MemtableManager() {
    // flushujemo sve preostale memtable pre gasenja da ne izgubimo podatke
    while (!memtables_.empty()) {
        flushOldest();
    }
}

void MemtableManager::put(const std::string& key, const std::string& value) {
    memtables_[activeIndex_]->put(key, value);
    std::cout << "[MemtableManager] Key '" << key << "' added.\n";
}

void MemtableManager::remove(const std::string& key) {
    memtables_[activeIndex_]->remove(key);
    std::cout << "[MemtableManager] Key '" << key << "' marked for deletion.\n";
    if (checkFlushIfNeeded()) {
        flushMemtable();
    }
}

bool MemtableManager::checkFlushIfNeeded() {
    if (memtables_[activeIndex_]->size() >= maxSize_) {
        if (memtables_.size() < N_) {
            std::cout << "[MemtableManager] Active memtable is full. Switching to a new one.\n";
            switchToNewMemtable();
            return false;
        }
        else {
            std::cout << "[MemtableManager] All memtable slots are full. Flushing the oldest one to make space.\n";
            //flushOldest
            //switchToNewMemtable
            return true;
        }
    }
}

void MemtableManager::flushMemtable() {
    flushOldest();
    switchToNewMemtable();
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

// (Milan kaze) NAPOMENA: OVO TREBA PREBACITI U LSM MANAGER DEO (vedran pita zasto??)
std::optional<std::string> MemtableManager::get(const std::string& key, bool& deleted) const {
    // prvo pretrazujemo memtable, od najnovije ka najstarijoj
    deleted = false;
    for (int i = static_cast<int>(memtables_.size()) - 1; i >= 0; i--) {
        auto val = memtables_[i]->get(key, deleted);
        if (deleted) {
            return nullopt;
        }
        if (val.has_value()) {
            return val;
        }
    }

    return std::nullopt;
}

IMemtable* MemtableManager::createNewMemtable() const {
    return MemtableFactory::createMemtable();
}

void MemtableManager::loadFromWal(const std::vector<Record>& records) {
    for (const auto& record : records) {
        if (static_cast<bool>(record.tombstone)) {
            memtables_[activeIndex_]->remove(record.key);
        }
        else {
            memtables_[activeIndex_]->put(record.key, record.value);
        }
        if (checkFlushIfNeeded()) {
            flushMemtable();
        }
    }
    std::cout << "[MemtableManager] " << records.size() << " records from WAL loaded into Memtable.\n";
}

void MemtableManager::printAllData() const {
    for (size_t i = 0; i < memtables_.size(); ++i) {
        std::cout << "Memtable " << i << (i == activeIndex_ ? " (READ WRITE)" : " (READ ONLY)") << ":\n";
        std::vector<MemtableEntry> entries = memtables_[i]->getAllMemtableEntries();
        for (const auto& entry : entries) {
            std::cout << "\033[31m" << "  Key: " << entry.key
                << "\033[0m" << ", " << "\033[32m" << "Value: " << entry.value
                << "\033[0m" << ", Tombstone: " << entry.tombstone
                << ", Timestamp: " << entry.timestamp << "\n";
        }
    }
// red: << "\033[31m" << "This text is red!" << "\033[0m" <<
// greem" "\033[32m" << "This text is green!" << "\033[0m"
}

//returns records from oldest memtable
vector<Record> MemtableManager::getRecordsFromOldest() {
    auto& oldestMemtable = memtables_.front();
    std::vector<MemtableEntry> entries = oldestMemtable->getAllMemtableEntries();

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
        return records;
    }
}