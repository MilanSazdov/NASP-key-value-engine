#include "MemtableManager.h"
#include "MemtableFactory.h"
#include <memory>

MemtableManager::MemtableManager(const std::string& type,
    size_t N,
    size_t maxSizePerTable,
    const std::string& directory)
    : type_(type),
    N_(N),
    maxSize_(maxSizePerTable),
    sstManager(directory),
    lsmManager(directory, /* maxLevel = */ 4, /* compactionThreshold = */ 3)
{
    memtables_.reserve(N_);
    // Kreiramo prvu memtable (aktivnu)
    auto first = std::unique_ptr<IMemtable>(createNewMemtable());
    first->setMaxSize(maxSizePerTable);
    memtables_.push_back(std::move(first));
    activeIndex_ = 0;

    lsmManager.initialize();
}

MemtableManager::MemtableManager(const std::string& type, size_t N, size_t maxSizePerTable)
    : type_(type),
    N_(N),
    maxSize_(maxSizePerTable),
    sstManager("../data"),
    lsmManager("../data", 4, 3)
{
    memtables_.reserve(N_);
    // Kreiramo prvu memtable (aktivnu)
    auto first = std::unique_ptr<IMemtable>(createNewMemtable());
    first->setMaxSize(maxSizePerTable);
    memtables_.push_back(std::move(first));
    activeIndex_ = 0;

    lsmManager.initialize();
}

MemtableManager::~MemtableManager() {
    flushAll();
}

void MemtableManager::put(const std::string& key, const std::string& value) {
    // Ubacujemo u aktivnu
    auto& active = memtables_[activeIndex_];
    active->put(key, value);

	std::cout << "[MemtableManager] Key '" << key << "' added.\n";

    // Ako predje maxSize -> prelazimo na nov memtable
    if (active->size() >= maxSize_) {
        if (memtables_.size() < N_) {
            cout << "Switching to new memtable\n";
            switchToNewMemtable();
        }
        else {
            // TODO: Kada popunimo svih N, FLUSUHJEMO PRVU NAPRAVLJENU (NAJSTARIJU) i od nje pravimo novi SSTable
            // Popunili smo svih N -> flushAll
            // Pre flushovanja, Memtable treba da bude sortirana po kljucevima
            // TO ce omoguciti da se napravi ispravan SSTable koji ocekuje sortirane zapise
            flushAll();
            // i ponovo kreiramo jednu memtable
            memtables_.clear();
            auto fresh = std::unique_ptr<IMemtable>(createNewMemtable());
            fresh->setMaxSize(maxSize_);
            memtables_.push_back(std::move(fresh));
            activeIndex_ = 0;
        }
    }
}

void MemtableManager::remove(const std::string& key) {
    auto& active = memtables_[activeIndex_];
    active->remove(key);

    if (active->size() >= maxSize_) {
        if (memtables_.size() < N_) {
            switchToNewMemtable();
        }
        else {
            flushAll();
            memtables_.clear();
            auto fresh = std::unique_ptr<IMemtable>(createNewMemtable());
            fresh->setMaxSize(maxSize_);
            memtables_.push_back(std::move(fresh));
            activeIndex_ = 0;
        }
    }
}

std::optional<std::string> MemtableManager::get(const std::string& key) const {
    // Provera da li je u memtable, ako ne, prosleduje sstManageru
    for (int i = static_cast<int>(memtables_.size()) - 1; i >= 0; i--) {
        auto val = memtables_[i]->get(key);
        if (val.has_value()) {
            return val;
        }
    }

    // Ako nije ni u jednoj -> probamo u SSTable
    auto result = sstManager.get(key);
    if (!result.empty()) {
        return result;
    }

    return std::nullopt;
}

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


IMemtable* MemtableManager::createNewMemtable() const {
    return MemtableFactory::createMemtable(type_);
}

void MemtableManager::switchToNewMemtable() {
    // kreiramo novu i stavljamo je kao aktivnu
    auto newMem = std::unique_ptr<IMemtable>(createNewMemtable());
    newMem->setMaxSize(maxSize_);
    memtables_.push_back(std::move(newMem));
    activeIndex_ = memtables_.size() - 1;
}

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