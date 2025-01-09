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
    sstManager(directory)
{
    memtables_.reserve(N_);
    // Kreiramo prvu memtable (aktivnu)
    auto first = std::unique_ptr<IMemtable>(createNewMemtable());
    first->setMaxSize(maxSizePerTable);
    memtables_.push_back(std::move(first));
    activeIndex_ = 0;
}

MemtableManager::MemtableManager(const std::string& type, size_t N, size_t maxSizePerTable)
    : type_(type),
    N_(N),
    maxSize_(maxSizePerTable),
    sstManager("./")
{
    memtables_.reserve(N_);
    // Kreiramo prvu memtable (aktivnu)
    auto first = std::unique_ptr<IMemtable>(createNewMemtable());
    first->setMaxSize(maxSizePerTable);
    memtables_.push_back(std::move(first));
    activeIndex_ = 0;

}

MemtableManager::~MemtableManager() {
    flushAll();
}

void MemtableManager::put(const std::string& key, const std::string& value) {
    // Ubacujemo u aktivnu
    auto& active = memtables_[activeIndex_];
    active->put(key, value);

    // Ako predje maxSize -> prelazimo na nov memtable
    if (active->size() >= maxSize_) {
        if (memtables_.size() < N_) {
            switchToNewMemtable();
        }
        else {
            // TODO: Kada popunimo svih N, FLUSUHJEMO PRVU NAPRAVLJENU (NAJSTARIJU) i od nje pravimo novi SSTable
            // Popunili smo svih N -> flushAll
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
    if (memtables_.empty()) {
        return; // Nema aktivnih memtables za flush
    }

    // Uzimamo najstariju memtable (prva u vektoru)
    auto& oldestMemtable = memtables_.front();

    // Pretvaramo njene unose u zapise za SSTable
    std::vector<Record> records;
    for (const auto& entry : oldestMemtable->getAllMemtableEntries()) {
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
    sstManager.write(records);

    // Brišemo najstariju memtable iz memorije
    memtables_.erase(memtables_.begin());
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