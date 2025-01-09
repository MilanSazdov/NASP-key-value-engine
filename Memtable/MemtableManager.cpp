#include "MemtableManager.h"
#include "MemtableFactory.h"

// Pretpostavimo da imate definisanu klasu `SSTable` 
// s metodom build(...) koji prima vector<Record> (ili nesto slicno).
#include "../SSTable/SSTable.h"

// Takodje pretpostavljamo da imate definisanu `Record` strukturu 
// negde (u Wal ili slicno) koja sadrzi: key, value, tombstone, itd.

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

void MemtableManager::flushAll() const
{
    std::vector<Record> allRecords;

    // getAllMemEntries vraca sortirano, radimo merge kao u mergesort
    for (auto& mem : memtables_) {
        std::vector<Record> merged;
        vector<MemtableEntry> entries = mem->getAllMemtableEntries();

        merged.reserve(entries.size()+allRecords.size());

        int i = 0, j = 0;
        int allRecordsSize = allRecords.size();
        int entriesSize = entries.size();

        while (i < allRecordsSize && j < entriesSize) {
            if (allRecords[i].key < entries[j].key)
            {
                merged.push_back(allRecords[i]);
                ++i;
            }
            else
            {
                MemtableEntry e = entries[j];
                Record r;
                r.key = e.key;
                r.key_size = e.key.size();
                r.value = e.value;
                r.value_size = e.value.size();
                r.tombstone = (e.tombstone ? std::byte{ 1 } : std::byte{ 0 });
                r.timestamp = e.timestamp;
                merged.push_back(r);
                ++j;
            }
        }

        while (i < allRecordsSize)
            merged.push_back(allRecords[i++]);
        
        while (j < entriesSize)
        {
            MemtableEntry e = entries[j];
            Record r;
            r.key = e.key;
            r.key_size = e.key.size();
            r.value = e.value;
            r.value_size = e.value.size();
            r.tombstone = (e.tombstone ? std::byte{ 1 } : std::byte{ 0 });
            r.timestamp = e.timestamp;
            merged.push_back(r);
            ++j;
        }

        allRecords = merged;
    }
    
    sstManager.write(allRecords);
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
