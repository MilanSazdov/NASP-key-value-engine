#pragma once

#include "IMemtable.h"
#include "wal.h"
#include <unordered_map>
#include <string>
#include <optional>
#include <cstdint>   // za uint64_t
#include <chrono>
#include <vector>
#include <algorithm> // za std::sort

// Proširujemo IMemtable tako da koristimo struct MemtableEntry,
// koji sadrži i tombstone i timestamp (videti definiciju u IMemtable).
class MemtableHashMap : public IMemtable {

public:
    MemtableHashMap();
    ~MemtableHashMap() override = default;

    // Implementacije osnovnih metoda:
    void put(const std::string& key, const std::string& value) override;
    void remove(const std::string& key) override;
    std::optional<std::string> get(const std::string& key) const override;
    size_t size() const override;
    void setMaxSize(size_t maxSize) override;
    void loadFromWal(const std::string& wal_file) override;
    // void loadFromRecords(const std::vector<Record>& records) override;

    // NOVO: vraća sve zapise sa (key, value, tombstone, timestamp)
    std::vector<MemtableEntry> getAllMemtableEntries() const override;

	// NOVO: dohvata jedan zapis
    std::optional<MemtableEntry> getEntry(const std::string& key) const override;

	// NOVO: ažurira zapis
	void updateEntry(const std::string& key, const MemtableEntry& entry) override;

    virtual std::vector<MemtableEntry> getSortedEntries() const override;

private:
    // Struktura koju čuvamo u memoriji: (value, tombstone, timestamp)
    struct Entry {
        std::string value;
        bool tombstone;
        uint64_t timestamp;
    };

    // Mapa: key -> Entry
    std::unordered_map<std::string, Entry> table_;

    // Maksimalan broj ključeva
    size_t maxSize;

    // Pomoćna funkcija: dohvat "sadašnjeg" UNIX vremena u sekundama
    uint64_t currentTime() const {
        return static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count());
    }
};
