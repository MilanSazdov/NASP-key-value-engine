
#pragma once

#include "IMemtable.h"
#include "SkipList.h"
#include <string>
#include <optional>
#include <iostream>
#include <fstream>
#include <chrono>

// Ova klasa implementira IMemtable koristeci skip listu kao pozadinsku strukturu podataka.

class MemtableSkipList : public IMemtable {
public:
    MemtableSkipList();

    // put ubacuje ili azurira kljuc i vrednost
    void put(const std::string& key, const std::string& value) override;

    // remove brise dati kljuc ako postoji
    void remove(const std::string& key) override;

    // get vraca vrednost ako kljuc postoji, ili nullopt ako ne postoji
    std::optional<std::string> get(const std::string& key, bool& deleted) const override;

    // size vraca broj elemenata u memtejblu
    size_t size() const override;

    // setMaxSize postavlja maksimalnu velicinu memtejbla
    void setMaxSize(size_t maxSize) override;
    
    // vector<pair<string, string>> getAllKeyValuePairs() const override;

    std::vector<MemtableEntry> getAllMemtableEntries() const override;

	// NOVO: dohvata jedan zapis
	std::optional<MemtableEntry> getEntry(const std::string& key) const override;

	// NOVO: ažurira zapis
	void updateEntry(const std::string& key, const MemtableEntry& entry) override;

    virtual std::vector<MemtableEntry> getSortedEntries() const override;

private:
    SkipList skiplist_;
    size_t maxSize_;

    // Pomoćna funkcija: dohvat trenutnog UNIX vremena
    uint64_t currentTime() const {
        return static_cast<uint64_t>(std::chrono::system_clock::now().time_since_epoch().count());
    }
};