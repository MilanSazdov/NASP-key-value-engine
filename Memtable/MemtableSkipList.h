#pragma once

#include "IMemtable.h"
#include "SkipList.h"
#include <string>
#include <optional>
#include <iostream>
#include <fstream>

// Ova klasa implementira IMemtable koristeci skip listu kao pozadinsku strukturu podataka.

class MemtableSkipList : public IMemtable {
public:
    MemtableSkipList();

    // put ubacuje ili azurira kljuc i vrednost
    void put(const std::string& key, const std::string& value) override;

    // remove brise dati kljuc ako postoji
    void remove(const std::string& key) override;

    // get vraca vrednost ako kljuc postoji, ili nullopt ako ne postoji
    std::optional<std::string> get(const std::string& key) const override;

    // size vraca broj elemenata u memtejblu
    size_t size() const override;

    // setMaxSize postavlja maksimalnu velicinu memtejbla
    void setMaxSize(size_t maxSize) override;

    // loadFromWal ucitava podatke iz WAL fajla.
    void loadFromWal(const std::string& wal_file) override;

private:
    SkipList skiplist_;
    size_t maxSize_;
};
