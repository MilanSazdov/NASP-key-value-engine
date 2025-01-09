#include <fstream>
#include <optional>
#include <string>
#include <vector>
#include <algorithm> // Za sort
#include <iostream>
#include "MemtableHashMap.h"


MemtableHashMap::MemtableHashMap()
    : maxSize(1000) // default TODO: Citanje iz config
{}

// put => upis (key, value), tombstone= false, timestamp = currentTime
void MemtableHashMap::put(const string& key, const string& value) {
    // Ako smo premašili maxSize, a ključ ne postoji, nećemo ubaciti (ili flush?)
    if (table_.size() >= maxSize && table_.find(key) == table_.end()) {
        std::cerr << "[MemtableHashMap] Max size reached, cannot insert new key: " << key << "\n";
        return;
    }
    Entry e{ value, false, currentTime() };
    table_[key] = e;
}




// remove => postavimo tombstone = true, value = ""
void MemtableHashMap::remove(const std::string& key) {
    auto it = table_.find(key);
    // Kljuc ne postoji, provera da li mozemo da ga insertujemo
    if (it == table_.end() && table_.size() >= maxSize) {
        std::cerr << "[MemtableHashMap] Max size reached, cannot insert new key: " << key << "\n";
        return;
    }

    // Kljuc postoji, ili ima mesta da ga dodamo. Gazimo stari entry / upisujemo novi
    Entry e{ "", true, currentTime() };
    table_[key] = e;
}

// get => vraća value ako tombstone=false, inače nullopt
optional<string> MemtableHashMap::get(const string& key) const {
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullopt;
    }
    if (it->second.tombstone) {
        // obrisan
        return nullopt;
    }
    return it->second.value;
}

size_t MemtableHashMap::size() const {
    return table_.size();
}

void MemtableHashMap::setMaxSize(size_t maxSize) {
    this->maxSize = maxSize;
}

// loadFromWal (string path) => čita fajl i ubacuje (key, value) 
// (primer je trivijalan, u praksi WAL je binaran)
void MemtableHashMap::loadFromWal(const string& wal_file) {
    ifstream file(wal_file);
    if (!file.is_open()) {
        cerr << "[MemtableHashMap] Could not open WAL file: " << wal_file << "\n";
        return;
    }

    // Napomena: ovo je samo primer. Pravi WAL je binaran i sadrži Record polja.
    // Ovde, iz teksta citamo "key value" u jednoj liniji
    string key, value;
    while (file >> key >> value) {
        // moze se desiti i da je value == "TOMB" i da interpretiramo to kao tombstone, itd.
        // ovde radimo simplifikovano
        Entry e;
        e.value = value;
        e.tombstone = false;
        e.timestamp = currentTime();
        table_[key] = e;
    }
    file.close();
}

// loadFromWal (vector<Record>) => punimo memtable 
// ako record.tombstone == 1 => remove, else => put
/*
void MemtableHashMap::loadFromRecords(const vector<Record>& records) {
    for (const auto& r : records) {
        if (static_cast<int>(r.tombstone) == 1) {
            // remove
            Entry e;
            e.value = "";
            e.tombstone = true;
            e.timestamp = r.timestamp;  // preuzmemo iz Record
            table_[r.key] = e;
        }
        else {
            // put
            Entry e;
            e.value = r.value;
            e.tombstone = false;
            e.timestamp = r.timestamp;
            table_[r.key] = e;
        }
    }
}
*/
// getAllKeyValuePairs => samo (key, value) za one koji nisu tombstone
/*
vector<pair<string, string>> MemtableHashMap::getAllKeyValuePairs() const {
    vector<pair<string, string>> result;
    result.reserve(table_.size());
    for (const auto& [k, e] : table_) {
        if (!e.tombstone) {
            result.push_back({ k, e.value });
        }
    }
    return result;
}
*/

vector<MemtableEntry> MemtableHashMap::getAllMemtableEntries() const {
    vector<MemtableEntry> result;
    result.reserve(table_.size());
    for (const auto& [k, e] : table_) {
        MemtableEntry me;
        me.key = k;
        me.value = e.value;
        me.tombstone = e.tombstone;
        me.timestamp = e.timestamp;
        result.push_back(me);
    }

    sort(result.begin(), result.end());
    return result;
}