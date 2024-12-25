#pragma once

#include "IMemtable.h"
#include "MemtableFactory.h"
#include <string>

using namespace std;

template<int MEMTABLE_NUM> class MemtableMaster {

    IMemtable* tables[MEMTABLE_NUM];

    public:
      MemtableMaster(string type) {
        for (int i = 0; i < MEMTABLE_NUM; i++) {
          tables[i] = MemtableFactory::createMemtable(type);
        }
      }
      ~MemtableMaster() {}

    void put(const string& key, const string& value);

    void remove(const string& key);

    optional<string> get(const string& key) const;

    void loadFromWal(const string& wal_file);

    void flushMemtables();
};

