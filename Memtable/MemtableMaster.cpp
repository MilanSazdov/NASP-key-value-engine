#include "MemtableMaster.h"

template <int MEMTABLE_NUM>
void MemtableMaster<MEMTABLE_NUM>::put(const string& key, const string& value) {
    for (int i = 0; i < MEMTABLE_NUM; i++) {
         //TODO: isFull implementacije za sve memtable
        if(!tables[i]->isFull()) {
              tables[i]->put(key, value);
              return;
        }
    }
    //Sve je puno, praznimo.
    flushMemtables();
    tables[0].put(key, value);
}

template <int MEMTABLE_NUM>
optional<string> MemtableMaster<MEMTABLE_NUM>::get(const string& key) const {
  for (int i = 0; i < MEMTABLE_NUM; i++) {
    if(!tables[i]->size()==0) {
      return tables[i]->get(key);
    }
  }
  return std::nullopt;
}

template <int MEMTABLE_NUM>
void MemtableMaster<MEMTABLE_NUM>::remove(const string& key) {
  //TODO: Problem, sta ako key nije u poslednjem?
}

template <int MEMTABLE_NUM>
void MemtableMaster<MEMTABLE_NUM>::loadFromWal(const std::string& wal_file)
{
  std::ifstream file(wal_file);
  if (!file.is_open()) {
    std::cerr << "[MemtableMaster] Ne mogu da otvorim WAL fajl: " << wal_file << "\n";
    return;
  }

  std::string op, key, value;
  while (file >> op >> key) {
    if (op == "INSERT") {
      file >> value;
      put(key, value);
    }
    else if (op == "REMOVE") {
      remove(key);
    }
  }
  file.close();
}

