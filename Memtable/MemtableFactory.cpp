#include "MemtableFactory.h"

IMemtable* MemtableFactory::createMemtable(const std::string& type) {

    if (type == "hash_map") {
        return new MemtableHashMap();
    }
    else if (type == "skiplist") {
        return new MemtableSkipList();
    }

    else if (type == "btree") {
        return new BTree<4>(Config::memtable_max_size);
    }

    // Ako tip nije prepoznat, vracamo default, npr. hash
    return new MemtableHashMap();
}