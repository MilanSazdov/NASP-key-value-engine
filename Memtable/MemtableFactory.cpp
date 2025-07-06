#include "MemtableFactory.h"

IMemtable* MemtableFactory::createMemtable() {

    if (Config::memtable_type == "hash_map") {
        return new MemtableHashMap();
    }
    else if (Config::memtable_type == "skiplist") {
        return new MemtableSkipList();
    }

    else if (type == "btree") {
        return new BTree<16>(Config::memtable_max_size);
    }
    // Ako tip nije prepoznat, vracamo default, npr. hash
    return new MemtableHashMap();
}