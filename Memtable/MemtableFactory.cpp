#include "MemtableFactory.h"

IMemtable* MemtableFactory::createMemtable() {

    if (Config::memtable_type == "hash_map") {
        return new MemtableHashMap();
    }
    else if (Config::memtable_type == "skiplist") {
        return new MemtableSkipList();
    }

    else if (Config::memtable_type == "btree") {
        return new BTree<4>(5);
    }

    // Ako tip nije prepoznat, vracamo default, npr. hash
    return new MemtableHashMap();
}