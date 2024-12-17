#include "MemtableFactory.h"

IMemtable* MemtableFactory::createMemtable(const std::string& type) {
    if (type == "hash") {
        return new MemtableHashMap();
    }
    else if (type == "skiplist") {
        return new MemtableSkipList();
    }

    // Ako tip nije prepoznat, vracamo default, npr. hash
    return new MemtableHashMap();
}
