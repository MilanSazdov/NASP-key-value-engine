#pragma once

#include "IMemtable.h"
#include <string>
#include "MemtableHashMap.h"
#include "MemtableSkipList.h"
#include "BTree.h"

class MemtableFactory {
public:
	static IMemtable* createMemtable(const string& type);
};