#pragma once


#include "wal.h"
#include "MemtableManager.h"
#include "Config.h"

class System {
	
public:
	Wal* wal;
	MemtableManager* memtable;
	SSTManager* sstable;
	Cache<string>* cache;

public:
	System();

	~System();

	void put(const std::string& key, const std::string& value);
	void del(const std::string& key);
	std::optional<std::string> get(const std::string& key);

	void debugWal() const;
	void debugMemtable() const;

private:

	// Onemogucavamo kopiranje da bismo izbegli probleme sa vlasnistvom pokazivaca
	System(const System&) = delete; // Prevent copying
	System& operator=(const System&) = delete; // Prevent assignment

};