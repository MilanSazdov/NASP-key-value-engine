#pragma once


#include "wal.h"
#include "MemtableManager.h"
#include "Config.h"
#include "TypesManager.h"
#include "../TokenBucket/TokenBucket.h"

class System {
	
public:
	Wal* wal;
	MemtableManager* memtable;
	SSTManager* sstable;
	Block_manager* sharedInstanceBM;
	Cache<string>* cache;
	TokenBucket* tokenBucket;
	TypesManager* typesManager;

public:
	System();

	~System();

	void put(const std::string& key, const std::string& value);
	void del(const std::string& key);
	std::optional<std::string> get(const std::string& key);

	TypesManager* getTypesManager();

	void debugWal() const;
	void debugMemtable() const;

private:

	// Onemogucavamo kopiranje da bismo izbegli probleme sa vlasnistvom pokazivaca
	System(const System&) = delete; // Prevent copying
	System& operator=(const System&) = delete; // Prevent assignment
	void add_records_to_cache(vector<Record> records);
	
	// Rate limiting 
	int requestCounter;
	static const int SAVE_INTERVAL = 10; // Cuvanje stanja svakih 10 sekundi
	static const std::string RATE_LIMIT_KEY;

	void saveTokenBucket();
	void loadTokenBucket();
	bool checkRateLimit(); // Vraca true ako je prihvacen
};